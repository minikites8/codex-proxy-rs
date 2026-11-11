//! BPS 文本即时透传，工具仅在完整成功终态后成批交付

use super::{BpsProtocolError, BpsRequest, ToolReplay};
use serde_json::{Value, json};
use std::collections::BTreeSet;

/// 一次 BPS 响应的工具转换状态，不持有输入历史或网络资源
pub struct BpsResponse {
    tools: super::tools::ToolCatalog,
    parallel_tools: bool,
    tool_indices: BTreeSet<u64>,
    replays: Vec<ToolReplay>,
    terminal: bool,
}

impl std::fmt::Debug for BpsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BpsResponse")
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

impl BpsResponse {
    /// 创建当前请求的响应转换器
    #[must_use]
    pub fn new(request: BpsRequest) -> Self {
        Self {
            tools: request.tools,
            parallel_tools: request.parallel_tools,
            tool_indices: BTreeSet::new(),
            replays: Vec::new(),
            terminal: false,
        }
    }

    /// 转换一个完整 JSON 事件，普通文本事件保持原序
    ///
    /// # Errors
    /// 成功终态中的工具身份或信封不合法时拒绝整批工具
    pub fn event(&mut self, mut event: Value) -> Result<Vec<Value>, BpsProtocolError> {
        if self.terminal {
            return Err(BpsProtocolError(
                "Basispoints emitted data after its terminal event.",
            ));
        }
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or(BpsProtocolError("Invalid Basispoints event."))?;
        if matches!(
            kind,
            "error" | "response.failed" | "response.cancelled" | "response.incomplete"
        ) {
            self.terminal = true;
            withhold_native_tools(&mut event);
            return Ok(vec![event]);
        }
        if kind == "response.completed" {
            self.terminal = true;
            let response = event
                .get_mut("response")
                .and_then(Value::as_object_mut)
                .ok_or(BpsProtocolError("Invalid Basispoints terminal response."))?;
            if response
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| status != "completed")
                || response.get("error").is_some_and(|error| !error.is_null())
            {
                withhold_native_tools(&mut event);
                return Ok(vec![event]);
            }
            let output = response
                .get_mut("output")
                .and_then(Value::as_array_mut)
                .ok_or(BpsProtocolError("Missing Basispoints terminal output."))?;
            let replays = self.tools.translate_output(output, self.parallel_tools)?;
            let mut events = Vec::new();
            for (index, item) in output.iter().enumerate() {
                if !matches!(
                    item.get("type").and_then(Value::as_str),
                    Some("function_call" | "custom_tool_call")
                ) {
                    continue;
                }
                events.push(
                    json!({"type":"response.output_item.added","output_index":index,"item":item}),
                );
                events.push(
                    json!({"type":"response.output_item.done","output_index":index,"item":item}),
                );
            }
            self.replays = replays;
            events.push(event);
            return Ok(events);
        }
        let index = event.get("output_index").and_then(Value::as_u64);
        if matches!(
            kind,
            "response.function_call_arguments.delta"
                | "response.function_call_arguments.done"
                | "response.custom_tool_call_input.delta"
                | "response.custom_tool_call_input.done"
        ) {
            return Ok(Vec::new());
        }
        if matches!(
            kind,
            "response.output_item.added" | "response.output_item.done"
        ) {
            match event
                .get("item")
                .and_then(|item| item.get("type"))
                .and_then(Value::as_str)
            {
                Some("function_call" | "custom_tool_call") => {
                    if let Some(index) = index {
                        self.tool_indices.insert(index);
                    }
                    return Ok(Vec::new());
                }
                Some("message" | "reasoning" | "compaction") => {}
                _ => {
                    return Err(BpsProtocolError(
                        "Basispoints emitted an unsupported native output item.",
                    ));
                }
            }
        }
        if index.is_some_and(|index| self.tool_indices.contains(&index)) {
            return Ok(Vec::new());
        }
        Ok(vec![event])
    }

    /// 返回整批验证通过后可保存的原生调用
    #[must_use]
    pub fn replays(&self) -> &[ToolReplay] {
        &self.replays
    }

    /// 验证上游在断流前已给出明确终态
    ///
    /// # Errors
    /// 截断或仅有 DONE 标记不能作为模型成功的证据
    pub fn finish(&self) -> Result<(), BpsProtocolError> {
        if self.terminal {
            Ok(())
        } else {
            Err(BpsProtocolError(
                "Basispoints stream ended before its terminal event.",
            ))
        }
    }

    /// 是否已观察到明确终态
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }
}

// 未成功终态不能授权客户端执行工具，错误及实际用量仍原样保留
fn withhold_native_tools(event: &mut Value) {
    if let Some(output) = event
        .pointer_mut("/response/output")
        .and_then(Value::as_array_mut)
    {
        output.retain(|item| {
            matches!(
                item.get("type").and_then(Value::as_str),
                Some("message" | "reasoning" | "compaction")
            )
        });
    }
}
