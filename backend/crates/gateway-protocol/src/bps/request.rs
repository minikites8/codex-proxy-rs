//! 将 Responses 请求转成 BPS 白名单正文，能力限制由适配合同显式说明

use serde_json::{Value, json};

use super::{BpsProtocolError, ToolReplay, tools::ToolCatalog};

/// Provider 按账号、Client Key 和会话隔离后生成的关联标识
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestIdentity {
    pub task_id: String,
    pub turn_id: String,
    pub prompt_cache_key: Option<String>,
}

/// 一次请求的 BPS 正文与原始客户端工具目录
pub struct BpsRequest {
    pub(super) body: Value,
    pub(super) tools: ToolCatalog,
    pub(super) parallel_tools: bool,
}

impl std::fmt::Debug for BpsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BpsRequest").finish_non_exhaustive()
    }
}

impl BpsRequest {
    /// 解析请求并转换工具历史；不执行工具，也不补造缺失的调用
    ///
    /// # Errors
    /// 不能保留的能力、非法工具目录或缺失的历史调用返回固定安全错误
    pub fn prepare(
        source: &Value,
        model: &str,
        identity: RequestIdentity,
        replays: &[ToolReplay],
    ) -> Result<Self, BpsProtocolError> {
        Self::prepare_with_policy(
            source,
            model,
            identity,
            replays,
            super::BpsRequestPolicy::default(),
        )
    }

    /// 使用显式冻结的兼容策略准备请求
    ///
    /// # Errors
    /// 未支持的能力、非法工具或缺失调用历史返回固定安全错误
    pub fn prepare_with_policy(
        source: &Value,
        model: &str,
        identity: RequestIdentity,
        replays: &[ToolReplay],
        policy: super::BpsRequestPolicy,
    ) -> Result<Self, BpsProtocolError> {
        let source = source
            .as_object()
            .ok_or(BpsProtocolError("Invalid Basispoints request."))?;
        if model.trim().is_empty() {
            return Err(BpsProtocolError("Basispoints requires a model."));
        }
        if source
            .get("previous_response_id")
            .is_some_and(|id| !id.is_null() && id.as_str() != Some(""))
        {
            return Err(BpsProtocolError(
                "Basispoints requires complete history instead of previous_response_id.",
            ));
        }
        if source
            .get("text")
            .and_then(|text| text.get("format"))
            .is_some_and(|format| {
                !format.is_null() && format.get("type").and_then(Value::as_str) != Some("text")
            })
        {
            return Err(BpsProtocolError(
                "Basispoints does not support structured output.",
            ));
        }
        let choice = source
            .get("tool_choice")
            .and_then(Value::as_str)
            .unwrap_or("auto");
        if !matches!(choice, "auto" | "none")
            || source
                .get("tool_choice")
                .is_some_and(|value| !value.is_null() && !value.is_string())
        {
            return Err(BpsProtocolError(
                "Basispoints supports tool_choice auto or none only.",
            ));
        }
        let parallel_tools = match source.get("parallel_tool_calls") {
            Some(Value::Bool(value)) => *value,
            None => true,
            _ => return Err(BpsProtocolError("Invalid parallel_tool_calls.")),
        };
        let tools = if choice == "none" {
            ToolCatalog::default()
        } else {
            ToolCatalog::parse(source, policy.omit_unsupported_hosted_tools)?
        };
        let mut input = match source.get("input") {
            Some(Value::String(text)) => vec![message("user", text)],
            Some(Value::Array(items)) => items.clone(),
            _ => {
                return Err(BpsProtocolError(
                    "Basispoints input must be text or a Responses item array.",
                ));
            }
        };
        let iteration = input
            .iter()
            .rev()
            .take_while(|item| item.get("role").and_then(Value::as_str) != Some("user"))
            .filter(|item| {
                item.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind.ends_with("_call_output"))
            })
            .count()
            + 1;
        let notices = super::policy::normalize_input(&mut input, policy)?;
        tools.translate_history(&mut input, replays)?;
        for item in &mut input {
            match item
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("message")
            {
                "message" => validate_content(item.get("content"), policy.images, false)?,
                "function_call_output" => {
                    validate_content(item.get("output"), policy.images, true)?
                }
                "function_call" | "reasoning" | "compaction" => {}
                "configuration_update" => {
                    return Err(BpsProtocolError(
                        "Basispoints does not support configuration_update history. Start a new conversation with the desired reasoning effort.",
                    ));
                }
                "item_reference" => {
                    return Err(BpsProtocolError(
                        "Basispoints requires complete history instead of item_reference.",
                    ));
                }
                "compaction_trigger" => {
                    return Err(BpsProtocolError(
                        "Basispoints does not support compaction_trigger requests through this endpoint.",
                    ));
                }
                _ => {
                    return Err(BpsProtocolError(
                        "Unsupported Basispoints history item. Resend complete plaintext history.",
                    ));
                }
            }
        }
        let mut prologue = Vec::new();
        if let Some(instructions) = source.get("instructions").and_then(Value::as_str) {
            prologue.push(message("developer", instructions));
        }
        prologue.push(message("developer", &tools.instructions()));
        prologue.extend(notices);
        prologue.append(&mut input);
        let reasoning = source.get("reasoning");
        if reasoning
            .and_then(|value| value.get("mode"))
            .and_then(Value::as_str)
            .is_some_and(|mode| mode != "standard")
        {
            return Err(BpsProtocolError(
                "Basispoints does not support the requested reasoning mode.",
            ));
        }
        let effort = reasoning
            .and_then(|value| value.get("effort"))
            .or_else(|| source.get("reasoning_effort"))
            .and_then(Value::as_str)
            .unwrap_or("medium");
        let effort = match effort {
            "low" | "medium" | "high" | "xhigh" => effort,
            "none" | "minimal" => "low",
            "max" | "ultra" => "xhigh",
            _ => {
                return Err(BpsProtocolError(
                    "Unsupported Basispoints reasoning effort.",
                ));
            }
        };
        // BPS 固定工具由上游提供，客户端工具仅通过显式目录和信封中继
        // service_tier 统一采用 BPS 默认档位，不写入上游白名单
        let mut body = json!({"model":model,"model_selection":"explicit","stream":true,"store":false,
            "input":prologue,"reasoning_effort":effort,
            "metadata":{"task_id":identity.task_id,"turn_id":identity.turn_id,"agent_iteration":iteration.to_string()}});
        if let Some(key) = identity.prompt_cache_key {
            body["prompt_cache_key"] = key.into();
        }
        if let Some(management) = source.get("context_management") {
            body["context_management"] = management.clone();
        }
        Ok(Self {
            body,
            tools,
            parallel_tools,
        })
    }

    /// 返回可发送给 BPS 的正文
    #[must_use]
    pub const fn body(&self) -> &Value {
        &self.body
    }
}

pub(super) fn message(role: &str, text: &str) -> Value {
    json!({"type":"message","role":role,"content":[{"type":"input_text","text":text}]})
}

fn validate_content(
    content: Option<&Value>,
    images: super::BpsImagePolicy,
    tool_output: bool,
) -> Result<(), BpsProtocolError> {
    match content {
        Some(Value::String(_)) | None => Ok(()),
        Some(Value::Array(parts)) => {
            for part in parts {
                match part.get("type").and_then(Value::as_str) {
                    Some("input_text" | "output_text" | "text" | "refusal") => {}
                    Some("input_image") if images == super::BpsImagePolicy::Enabled => {
                        super::policy::validate_image(
                            part.as_object()
                                .ok_or(BpsProtocolError("Invalid image content."))?,
                            tool_output,
                        )?
                    }
                    Some("input_image") => {
                        return Err(BpsProtocolError(
                            "Basispoints image input is not supported by this provider.",
                        ));
                    }
                    _ => {
                        return Err(BpsProtocolError(
                            "Unsupported Basispoints content. Resend plaintext content.",
                        ));
                    }
                }
            }
            Ok(())
        }
        _ => Err(BpsProtocolError("Invalid Basispoints content.")),
    }
}
