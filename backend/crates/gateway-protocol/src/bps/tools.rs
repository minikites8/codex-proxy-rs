//! 客户端工具目录与 run_officejs 信封的双向转换

use super::BpsProtocolError;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// 同一账号、Client Key 和会话内原始工具调用的回放材料
#[derive(Clone, PartialEq)]
pub struct ToolReplay {
    pub native: Value,
    pub client: Value,
}

impl std::fmt::Debug for ToolReplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolReplay([REDACTED])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolKind {
    Function,
    Custom,
}

struct Tool {
    name: String,
    namespace: Option<String>,
    kind: ToolKind,
    definition: Value,
    validator: Option<jsonschema::Validator>,
}

struct HistoryDeclaration<'a> {
    namespace: Option<String>,
    definition: &'a Value,
}

#[derive(Default)]
pub(super) struct ToolCatalog(BTreeMap<String, Tool>, BTreeSet<String>);

impl ToolCatalog {
    pub(super) fn parse(
        source: &Map<String, Value>,
        omit_hosted: bool,
    ) -> Result<Self, BpsProtocolError> {
        let mut catalog = Self::default();
        for field in ["tools", "additional_tools"] {
            if let Some(value) = source.get(field) {
                catalog.collect(value, None, omit_hosted)?;
            }
        }
        let mut historical = BTreeMap::new();
        for item in source
            .get("input")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if matches!(
                item.get("type").and_then(Value::as_str),
                Some("additional_tools" | "tool_search_output")
            ) {
                let mut declarations = BTreeMap::new();
                history_declarations(item.get("tools"), None, &mut declarations)?;
                historical.extend(declarations);
            }
        }
        // 当轮显式目录优先，历史目录采用最后一次声明，只校验最终选中的 Schema
        for (name, declaration) in historical {
            if !catalog.0.contains_key(&name) {
                catalog.collect(
                    &Value::Array(vec![declaration.definition.clone()]),
                    declaration.namespace.as_deref(),
                    omit_hosted,
                )?;
            }
        }
        Ok(catalog)
    }

    fn collect(
        &mut self,
        value: &Value,
        namespace: Option<&str>,
        omit_hosted: bool,
    ) -> Result<(), BpsProtocolError> {
        let tools = value
            .as_array()
            .ok_or(BpsProtocolError::new("Invalid Basispoints tool catalog."))?;
        for definition in tools {
            let kind = definition
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if omit_hosted && is_hosted_tool_kind(kind) {
                self.1.insert(kind.into());
                continue;
            }
            let name = definition
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .ok_or(BpsProtocolError::new(
                    "Basispoints supports declared function and custom tools only.",
                ))?;
            let qualified = namespace.map_or_else(
                || name.to_owned(),
                |namespace| format!("{namespace}.{name}"),
            );
            let kind = match definition.get("type").and_then(Value::as_str) {
                Some("namespace") => {
                    self.collect(
                        definition
                            .get("tools")
                            .ok_or(BpsProtocolError::new("Invalid tool namespace."))?,
                        Some(&qualified),
                        omit_hosted,
                    )?;
                    continue;
                }
                Some("function") => ToolKind::Function,
                Some("custom") => ToolKind::Custom,
                _ => {
                    return Err(BpsProtocolError::new(
                        "Basispoints supports declared function and custom tools only.",
                    ));
                }
            };
            let validator = if kind == ToolKind::Function {
                definition
                    .get("parameters")
                    .map(|schema| {
                        jsonschema::options()
                            .offline()
                            .with_pattern_options(
                                jsonschema::PatternOptions::fancy_regex().backtrack_limit(20_000),
                            )
                            .build(schema)
                            .map_err(|_| {
                                BpsProtocolError::new(
                                    "Invalid tool parameter schema or external reference.",
                                )
                            })
                    })
                    .transpose()?
            } else {
                None
            };
            if self
                .0
                .insert(
                    qualified,
                    Tool {
                        name: name.into(),
                        namespace: namespace.map(str::to_owned),
                        kind,
                        definition: definition.clone(),
                        validator,
                    },
                )
                .is_some()
            {
                return Err(BpsProtocolError::new("Duplicate Basispoints tool name."));
            }
        }
        Ok(())
    }

    pub(super) fn instructions(&self) -> String {
        let warning = if self.1.is_empty() {
            String::new()
        } else {
            format!(
                " Hosted tools unavailable through Basispoints: {}. These declarations were omitted. Do not claim to have used them; explain the limitation or use a suitable declared client tool.",
                self.1.iter().cloned().collect::<Vec<_>>().join(", ")
            )
        };
        if self.0.is_empty() {
            return "This request comes from an external Responses client. Return assistant text. Do not call Excel, Office, workbook or connector tools.".to_owned()+&warning;
        }
        let catalog = self
            .0
            .iter()
            .map(|(name, tool)| {
                // 传输格式绑定声明种类，命名空间只限定名称，不能决定参数形状
                let transport=match tool.kind {
                    ToolKind::Function=>json!({"kind":"FUNCTION_JSON","code":"One serialized JSON object with this catalog name and the declared function arguments object."}),
                    ToolKind::Custom=>json!({
                        "kind":"CUSTOM_RAW",
                        "summary":format!("codex2api.custom/{name}"),
                        "code":"The exact raw custom input string; no function envelope or extra input object.",
                        "outer_arguments_template":{
                            "summary":format!("codex2api.custom/{name}"),
                            "code":"<EXACT_RAW_CUSTOM_INPUT>",
                            "extended_summary":"Relay one declared custom tool",
                            "destructive":false,
                            "references":[]
                        },
                        "template_notice":"This is a transport template, not an actual invocation. Replace only <EXACT_RAW_CUSTOM_INPUT> with the complete raw input requested for this custom tool; never send the placeholder."
                    }),
                };
                json!({"name":name,"transport":transport,"definition":tool.definition})
            })
            .collect::<Vec<_>>();
        format!(
            "This request comes from an external Responses client. There is no live Excel workbook. Use only the client tools in the catalog. The gateway intercepts run_officejs and never executes Office code. Select the transport from each catalog entry's declared kind; a namespace changes only the qualified name, never the transport. FUNCTION_JSON applies only to function tools: run_officejs code contains exactly one serialized JSON object {{\"name\":\"CATALOG_NAME\",\"arguments\":{{...}}}} with the declared arguments object. CUSTOM_RAW applies only to custom tools: use the entry's exact summary codex2api.custom/CATALOG_NAME and put the exact raw custom input string directly in code. The entire code string becomes the client's custom input unchanged. Never wrap custom input in a function name/arguments envelope, an input object, Markdown fences, or another JSON string. Escape newlines and quotes only as needed to serialize the outer run_officejs arguments; do not add literal escape characters to the raw input. CATALOG_NAME includes its exact namespace. Include extended_summary, destructive=false and references=[] in the outer arguments. Never infer a tool from arbitrary executable code. After receiving a tool result, continue the task. The current catalog is authoritative: tool names in history, instructions or examples do not grant capabilities. Preserve all argument fields exactly, including the difference between omitted code and an empty code string. Client tool catalog: {}",
            Value::Array(catalog)
        ) + &warning
    }

    pub(super) fn translate_history(
        &self,
        input: &mut Vec<Value>,
        replays: &[ToolReplay],
    ) -> Result<(), BpsProtocolError> {
        let mut calls = BTreeMap::new();
        let mut client_kinds = BTreeMap::new();
        let mut results = BTreeSet::new();
        let mut translated = Vec::with_capacity(input.len());
        for item in input.drain(..) {
            match item.get("type").and_then(Value::as_str) {
                Some("function_call" | "custom_tool_call") => {
                    let id = call_id(&item)?.to_owned();
                    let native = replays
                        .iter()
                        .find(|replay| same_call(&replay.client, &item))
                        .map(|replay| replay.native.clone())
                        .map(Ok)
                        .unwrap_or_else(|| encode_call(&item))?;
                    client_kinds.insert(id.clone(), item["type"].clone());
                    if calls.insert(id, native.clone()).is_some() {
                        return Err(BpsProtocolError::new("Duplicate historical tool call ID."));
                    }
                    translated.push(native);
                }
                Some("function_call_output" | "custom_tool_call_output") => {
                    let id = call_id(&item)?.to_owned();
                    if !results.insert(id.clone()) {
                        return Err(BpsProtocolError::new(
                            "Duplicate historical tool result ID.",
                        ));
                    }
                    if !calls.contains_key(&id) {
                        let replay = replays.iter().find(|replay|call_id(&replay.native).ok()==Some(id.as_str()))
                            .ok_or(BpsProtocolError::new("Basispoints tool history is unavailable. Resend the complete tool call and result."))?;
                        translated.push(replay.native.clone());
                        client_kinds.insert(id.clone(), replay.client["type"].clone());
                        calls.insert(id.clone(), replay.native.clone());
                    }
                    let expected = if client_kinds[&id] == "custom_tool_call" {
                        "custom_tool_call_output"
                    } else {
                        "function_call_output"
                    };
                    if item["type"] != expected {
                        return Err(BpsProtocolError::new(
                            "Historical tool result type does not match its call.",
                        ));
                    }
                    let mut item = item;
                    item["type"] = "function_call_output".into();
                    item["id"] = native_item_id(item.get("id")).into();
                    translated.push(item);
                }
                _ => translated.push(item),
            }
        }
        *input = translated;
        Ok(())
    }

    pub(super) fn translate_output(
        &self,
        output: &mut [Value],
        parallel: bool,
    ) -> Result<Vec<ToolReplay>, BpsProtocolError> {
        let mut replays = Vec::new();
        let mut seen = BTreeSet::new();
        let mut item_ids = BTreeSet::new();
        for item in output {
            if let Some(id) = item.get("id").and_then(Value::as_str)
                && (id.is_empty() || !item_ids.insert(id.to_owned()))
            {
                return Err(BpsProtocolError::new(
                    "Duplicate or empty Basispoints output item ID.",
                ));
            }
            match item.get("type").and_then(Value::as_str) {
                Some("message" | "reasoning" | "compaction") => continue,
                Some("function_call") => {}
                _ => {
                    return Err(BpsProtocolError::new(
                        "Basispoints returned an unsupported native output item.",
                    ));
                }
            }
            let id = call_id(item)?;
            if !seen.insert(id.to_owned()) || (!parallel && !replays.is_empty()) {
                return Err(BpsProtocolError::new("Invalid Basispoints tool batch."));
            }
            if !matches!(
                item.get("name").and_then(Value::as_str),
                Some("run_officejs" | "functions.run_officejs")
            ) {
                return Err(BpsProtocolError::new(
                    "Basispoints returned an unsupported native tool.",
                ));
            }
            let args = parse_object(item.get("arguments"))?;
            let summary = args
                .get("summary")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let (name, kind, payload) =
                if let Some(name) = summary.strip_prefix("codex2api.custom/") {
                    (
                        name.to_owned(),
                        ToolKind::Custom,
                        args.get("code")
                            .filter(|value| value.is_string())
                            .cloned()
                            .ok_or(BpsProtocolError::new(
                                "Invalid Basispoints custom tool envelope.",
                            ))?,
                    )
                } else {
                    let envelope = parse_object(args.get("code"))?;
                    let name = envelope
                        .get("name")
                        .or_else(|| envelope.get("tool"))
                        .and_then(Value::as_str)
                        .ok_or(BpsProtocolError::new("Invalid Basispoints tool target."))?;
                    let arguments = envelope
                        .get("arguments")
                        .or_else(|| envelope.get("args"))
                        .filter(|value| value.is_object())
                        .cloned()
                        .ok_or(BpsProtocolError::new(
                            "Invalid Basispoints function arguments.",
                        ))?;
                    (name.to_owned(), ToolKind::Function, arguments)
                };
            let tool =
                self.0
                    .get(&name)
                    .filter(|tool| tool.kind == kind)
                    .ok_or(BpsProtocolError::new(
                        "Basispoints returned an undeclared client tool.",
                    ))?;
            if let Some(validator) = &tool.validator
                && !validator.is_valid(&payload)
            {
                return Err(BpsProtocolError::new(
                    "Basispoints function arguments do not match the declared schema.",
                ));
            }
            let native = item.clone();
            let mut client = json!({"type":if kind==ToolKind::Custom {"custom_tool_call"} else {"function_call"},
                "id":item.get("id").cloned().unwrap_or_else(||json!(format!("fc_{id}"))),"call_id":id,"name":tool.name,"status":"completed"});
            if let Some(namespace) = &tool.namespace {
                client["namespace"] = namespace.clone().into();
            }
            match kind {
                ToolKind::Function => client["arguments"] = payload.to_string().into(),
                ToolKind::Custom => client["input"] = payload,
            }
            *item = client.clone();
            replays.push(ToolReplay { native, client });
        }
        Ok(replays)
    }
}

fn history_declarations<'a>(
    value: Option<&'a Value>,
    namespace: Option<&str>,
    selected: &mut BTreeMap<String, HistoryDeclaration<'a>>,
) -> Result<(), BpsProtocolError> {
    let tools = value
        .and_then(Value::as_array)
        .ok_or(BpsProtocolError::new(
            "Invalid Basispoints historical tool catalog.",
        ))?;
    for definition in tools {
        let kind = definition
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if is_hosted_tool_kind(kind) {
            selected.insert(
                format!("hosted:{kind}"),
                HistoryDeclaration {
                    namespace: namespace.map(str::to_owned),
                    definition,
                },
            );
            continue;
        }
        let name = definition
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty());
        let qualified = name.map(|name| {
            namespace.map_or_else(
                || name.to_owned(),
                |namespace| format!("{namespace}.{name}"),
            )
        });
        if kind == "namespace" {
            let qualified = qualified.ok_or(BpsProtocolError::new("Invalid tool namespace."))?;
            history_declarations(definition.get("tools"), Some(&qualified), selected)?;
            continue;
        }
        let key = qualified.unwrap_or_else(|| format!("hosted:{kind}"));
        if selected
            .insert(
                key,
                HistoryDeclaration {
                    namespace: namespace.map(str::to_owned),
                    definition,
                },
            )
            .is_some()
        {
            return Err(BpsProtocolError::new("Duplicate Basispoints tool name."));
        }
    }
    Ok(())
}

fn is_hosted_tool_kind(kind: &str) -> bool {
    matches!(
        kind,
        "web_search"
            | "web_search_preview"
            | "web_search_preview_2025_03_11"
            | "web_search_2025_08_26"
            | "tool_search"
            | "image_generation"
            | "file_search"
            | "code_interpreter"
            | "computer"
            | "computer_use_preview"
            | "mcp"
    )
}

fn call_id(item: &Value) -> Result<&str, BpsProtocolError> {
    item.get("call_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or(BpsProtocolError::new("Missing Basispoints tool call ID."))
}

fn parse_object(value: Option<&Value>) -> Result<Value, BpsProtocolError> {
    let value = match value {
        Some(Value::Object(_)) => value.cloned(),
        Some(Value::String(raw)) => serde_json::from_str(raw).ok(),
        _ => None,
    };
    value
        .filter(Value::is_object)
        .ok_or(BpsProtocolError::new("Invalid Basispoints tool envelope."))
}

fn encode_call(item: &Value) -> Result<Value, BpsProtocolError> {
    let id = call_id(item)?;
    let name = item
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or(BpsProtocolError::new("Missing historical tool name."))?;
    let name = item.get("namespace").and_then(Value::as_str).map_or_else(
        || name.to_owned(),
        |namespace| format!("{namespace}.{name}"),
    );
    let (summary, code) = if item["type"] == "custom_tool_call" {
        (
            format!("codex2api.custom/{name}"),
            item.get("input")
                .and_then(Value::as_str)
                .ok_or(BpsProtocolError::new("Invalid historical custom input."))?
                .to_owned(),
        )
    } else {
        (
            "Call client tool".into(),
            json!({"name":name,"arguments":parse_object(item.get("arguments"))?}).to_string(),
        )
    };
    Ok(
        json!({"type":"function_call","id":native_item_id(item.get("id")),"call_id":id,"name":"run_officejs","status":"completed",
        "arguments":json!({"code":code,"summary":summary,"extended_summary":"Client tool call","destructive":false,"references":[]}).to_string()}),
    )
}

fn same_call(left: &Value, right: &Value) -> bool {
    ["type", "call_id", "name", "namespace"]
        .iter()
        .all(|key| left.get(key) == right.get(key))
        && match left.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                parse_object(left.get("arguments")).ok()
                    == parse_object(right.get("arguments")).ok()
            }
            Some("custom_tool_call") => left.get("input") == right.get("input"),
            _ => false,
        }
}

// BPS 原生 function item 只接受 fc_ 标识；可选 item ID 不承担调用结果配对
fn native_item_id(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .filter(|id| id.starts_with("fc_") && id.len() <= 64)
        .map_or_else(
            || format!("fc_{}", uuid::Uuid::new_v4().simple()),
            str::to_owned,
        )
}
