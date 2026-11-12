//! BPS 生成冷流、调用回放与规范化用量观测

use crate::{
    BpsError, BpsUpstreamTransport,
    credential::Credential,
    images::ImagePlan,
    network,
    provider::{BpsProvider, ReplayEntry},
    transport,
};
use futures::StreamExt;
use gateway_core::{
    account::{AccountHistorySnapshot, ProviderAccount},
    engine::{
        AttemptContext,
        provider::{NativeResponseTranslator, ProviderCallMetadata, ProviderStream},
    },
    error::{ClientVisibleUpstreamResponse, ProviderError, ProviderErrorKind},
    event::{
        ContentItem, ContentKind, GatewayEvent, ProtocolWireEvent, ProviderEvent, ReasoningDelta,
        ResponseMeta, TextDelta, ToolCallDelta,
    },
    metering::{ModelPriceOverride, Usage},
    operation::Operation,
    provider_ports::ProviderLeaseGuard,
    routing::UpstreamModelId,
    upstream::{UpstreamSendState, UpstreamTransport},
};
use gateway_protocol::{
    bps::{BpsRequest, BpsResponse, RequestIdentity, ToolReplay},
    openai::OPENAI_PROTOCOL,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

impl BpsProvider {
    pub(crate) async fn open_stream(
        self: Arc<Self>,
        operation: Operation,
        model: UpstreamModelId,
        account: ProviderAccount,
        lease: Box<dyn ProviderLeaseGuard>,
        context: AttemptContext,
        (headers, selection): (
            Vec<gateway_core::engine::middleware::MiddlewareHeader>,
            gateway_core::engine::provider::ProviderSelectionObservation,
        ),
    ) -> Result<ProviderStream, ProviderError> {
        let not_sent = |error: BpsError| error.execution(UpstreamSendState::NotSent);
        let Operation::Generate(generate) = operation else {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                UpstreamSendState::NotSent,
            ));
        };
        let original = generate.protocol_payload().body();
        let translator = if generate.protocol_payload().protocol()
            == gateway_protocol::anthropic::ANTHROPIC_PROTOCOL
        {
            Some(MessagesTranslator(
                gateway_protocol::anthropic::ResponsesToMessagesStream::new(original),
            ))
        } else if generate.protocol_payload().protocol() == OPENAI_PROTOCOL {
            None
        } else {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                UpstreamSendState::NotSent,
            ));
        };
        let source = Value::Object(if translator.is_some() {
            gateway_protocol::anthropic::messages_to_responses_request(original)
                .map_err(|_| BpsError::Protocol.execution(UpstreamSendState::NotSent))?
        } else {
            original.clone()
        });
        let input = source.get("input").unwrap_or(&Value::Null);
        let first = input
            .as_array()
            .and_then(|items| items.first())
            .unwrap_or(input);
        let conversation = source.get("prompt_cache_key").unwrap_or(first);
        let scope = hash(&json!([
            account.id().as_str(),
            context.client_api_key_ref().as_str(),
            conversation
        ]));
        let identity = RequestIdentity {
            task_id: scope.clone(),
            turn_id: hash(&json!([scope, input])),
            prompt_cache_key: source
                .get("prompt_cache_key")
                .map(|_| format!("bps-{scope}")),
        };
        let replays = self.replays(&scope);
        let policy = self.config.request_policy();
        let image_plan = if self.config.image_input_enabled {
            Some(ImagePlan::prepare(source.clone()).map_err(not_sent)?)
        } else {
            None
        };
        let prepared = BpsRequest::prepare_with_policy(
            image_plan.as_ref().map_or(&source, ImagePlan::source),
            model.as_str(),
            identity.clone(),
            &replays,
            policy,
        )
        .map_err(BpsError::from)
        .map_err(not_sent)?;
        let loaded = self
            .ports
            .accounts()
            .load_credential(account.id(), account.revision())
            .await
            .map_err(|_| not_sent(BpsError::Unavailable))?;
        let credential =
            Credential::parse(loaded.credential.expose_to_provider()).map_err(not_sent)?;
        let client = network::client(account.outbound_proxy()).map_err(not_sent)?;
        let url = self
            .config
            .endpoint("basispoints/api/responses")
            .map_err(not_sent)?;
        let metadata = ProviderCallMetadata::new(
            self.kind.clone(),
            model.clone(),
            account.id().clone(),
            UpstreamTransport::new(match self.config.upstream_transport {
                BpsUpstreamTransport::Ws => "websocket",
                BpsUpstreamTransport::Sse => "http_sse",
            })
            .expect("static transport name"),
        )
        .with_selection_observation(selection)
        .with_credential_revision(account.revision())
        .with_account_snapshot(Some(AccountHistorySnapshot::from_account(&account)));
        let stats = self.ports.account_feedback();
        let pricing = context
            .pricing()
            .get(self.kind.as_str())
            .and_then(|models| models.get(model.as_str()))
            .cloned();
        let response_transport = metadata.transport().clone();
        let stream = async_stream::try_stream! {
            // execute 只准备账号与租约，实际发送发生在首次 poll 之后
            let prepared = if let Some(plan) = image_plan.filter(ImagePlan::has_uploads) {
                let uploaded = plan.upload(&self, &account, &context, &model).await?;
                BpsRequest::prepare_with_policy(
                    &uploaded,
                    model.as_str(),
                    identity,
                    &replays,
                    policy,
                )
                .map_err(|error| {
                    BpsError::from(error)
                        .execution(UpstreamSendState::NotSent)
                        .with_retry_prohibited()
                })?
            } else {
                prepared
            };
            let body = prepared.body().clone();
            let mut bridge = BpsResponse::new(prepared);
            let request = transport::GenerationRequest {
                client: &client,
                url,
                transport: self.config.upstream_transport,
                body,
                headers: &headers,
                credential: &credential,
                context: &context,
                provider: &self.kind,
                model: &model,
                account: &account,
            };
            let transport::Opening { response, payload } = request.open().await?;
            let status = response.status();
            let mut observation = crate::observation::Observation::new(
                context.timing_started_at(), response_transport, status.as_u16(),
            );
            let quota_observed_at = SystemTime::now();
            // 额度头在流消费前落库，正文失败也不能丢失已经收到的明确额度事实
            if context
                .account_eligibility()
                .allows_account_state_mutation(account.enabled())
            {
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    self.observe_quota_headers(&account, response.headers(), quota_observed_at),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    _ => tracing::warn!(
                        provider = "bps",
                        stage = "quota_observation",
                        "BPS quota observation could not be persisted"
                    ),
                }
            }
            let response =
                if !status.is_success() && !(payload.is_some() && status == reqwest::StatusCode::SWITCHING_PROTOCOLS) {
                    let retry_after = response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(gateway_protocol::openai::parse_retry_after_seconds)
                        .map(Duration::from_secs);
                    let content_type = response
                        .headers()
                        .get(reqwest::header::CONTENT_TYPE)
                        .map(|value| value.as_bytes().to_vec());
                    let state = if payload.is_some() { UpstreamSendState::NotSent } else { UpstreamSendState::Sent };
                    let body = read_error_body(response, state).await?;
                    let usage = serde_json::from_slice::<Value>(&body)
                        .ok()
                        .and_then(|value| response_usage(&value));
                    let mut error = BpsError::Upstream(status.as_u16())
                        .execution(state)
                        .with_client_visible_upstream_response(ClientVisibleUpstreamResponse::new(
                            status.as_u16(),
                            content_type,
                            body,
                        ));
                    let mut facts = ProviderEvent::observation(observation.snapshot());
                    if let Some(usage) = usage {
                        facts = ProviderEvent::canonical(GatewayEvent::Usage(usage));
                        facts.attach_observation(observation.snapshot());
                    }
                    error = error.with_atomic_client_events(vec![facts]);
                    if let Some(delay) = retry_after {
                        error = error.with_retry_after(delay);
                    }
                    self.record_failure(&account, &context, error.kind(), retry_after)
                        .await;
                    Err::<reqwest::Response, _>(error)?
                } else {
                    response
                };
            let mut upstream = transport::events(response, payload, context.clone()).await?;
            let mut canonical = Canonical::new(model.as_str().into(), pricing);
            let mut terminal = false;
            while let Some(value) = upstream.next().await {
                let value = value?;
                let timing_changed = observation.observe(&value);
                    let failed=matches!(value.get("type").and_then(Value::as_str),Some("error"|"response.failed"|"response.cancelled"))
                        || (value.get("type").and_then(Value::as_str)==Some("response.completed") && (matches!(value.pointer("/response/status").and_then(Value::as_str),Some("failed"|"cancelled")) || value.pointer("/response/error").is_some_and(|error|!error.is_null())));
                    let events=if failed {
                        let (kind,status)=stream_failure(&value);
                        let retry_after=value.pointer("/response/error/headers/retry-after").or_else(||value.pointer("/error/headers/retry-after")).and_then(Value::as_str).and_then(gateway_protocol::openai::parse_retry_after_seconds).map(Duration::from_secs);
                        self.record_failure(&account,&context,kind,retry_after).await;
                        let value=bridge.event(value).map_err(|_|BpsError::Protocol.execution(UpstreamSendState::Sent))?.pop().ok_or_else(||BpsError::Protocol.execution(UpstreamSendState::Sent))?;
                        let mut facts=canonical.event(value)?;
                        facts.attach_observation(observation.snapshot());
                        let mut error=ProviderError::new(kind,UpstreamSendState::Sent).with_atomic_client_events(vec![facts]).with_retry_prohibited();
                        if let Some(status)=status {error=error.with_status(status);}
                        if let Some(delay)=retry_after {error=error.with_retry_after(delay);}
                        Err::<Vec<Value>,_>(error)?
                    } else {
                        let usage=response_usage(&value);
                        bridge.event(value).map_err(|error| {
                            // 协议错误只含安全原因，不记录原始事件、工具输入或凭据
                            tracing::warn!(provider="bps",stage="bridge_protocol_conversion",reason=%error,"BPS response conversion rejected");
                            let error=ProviderError::new(ProviderErrorKind::Protocol,UpstreamSendState::Sent).with_source(error).with_retry_prohibited();
                            let mut facts=ProviderEvent::observation(observation.snapshot());
                            if let Some(usage)=usage {facts=ProviderEvent::canonical(GatewayEvent::Usage(usage));facts.attach_observation(observation.snapshot());}
                            error.with_atomic_client_events(vec![facts])
                        })?
                    };
                    if bridge.is_terminal() {self.remember(&scope,bridge.replays());terminal=true;}
                    if events.is_empty() && timing_changed {
                        yield ProviderEvent::observation(observation.snapshot());
                    }
                    for event in events {
                        let mut event = canonical.event(event)?;
                        event.attach_observation(observation.snapshot());
                        yield event;
                    }
                    if terminal {break;}
            }
            if !terminal {Err(BpsError::Protocol.execution(UpstreamSendState::Sent))?;}
            drop(context);
        };
        let stream = ProviderStream::new(metadata, stream, lease)
            .with_filtered_account_feedback(stats, score_upstream_failure);
        Ok(match translator {
            Some(translator) => stream.with_native_response_translator(translator),
            None => stream,
        })
    }

    fn replays(&self, scope: &str) -> Vec<ToolReplay> {
        let mut entries = self.replay.lock().expect("BPS replay lock poisoned");
        entries.retain(|entry| entry.used.elapsed() < Duration::from_secs(2 * 60 * 60));
        entries
            .iter_mut()
            .filter(|entry| entry.scope == scope)
            .map(|entry| {
                entry.used = Instant::now();
                entry.replay.clone()
            })
            .collect()
    }

    fn remember(&self, scope: &str, replays: &[ToolReplay]) {
        // 与参考适配器相同的回放预算，完整历史在缓存失效后仍可重建
        const MAX_BYTES: usize = 16 * 1024 * 1024;
        const MAX_ENTRY_BYTES: usize = 1024 * 1024;
        const MAX_ENTRIES: usize = 1024;
        let mut entries = self.replay.lock().expect("BPS replay lock poisoned");
        entries.retain(|entry| entry.used.elapsed() < Duration::from_secs(2 * 60 * 60));
        for replay in replays {
            entries.retain(|entry| {
                entry.scope != scope
                    || entry.replay.native.get("call_id") != replay.native.get("call_id")
            });
            let bytes =
                replay.native.to_string().len() + replay.client.to_string().len() + scope.len();
            if bytes > MAX_ENTRY_BYTES {
                continue;
            }
            entries.push(ReplayEntry {
                scope: scope.into(),
                replay: replay.clone(),
                used: Instant::now(),
                bytes,
            });
        }
        entries.sort_by_key(|entry| entry.used);
        let mut bytes: usize = entries.iter().map(|entry| entry.bytes).sum();
        while entries.len() > MAX_ENTRIES || bytes > MAX_BYTES {
            bytes -= entries.remove(0).bytes;
        }
    }
}

fn hash(value: &Value) -> String {
    Sha256::digest(value.to_string().as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct Canonical {
    model: String,
    response_id: Option<String>,
    content: BTreeSet<u32>,
    pricing: Option<ModelPriceOverride>,
}

impl Canonical {
    fn new(model: String, pricing: Option<ModelPriceOverride>) -> Self {
        Self {
            model,
            response_id: None,
            content: BTreeSet::new(),
            pricing,
        }
    }
    fn event(&mut self, event: Value) -> Result<ProviderEvent, ProviderError> {
        let protocol = || BpsError::Protocol.execution(UpstreamSendState::Sent);
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(protocol)?;
        let response = event.get("response");
        let mut facts = Vec::new();
        if self.response_id.is_none()
            && let Some(id) = response
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
        {
            self.response_id = Some(id.to_owned());
            facts.push(GatewayEvent::Started(ResponseMeta::new(id, &self.model)));
        }
        let index = event
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            .checked_mul(1024)
            .and_then(|value| {
                value.checked_add(
                    event
                        .get("content_index")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                )
            })
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(protocol)?;
        if let Some(usage) = response_usage(&event) {
            if matches!(
                kind,
                "response.completed"
                    | "response.incomplete"
                    | "response.failed"
                    | "response.cancelled"
                    | "error"
            ) && let Some(cost) = crate::pricing::calculate(&usage, self.pricing.as_ref())
            {
                facts.push(GatewayEvent::CalculatedCost(cost.calculated_cost()));
            }
            facts.push(GatewayEvent::Usage(usage));
        }
        if self.response_id.is_some() {
            match kind {
                "response.output_text.delta" | "response.reasoning_summary_text.delta" => {
                    let text = event
                        .get("delta")
                        .and_then(Value::as_str)
                        .ok_or_else(protocol)?
                        .to_owned();
                    let reasoning = kind == "response.reasoning_summary_text.delta";
                    if self.content.insert(index) {
                        facts.push(GatewayEvent::ContentAdded(ContentItem::new(
                            index,
                            if reasoning {
                                ContentKind::Reasoning
                            } else {
                                ContentKind::Text
                            },
                        )));
                    }
                    facts.push(if reasoning {
                        GatewayEvent::ReasoningDelta(ReasoningDelta {
                            content_index: index,
                            text,
                        })
                    } else {
                        GatewayEvent::TextDelta(TextDelta {
                            content_index: index,
                            text,
                        })
                    });
                }
                "response.output_item.done"
                    if matches!(
                        event.pointer("/item/type").and_then(Value::as_str),
                        Some("function_call" | "custom_tool_call")
                    ) =>
                {
                    let item = &event["item"];
                    if self.content.insert(index) {
                        facts.push(GatewayEvent::ContentAdded(ContentItem::new(
                            index,
                            ContentKind::ToolCall,
                        )));
                    }
                    facts.push(GatewayEvent::ToolCallDelta(ToolCallDelta {
                        content_index: index,
                        call_id: item["call_id"].as_str().ok_or_else(protocol)?.into(),
                        name: item["name"].as_str().map(str::to_owned),
                        arguments_delta: item
                            .get("arguments")
                            .or_else(|| item.get("input"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .into(),
                    }));
                }
                _ => {}
            }
            if matches!(kind, "response.completed" | "response.incomplete")
                && !response
                    .and_then(|value| value.get("status"))
                    .and_then(Value::as_str)
                    .is_some_and(|status| matches!(status, "failed" | "cancelled"))
                && !response
                    .and_then(|value| value.get("error"))
                    .is_some_and(|error| !error.is_null())
            {
                facts.push(GatewayEvent::Completed(ResponseMeta::new(
                    self.response_id
                        .as_deref()
                        .expect("response identity checked"),
                    &self.model,
                )));
            }
        }
        let wire = ProtocolWireEvent::json(OPENAI_PROTOCOL, Some(kind.into()), event)
            .map_err(|_| protocol())?;
        Ok(ProviderEvent::canonical_with_wire(facts, wire))
    }
}

// 转换状态由单条响应独占，账号与结算事实仍保留 Responses 原始观测
struct MessagesTranslator(gateway_protocol::anthropic::ResponsesToMessagesStream);
impl NativeResponseTranslator for MessagesTranslator {
    fn source_protocol(&self) -> &str {
        OPENAI_PROTOCOL
    }
    fn target_protocol(&self) -> &str {
        gateway_protocol::anthropic::ANTHROPIC_PROTOCOL
    }
    fn translate(
        &mut self,
        event: &ProtocolWireEvent,
    ) -> Result<Vec<ProtocolWireEvent>, ProviderError> {
        let error = || BpsError::Protocol.execution(UpstreamSendState::Sent);
        let mut cancelled;
        let source =
            if event.data().get("type").and_then(Value::as_str) == Some("response.cancelled") {
                // Messages 没有 cancelled 终态，以其原生错误事件表达上游中止
                cancelled = event.data().clone();
                cancelled["type"] = "response.failed".into();
                &cancelled
            } else {
                event.data()
            };
        let values = self.0.push(source).map_err(|_| error())?;
        if matches!(
            event.data().get("type").and_then(Value::as_str),
            Some("response.completed" | "response.incomplete")
        ) {
            self.0.finish().map_err(|_| error())?;
        }
        values
            .into_iter()
            .map(|value| {
                ProtocolWireEvent::json(
                    gateway_protocol::anthropic::ANTHROPIC_PROTOCOL,
                    value.get("type").and_then(Value::as_str).map(str::to_owned),
                    value,
                )
                .map_err(|_| error())
            })
            .collect()
    }
}

async fn read_error_body(
    mut response: reqwest::Response,
    state: UpstreamSendState,
) -> Result<bytes::Bytes, ProviderError> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| BpsError::Protocol.execution(state))?
    {
        if body.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 {
            return Err(BpsError::Protocol.execution(state));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.into())
}

// 用量是独立的结算事实，失败终态缺少响应 ID 时也必须保留
fn response_usage(event: &Value) -> Option<Usage> {
    let raw = event
        .pointer("/response/usage")
        .or_else(|| event.get("usage"))?
        .as_object()?;
    Some(Usage {
        input_tokens: raw.get("input_tokens").and_then(Value::as_u64),
        output_tokens: raw.get("output_tokens").and_then(Value::as_u64),
        cached_input_tokens: raw
            .get("input_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64),
        cache_write_input_tokens: raw
            .get("input_tokens_details")
            .and_then(|details| details.get("cache_write_tokens"))
            .or_else(|| raw.get("cache_write_tokens"))
            .and_then(Value::as_u64),
        reasoning_output_tokens: raw
            .get("output_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64),
        total_tokens: raw.get("total_tokens").and_then(Value::as_u64),
        ..Usage::new()
    })
}

// 转换、请求和能力错误由本地边界负责，不应损伤账号成功率
fn score_upstream_failure(error: &ProviderError) -> bool {
    matches!(
        error.kind(),
        ProviderErrorKind::Unauthorized
            | ProviderErrorKind::RateLimited
            | ProviderErrorKind::QuotaExhausted
            | ProviderErrorKind::PermissionDenied
            | ProviderErrorKind::Unavailable
            | ProviderErrorKind::Transport
    )
}

// 只使用明确错误标识或错误对象的状态，不把消息文本推断成认证失效
fn stream_failure(event: &Value) -> (ProviderErrorKind, Option<u16>) {
    let detail = event
        .pointer("/response/error")
        .or_else(|| event.get("error"))
        .unwrap_or(event);
    let status = detail
        .get("status")
        .or_else(|| event.get("status"))
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok());
    let identifier = detail
        .get("code")
        .or_else(|| detail.get("type"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let kind = if status == Some(403) {
        ProviderErrorKind::PermissionDenied
    } else {
        match identifier {
            "rate_limit_exceeded" | "rate_limit_error" => ProviderErrorKind::RateLimited,
            "usage_limit_reached" | "insufficient_quota" => ProviderErrorKind::QuotaExhausted,
            "unauthorized" | "invalid_api_key" | "authentication_error" => {
                ProviderErrorKind::Unauthorized
            }
            // 实测同账号的 HTTP 403 在 BPS WS 中以 request_rejected 返回
            "permission_error" | "permission_denied" | "request_rejected" => {
                ProviderErrorKind::PermissionDenied
            }
            _ => match status {
                Some(401) => ProviderErrorKind::Unauthorized,
                Some(429) => ProviderErrorKind::RateLimited,
                _ => ProviderErrorKind::Unavailable,
            },
        }
    };
    (kind, status)
}
