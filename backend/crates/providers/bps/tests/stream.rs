//! 本地 HTTP 验证 BPS 冷流、原始错误、失败用量和 Messages 转换
use futures::StreamExt;
use gateway_core::{
    account::{
        CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
        ProviderAccountId, ProviderAccountStore,
        scope::{ClientRoutingScope, FrozenAccountScope, RuntimeAccount, RuntimeAccountDirectory},
    },
    engine::{
        AccountAttemptContext, AttemptContext, ModelRequestId, RequestAttemptContext,
        provider::{ProviderRequest, ProviderStream},
    },
    lifecycle::CancellationToken,
    operation::{GenerateRequest, Operation, OperationKind, ProtocolPayload},
    policy::ClientApiKeyId,
    routing::{
        ConfigRevision, ModelCapabilities, ProviderKind, ProviderModel, PublicModelId,
        RoutingContext, RuntimeSnapshot, UpstreamModelId,
    },
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

async fn stream(server: &MockServer, protocol: &str) -> ProviderStream {
    stream_with_pricing(server, protocol, BTreeMap::new()).await
}

async fn stream_with_pricing(
    server: &MockServer,
    protocol: &str,
    pricing: gateway_core::metering::PricingOverrides,
) -> ProviderStream {
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    let kind = ProviderKind::new("bps").unwrap();
    let account = ProviderAccount::new(
        ProviderAccountId::new("acct_bps_stream").unwrap(),
        kind.clone(),
        "test".into(),
        Some("user-test".into()),
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_profile(None, Some("workspace-test".into()), None)
    .with_account_facts(
        true,
        gateway_core::account::CredentialState::Ready,
        gateway_core::account::QuotaState::unknown(),
        None,
        None,
    );
    accounts.create_account(NewProviderAccount{account:account.clone(),model_access:None,credential:PlaintextCredential::new(json!({"client_id":"app_fnr0pYvVwwFDocDumLG3H2Bp","access_token":crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp")}).as_object().unwrap().clone())}).await.unwrap();
    stream_with_ports(
        server,
        protocol,
        pricing,
        crate::support::ports_with(accounts),
        account,
    )
    .await
}

pub(crate) async fn stream_with_ports(
    server: &MockServer,
    protocol: &str,
    pricing: gateway_core::metering::PricingOverrides,
    ports: gateway_core::provider_ports::ProviderStorePorts,
    account: ProviderAccount,
) -> ProviderStream {
    let source = if protocol == "anthropic" {
        json!({"model":"model","max_tokens":20,"messages":[{"role":"user","content":"hello"}]})
    } else {
        json!({"model":"model","input":"hello"})
    };
    stream_with_source(
        server,
        protocol,
        ports,
        account,
        source,
        RequestAttemptContext::new(
            ModelRequestId::new("req_bps_stream").unwrap(),
            ClientApiKeyId::new("key_bps_stream").unwrap(),
        )
        .with_pricing(Arc::new(pricing)),
        CancellationToken::new(),
    )
    .await
    .unwrap()
}

pub(crate) async fn stream_with_source(
    server: &MockServer,
    protocol: &str,
    ports: gateway_core::provider_ports::ProviderStorePorts,
    account: ProviderAccount,
    source: serde_json::Value,
    request_context: RequestAttemptContext,
    cancellation: CancellationToken,
) -> Result<ProviderStream, gateway_core::error::ProviderError> {
    stream_with_config(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: server.uri(),
            upstream_transport: provider_bps::BpsUpstreamTransport::Sse,
            ..Default::default()
        },
        protocol,
        ports,
        account,
        source,
        request_context,
        cancellation,
    )
    .await
}

pub(crate) async fn stream_with_config(
    config: provider_bps::BpsConfig,
    protocol: &str,
    ports: gateway_core::provider_ports::ProviderStorePorts,
    account: ProviderAccount,
    source: serde_json::Value,
    request_context: RequestAttemptContext,
    cancellation: CancellationToken,
) -> Result<ProviderStream, gateway_core::error::ProviderError> {
    let kind = account.provider().clone();
    let provider = provider_bps::initialize(config, ports)
        .unwrap()
        .core_provider();
    let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(protocol, source.as_object().unwrap().clone()).unwrap(),
    ));
    let scope = Arc::new(FrozenAccountScope::new(
        Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([(
            account.id().clone(),
            RuntimeAccount::new(kind.clone(), BTreeSet::new()),
        )]))),
        ClientRoutingScope::all_accounts(),
    ));
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        gateway_core::settings::SettingsValues::new(2, "smart", Default::default(), None, None),
        vec![kind.clone()],
        vec![ProviderModel::new(
            kind,
            UpstreamModelId::new("model").unwrap(),
            ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None)
                .with_feature(
                    gateway_core::operation::Feature::Tools,
                    gateway_core::routing::SupportLevel::Emulated,
                )
                .with_feature(
                    gateway_core::operation::Feature::Reasoning,
                    gateway_core::routing::SupportLevel::Native,
                )
                .with_feature(
                    gateway_core::operation::Feature::Vision,
                    gateway_core::routing::SupportLevel::Native,
                ),
        )],
        vec![],
    )
    .unwrap();
    let plan = snapshot
        .plan(
            &PublicModelId::new("model").unwrap(),
            &operation,
            scope.clone(),
            &RoutingContext::default(),
        )
        .unwrap();
    let context = AttemptContext::new(
        request_context,
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(30),
        plan.account_selection_policy(),
        if account.enabled() {
            AccountAttemptContext::new(BTreeSet::new(), None, None).with_account_scope(scope)
        } else {
            AccountAttemptContext::diagnostic(
                BTreeSet::new(),
                gateway_core::engine::probe::AccountProbeTarget::ConnectionTest(
                    account.id().clone(),
                ),
                None,
            )
            .with_account_scope(scope)
        },
        None,
        cancellation,
    );
    provider
        .execute(
            ProviderRequest::new(operation, plan.candidates()[0].clone()),
            context,
        )
        .await
}

async fn server(response: ResponseTemplate) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"allowed":true,"model_catalog":{"models":[{"id":"model"}]}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/basispoints/api/responses"))
        .respond_with(response)
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn preserves_http_upstream_error_body_and_status() {
    let raw = r#"{"error":{"code":"denied","message":"original rejection"},"usage":{"input_tokens":7,"output_tokens":0}}"#;
    let server = server(ResponseTemplate::new(403).set_body_raw(raw, "application/json")).await;
    let mut stream = stream(&server, "openai").await;
    let mut error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.upstream_status(), Some(403));
    assert_eq!(
        error
            .client_visible_upstream_response()
            .unwrap()
            .body()
            .as_ref(),
        raw.as_bytes()
    );
    assert!(error.take_atomic_client_events().iter().any(|event|event.canonical_facts().iter().any(|fact|matches!(fact,gateway_core::event::GatewayEvent::Usage(usage) if usage.input_tokens==Some(7)))));
}

#[tokio::test]
async fn truncated_sse_fails_after_preserving_text() {
    let body = format!(
        "data: {}\n\ndata: {}\n\n",
        json!({"type":"response.created","response":{"id":"resp_a"}}),
        json!({"type":"response.output_text.delta","delta":"partial","output_index":0,"content_index":0})
    );
    let server = server(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream")).await;
    let mut stream = stream(&server, "openai").await;
    let mut text = false;
    let mut failed = false;
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                text |= event
                    .wire_event()
                    .is_some_and(|event| event.data()["delta"] == "partial")
            }
            Err(_) => failed = true,
        }
    }
    assert!(text && failed);
}

#[tokio::test]
async fn failed_terminal_retains_usage_and_original_event() {
    let failed = json!({"type":"response.failed","response":{"id":"resp_failed","status":"failed","error":{"code":"rate_limit_exceeded","message":"precise upstream failure"},"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15}}});
    let server = server(
        ResponseTemplate::new(200).set_body_raw(format!("data: {failed}\n\n"), "text/event-stream"),
    )
    .await;
    let mut stream = stream(&server, "openai").await;
    let mut error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(
        error.kind(),
        gateway_core::error::ProviderErrorKind::RateLimited
    );
    let events = error.take_atomic_client_events();
    assert_eq!(events[0].wire_event().unwrap().data(), &failed);
    assert!(events[0].canonical_facts().iter().any(|fact|matches!(fact,gateway_core::event::GatewayEvent::Usage(usage) if usage.input_tokens==Some(12)&&usage.output_tokens==Some(3))));
    assert!(
        !events[0]
            .canonical_facts()
            .iter()
            .any(|fact| matches!(fact, gateway_core::event::GatewayEvent::Completed(_)))
    );
}

#[tokio::test]
async fn messages_source_uses_shared_native_response_conversion() {
    let events = [
        json!({"type":"response.created","response":{"id":"resp_messages","model":"model","status":"in_progress","output":[]}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","content":[]}}),
        json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}),
        json!({"type":"response.output_text.done","output_index":0,"content_index":0,"text":"hello"}),
        json!({"type":"response.completed","response":{"id":"resp_messages","model":"model","status":"completed","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}),
    ];
    let body = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    let server = server(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream")).await;
    let mut stream = stream(&server, "anthropic").await;
    // execute 已准备账号，模型 POST 仍由首次 poll 驱动
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method.as_str() != "POST")
    );
    let mut translated = Vec::new();
    while let Some(event) = stream.next().await {
        let event = event.unwrap();
        let terminal = event
            .canonical_facts()
            .iter()
            .any(|fact| matches!(fact, gateway_core::event::GatewayEvent::Completed(_)));
        translated.extend(stream.translate_native_response(event, terminal).unwrap());
    }
    assert!(translated.iter().any(
        |event| event.wire_event().is_some_and(
            |wire| wire.protocol() == "anthropic" && wire.data()["type"] == "message_stop"
        )
    ));
    assert!(translated.iter().any(|event| {
        event
            .wire_event()
            .is_some_and(|wire| wire.data().pointer("/delta/text") == Some(&json!("hello")))
    }));
    let requests = server.received_requests().await.unwrap();
    let posted = requests
        .iter()
        .find(|request| request.method.as_str() == "POST")
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&posted.body).unwrap();
    assert_eq!(payload["model"], "model");
    assert!(payload.get("messages").is_none());
}

#[tokio::test]
async fn rejected_native_tool_batch_retains_usage_without_exposing_calls() {
    let terminal = json!({"type":"response.completed","response":{"id":"resp_rejected","status":"completed","output":[{"type":"function_call","call_id":"call_wrong","name":"run_officejs","arguments":"{\"code\":\"{\\\"name\\\":\\\"undeclared\\\",\\\"arguments\\\":{}}\"}"}],"usage":{"input_tokens":8,"output_tokens":4}}});
    let server = server(
        ResponseTemplate::new(200)
            .set_body_raw(format!("data: {terminal}\n\n"), "text/event-stream"),
    )
    .await;
    let mut stream = stream(&server, "openai").await;
    let mut error = stream.next().await.unwrap().unwrap_err();
    let facts = error.take_atomic_client_events();
    assert_eq!(facts.len(), 1);
    assert!(facts[0].wire_event().is_none());
    assert!(
        matches!(&facts[0].canonical_facts()[0],gateway_core::event::GatewayEvent::Usage(usage) if usage.input_tokens==Some(8)&&usage.output_tokens==Some(4))
    );
}

#[tokio::test]
async fn complete_terminal_usage_uses_only_frozen_bps_model_price() {
    let terminal = json!({"type":"response.completed","response":{"id":"resp_cost","status":"completed","output":[],"usage":{"input_tokens":10,"output_tokens":2,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0}}}});
    let body = format!("data: {terminal}");
    let server = server(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream")).await;
    let prices=serde_json::from_value(json!({"bps":{"model":{"multiplierBps":10000,"bands":{"standard":{"input":"1","output":"2","cacheRead":"0.5","cacheWrite":"1"}}}}})).unwrap();
    let mut priced = stream_with_pricing(&server, "openai", prices).await;
    let event = priced.next().await.unwrap().unwrap();
    assert!(event.canonical_facts().iter().any(|fact|matches!(fact,gateway_core::event::GatewayEvent::CalculatedCost(cost) if cost.total().amount().canonical()=="0.000014")));
    let mut unpriced = stream(&server, "openai").await;
    let event = unpriced.next().await.unwrap().unwrap();
    assert!(
        !event
            .canonical_facts()
            .iter()
            .any(|fact| matches!(fact, gateway_core::event::GatewayEvent::CalculatedCost(_)))
    );
}

#[tokio::test]
async fn cancelled_response_remains_a_messages_error() {
    let terminal = json!({"type":"response.cancelled","response":{"id":"resp_cancelled","status":"cancelled","error":{"code":"cancelled","message":"upstream cancelled the generation"},"usage":{"output_tokens":1}}});
    let server = server(
        ResponseTemplate::new(200)
            .set_body_raw(format!("data: {terminal}\n\n"), "text/event-stream"),
    )
    .await;
    let mut stream = stream(&server, "anthropic").await;
    let mut error = stream.next().await.unwrap().unwrap_err();
    let facts = error.take_atomic_client_events();
    let translated = stream
        .translate_native_response(facts.into_iter().next().unwrap(), true)
        .unwrap();
    assert!(translated.iter().any(|event| {
        event.wire_event().is_some_and(|wire| {
            wire.data()["type"] == "error"
                && wire.data()["error"]["message"] == "upstream cancelled the generation"
        })
    }));
    assert!(!translated.iter().any(|event| {
        event
            .canonical_facts()
            .iter()
            .any(|fact| matches!(fact, gateway_core::event::GatewayEvent::Completed(_)))
    }));
}

#[tokio::test]
async fn completed_event_with_failed_status_does_not_report_success() {
    let terminal = json!({"type":"response.completed","response":{"id":"resp_failed_status","status":"failed","error":{"code":"server_error","message":"actual failed status"},"usage":{"input_tokens":1},"output":[]}});
    let server = server(
        ResponseTemplate::new(200)
            .set_body_raw(format!("data: {terminal}\n\n"), "text/event-stream"),
    )
    .await;
    let mut stream = stream(&server, "openai").await;
    let mut error = stream.next().await.unwrap().unwrap_err();
    let facts = error.take_atomic_client_events();
    assert_eq!(facts[0].wire_event().unwrap().data(), &terminal);
    assert!(!facts.iter().any(|event| {
        event
            .canonical_facts()
            .iter()
            .any(|fact| matches!(fact, gateway_core::event::GatewayEvent::Completed(_)))
    }));
}
