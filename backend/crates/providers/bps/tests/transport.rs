//! 通过本地 WebSocket 验证 BPS 帧、代理、图片、失败与连接释放

use futures::{SinkExt, StreamExt};
use gateway_core::{
    account::{
        CredentialRevision, NewProviderAccount, OutboundProxy, PlaintextCredential,
        ProviderAccount, ProviderAccountId, ProviderAccountStore,
    },
    engine::{ModelRequestId, RequestAttemptContext, provider::ProviderStream},
    error::ProviderErrorKind,
    lifecycle::CancellationToken,
    policy::ClientApiKeyId,
    routing::ProviderKind,
    upstream::UpstreamSendState,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};

async fn stream(
    base: String,
    source: Value,
    proxy: Option<OutboundProxy>,
    cancellation: CancellationToken,
) -> ProviderStream {
    stream_with_context(
        base,
        source,
        proxy,
        cancellation,
        RequestAttemptContext::new(
            ModelRequestId::new("req_ws").unwrap(),
            ClientApiKeyId::new("key_ws").unwrap(),
        ),
    )
    .await
}

async fn stream_with_context(
    base: String,
    source: Value,
    proxy: Option<OutboundProxy>,
    cancellation: CancellationToken,
    context: RequestAttemptContext,
) -> ProviderStream {
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    // 诊断调用固定账号，避免把模型目录 HTTP 请求混入 WS 测试服务
    let account = ProviderAccount::new(
        ProviderAccountId::new("acct_ws").unwrap(),
        ProviderKind::new("bps").unwrap(),
        "ws".into(),
        Some("user-test".into()),
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_profile(None, Some("workspace-test".into()), None)
    .with_account_facts(
        false,
        gateway_core::account::CredentialState::Ready,
        gateway_core::account::QuotaState::unknown(),
        None,
        None,
    )
    .with_outbound_proxy(proxy);
    accounts
        .create_account(NewProviderAccount {
            account: account.clone(),
            model_access: None,
            credential: PlaintextCredential::new(
                json!({"access_token":crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp")})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        })
        .await
        .unwrap();
    crate::stream::stream_with_config(
        provider_bps::BpsConfig {
            base_url: base,
            ..Default::default()
        },
        "openai",
        crate::support::ports_with(accounts),
        account,
        source,
        context,
        cancellation,
    )
    .await
    .unwrap()
}

async fn server() -> (String, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    (
        format!("http://{}/", listener.local_addr().unwrap()),
        listener,
    )
}

async fn accept(listener: TcpListener) -> WebSocketStream<TcpStream> {
    let (tcp, _) = listener.accept().await.unwrap();
    accept_hdr_async(tcp, AcceptOpening::Native).await.unwrap()
}

enum AcceptOpening {
    Native,
    Prepared,
}

impl tokio_tungstenite::tungstenite::handshake::server::Callback for AcceptOpening {
    fn on_request(
        self,
        request: &Request,
        mut response: Response,
    ) -> Result<Response, tokio_tungstenite::tungstenite::handshake::server::ErrorResponse> {
        if matches!(self, Self::Prepared) {
            assert_eq!(request.headers()["x-outbound-test"], "prepared");
            response
                .headers_mut()
                .insert("sec-websocket-protocol", "responses".parse().unwrap());
            return Ok(response);
        }
        assert_eq!(request.uri().path(), "/basispoints/api/responses");
        assert_eq!(request.headers()["x-basispoints-auth-mode"], "chatgpt");
        assert!(
            request.headers()["authorization"]
                .to_str()
                .unwrap()
                .starts_with("Bearer ")
        );
        let protocols = request.headers()["sec-websocket-protocol"]
            .to_str()
            .unwrap();
        assert!(protocols.starts_with("responses, openai-bearer."));
        let url = url::Url::parse(&format!(
            "http://localhost{}",
            request.uri().path_and_query().unwrap()
        ))
        .unwrap();
        let query: std::collections::BTreeMap<_, _> = url.query_pairs().collect();
        assert_eq!(query["bps_auth_mode"], "chatgpt");
        assert_eq!(query["bps_control_frames"], "upstream_sent,heartbeat");
        let profile: Value = serde_json::from_str(&query["bps_client_info"]).unwrap();
        assert_eq!(
            profile["X-Openai-Internal-Basispoints-Office-Host"],
            "Excel"
        );
        assert!(!query["bps_client_info"].contains("Bearer"));
        response
            .headers_mut()
            .insert("sec-websocket-protocol", "responses".parse().unwrap());
        Ok(response)
    }
}

async fn payload(socket: &mut WebSocketStream<TcpStream>) -> Value {
    let value: Value =
        serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(value["type"], "response.create");
    assert!(uuid::Uuid::parse_str(value["basispoints_request_id"].as_str().unwrap()).is_ok());
    assert!(
        value["basispoints_resume_token"]
            .as_str()
            .unwrap()
            .starts_with("resume_")
    );
    value
}

async fn send(socket: &mut WebSocketStream<TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

fn completed() -> Value {
    json!({"type":"response.completed","basispoints_replay_cursor":42,"response":{"id":"resp_ws","status":"completed","output":[],"usage":{"input_tokens":7,"output_tokens":2,"total_tokens":9}}})
}

async fn joined(task: JoinHandle<()>) {
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn websocket_is_cold_and_uses_account_proxy_and_native_images() {
    let (proxy, listener) = server().await;
    let mut response = stream("http://bps.invalid/".into(), json!({"model":"model","input":[{"role":"user","content":[{"type":"input_text","text":"describe"},{"type":"input_image","image_url":"https://example.test/image.png"},{"type":"input_image","file_id":"file-native"}]}],"reasoning":{"effort":"medium"}}), Some(OutboundProxy::parse(&proxy).unwrap()), CancellationToken::new()).await;
    assert_eq!(response.metadata().transport().as_str(), "websocket");
    assert!(
        timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err(),
        "execute must not open a WS before polling"
    );
    let task = tokio::spawn(async move {
        let mut socket = accept(listener).await;
        let request = payload(&mut socket).await;
        assert!(
            request
                .to_string()
                .contains("https://example.test/image.png")
        );
        assert!(request.to_string().contains("file-native"));
        assert_eq!(request["reasoning_effort"], "medium");
        for kind in ["resume_token", "metadata", "upstream_sent", "heartbeat"] {
            send(
                &mut socket,
                json!({"type":format!("basispoints.response.{kind}")}),
            )
            .await;
        }
        socket
            .send(Message::Ping(bytes::Bytes::from_static(b"ping")))
            .await
            .unwrap();
        assert!(
            matches!(socket.next().await.unwrap().unwrap(), Message::Pong(data) if data.as_ref() == b"ping")
        );
        send(&mut socket, json!({"type":"response.output_text.delta","delta":"hello","output_index":0,"content_index":0,"basispoints_replay_cursor":1})).await;
        send(&mut socket, completed()).await;
        assert!(
            socket
                .next()
                .await
                .is_none_or(|message| message.is_err() || matches!(message, Ok(Message::Close(_)))),
            "terminal consumption must release the socket"
        );
    });
    let mut wire = Vec::new();
    let mut timings = gateway_core::event::ProviderResponseTimings::default();
    while let Some(event) = response.next().await {
        let event = event.unwrap();
        timings = event.response_observation().unwrap().timings();
        if let Some(event) = event.wire_event() {
            wire.push(event.data().clone());
        }
    }
    assert!(timings.first_token_ms.is_some());
    assert!(timings.first_text_ms.is_some());
    assert_eq!(timings.first_reasoning_ms, None);
    assert_eq!(wire.len(), 2);
    assert_eq!(wire[0]["delta"], "hello");
    assert!(
        wire.iter()
            .all(|event| event.get("basispoints_replay_cursor").is_none())
    );
    joined(task).await;
}

#[tokio::test]
async fn websocket_failed_terminal_keeps_error_and_usage() {
    let (base, listener) = server().await;
    let failed = json!({"type":"response.failed","response":{"id":"resp_failed","status":"failed","error":{"code":"rate_limit_exceeded","message":"original WS rejection"},"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15}}});
    let upstream = failed.clone();
    let task = tokio::spawn(async move {
        let mut socket = accept(listener).await;
        payload(&mut socket).await;
        send(&mut socket, upstream).await;
    });
    let mut response = stream(
        base,
        json!({"model":"model","input":"hello"}),
        None,
        CancellationToken::new(),
    )
    .await;
    let mut error = response.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ProviderErrorKind::RateLimited);
    assert_eq!(error.send_state(), UpstreamSendState::Sent);
    assert!(error.retry_is_prohibited());
    let events = error.take_atomic_client_events();
    assert_eq!(events[0].wire_event().unwrap().data(), &failed);
    assert!(events[0].canonical_facts().iter().any(|event| matches!(event,gateway_core::event::GatewayEvent::Usage(usage) if usage.input_tokens == Some(12) && usage.output_tokens == Some(3))));
    joined(task).await;
}

#[tokio::test]
async fn handshake_rejection_preserves_original_error_without_sending_generation() {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };
    let server = MockServer::start().await;
    let raw = r#"{"error":{"code":"denied","message":"original handshake rejection"}}"#;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses"))
        .respond_with(ResponseTemplate::new(403).set_body_raw(raw, "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    let mut response = stream(
        server.uri(),
        json!({"model":"model","input":"hello"}),
        None,
        CancellationToken::new(),
    )
    .await;
    let error = response.next().await.unwrap().unwrap_err();
    assert_eq!(error.send_state(), UpstreamSendState::NotSent);
    assert_eq!(error.kind(), ProviderErrorKind::PermissionDenied);
    assert_eq!(error.upstream_status(), Some(403));
    assert_eq!(
        error
            .client_visible_upstream_response()
            .unwrap()
            .body()
            .as_ref(),
        raw.as_bytes()
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.body.is_empty())
    );
}

#[tokio::test]
async fn websocket_rejects_truncation_draining_and_invalid_frames() {
    for message in [
        Message::Close(None),
        Message::Text("invalid JSON".into()),
        Message::Binary(bytes::Bytes::from_static(b"binary")),
        Message::Text(
            json!({"type":"basispoints.response.server_draining"})
                .to_string()
                .into(),
        ),
    ] {
        let (base, listener) = server().await;
        let task = tokio::spawn(async move {
            let mut socket = accept(listener).await;
            payload(&mut socket).await;
            socket.send(message).await.unwrap();
        });
        let mut response = stream(
            base,
            json!({"model":"model","input":"hello"}),
            None,
            CancellationToken::new(),
        )
        .await;
        let error = response.next().await.unwrap().unwrap_err();
        assert_eq!(error.send_state(), UpstreamSendState::Sent);
        assert!(error.retry_is_prohibited());
        joined(task).await;
    }
}

#[tokio::test]
async fn websocket_permission_rejection_preserves_actual_status_and_classification() {
    for (failed, status) in [
        (
            json!({"type":"error","status":403,"error":{"code":"upstream_denied","message":"blocked"}}),
            Some(403),
        ),
        (
            json!({"type":"error","status":403,"error":{"code":"rate_limit_exceeded","message":"blocked"}}),
            Some(403),
        ),
        (
            json!({"type":"response.failed","response":{"id":"resp_denied","status":"failed","error":{"status":403,"message":"blocked"}}}),
            Some(403),
        ),
        (
            json!({"type":"error","code":"request_rejected","message":"This request was blocked by our usage policy."}),
            None,
        ),
    ] {
        let (base, listener) = server().await;
        let upstream = failed.clone();
        let task = tokio::spawn(async move {
            let mut socket = accept(listener).await;
            payload(&mut socket).await;
            send(&mut socket, upstream).await;
        });
        let mut response = stream(
            base,
            json!({"model":"model","input":"hello"}),
            None,
            CancellationToken::new(),
        )
        .await;
        let mut error = response.next().await.unwrap().unwrap_err();
        assert_eq!(error.upstream_status(), status);
        assert_eq!(error.kind(), ProviderErrorKind::PermissionDenied);
        assert_eq!(error.send_state(), UpstreamSendState::Sent);
        assert_eq!(
            error.take_atomic_client_events()[0]
                .wire_event()
                .unwrap()
                .data(),
            &failed
        );
        joined(task).await;
    }
}

#[tokio::test]
async fn cancellation_releases_inflight_websocket() {
    let (base, listener) = server().await;
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let task = tokio::spawn(async move {
        let mut socket = accept(listener).await;
        payload(&mut socket).await;
        cancel.cancel();
        assert!(
            socket
                .next()
                .await
                .is_none_or(|message| message.is_err() || matches!(message, Ok(Message::Close(_))))
        );
    });
    let mut response = stream(
        base,
        json!({"model":"model","input":"hello"}),
        None,
        cancellation,
    )
    .await;
    let error = response.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ProviderErrorKind::Cancelled);
    assert_eq!(error.send_state(), UpstreamSendState::Sent);
    joined(task).await;
}

#[derive(Debug)]
struct RewriteOpening(String);

impl gateway_core::engine::middleware::MiddlewarePlan for RewriteOpening {
    fn has_outbound(&self, _: &gateway_core::engine::middleware::MiddlewareContext) -> bool {
        true
    }

    fn handle_outbound(
        &self,
        _: gateway_core::engine::middleware::MiddlewareContext,
        mut request: gateway_core::engine::middleware::outbound::Request,
        next: gateway_core::engine::middleware::outbound::Next,
    ) -> futures::future::BoxFuture<
        'static,
        Result<
            gateway_core::engine::middleware::outbound::Request,
            gateway_core::engine::middleware::MiddlewareError,
        >,
    > {
        use gateway_core::engine::middleware::{MiddlewareHeader, outbound::Target};
        let headers = request.header_map().unwrap();
        if let Target::WebSocket { uri } = &mut request.target {
            assert_eq!(headers["x-basispoints-auth-mode"], "chatgpt");
            let mut body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["type"], "response.create");
            assert!(body.to_string().contains("file-uploaded"));
            body["metadata"]["outbound_test"] = json!(true);
            request.body = body.to_string().into();
            let mut target = url::Url::parse(&uri.to_string()).unwrap();
            let replacement = url::Url::parse(&self.0).unwrap();
            target.set_host(replacement.host_str()).unwrap();
            target.set_port(replacement.port()).unwrap();
            *uri = target.as_str().parse().unwrap();
            request
                .headers
                .retain(|header| !header.name().eq_ignore_ascii_case("host"));
            request.headers.push(MiddlewareHeader::new(
                "host",
                replacement[url::Position::BeforeHost..url::Position::AfterPort]
                    .to_string()
                    .into(),
            ));
            request.headers.push(MiddlewareHeader::new(
                "x-outbound-test",
                bytes::Bytes::from_static(b"prepared"),
            ));
        }
        Box::pin(async move { next.run(request).await })
    }

    fn handle(
        &self,
        _: gateway_core::engine::middleware::MiddlewareContext,
        request: gateway_core::engine::middleware::MiddlewareRequest,
        next: gateway_core::engine::middleware::MiddlewareNext,
    ) -> futures::future::BoxFuture<
        'static,
        Result<
            gateway_core::engine::middleware::MiddlewareResponse,
            gateway_core::engine::middleware::MiddlewareError,
        >,
    > {
        next.run(request)
    }
}

#[derive(Debug)]
struct ReadyGeneration;
impl gateway_core::routing::extensions::ExtensionSetLease for ReadyGeneration {
    fn is_ready(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn inline_image_upload_and_tool_relay_share_websocket_outbound_preparation() {
    use gateway_core::{
        engine::middleware::FrozenMiddlewarePlan,
        routing::extensions::{ExtensionSetId, ExtensionSetReference},
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };
    const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==";
    let upload = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/basispoints/api/attachments"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"openai_file_id":"file-uploaded"})),
        )
        .expect(1)
        .mount(&upload)
        .await;
    let (base, listener) = server().await;
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(tcp, AcceptOpening::Prepared)
            .await
            .unwrap();
        let request = payload(&mut socket).await;
        assert_eq!(request["metadata"]["outbound_test"], true);
        assert!(request.to_string().contains("file-uploaded"));
        assert!(!request.to_string().contains(PNG));
        let native = json!({"type":"function_call","id":"fc_native","call_id":"call_native","name":"run_officejs","arguments":json!({"code":json!({"name":"shell","arguments":{"cmd":"echo hello"}}).to_string()}).to_string()});
        send(
            &mut socket,
            json!({"type":"response.output_item.added","output_index":0,"item":native}),
        )
        .await;
        send(&mut socket,json!({"type":"response.completed","response":{"id":"resp_tool","status":"completed","output":[native]}})).await;
    });
    let middleware = FrozenMiddlewarePlan::new(
        Arc::new(RewriteOpening(base)),
        ExtensionSetReference::new(
            ExtensionSetId::new("ws-outbound".into()).unwrap(),
            Arc::new(ReadyGeneration),
        ),
    );
    let context = RequestAttemptContext::new(
        ModelRequestId::new("req_ws_image").unwrap(),
        ClientApiKeyId::new("key_ws_image").unwrap(),
    )
    .with_middleware(
        Some(middleware),
        Arc::from([]),
        "/v1/responses".into(),
        gateway_core::engine::execution::ClientTransport::HttpJson,
    );
    let source = json!({"model":"model","input":[{"role":"user","content":[{"type":"input_image","image_url":format!("data:image/png;base64,{PNG}")}]}],"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}]});
    let mut response = stream_with_context(
        upload.uri(),
        source,
        None,
        CancellationToken::new(),
        context,
    )
    .await;
    assert!(upload.received_requests().await.unwrap().is_empty());
    let mut events = Vec::new();
    while let Some(event) = response.next().await {
        let event = event.unwrap();
        if let Some(wire) = event.wire_event() {
            events.push(wire.data().clone());
        }
    }
    assert_eq!(
        events.len(),
        3,
        "native tool events must be withheld until the successful terminal batch"
    );
    assert_eq!(events[0]["item"]["name"], "shell");
    assert_eq!(events[2]["response"]["output"][0]["call_id"], "call_native");
    joined(task).await;
}
