//! 本地 HTTP 验证 BPS 冷流、原始错误、失败用量和 Messages 转换
use futures::StreamExt;
use gateway_core::{
    account::{
        CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
        ProviderAccountId, ProviderAccountStore,
    },
    engine::provider::ProviderStream,
    lifecycle::CancellationToken,
    routing::ProviderKind,
};
use serde_json::json;
use std::sync::Arc;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

async fn image_stream(
    server: &MockServer,
    protocol: &str,
    ports: gateway_core::provider_ports::ProviderStorePorts,
    account: ProviderAccount,
    source: serde_json::Value,
) -> Result<ProviderStream, gateway_core::error::ProviderError> {
    crate::stream::stream_with_source(
        server,
        protocol,
        ports,
        account,
        source,
        request_context(),
        CancellationToken::new(),
    )
    .await
}

const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==";

#[tokio::test]
async fn account_image_policy_rejects_or_replaces_images_without_uploading() {
    for omit in [false, true] {
        let (server, accounts, account) = setup().await;
        let account = crate::support::set_connection_options(
            &accounts,
            &account,
            json!({"image_input_enabled":false,"omit_images_when_disabled":omit}),
        )
        .await;
        let result = image_stream(&server,"openai",crate::support::ports_with(accounts),account,
            json!({"model":"model","input":[{"role":"user","content":[{"type":"input_text","text":"keep this text"},{"type":"input_image","image_url":format!("data:image/png;base64,{PNG}")}]}]})
        ).await;
        if omit {
            let mut stream = result.unwrap();
            while let Some(event) = stream.next().await {
                event.unwrap();
            }
            let requests = server.received_requests().await.unwrap();
            let request = requests
                .iter()
                .find(|request| request.url.path() == "/basispoints/api/responses")
                .unwrap();
            let body = String::from_utf8(request.body.clone()).unwrap();
            assert!(body.contains("keep this text") && body.contains("Image omitted"));
            assert!(!body.contains("data:image") && !body.contains("input_image"));
        } else {
            assert!(result.is_err());
        }
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(
                    |request| request.url.path() == "/basispoints/api/responses/access"
                        || (omit && request.url.path() == "/basispoints/api/responses")
                )
        );
    }
}

async fn setup() -> (
    MockServer,
    Arc<crate::support::MemoryProviderAccountStore>,
    ProviderAccount,
) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"allowed":true,"model_catalog":{"models":[{"id":"model"}]}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST")).and(path("/basispoints/api/responses")).respond_with(ResponseTemplate::new(200).set_body_raw(format!("data: {}\n\n",json!({"type":"response.completed","response":{"id":"resp_image","status":"completed","output":[]}})),"text/event-stream")).mount(&server).await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    let account = ProviderAccount::new(
        ProviderAccountId::new("acct_image").unwrap(),
        ProviderKind::new("bps").unwrap(),
        "image".into(),
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
    (server, accounts, account)
}

#[tokio::test]
async fn inline_message_image_uploads_only_when_polled_and_uses_native_attachment() {
    let (server, accounts, account) = setup().await;
    Mock::given(method("POST"))
        .and(path("/basispoints/api/attachments"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"openai_file_id":"file-nativeImage"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let source = json!({"model":"model","input":[{"role":"user","content":[{"type":"input_text","text":"describe"},{"type":"input_image","image_url":format!("data:image/png;base64,{PNG}")}]}]});
    let mut stream = image_stream(
        &server,
        "openai",
        crate::support::ports_with(accounts),
        account,
        source,
    )
    .await
    .unwrap();
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/basispoints/api/attachments")
    );
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    let upload = requests
        .iter()
        .find(|request| request.url.path() == "/basispoints/api/attachments")
        .unwrap();
    assert!(
        upload
            .headers
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("multipart/form-data; boundary=")
    );
    assert!(upload.headers.contains_key("authorization"));
    assert!(upload.body.windows(9).any(|bytes| bytes == b"image/png"));
    let generation = requests
        .iter()
        .find(|request| request.url.path() == "/basispoints/api/responses")
        .unwrap();
    let generation = String::from_utf8_lossy(&generation.body);
    assert!(generation.contains("file-nativeImage"));
    assert!(!generation.contains(PNG));
}

#[tokio::test]
async fn https_and_native_file_references_are_not_downloaded_or_uploaded() {
    let (server, accounts, account) = setup().await;
    let source = json!({"model":"model","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://images.example.org/photo.png"},{"type":"input_image","file_id":"file-existing"}]}]});
    let mut stream = image_stream(
        &server,
        "openai",
        crate::support::ports_with(accounts),
        account,
        source,
    )
    .await
    .unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/basispoints/api/attachments")
    );
}

#[tokio::test]
async fn malformed_image_or_unsafe_reference_fails_before_any_upload() {
    for image in [
        "data:image/png;base64,ZmFrZQ==".to_owned(),
        "http://127.0.0.1/private".to_owned(),
        "https://user:password@example.com/private".to_owned(),
    ] {
        let (server, accounts, account) = setup().await;
        let source = json!({"model":"model","input":[{"role":"user","content":[{"type":"input_image","image_url":image}]}]});
        let result = image_stream(
            &server,
            "openai",
            crate::support::ports_with(accounts),
            account,
            source,
        )
        .await;
        assert_eq!(
            result.err().unwrap().kind(),
            gateway_core::error::ProviderErrorKind::InvalidRequest
        );
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/basispoints/api/attachments")
        );
    }
}

#[tokio::test]
async fn messages_base64_source_uses_the_same_native_upload_path() {
    let (server, accounts, account) = setup().await;
    Mock::given(method("POST"))
        .and(path("/basispoints/api/attachments"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"openai_file_id":"file-messagesImage"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let source = json!({"model":"model","max_tokens":20,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":PNG}},{"type":"text","text":"describe"}]}]});
    let mut stream = image_stream(
        &server,
        "anthropic",
        crate::support::ports_with(accounts),
        account,
        source,
    )
    .await
    .unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    let generation = requests
        .iter()
        .find(|request| request.url.path() == "/basispoints/api/responses")
        .unwrap();
    assert!(String::from_utf8_lossy(&generation.body).contains("file-messagesImage"));
}

#[tokio::test]
async fn attachment_redirect_is_not_followed_and_generation_is_not_sent() {
    let (server, accounts, account) = setup().await;
    let redirect = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/basispoints/api/attachments"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("location", format!("{}/secret", redirect.uri())),
        )
        .expect(1)
        .mount(&server)
        .await;
    let source = json!({"model":"model","input":[{"role":"user","content":[{"type":"input_image","image_url":format!("data:image/png;base64,{PNG}")}]}]});
    let mut stream = image_stream(
        &server,
        "openai",
        crate::support::ports_with(accounts),
        account,
        source,
    )
    .await
    .unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(error.retry_is_prohibited());
    assert!(redirect.received_requests().await.unwrap().is_empty());
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/basispoints/api/responses")
    );
}

#[tokio::test]
async fn tool_screenshot_remains_inline_and_is_not_uploaded_as_message_attachment() {
    let (server, accounts, account) = setup().await;
    let source = json!({"model":"model","input":[{"type":"function_call","call_id":"call_screen","name":"capture","arguments":"{}"},{"type":"function_call_output","call_id":"call_screen","output":[{"type":"input_text","text":"screenshot"},{"type":"input_image","image_url":format!("data:image/png;base64,{PNG}")}]}]});
    let mut stream = image_stream(
        &server,
        "openai",
        crate::support::ports_with(accounts),
        account,
        source,
    )
    .await
    .unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() != "/basispoints/api/attachments")
    );
    let generation = requests
        .iter()
        .find(|request| request.url.path() == "/basispoints/api/responses")
        .unwrap();
    assert!(String::from_utf8_lossy(&generation.body).contains(PNG));
}

#[tokio::test]
async fn cancelling_a_gated_attachment_upload_releases_the_account_lease() {
    use std::{sync::atomic::Ordering, time::Duration};
    let (server, accounts, account) = setup().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = entered.clone();
    Mock::given(method("POST"))
        .and(path("/basispoints/api/attachments"))
        .respond_with(move |_: &wiremock::Request| {
            notify.notify_one();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(60))
                .set_body_json(json!({"openai_file_id":"file-neverDelivered"}))
        })
        .expect(1)
        .mount(&server)
        .await;
    let leases = Arc::new(crate::support::CountingLeases::default());
    let cancellation = CancellationToken::new();
    let source = json!({"model":"model","input":[{"role":"user","content":[{"type":"input_image","image_url":format!("data:image/png;base64,{PNG}")}]}]});
    let stream = crate::stream::stream_with_source(
        &server,
        "openai",
        crate::support::ports_with_leases(accounts, leases.clone()),
        account,
        source,
        request_context(),
        cancellation.clone(),
    )
    .await
    .unwrap();
    assert_eq!(leases.active.load(Ordering::SeqCst), 1);
    let polling = tokio::spawn(async move {
        let mut stream = stream;
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(
            error.kind(),
            gateway_core::error::ProviderErrorKind::Cancelled
        );
        assert!(error.retry_is_prohibited());
        drop(stream);
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), polling)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leases.active.load(Ordering::SeqCst), 0);
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/basispoints/api/responses")
    );
}

fn request_context() -> gateway_core::engine::RequestAttemptContext {
    gateway_core::engine::RequestAttemptContext::new(
        gateway_core::engine::ModelRequestId::new("req_bps_image").unwrap(),
        gateway_core::policy::ClientApiKeyId::new("key_bps_image").unwrap(),
    )
}

#[derive(Debug)]
struct RejectAttachment;
impl gateway_core::engine::middleware::MiddlewarePlan for RejectAttachment {
    fn has_outbound(&self, _: &gateway_core::engine::middleware::MiddlewareContext) -> bool {
        true
    }
    fn handle_outbound(
        &self,
        _: gateway_core::engine::middleware::MiddlewareContext,
        request: gateway_core::engine::middleware::outbound::Request,
        next: gateway_core::engine::middleware::outbound::Next,
    ) -> futures::future::BoxFuture<
        'static,
        Result<
            gateway_core::engine::middleware::outbound::Request,
            gateway_core::engine::middleware::MiddlewareError,
        >,
    > {
        Box::pin(async move {
            if matches!(&request.target,gateway_core::engine::middleware::outbound::Target::Http{uri,..} if uri.path()=="/basispoints/api/attachments")
            {
                assert!(request.header_map().unwrap().contains_key("authorization"));
                assert!(request.body.windows(9).any(|bytes| bytes == b"image/png"));
                return Err(gateway_core::engine::middleware::MiddlewareError::Rejected);
            }
            next.run(request).await
        })
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
async fn outbound_middleware_can_reject_attachment_before_upload_or_generation() {
    use gateway_core::{
        engine::middleware::FrozenMiddlewarePlan,
        routing::extensions::{ExtensionSetId, ExtensionSetReference},
    };
    let (server, accounts, account) = setup().await;
    let middleware = FrozenMiddlewarePlan::new(
        Arc::new(RejectAttachment),
        ExtensionSetReference::new(
            ExtensionSetId::new("image-outbound".into()).unwrap(),
            Arc::new(ReadyGeneration),
        ),
    );
    let request = request_context().with_middleware(
        Some(middleware),
        Arc::from([]),
        "/v1/responses".into(),
        gateway_core::engine::execution::ClientTransport::HttpJson,
    );
    let source = json!({"model":"model","input":[{"role":"user","content":[{"type":"input_image","image_url":format!("data:image/png;base64,{PNG}")}]}]});
    let mut stream = crate::stream::stream_with_source(
        &server,
        "openai",
        crate::support::ports_with(accounts),
        account,
        source,
        request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(error.retry_is_prohibited());
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| !matches!(
                request.url.path(),
                "/basispoints/api/responses" | "/basispoints/api/attachments"
            ))
    );
}
