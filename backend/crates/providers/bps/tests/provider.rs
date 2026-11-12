//! 通过真实 Provider 调用验证冻结范围、目录撤回和并发队位生命周期

use futures::{future::BoxFuture, poll};
use gateway_core::{
    account::{
        AccountRuntimeSignals, AccountSelectionPolicy, CredentialRevision, CredentialState,
        NewProviderAccount, PlaintextCredential, ProviderAccount, ProviderAccountId,
        ProviderAccountStore, QuotaState, RotationStrategy,
        scope::{ClientRoutingScope, FrozenAccountScope, RuntimeAccount, RuntimeAccountDirectory},
    },
    concurrency::ConcurrencyQueuePolicy,
    engine::{
        AccountAttemptContext, AttemptContext, ModelRequestId, RequestAttemptContext,
        provider::{Provider, ProviderRequest},
    },
    error::ProviderErrorKind,
    lifecycle::CancellationToken,
    operation::{GenerateRequest, Operation, OperationKind, ProtocolPayload},
    policy::ClientApiKeyId,
    provider_ports::{
        ProviderConcurrencyPool, ProviderLeaseAcquisition, ProviderLeaseGuard, ProviderLeasePort,
        ProviderLeaseRequest, ProviderSchedulingState, ProviderStoreError,
    },
    routing::{
        ConfigRevision, ModelCapabilities, ProviderKind, ProviderModel, PublicModelId,
        RoutingContext, RuntimeSnapshot, UpstreamModelId,
    },
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[derive(Default)]
struct Capacity {
    active: Arc<AtomicU32>,
    acquisitions: AtomicUsize,
    reads: AtomicUsize,
    // 模拟外部租约在下一次读取前释放，验证队列超时之后的无排队重选
    busy_reads: usize,
}

impl ProviderLeasePort for Capacity {
    fn load_state<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ProviderKind,
        accounts: &'a [ProviderAccountId],
        _: ProviderConcurrencyPool,
    ) -> BoxFuture<'a, Result<ProviderSchedulingState, ProviderStoreError>> {
        Box::pin(async move {
            let read = self.reads.fetch_add(1, Ordering::SeqCst);
            let in_flight = self
                .active
                .load(Ordering::SeqCst)
                .max(u32::from(read < self.busy_reads));
            Ok(ProviderSchedulingState::new(
                accounts
                    .iter()
                    .cloned()
                    .map(|id| {
                        (
                            id,
                            AccountRuntimeSignals {
                                in_flight,
                                last_started_at: None,
                                quota_reset_at: None,
                                quota_remaining_rank: None,
                                cooldown: None,
                                failure_rate_basis_points: None,
                                first_output_latency_ms: None,
                            },
                        )
                    })
                    .collect(),
                0,
            ))
        })
    }
    fn try_acquire(
        &self,
        request: ProviderLeaseRequest,
    ) -> BoxFuture<'_, Result<ProviderLeaseAcquisition, ProviderStoreError>> {
        Box::pin(async move {
            assert!(matches!(request, ProviderLeaseRequest::Scheduling(_)));
            if self
                .active
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return Ok(ProviderLeaseAcquisition::Busy { retry_after: None });
            }
            self.acquisitions.fetch_add(1, Ordering::SeqCst);
            Ok(ProviderLeaseAcquisition::Acquired(Box::new(Slot(
                self.active.clone(),
            ))))
        })
    }
}
struct Slot(Arc<AtomicU32>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ProviderLeaseGuard for Slot {
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }
}

struct Fixture {
    server: MockServer,
    accounts: Arc<crate::support::MemoryProviderAccountStore>,
    capacity: Arc<Capacity>,
    provider: Arc<dyn Provider>,
    account: ProviderAccount,
}
impl Fixture {
    async fn new(capacity: Capacity) -> Self {
        let server = MockServer::start().await;
        let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
        let account = ProviderAccount::new(
            ProviderAccountId::new("acct_bps_selection").unwrap(),
            ProviderKind::new("bps").unwrap(),
            "test".into(),
            Some("user-test".into()),
            "oauth".into(),
            CredentialRevision::new(1).unwrap(),
            None,
        )
        .with_profile(None, Some("workspace-test".into()), None)
        .with_account_facts(
            true,
            CredentialState::Ready,
            QuotaState::unknown(),
            None,
            None,
        );
        accounts.create_account(NewProviderAccount {
            account: account.clone(), model_access: None,
            credential: PlaintextCredential::new(json!({"access_token":crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp")}).as_object().unwrap().clone()),
        }).await.unwrap();
        let capacity = Arc::new(capacity);
        let provider = provider_bps::initialize(
            provider_bps::BpsConfig {
                base_url: server.uri(),
                upstream_transport: provider_bps::BpsUpstreamTransport::Sse,
                auth_base_url: server.uri(),
                quota_base_url: server.uri(),
                ..Default::default()
            },
            crate::support::ports_with_leases(accounts.clone(), capacity.clone()),
        )
        .unwrap()
        .core_provider();
        Self {
            server,
            accounts,
            capacity,
            provider,
            account,
        }
    }
    async fn catalog(&self, allowed: bool) {
        self.server.reset().await;
        Mock::given(method("GET"))
            .and(path("/basispoints/api/responses/access"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"allowed":allowed,"model_catalog":{"models":[{"id":"model"}]}}),
            ))
            .mount(&self.server)
            .await;
    }
    fn scope(&self) -> Arc<FrozenAccountScope> {
        self.scope_for(self.account.id().clone())
    }
    fn scope_for(&self, id: ProviderAccountId) -> Arc<FrozenAccountScope> {
        Arc::new(FrozenAccountScope::new(
            Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([(
                id,
                RuntimeAccount::new(ProviderKind::new("bps").unwrap(), BTreeSet::new()),
            )]))),
            ClientRoutingScope::all_accounts(),
        ))
    }
    fn request(&self) -> ProviderRequest {
        self.request_with_source(json!({"model":"model","input":"hello"}))
    }
    fn request_with_source(&self, source: serde_json::Value) -> ProviderRequest {
        let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object("openai", source.as_object().unwrap().clone()).unwrap(),
        ));
        let kind = ProviderKind::new("bps").unwrap();
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
                        gateway_core::operation::Feature::Vision,
                        gateway_core::routing::SupportLevel::Native,
                    )
                    .with_feature(
                        gateway_core::operation::Feature::Reasoning,
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
                self.scope(),
                &RoutingContext::default(),
            )
            .unwrap();
        ProviderRequest::new(operation, plan.candidates()[0].clone())
    }
    fn context(
        &self,
        scope: Arc<FrozenAccountScope>,
        policy: AccountSelectionPolicy,
        cancellation: CancellationToken,
    ) -> AttemptContext {
        AttemptContext::new(
            RequestAttemptContext::new(
                ModelRequestId::new("req_bps_selection").unwrap(),
                ClientApiKeyId::new("key_bps_selection").unwrap(),
            ),
            NonZeroU32::new(1).unwrap(),
            SystemTime::now() + Duration::from_secs(5),
            policy,
            AccountAttemptContext::new(BTreeSet::new(), None, None).with_account_scope(scope),
            None,
            cancellation,
        )
    }
}
fn policy() -> AccountSelectionPolicy {
    AccountSelectionPolicy::new(RotationStrategy::Smart, NonZeroU32::new(1).unwrap())
}
fn queued_policy() -> AccountSelectionPolicy {
    policy().with_queue(ConcurrencyQueuePolicy {
        max_waiting: 1,
        timeout: Duration::from_secs(2),
    })
}

#[tokio::test]
async fn request_rejections_preserve_safe_reasons_in_management_diagnostics() {
    let fixture = Fixture::new(Capacity::default()).await;
    fixture.catalog(true).await;
    for (patch, expected, code) in [
        (
            json!({"tools":[{"type":"web_search"}]}),
            "Basispoints supports declared function and custom tools only.",
            "basispoints_request_invalid",
        ),
        (
            json!({"input":[{"type":"web_search_call","content":"PRIVATE_REQUEST_TEXT","encrypted_content":"PRIVATE_REQUEST_TEXT"}]}),
            "Unsupported Basispoints history item type 'web_search_call'.",
            "basispoints_request_invalid",
        ),
        (
            json!({"input":[{"type":"web_search_call\nPRIVATE_REQUEST_TEXT","content":"PRIVATE_REQUEST_TEXT"}]}),
            "Unsupported Basispoints history item type '<invalid>'.",
            "basispoints_request_invalid",
        ),
        (
            json!({"input":[{"role":"user","content":[{"type":"input_image","image_url":"http://example.com/PRIVATE_REQUEST_TEXT.png"}]}]}),
            "Invalid or unsupported Basispoints image format, reference, or size.",
            "basispoints_image_invalid",
        ),
    ] {
        let mut source = json!({"model":"model","input":"PRIVATE_REQUEST_TEXT"});
        source
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        let error = fixture
            .provider
            .clone()
            .execute(
                fixture.request_with_source(source),
                fixture.context(fixture.scope(), policy(), CancellationToken::new()),
            )
            .await
            .err()
            .expect("unsupported capability must be rejected before sending");
        assert_eq!(error.kind(), ProviderErrorKind::InvalidRequest);
        assert_eq!(
            error.send_state(),
            gateway_core::upstream::UpstreamSendState::NotSent
        );
        let gateway = gateway_core::error::GatewayError::from_provider(&error);
        assert_eq!(gateway.client_message(), expected);
        let diagnostic = gateway
            .diagnostic()
            .expect("management history needs the same safe rejection reason");
        assert_eq!(diagnostic.as_str(), expected);
        assert_eq!(diagnostic.stage(), Some("bps_request_prepare"));
        assert_eq!(diagnostic.code(), Some(code));
        assert!(!diagnostic.as_str().contains("PRIVATE_REQUEST_TEXT"));
        assert_eq!(fixture.capacity.active.load(Ordering::SeqCst), 0);
    }
    assert!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/basispoints/api/responses")
    );
}

#[tokio::test]
async fn codex_compatibility_omits_hosted_tools_and_encrypted_history_but_keeps_images() {
    use futures::StreamExt as _;

    let mut fixture = Fixture::new(Capacity::default()).await;
    fixture.account = crate::support::set_connection_options(&fixture.accounts, &fixture.account,
        json!({"image_input_enabled":true,"omit_unsupported_hosted_tools":true,"ignore_encrypted_history":true})
    ).await;
    fixture.catalog(true).await;
    Mock::given(method("POST"))
        .and(path("/basispoints/api/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            format!("data: {}\n\n", json!({"type":"response.completed","response":{"id":"resp_compat","status":"completed","output":[]}})),
            "text/event-stream",
        ))
        .expect(1)
        .mount(&fixture.server)
        .await;
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: fixture.server.uri(),
            upstream_transport: provider_bps::BpsUpstreamTransport::Sse,
            image_input_enabled: false,
            ..Default::default()
        },
        crate::support::ports_with_leases(fixture.accounts.clone(), fixture.capacity.clone()),
    )
    .unwrap()
    .core_provider();
    let source = json!({
        "model":"model", "stream":true, "store":false, "service_tier":"priority",
        "tool_choice":"auto", "parallel_tool_calls":true,
        "reasoning":{"effort":"high"},
        "tools":[
            {"type":"web_search"},
            {"type":"image_generation"},
            {"type":"function","name":"exec_command","parameters":{"type":"object","properties":{}}}
        ],
        "input":[
            {"type":"additional_tools","role":"developer","tools":[{"type":"namespace","name":"functions","tools":[{"type":"function","name":"read_file","parameters":{"type":"object"}}]}]},
            {"type":"tool_search_output","status":"completed","execution":"client","call_id":null,"tools":[{"type":"custom","name":"apply_patch"}]},
            {"type":"reasoning","summary":[{"type":"summary_text","text":"Plaintext context"}],"encrypted_content":"PRIVATE_ENCRYPTED_HISTORY"},
            {"role":"user","content":[{"type":"input_text","text":"Describe this image"},{"type":"input_image","image_url":"https://example.com/image.png"}]}
        ]
    });
    let mut stream = provider
        .execute(
            fixture.request_with_source(source),
            fixture.context(fixture.scope(), policy(), CancellationToken::new()),
        )
        .await
        .unwrap();
    assert!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/basispoints/api/responses")
    );
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    drop(stream);
    let requests = fixture.server.received_requests().await.unwrap();
    let generation = requests
        .iter()
        .find(|request| request.url.path() == "/basispoints/api/responses")
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&generation.body).unwrap();
    assert!(body.get("service_tier").is_none());
    let encoded = body.to_string();
    assert!(encoded.contains("Hosted tools unavailable"));
    assert!(encoded.contains("exec_command"));
    assert!(encoded.contains("functions.read_file"));
    assert!(encoded.contains("apply_patch"));
    assert!(
        body["input"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| !matches!(
                item["type"].as_str(),
                Some("additional_tools" | "tool_search_output")
            ))
    );
    assert!(encoded.contains("Plaintext context"));
    assert!(!encoded.contains("PRIVATE_ENCRYPTED_HISTORY"));
    assert!(body["input"].as_array().unwrap().iter().any(|item| {
        item["content"].as_array().is_some_and(|parts| {
            parts.iter().any(|part| {
                part["type"] == "input_image"
                    && part["image_url"] == "https://example.com/image.png"
            })
        })
    }));
    assert_eq!(fixture.capacity.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn vision_request_selects_an_account_that_allows_images() {
    use futures::StreamExt as _;
    let mut fixture = Fixture::new(Capacity::default()).await;
    fixture.account = crate::support::set_connection_options(
        &fixture.accounts,
        &fixture.account,
        json!({"image_input_enabled":false,"omit_images_when_disabled":false}),
    )
    .await;
    let second = ProviderAccount::new(
        ProviderAccountId::new("acct_vision_enabled").unwrap(),
        ProviderKind::new("bps").unwrap(),
        "images".into(),
        Some("user-test".into()),
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_profile(None, Some("workspace-test".into()), None)
    .with_account_facts(
        true,
        CredentialState::Ready,
        QuotaState::unknown(),
        None,
        None,
    );
    fixture.accounts.create_account(NewProviderAccount {
        account:second.clone(),model_access:None,
        credential:PlaintextCredential::new(json!({"access_token":crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp"),"image_input_enabled":true}).as_object().unwrap().clone())
    }).await.unwrap();
    fixture.catalog(true).await;
    Mock::given(method("POST")).and(path("/basispoints/api/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(format!("data: {}\n\n",json!({"type":"response.completed","response":{"id":"resp_vision","status":"completed","output":[]}})),"text/event-stream"))
        .expect(1).mount(&fixture.server).await;
    let scope = Arc::new(FrozenAccountScope::new(
        Arc::new(RuntimeAccountDirectory::new(
            [fixture.account.id().clone(), second.id().clone()]
                .into_iter()
                .map(|id| {
                    (
                        id,
                        RuntimeAccount::new(ProviderKind::new("bps").unwrap(), BTreeSet::new()),
                    )
                })
                .collect(),
        )),
        ClientRoutingScope::all_accounts(),
    ));
    let mut stream = fixture.provider.clone().execute(
        fixture.request_with_source(json!({"model":"model","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.test/image.png"}]}]})),
        fixture.context(scope,policy(),CancellationToken::new()),
    ).await.unwrap();
    assert_eq!(stream.metadata().provider_account_id(), second.id());
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
}

#[tokio::test]
async fn frozen_scope_rejects_an_account_outside_the_key_scope_before_catalog_or_lease() {
    let fixture = Fixture::new(Capacity::default()).await;
    let scope = fixture.scope_for(ProviderAccountId::new("acct_other-authorized").unwrap());
    let error = fixture
        .provider
        .clone()
        .execute(
            fixture.request(),
            fixture.context(scope, policy(), CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ProviderErrorKind::NoEligibleAccount);
    assert_eq!(fixture.capacity.acquisitions.load(Ordering::SeqCst), 0);
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn upstream_model_permission_withdrawal_rejects_selection_without_a_lease() {
    let fixture = Fixture::new(Capacity::default()).await;
    fixture.catalog(true).await;
    assert_eq!(
        fixture
            .provider
            .query_model_capabilities()
            .await
            .unwrap()
            .len(),
        1
    );
    let loaded = fixture
        .accounts
        .load_current_credential(fixture.account.id())
        .await
        .unwrap();
    fixture
        .accounts
        .compare_and_swap_credential(
            gateway_core::account::CredentialCasUpdate::new(
                fixture.account.id().clone(),
                loaded.account.revision(),
                gateway_core::account::ProviderAccountUpdate {
                    account_id: fixture.account.id().clone(),
                    name: "test".into(),
                    email: None,
                    plan_type: None,
                },
                loaded.credential,
                false,
                None,
                None,
            )
            .unwrap()
            .preserving_profile(),
        )
        .await
        .unwrap();
    fixture.catalog(false).await;
    let error = fixture
        .provider
        .clone()
        .execute(
            fixture.request(),
            fixture.context(fixture.scope(), policy(), CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ProviderErrorKind::NoEligibleAccount);
    assert_eq!(fixture.capacity.acquisitions.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn cancelling_a_waiter_releases_its_queue_place_and_stream_drop_releases_capacity() {
    let fixture = Fixture::new(Capacity::default()).await;
    fixture.catalog(true).await;
    // 预热目录，使下面的首次 poll 已到达容量队列，不依赖 HTTP 调度时序
    fixture.provider.query_model_capabilities().await.unwrap();
    let held = fixture
        .provider
        .clone()
        .execute(
            fixture.request(),
            fixture.context(fixture.scope(), policy(), CancellationToken::new()),
        )
        .await
        .unwrap();
    assert_eq!(fixture.capacity.active.load(Ordering::SeqCst), 1);
    let cancelled = CancellationToken::new();
    let waiter = fixture.provider.clone().execute(
        fixture.request(),
        fixture.context(fixture.scope(), queued_policy(), cancelled.clone()),
    );
    futures::pin_mut!(waiter);
    assert!(poll!(&mut waiter).is_pending());
    let full = fixture
        .provider
        .clone()
        .execute(
            fixture.request(),
            fixture.context(fixture.scope(), queued_policy(), CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(full.kind(), ProviderErrorKind::ConcurrencyQueueFull);
    cancelled.cancel();
    assert_eq!(
        waiter.await.err().unwrap().kind(),
        ProviderErrorKind::Cancelled
    );
    let follower = fixture.provider.clone().execute(
        fixture.request(),
        fixture.context(fixture.scope(), queued_policy(), CancellationToken::new()),
    );
    futures::pin_mut!(follower);
    assert!(
        poll!(&mut follower).is_pending(),
        "cancelled waiter must leave room for another waiter"
    );
    drop(held);
    let stream = tokio::time::timeout(Duration::from_secs(1), follower)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.capacity.active.load(Ordering::SeqCst), 1);
    drop(stream);
    assert_eq!(fixture.capacity.active.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method.as_str() == "GET")
    );
}

#[tokio::test]
async fn queue_timeout_reselects_without_queueing_when_external_capacity_returns() {
    let fixture = Fixture::new(Capacity {
        busy_reads: 1,
        ..Default::default()
    })
    .await;
    fixture.catalog(true).await;
    let policy = policy()
        .with_queue(ConcurrencyQueuePolicy {
            max_waiting: 1,
            timeout: Duration::from_millis(10),
        })
        .with_switch_account_on_queue_failure(true);
    let stream = fixture
        .provider
        .clone()
        .execute(
            fixture.request(),
            fixture.context(fixture.scope(), policy, CancellationToken::new()),
        )
        .await
        .unwrap();
    assert!(fixture.capacity.reads.load(Ordering::SeqCst) >= 2);
    assert_eq!(fixture.capacity.acquisitions.load(Ordering::SeqCst), 1);
    drop(stream);
    assert_eq!(fixture.capacity.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unavailable_fallback_preserves_the_original_queue_timeout() {
    let fixture = Fixture::new(Capacity {
        busy_reads: usize::MAX,
        ..Default::default()
    })
    .await;
    fixture.catalog(true).await;
    let policy = policy()
        .with_queue(ConcurrencyQueuePolicy {
            max_waiting: 1,
            timeout: Duration::from_millis(10),
        })
        .with_switch_account_on_queue_failure(true);
    let error = fixture
        .provider
        .clone()
        .execute(
            fixture.request(),
            fixture.context(fixture.scope(), policy, CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ProviderErrorKind::ConcurrencyQueueTimeout);
    assert!(fixture.capacity.reads.load(Ordering::SeqCst) >= 2);
    assert_eq!(fixture.capacity.acquisitions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn changing_only_the_proxy_invalidates_cached_model_permissions() {
    let fixture = Fixture::new(Capacity::default()).await;
    fixture.catalog(true).await;
    assert_eq!(
        fixture
            .provider
            .query_model_capabilities()
            .await
            .unwrap()
            .len(),
        1
    );
    // 新代理返回拒绝，缓存不能继续借用旧出口的成功观测
    let proxy = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&proxy)
        .await;
    fixture.accounts.set_outbound_proxy(
        fixture.account.id(),
        Some(gateway_core::account::OutboundProxy::parse(&proxy.uri()).unwrap()),
    );
    assert!(fixture.provider.query_model_capabilities().await.is_err());
    assert_eq!(fixture.server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn model_cooldown_blocks_only_its_named_model() {
    use gateway_core::provider_ports::{
        ProviderCooldownPort, ProviderCooldownScope, ProviderScopedCooldown,
    };
    let fixture = Fixture::new(Capacity::default()).await;
    fixture.catalog(true).await;
    let cooldowns = Arc::new(crate::support::TestCooldown::default());
    let blocked_model =
        ProviderCooldownScope::upstream_model(UpstreamModelId::new("model").unwrap());
    cooldowns
        .put_scoped_if_later(ProviderScopedCooldown::new(
            fixture.account.id().clone(),
            fixture.account.revision(),
            blocked_model,
            SystemTime::now() + Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: fixture.server.uri(),
            upstream_transport: provider_bps::BpsUpstreamTransport::Sse,
            auth_base_url: fixture.server.uri(),
            quota_base_url: fixture.server.uri(),
            ..Default::default()
        },
        crate::support::ports_with_services(
            fixture.accounts.clone(),
            fixture.capacity.clone(),
            cooldowns,
        ),
    )
    .unwrap()
    .core_provider();
    let error = provider
        .clone()
        .execute(
            fixture.request(),
            fixture.context(fixture.scope(), policy(), CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ProviderErrorKind::RateLimited);
    assert!(error.retry_after().is_some());
    assert_eq!(fixture.capacity.acquisitions.load(Ordering::SeqCst), 0);

    // 另一模型没有这条作用域冷却，同一个账号仍可正常获得执行租约
    fixture.server.reset().await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"allowed":true,"model_catalog":{"models":[{"id":"model"}]}})),
        )
        .mount(&fixture.server)
        .await;
    let unrelated = Arc::new(crate::support::TestCooldown::default());
    unrelated
        .put_scoped_if_later(ProviderScopedCooldown::new(
            fixture.account.id().clone(),
            fixture.account.revision(),
            ProviderCooldownScope::upstream_model(UpstreamModelId::new("another-model").unwrap()),
            SystemTime::now() + Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: fixture.server.uri(),
            upstream_transport: provider_bps::BpsUpstreamTransport::Sse,
            auth_base_url: fixture.server.uri(),
            quota_base_url: fixture.server.uri(),
            ..Default::default()
        },
        crate::support::ports_with_services(
            fixture.accounts.clone(),
            fixture.capacity.clone(),
            unrelated,
        ),
    )
    .unwrap()
    .core_provider();
    let stream = provider
        .execute(
            fixture.request(),
            fixture.context(fixture.scope(), policy(), CancellationToken::new()),
        )
        .await
        .unwrap();
    assert_eq!(fixture.capacity.acquisitions.load(Ordering::SeqCst), 1);
    drop(stream);
}
