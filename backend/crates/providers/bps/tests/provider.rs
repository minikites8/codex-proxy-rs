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
        let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object(
                "openai",
                json!({"model":"model","input":"hello"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .unwrap(),
        ));
        let kind = ProviderKind::new("bps").unwrap();
        let snapshot = RuntimeSnapshot::new(
            ConfigRevision::new(1).unwrap(),
            gateway_core::settings::SettingsValues::new(2, "smart", Default::default(), None, None),
            vec![kind.clone()],
            vec![ProviderModel::new(
                kind,
                UpstreamModelId::new("model").unwrap(),
                ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None),
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
