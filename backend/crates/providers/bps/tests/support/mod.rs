//! BPS 集成测试使用的内存端口，不执行外部账号操作
use async_trait::async_trait;
use futures::future::BoxFuture;
use gateway_core::error::{StoreError, StoreErrorKind};
use gateway_core::{account::*, provider_ports::*, routing::ProviderKind};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

struct StoredAccount {
    account: ProviderAccount,
    credential: PlaintextCredential,
    quota: Option<QuotaObservation>,
}

#[derive(Default)]
pub struct MemoryProviderAccountStore {
    accounts: Mutex<BTreeMap<ProviderAccountId, StoredAccount>>,
    quota_reads: AtomicUsize,
    fail_provider_listing: AtomicBool,
}

#[async_trait]
impl ProviderAccountStore for MemoryProviderAccountStore {
    async fn load_account_catalogs(
        &self,
        _: &gateway_core::routing::ProviderKind,
    ) -> Result<Vec<gateway_core::account::StoredAccountCatalog>, gateway_core::error::StoreError>
    {
        Ok(Vec::new())
    }
    async fn save_account_catalog(
        &self,
        _: &gateway_core::account::AccountCatalogObservation,
        _: &[u8],
    ) -> Result<bool, gateway_core::error::StoreError> {
        unreachable!("fixture does not discover account catalogs")
    }

    async fn create_account(&self, account: NewProviderAccount) -> Result<(), StoreError> {
        let mut accounts = lock(&self.accounts);
        if accounts.contains_key(account.account.id()) {
            return Err(conflict());
        }
        accounts.insert(
            account.account.id().clone(),
            StoredAccount {
                account: account.account,
                credential: account.credential,
                quota: None,
            },
        );
        Ok(())
    }

    async fn get_account(
        &self,
        account: &ProviderAccountId,
    ) -> Result<Option<ProviderAccount>, StoreError> {
        Ok(lock(&self.accounts)
            .get(account)
            .map(|stored| stored.account.clone()))
    }

    async fn list_accounts(&self) -> Result<Vec<ProviderAccount>, StoreError> {
        Ok(lock(&self.accounts)
            .values()
            .map(|stored| stored.account.clone())
            .collect())
    }

    async fn list_for_provider(
        &self,
        provider: &ProviderKind,
    ) -> Result<Vec<ProviderAccount>, StoreError> {
        if self.fail_provider_listing.load(Ordering::SeqCst) {
            return Err(invalid());
        }
        // 与 Postgres 实现的调度列表语义一致：停用账号不进入常规候选
        Ok(lock(&self.accounts)
            .values()
            .filter(|stored| stored.account.provider() == provider && stored.account.enabled())
            .map(|stored| stored.account.clone())
            .collect())
    }

    async fn list_for_provider_accounts(
        &self,
        provider: &ProviderKind,
        account_ids: &[ProviderAccountId],
    ) -> Result<Vec<ProviderAccount>, StoreError> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }
        if self.fail_provider_listing.load(Ordering::SeqCst) {
            return Err(invalid());
        }
        // 与 Postgres 实现的调度列表语义一致：停用账号不进入常规候选
        Ok(lock(&self.accounts)
            .values()
            .filter(|stored| stored.account.provider() == provider && stored.account.enabled())
            .filter(|stored| account_ids.contains(stored.account.id()))
            .map(|stored| stored.account.clone())
            .collect())
    }

    async fn list_refresh_candidates(
        &self,
        query: ProviderRefreshQuery,
    ) -> Result<Vec<LoadedCredential>, StoreError> {
        let mut candidates = lock(&self.accounts)
            .values()
            .filter(|stored| query.contains(&stored.account))
            .map(|stored| LoadedCredential {
                account: stored.account.clone(),
                credential: stored.credential.clone(),
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|loaded| {
            (
                loaded.account.access_token_expires_at(),
                loaded.account.id().clone(),
            )
        });
        candidates.truncate(query.limit().get() as usize);
        Ok(candidates)
    }

    async fn load_credential(
        &self,
        account: &ProviderAccountId,
        expected_revision: CredentialRevision,
    ) -> Result<LoadedCredential, StoreError> {
        let loaded = self.load_current_credential(account).await?;
        if loaded.account.revision() != expected_revision {
            return Err(conflict());
        }
        Ok(loaded)
    }

    async fn load_current_credential(
        &self,
        account: &ProviderAccountId,
    ) -> Result<LoadedCredential, StoreError> {
        let accounts = lock(&self.accounts);
        let stored = accounts.get(account).ok_or_else(invalid)?;
        Ok(LoadedCredential {
            account: stored.account.clone(),
            credential: stored.credential.clone(),
        })
    }

    async fn compare_and_swap_credential(
        &self,
        update: CredentialCasUpdate,
    ) -> Result<CredentialCasOutcome, StoreError> {
        let CredentialCasUpdateParts {
            account_id,
            expected_revision,
            profile,
            preserve_profile,
            credential,
            has_refresh_token,
            access_token_expires_at,
            next_refresh_at,
            account_state,
        } = update.into_parts();
        let mut accounts = lock(&self.accounts);
        let stored = accounts.get_mut(&account_id).ok_or_else(invalid)?;
        if stored.account.revision() != expected_revision {
            return Ok(CredentialCasOutcome::Conflict);
        }
        let next = expected_revision.next().map_err(|_| invalid())?;
        let credential_state = account_state
            .as_ref()
            .map_or(stored.account.credential_state(), |state| {
                state.credential_state
            });
        let last_error_reason = account_state.as_ref().map_or_else(
            || stored.account.last_error_reason(),
            |state| state.error_reason,
        );
        let last_error_message = account_state.as_ref().map_or_else(
            || stored.account.last_error_message().map(str::to_owned),
            |state| state.message.clone(),
        );
        stored.account = rebuild_account(
            &stored.account,
            AccountReplacement {
                revision: next,
                access_token_expires_at,
                credential_state,
                quota: stored.account.quota(),
                last_error_reason,
                last_error_message,
                enabled: stored.account.enabled(),
                has_refresh_token,
                next_refresh_at,
                name: if preserve_profile {
                    stored.account.name().to_owned()
                } else {
                    profile.name
                },
                email: if preserve_profile {
                    stored.account.email().map(str::to_owned)
                } else {
                    profile.email
                },
                plan_type: if preserve_profile {
                    stored.account.plan_type().map(str::to_owned)
                } else {
                    profile.plan_type
                },
            },
        );
        stored.credential = credential;
        if preserve_profile {
            if let Some(quota) = &mut stored.quota {
                quota.expected_revision = next;
            }
        } else {
            stored.quota = None;
        }
        Ok(CredentialCasOutcome::Updated(next))
    }

    async fn get_quotas(
        &self,
        accounts: &[ProviderAccountId],
    ) -> Result<Vec<QuotaObservation>, StoreError> {
        self.quota_reads.fetch_add(1, Ordering::SeqCst);
        let stored = lock(&self.accounts);
        Ok(accounts
            .iter()
            .filter_map(|account| stored.get(account)?.quota.clone())
            .collect())
    }

    async fn compare_and_swap_quota(
        &self,
        observation: QuotaObservation,
    ) -> Result<QuotaWriteOutcome, StoreError> {
        let mut accounts = lock(&self.accounts);
        let stored = accounts
            .get_mut(&observation.account_id)
            .ok_or_else(invalid)?;
        if stored.account.revision() != observation.expected_revision {
            return Ok(QuotaWriteOutcome::Conflict);
        }
        if observation
            .expected_connection
            .as_ref()
            .is_some_and(|expected| {
                !stored.account.enabled()
                    || UpstreamConnection::for_account(&stored.account) != *expected
            })
            || stored
                .quota
                .as_ref()
                .is_some_and(|old| old.observed_at > observation.observed_at)
            || observation.quota_pause.as_ref().is_some_and(|decision| {
                decision.limits != *stored.account.quota_limits()
                    || decision.merge_base.is_some_and(|base| {
                        base.observed_at != stored.quota.as_ref().map(|old| old.observed_at)
                    })
            })
        {
            return Ok(QuotaWriteOutcome::Conflict);
        }
        let quota = if observation.state.observed_at() >= stored.account.quota().observed_at() {
            observation.state
        } else {
            stored.account.quota()
        };
        let plan = observation.plan_type.clone();
        let paused = observation
            .quota_pause
            .as_ref()
            .is_some_and(|decision| decision.paused);
        stored.quota = Some(observation);
        stored.account = rebuild_account(&stored.account, {
            let mut replacement = AccountReplacement::preserving(&stored.account).with_quota(quota);
            if plan.is_some() {
                replacement.plan_type = plan;
            }
            replacement
        })
        .with_quota_limits(stored.account.quota_limits().clone(), paused);
        Ok(QuotaWriteOutcome::Updated)
    }

    async fn touch_quota_observation(
        &self,
        touch: QuotaObservationTouch,
    ) -> Result<QuotaWriteOutcome, StoreError> {
        let mut accounts = lock(&self.accounts);
        let stored = accounts.get_mut(&touch.account_id).ok_or_else(invalid)?;
        if stored.account.revision() != touch.expected_revision {
            return Ok(QuotaWriteOutcome::Conflict);
        }
        let Some(quota) = &mut stored.quota else {
            return Ok(QuotaWriteOutcome::Conflict);
        };
        if quota.observed_at > touch.observed_at {
            return Ok(QuotaWriteOutcome::Conflict);
        }
        quota.observed_at = touch.observed_at;
        Ok(QuotaWriteOutcome::Updated)
    }

    async fn apply_quota_access(
        &self,
        change: QuotaAccessChange,
    ) -> Result<QuotaWriteOutcome, StoreError> {
        let mut accounts = lock(&self.accounts);
        let stored = accounts.get_mut(&change.account_id).ok_or_else(invalid)?;
        if stored.account.revision() != change.expected_revision {
            return Ok(QuotaWriteOutcome::Conflict);
        }
        stored.account = rebuild_account(
            &stored.account,
            AccountReplacement::preserving(&stored.account).with_quota(change.state),
        );
        if let Some(observation) = &mut stored.quota {
            observation.state = change.state;
        }
        Ok(QuotaWriteOutcome::Updated)
    }

    async fn apply_state_change(&self, change: AccountStateChange) -> Result<(), StoreError> {
        let mut accounts = lock(&self.accounts);
        let stored = accounts.get_mut(&change.account_id).ok_or_else(invalid)?;
        if stored.account.revision() != change.expected_revision {
            return Err(conflict());
        }
        stored.account = rebuild_account(
            &stored.account,
            AccountReplacement {
                revision: stored.account.revision(),
                access_token_expires_at: stored.account.access_token_expires_at(),
                credential_state: change.credential_state,
                quota: stored.account.quota(),
                last_error_reason: change.error_reason,
                last_error_message: change.message.clone(),
                enabled: stored.account.enabled(),
                has_refresh_token: stored.account.has_refresh_token(),
                next_refresh_at: stored.account.next_refresh_at(),
                name: stored.account.name().to_owned(),
                email: stored.account.email().map(str::to_owned),
                plan_type: stored.account.plan_type().map(str::to_owned),
            },
        );
        Ok(())
    }

    async fn update_account(&self, update: ProviderAccountUpdate) -> Result<(), StoreError> {
        let mut accounts = lock(&self.accounts);
        let stored = accounts.get_mut(&update.account_id).ok_or_else(invalid)?;
        stored.account = rebuild_account(
            &stored.account,
            AccountReplacement {
                revision: stored.account.revision(),
                access_token_expires_at: stored.account.access_token_expires_at(),
                credential_state: stored.account.credential_state(),
                quota: stored.account.quota(),
                last_error_reason: stored.account.last_error_reason(),
                last_error_message: stored.account.last_error_message().map(str::to_owned),
                enabled: stored.account.enabled(),
                has_refresh_token: stored.account.has_refresh_token(),
                next_refresh_at: stored.account.next_refresh_at(),
                name: update.name,
                email: update.email,
                plan_type: update.plan_type,
            },
        );
        Ok(())
    }

    async fn set_enabled(
        &self,
        account: &ProviderAccountId,
        enabled: bool,
    ) -> Result<(), StoreError> {
        let mut accounts = lock(&self.accounts);
        let stored = accounts.get_mut(account).ok_or_else(invalid)?;
        stored.account = rebuild_account(
            &stored.account,
            AccountReplacement {
                revision: stored.account.revision(),
                access_token_expires_at: stored.account.access_token_expires_at(),
                credential_state: stored.account.credential_state(),
                quota: stored.account.quota(),
                last_error_reason: stored.account.last_error_reason(),
                last_error_message: stored.account.last_error_message().map(str::to_owned),
                enabled,
                has_refresh_token: stored.account.has_refresh_token(),
                next_refresh_at: stored.account.next_refresh_at(),
                name: stored.account.name().to_owned(),
                email: stored.account.email().map(str::to_owned),
                plan_type: stored.account.plan_type().map(str::to_owned),
            },
        );
        Ok(())
    }

    async fn delete_account(&self, account: &ProviderAccountId) -> Result<(), StoreError> {
        lock(&self.accounts).remove(account).ok_or_else(invalid)?;
        Ok(())
    }
}

struct AccountReplacement {
    revision: CredentialRevision,
    access_token_expires_at: Option<SystemTime>,
    credential_state: CredentialState,
    quota: QuotaState,
    last_error_reason: Option<AccountErrorReason>,
    last_error_message: Option<String>,
    enabled: bool,
    has_refresh_token: bool,
    next_refresh_at: Option<SystemTime>,
    name: String,
    email: Option<String>,
    plan_type: Option<String>,
}

impl AccountReplacement {
    fn preserving(account: &ProviderAccount) -> Self {
        Self {
            revision: account.revision(),
            access_token_expires_at: account.access_token_expires_at(),
            credential_state: account.credential_state(),
            quota: account.quota(),
            last_error_reason: account.last_error_reason(),
            last_error_message: account.last_error_message().map(str::to_owned),
            enabled: account.enabled(),
            has_refresh_token: account.has_refresh_token(),
            next_refresh_at: account.next_refresh_at(),
            name: account.name().to_owned(),
            email: account.email().map(str::to_owned),
            plan_type: account.plan_type().map(str::to_owned),
        }
    }

    fn with_quota(mut self, quota: QuotaState) -> Self {
        self.quota = quota;
        self
    }
}

fn rebuild_account(previous: &ProviderAccount, replacement: AccountReplacement) -> ProviderAccount {
    ProviderAccount::new(
        previous.id().clone(),
        previous.provider().clone(),
        replacement.name,
        previous.upstream_user_id().map(str::to_owned),
        previous.authentication_kind().to_owned(),
        replacement.revision,
        replacement.access_token_expires_at,
    )
    .with_profile(
        replacement.email,
        previous.upstream_account_id().map(str::to_owned),
        replacement.plan_type,
    )
    .with_account_facts(
        replacement.enabled,
        replacement.credential_state,
        replacement.quota,
        replacement.last_error_reason,
        replacement.last_error_message,
    )
    .with_scheduling(previous.concurrency_limit(), previous.weight())
    .with_refresh_schedule(replacement.has_refresh_token, replacement.next_refresh_at)
    .with_outbound_proxy(previous.outbound_proxy().cloned())
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn conflict() -> StoreError {
    StoreError::new(StoreErrorKind::Conflict)
}

fn invalid() -> StoreError {
    StoreError::new(StoreErrorKind::InvalidData)
}

struct TestLeases;

impl ProviderLeasePort for TestLeases {
    fn load_state<'a>(
        &'a self,
        _: &'a gateway_core::policy::ClientApiKeyId,
        _: &'a gateway_core::routing::ProviderKind,
        accounts: &'a [ProviderAccountId],
        _pool: gateway_core::provider_ports::ProviderConcurrencyPool,
    ) -> BoxFuture<
        'a,
        Result<gateway_core::provider_ports::ProviderSchedulingState, ProviderStoreError>,
    > {
        Box::pin(async move {
            let signals = accounts
                .iter()
                .cloned()
                .map(|account| {
                    (
                        account,
                        AccountRuntimeSignals {
                            in_flight: 0,
                            last_started_at: None,
                            quota_reset_at: None,
                            quota_remaining_rank: None,
                            cooldown: None,
                            failure_rate_basis_points: None,
                            first_output_latency_ms: None,
                        },
                    )
                })
                .collect();
            Ok(gateway_core::provider_ports::ProviderSchedulingState::new(
                signals, 0,
            ))
        })
    }

    fn try_acquire(
        &self,
        _request: ProviderLeaseRequest,
    ) -> BoxFuture<'_, Result<ProviderLeaseAcquisition, ProviderStoreError>> {
        Box::pin(async { Ok(ProviderLeaseAcquisition::Acquired(Box::new(()))) })
    }
}

struct TestArtifactProfiles;

impl ProviderArtifactProfileCachePort for TestArtifactProfiles {
    fn replace_if_newer(
        &self,
        _profile: ProviderArtifactProfile,
        _ttl: Duration,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(true) })
    }

    fn read<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _artifact_key: &'a str,
    ) -> BoxFuture<'a, Result<Option<ProviderArtifactProfile>, ProviderStoreError>> {
        Box::pin(async { Ok(None) })
    }
}

struct TestCredentialState;

impl ProviderCredentialStatePort for TestCredentialState {
    fn replace(
        &self,
        _state: ProviderCredentialState,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn read<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<ProviderCredentialState>, ProviderStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn clear<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(false) })
    }

    fn record_refresh_backoff<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _window: Duration,
    ) -> BoxFuture<'a, Result<u32, ProviderStoreError>> {
        Box::pin(async { Ok(1) })
    }

    fn clear_refresh_backoff<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
pub struct TestCooldown(
    Mutex<BTreeMap<ProviderAccountId, ProviderCooldown>>,
    Mutex<BTreeMap<(ProviderAccountId, ProviderCooldownScope), ProviderScopedCooldown>>,
);

impl ProviderCooldownPort for TestCooldown {
    fn read_many<'a>(
        &'a self,
        account_ids: &'a [ProviderAccountId],
    ) -> BoxFuture<'a, Vec<Result<Option<ProviderCooldown>, ProviderStoreError>>> {
        Box::pin(async move {
            account_ids
                .iter()
                .map(|id| Ok(lock(&self.0).get(id).cloned()))
                .collect()
        })
    }

    fn read_scoped_many<'a>(
        &'a self,
        account_ids: &'a [ProviderAccountId],
        scope: &'a ProviderCooldownScope,
    ) -> BoxFuture<'a, Vec<Result<Option<ProviderScopedCooldown>, ProviderStoreError>>> {
        Box::pin(async move {
            let entries = lock(&self.1);
            account_ids
                .iter()
                .map(|id| Ok(entries.get(&(id.clone(), scope.clone())).cloned()))
                .collect()
        })
    }

    fn put_if_later(
        &self,
        cooldown: ProviderCooldown,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let mut entries = lock(&self.0);
            if entries
                .get(cooldown.account_id())
                .is_some_and(|old| old.until() >= cooldown.until())
            {
                return Ok(false);
            }
            entries.insert(cooldown.account_id().clone(), cooldown);
            Ok(true)
        })
    }

    fn read<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<ProviderCooldown>, ProviderStoreError>> {
        Box::pin(async move { Ok(lock(&self.0).get(account_id).cloned()) })
    }

    fn clear<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(false) })
    }

    fn put_scoped_if_later(
        &self,
        cooldown: ProviderScopedCooldown,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let mut entries = lock(&self.1);
            let key = (cooldown.account_id().clone(), cooldown.scope().clone());
            if entries
                .get(&key)
                .is_some_and(|old| old.until() >= cooldown.until())
            {
                return Ok(false);
            }
            entries.insert(key, cooldown);
            Ok(true)
        })
    }

    fn read_scoped<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        scope: &'a ProviderCooldownScope,
    ) -> BoxFuture<'a, Result<Option<ProviderScopedCooldown>, ProviderStoreError>> {
        Box::pin(async move {
            Ok(lock(&self.1)
                .get(&(account_id.clone(), scope.clone()))
                .cloned())
        })
    }

    fn clear_scoped<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _scope: &'a ProviderCooldownScope,
        _through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(false) })
    }

    fn clear_all<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(false) })
    }

    fn clear_after_success<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _through_revision: gateway_core::account::CredentialRevision,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Ok(()) })
    }
}

struct TestRuntimePolicy;

impl ProviderRuntimePolicyPort for TestRuntimePolicy {
    fn load_refresh_policy(
        &self,
    ) -> BoxFuture<'_, Result<ProviderRefreshPolicy, ProviderStoreError>> {
        Box::pin(async {
            ProviderRefreshPolicy::try_new(
                Duration::from_secs(300),
                NonZeroU32::new(4).expect("nonzero concurrency"),
            )
        })
    }
}

#[derive(Default)]
struct TestCatalogCache {
    values: Mutex<BTreeMap<String, OpaqueProviderData>>,
}

impl ProviderCatalogCachePort for TestCatalogCache {
    fn replace<'a>(
        &'a self,
        key: &'a ProviderCatalogCacheKey,
        catalog: &'a OpaqueProviderData,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            self.values
                .lock()
                .expect("catalog cache")
                .insert(key.scope().as_str().to_owned(), catalog.clone());
            Ok(())
        })
    }

    fn read<'a>(
        &'a self,
        key: &'a ProviderCatalogCacheKey,
    ) -> BoxFuture<'a, Result<Option<OpaqueProviderData>, ProviderStoreError>> {
        Box::pin(async move {
            Ok(self
                .values
                .lock()
                .expect("catalog cache")
                .get(key.scope().as_str())
                .cloned())
        })
    }
}

#[derive(Default)]
struct TestOAuthPending {
    values: Mutex<BTreeMap<PendingKey, PendingValue>>,
}

type PendingKey = (String, String);
type PendingValue = (String, OpaqueProviderData, SystemTime, Option<String>);

impl OAuthPendingFlowPort for TestOAuthPending {
    fn put_if_absent(
        &self,
        flow: NewOAuthPendingFlow,
    ) -> BoxFuture<'_, Result<OAuthPendingPutOutcome, ProviderStoreError>> {
        Box::pin(async move {
            let key = (
                flow.provider_kind().as_str().to_owned(),
                flow.flow().expose_to_store().to_owned(),
            );
            let mut values = self.values.lock().expect("OAuth pending");
            if values.contains_key(&key) {
                return Ok(OAuthPendingPutOutcome::AlreadyExists);
            }
            values.insert(
                key,
                (
                    flow.owner().expose_to_store().to_owned(),
                    flow.payload().clone(),
                    SystemTime::now() + flow.ttl(),
                    None,
                ),
            );
            Ok(OAuthPendingPutOutcome::Stored)
        })
    }

    fn claim_if_owner<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
        _claim_ttl: Duration,
    ) -> BoxFuture<'a, Result<OAuthPendingClaimOutcome, ProviderStoreError>> {
        Box::pin(async move {
            let key = (
                provider_kind.as_str().to_owned(),
                flow.expose_to_store().to_owned(),
            );
            let mut values = self.values.lock().expect("OAuth pending");
            let Some((stored_owner, payload, expires_at, stored_claim)) = values.get_mut(&key)
            else {
                return Ok(OAuthPendingClaimOutcome::NotFound);
            };
            if *expires_at <= SystemTime::now() {
                values.remove(&key);
                return Ok(OAuthPendingClaimOutcome::NotFound);
            }
            if stored_owner != owner.expose_to_store() {
                return Ok(OAuthPendingClaimOutcome::OwnerMismatch);
            }
            if stored_claim.is_some() {
                return Ok(OAuthPendingClaimOutcome::InProgress);
            }
            *stored_claim = Some(claim.expose_to_store().to_owned());
            Ok(OAuthPendingClaimOutcome::Claimed(payload.clone()))
        })
    }

    fn release_claim<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
    ) -> BoxFuture<'a, Result<OAuthPendingReleaseOutcome, ProviderStoreError>> {
        Box::pin(async move {
            let key = (
                provider_kind.as_str().to_owned(),
                flow.expose_to_store().to_owned(),
            );
            let mut values = self.values.lock().expect("OAuth pending");
            let Some((stored_owner, _, _, stored_claim)) = values.get_mut(&key) else {
                return Ok(OAuthPendingReleaseOutcome::NotFound);
            };
            if stored_owner != owner.expose_to_store() {
                return Ok(OAuthPendingReleaseOutcome::OwnerMismatch);
            }
            if stored_claim.as_deref() != Some(claim.expose_to_store()) {
                return Ok(OAuthPendingReleaseOutcome::ClaimMismatch);
            }
            *stored_claim = None;
            Ok(OAuthPendingReleaseOutcome::Released)
        })
    }

    fn consume_claim<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
    ) -> BoxFuture<'a, Result<OAuthPendingConsumeOutcome, ProviderStoreError>> {
        Box::pin(async move {
            let key = (
                provider_kind.as_str().to_owned(),
                flow.expose_to_store().to_owned(),
            );
            let mut values = self.values.lock().expect("OAuth pending");
            let Some((stored_owner, _, _, stored_claim)) = values.get(&key) else {
                return Ok(OAuthPendingConsumeOutcome::NotFound);
            };
            if stored_owner != owner.expose_to_store() {
                return Ok(OAuthPendingConsumeOutcome::OwnerMismatch);
            }
            if stored_claim.as_deref() != Some(claim.expose_to_store()) {
                return Ok(OAuthPendingConsumeOutcome::ClaimMismatch);
            }
            values.remove(&key);
            Ok(OAuthPendingConsumeOutcome::Consumed)
        })
    }
}

struct TestSessionAffinity;

impl ProviderSessionAffinityPort for TestSessionAffinity {
    fn load<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<gateway_core::provider_ports::ProviderSessionBinding>, ProviderStoreError>,
    > {
        Box::pin(async { Ok(None) })
    }
    fn round_robin_cursor<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a gateway_core::policy::ClientApiKeyId,
    ) -> futures::future::BoxFuture<'a, Result<u64, ProviderStoreError>> {
        panic!("xAI does not read session round robin cursors")
    }
    fn admit_account<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: gateway_core::provider_ports::ProviderAccountAdmission<'a>,
    ) -> futures::future::BoxFuture<'a, Result<bool, ProviderStoreError>> {
        panic!("xAI does not claim OpenAI sessions")
    }
    fn load_alias<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<gateway_core::provider_ports::ProviderSessionAlias>, ProviderStoreError>,
    > {
        Box::pin(async { Ok(None) })
    }
    fn bind_alias<'a>(
        &'a self,
        _: &'a ProviderKind,
        _: &'a ProviderSessionAffinityKey,
        _: &'a gateway_core::provider_ports::ProviderSessionAlias,
        _: Duration,
    ) -> futures::future::BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(true) })
    }
}

struct TestSessionExclusions;

impl ProviderSessionExclusionPort for TestSessionExclusions {
    fn load<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
    ) -> futures::future::BoxFuture<'a, Result<Option<ProviderSessionExclusions>, ProviderStoreError>>
    {
        Box::pin(async { Ok(None) })
    }

    fn record_failure<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
        _account_id: &'a ProviderAccountId,
        _ttl: Duration,
    ) -> futures::future::BoxFuture<'a, Result<ProviderSessionExclusions, ProviderStoreError>> {
        Box::pin(async {
            Ok(ProviderSessionExclusions::new(
                BTreeSet::new(),
                String::new(),
            ))
        })
    }

    fn clear<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
        _expected_revision: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(false) })
    }
}

#[derive(Default)]
struct RecordingDiagnostics;
#[async_trait]
impl gateway_core::diagnostics::OperationalDiagnostics for RecordingDiagnostics {
    async fn record_failure(
        &self,
        _failure: gateway_core::diagnostics::OperationalFailure,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
}

pub fn ports() -> ProviderStorePorts {
    ports_with(Arc::new(MemoryProviderAccountStore::default()))
}
pub fn ports_with(accounts: Arc<MemoryProviderAccountStore>) -> ProviderStorePorts {
    ports_with_leases(accounts, Arc::new(TestLeases))
}

pub fn ports_with_leases(
    accounts: Arc<MemoryProviderAccountStore>,
    leases: Arc<dyn ProviderLeasePort>,
) -> ProviderStorePorts {
    ports_with_services(accounts, leases, Arc::new(TestCooldown::default()))
}

pub fn ports_with_services(
    accounts: Arc<MemoryProviderAccountStore>,
    leases: Arc<dyn ProviderLeasePort>,
    cooldowns: Arc<dyn ProviderCooldownPort>,
) -> ProviderStorePorts {
    ProviderStorePorts::new(
        accounts,
        leases,
        Arc::new(TestSessionAffinity),
        Arc::new(TestSessionExclusions),
        Arc::new(TestCatalogCache::default()),
        Arc::new(TestArtifactProfiles),
        Arc::new(TestCredentialState),
        cooldowns,
        Arc::new(TestRuntimePolicy),
        Arc::new(TestOAuthPending::default()),
        Arc::new(RecordingDiagnostics),
    )
}

use chrono::{DateTime, Utc};
use gateway_admin::model::{Revision, accounts::AccountRecord};
pub fn account_record(account: &ProviderAccount) -> AccountRecord {
    let now = Utc::now();
    AccountRecord {
        quota_paused: false,
        quota_limits: Default::default(),
        auto_use_quota_credits: false,
        outbound_proxy_id: None,
        outbound_proxy_enabled: true,
        notes: None,
        model_access: Default::default(),
        outbound_proxy: None,
        id: account.id().to_string(),
        provider_kind: account.provider().clone(),
        groups: Vec::new(),
        name: account.name().to_owned(),
        email: account.email().map(str::to_owned),
        upstream_user_id: account.upstream_user_id().map(str::to_owned),
        upstream_account_id: account.upstream_account_id().map(str::to_owned),
        plan_type: account.plan_type().map(str::to_owned),
        authentication_kind: account.authentication_kind().to_owned(),
        credential_revision: Revision::new(account.revision().get()).expect("revision"),
        has_refresh_token: account.has_refresh_token(),
        access_token_expires_at: account.access_token_expires_at().map(DateTime::<Utc>::from),
        next_refresh_at: account.next_refresh_at().map(DateTime::<Utc>::from),
        enabled: account.enabled(),
        concurrency_limit: account.concurrency_limit(),
        weight: account.weight(),
        credential_state: account.credential_state(),
        credential_observed_at: now,
        quota: account.quota(),
        last_error_reason: account.last_error_reason(),
        last_error_message: account.last_error_message().map(str::to_owned),
        created_at: now,
        updated_at: now,
    }
}

impl MemoryProviderAccountStore {
    pub fn set_outbound_proxy(&self, id: &ProviderAccountId, proxy: Option<OutboundProxy>) {
        let mut accounts = lock(&self.accounts);
        let stored = accounts.get_mut(id).expect("fixture account exists");
        stored.account = stored.account.clone().with_outbound_proxy(proxy);
    }
}

/// 记录冷流占有的租约，供取消与 drop 回归验证资源结算
#[derive(Default)]
pub struct CountingLeases {
    pub active: Arc<AtomicUsize>,
}
struct CountedLease(Arc<AtomicUsize>);
impl Drop for CountedLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ProviderLeaseGuard for CountedLease {
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }
}
impl ProviderLeasePort for CountingLeases {
    fn load_state<'a>(
        &'a self,
        client: &'a gateway_core::policy::ClientApiKeyId,
        provider: &'a gateway_core::routing::ProviderKind,
        accounts: &'a [ProviderAccountId],
        pool: ProviderConcurrencyPool,
    ) -> BoxFuture<'a, Result<ProviderSchedulingState, ProviderStoreError>> {
        TestLeases.load_state(client, provider, accounts, pool)
    }
    fn try_acquire(
        &self,
        _: ProviderLeaseRequest,
    ) -> BoxFuture<'_, Result<ProviderLeaseAcquisition, ProviderStoreError>> {
        Box::pin(async move {
            self.active.fetch_add(1, Ordering::SeqCst);
            Ok(ProviderLeaseAcquisition::Acquired(Box::new(CountedLease(
                self.active.clone(),
            ))))
        })
    }
}
