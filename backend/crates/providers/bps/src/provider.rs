//! BPS 账号选择与冷流执行，跨账号重试仍由 Core 裁决

use crate::{BpsConfig, catalog::CatalogEntry};
use async_trait::async_trait;
use gateway_core::concurrency::{
    CapacityWait, ConcurrencyQueuePolicy, ConcurrencyWaitQueue, QueueRejection,
};
use gateway_core::engine::policy::AccountPolicyError;
use gateway_core::{
    account::{
        AccountCandidate, AccountSelectionContext, AccountSelector, ProviderAccount,
        ProviderAccountId,
    },
    engine::{
        AttemptContext,
        provider::{Provider, ProviderRequest, ProviderSelectionObservation, ProviderStream},
    },
    error::{ProviderError, ProviderErrorKind},
    provider_ports::{
        ProviderConcurrencyPool, ProviderLeaseAcquisition, ProviderLeaseGuard,
        ProviderLeaseRequest, ProviderSchedulingLeaseRequest, ProviderStorePorts,
    },
    routing::{ProviderCatalogGeneration, ProviderKind, ProviderModelCapabilities},
    upstream::UpstreamSendState,
};
use gateway_protocol::bps::ToolReplay;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Instant, SystemTime};

struct SelectionFailure(ProviderError);

impl From<QueueRejection> for SelectionFailure {
    fn from(reason: QueueRejection) -> Self {
        Self(ProviderError::new(
            reason.provider_kind(),
            UpstreamSendState::NotSent,
        ))
    }
}

pub(crate) fn kind() -> ProviderKind {
    ProviderKind::new(super::config::PROVIDER).expect("BPS provider slug is valid")
}

pub(crate) struct ReplayEntry {
    pub(crate) scope: String,
    pub(crate) replay: ToolReplay,
    pub(crate) used: Instant,
    pub(crate) bytes: usize,
}

pub(crate) struct BpsProvider {
    pub(crate) kind: ProviderKind,
    pub(crate) config: BpsConfig,
    pub(crate) ports: ProviderStorePorts,
    pub(crate) catalogs: RwLock<BTreeMap<ProviderAccountId, CatalogEntry>>,
    pub(crate) generation: AtomicU64,
    pub(crate) replay: Mutex<Vec<ReplayEntry>>,
    waiting: ConcurrencyWaitQueue<ProviderAccountId>,
}

impl BpsProvider {
    pub(crate) fn new(config: BpsConfig, ports: ProviderStorePorts) -> Self {
        Self {
            kind: kind(),
            config,
            ports,
            catalogs: RwLock::new(BTreeMap::new()),
            generation: AtomicU64::new(1),
            replay: Mutex::new(Vec::new()),
            waiting: ConcurrencyWaitQueue::default(),
        }
    }

    async fn select(
        &self,
        model: &str,
        context: &AttemptContext,
    ) -> Result<
        (
            ProviderAccount,
            Box<dyn ProviderLeaseGuard>,
            ProviderSelectionObservation,
        ),
        ProviderError,
    > {
        let policy = context.account_selection_policy();
        let select = |queue| async move {
            self.select_with_queue(model, context, queue)
                .await
                .map_err(SelectionFailure)
        };
        let result = select(policy.queue_policy()).await;
        let rejection = match &result {
            Err(SelectionFailure(error)) if context.required_account().is_none() => {
                match error.kind() {
                    ProviderErrorKind::ConcurrencyQueueFull => Some(QueueRejection::Full),
                    ProviderErrorKind::ConcurrencyQueueTimeout => Some(QueueRejection::Timeout),
                    _ => None,
                }
            }
            _ => None,
        };
        if let Some(reason) = rejection {
            return policy
                .reselect_after_queue_rejection(
                    reason,
                    context.deadline().remaining(),
                    select,
                    |error| {
                        matches!(
                            error.0.kind(),
                            ProviderErrorKind::AccountCapacityUnavailable
                                | ProviderErrorKind::NoEligibleAccount
                                | ProviderErrorKind::RateLimited
                        )
                    },
                )
                .await
                .map_err(|error| error.0);
        }
        result.map_err(|error| error.0)
    }

    async fn select_with_queue(
        &self,
        model: &str,
        context: &AttemptContext,
        queue: ConcurrencyQueuePolicy,
    ) -> Result<
        (
            ProviderAccount,
            Box<dyn ProviderLeaseGuard>,
            ProviderSelectionObservation,
        ),
        ProviderError,
    > {
        let infrastructure = || {
            ProviderError::new(
                ProviderErrorKind::ProviderInfrastructureUnavailable,
                UpstreamSendState::NotSent,
            )
        };
        let started = Instant::now();
        let accounts = self.ports.accounts();
        let leases = self.ports.leases();
        let mut waiting = CapacityWait::new(
            &self.waiting,
            queue,
            context.deadline().at(),
            context.concurrency_wait_budget(),
        );
        'reload: loop {
            if context.cancellation().is_cancelled() {
                return Err(ProviderError::new(
                    ProviderErrorKind::Cancelled,
                    UpstreamSendState::NotSent,
                ));
            }
            let mut pool = accounts
                .list_for_provider(&self.kind)
                .await
                .map_err(|_| infrastructure())?;
            if context.is_diagnostic_required_account()
                && let Some(id) = context.required_account()
                && !pool.iter().any(|account| account.id() == id)
                && let Some(account) = accounts
                    .get_account(id)
                    .await
                    .map_err(|_| infrastructure())?
                && account.provider() == &self.kind
            {
                pool.push(account);
            }
            let mut eligible = Vec::new();
            let mut catalog_failed = false;
            for account in pool {
                if context
                    .required_account()
                    .is_some_and(|id| id != account.id())
                    || context.excluded_accounts().contains(account.id())
                {
                    continue;
                }
                if !context.is_diagnostic_required_account()
                    && context
                        .account_scope()
                        .is_none_or(|scope| !scope.allows_model(account.id(), model))
                {
                    continue;
                }
                if account.authentication_kind() != "oauth" {
                    continue;
                }
                if !context.is_diagnostic_required_account() {
                    match self.account_catalog(&account, false).await {
                        Ok(catalog) if catalog.models.iter().any(|(id, _, _)| id == model) => {}
                        Ok(_) => continue,
                        Err(_) => {
                            catalog_failed = true;
                            continue;
                        }
                    }
                }
                eligible.push(account);
            }
            if eligible.is_empty() && catalog_failed {
                return Err(infrastructure());
            }
            let ids = eligible
                .iter()
                .map(|account| account.id().clone())
                .collect::<Vec<_>>();
            let scheduling = leases
                .load_state(
                    context.client_api_key_ref(),
                    &self.kind,
                    &ids,
                    ProviderConcurrencyPool::Shared,
                )
                .await
                .map_err(|_| infrastructure())?;
            let cooldown_port = self.ports.cooldowns();
            let model_scope = gateway_core::provider_ports::ProviderCooldownScope::upstream_model(
                gateway_core::routing::UpstreamModelId::new(model).map_err(|_| infrastructure())?,
            );
            let (cooldowns, model_cooldowns) = futures::join!(
                cooldown_port.read_many(&ids),
                cooldown_port.read_scoped_many(&ids, &model_scope)
            );
            let now = SystemTime::now();
            let mut earliest_cooldown = None;
            let mut candidates = Vec::with_capacity(eligible.len());
            for ((account, cooldown), model_cooldown) in
                eligible.into_iter().zip(cooldowns).zip(model_cooldowns)
            {
                let health = self
                    .ports
                    .account_feedback()
                    .scheduling_signals(&self.kind, account.id());
                let cooldown = cooldown
                    .map_err(|_| infrastructure())?
                    .map(|value| value.scheduling_state())
                    .filter(|state| state.is_active(now));
                let model_cooldown = model_cooldown
                    .map_err(|_| infrastructure())?
                    .filter(|value| value.until() > now)
                    .map(|value| gateway_core::account::AccountCooldown {
                        until: value.until(),
                        kind: gateway_core::account::AccountCooldownKind::RateLimit,
                    });
                let cooldown = match (cooldown, model_cooldown) {
                    (Some(account), Some(model)) => Some(
                        if !matches!(
                            account.kind,
                            gateway_core::account::AccountCooldownKind::RateLimit
                        ) || account.until >= model.until
                        {
                            account
                        } else {
                            model
                        },
                    ),
                    (account, model) => account.or(model),
                };
                if let Some(cooldown) = cooldown {
                    earliest_cooldown = Some(
                        earliest_cooldown.map_or(cooldown.until, |until: SystemTime| {
                            until.min(cooldown.until)
                        }),
                    );
                }
                let signals = scheduling
                    .signals()
                    .get(account.id())
                    .cloned()
                    .ok_or_else(infrastructure)?
                    .with_runtime_health(health.0, health.1)
                    .with_rate_limit(cooldown);
                candidates.push(AccountCandidate {
                    account,
                    signals,
                    inherited_concurrency: None,
                });
            }
            let mut selection = AccountSelectionContext {
                policy: context.account_selection_policy(),
                now: SystemTime::now(),
                excluded_accounts: context.excluded_accounts().clone(),
                busy_accounts: BTreeSet::new(),
                preferred_account: context.required_account().cloned(),
                preferred_account_overrides_weight: false,
                round_robin_cursor: scheduling.round_robin_cursor(),
                eligibility: context.account_eligibility(),
                account_scope: context.account_scope().cloned(),
                reserved_concurrency: 0,
            };
            for candidate in &candidates {
                if !waiting.can_try(candidate.account.id()) {
                    selection
                        .busy_accounts
                        .insert(candidate.account.id().clone());
                }
            }
            loop {
                let chosen = match context.request_policy_context() {
                    Some(policy) => match policy
                        .select_account(
                            context.attempt_index(),
                            &self.kind,
                            Some(model),
                            &candidates,
                            &mut selection,
                        )
                        .await
                    {
                        Ok(chosen) => chosen,
                        Err(AccountPolicyError::StaleCandidate) => continue 'reload,
                        Err(AccountPolicyError::Fault) => return Err(infrastructure()),
                        Err(_) => {
                            return Err(ProviderError::new(
                                ProviderErrorKind::RequestPolicyDenied,
                                UpstreamSendState::NotSent,
                            )
                            .with_retry_prohibited());
                        }
                    },
                    None => AccountSelector.select(&candidates, &selection),
                };
                let Some(chosen) = chosen else {
                    break;
                };
                let candidate = chosen.candidate();
                let account = &candidate.account;
                let request = ProviderSchedulingLeaseRequest::new(
                    self.kind.clone(),
                    account.id().clone(),
                    account.revision(),
                    selection.concurrency_limit(candidate),
                    context.deadline(),
                )
                .with_metadata(context.lease_metadata())
                .with_cancellation(context.cancellation().clone());
                match leases
                    .try_acquire(ProviderLeaseRequest::Scheduling(request))
                    .await
                    .map_err(|_| infrastructure())?
                {
                    ProviderLeaseAcquisition::Acquired(lease) => {
                        let capacity = AccountSelector
                            .capacity_snapshot(&candidates, &selection)
                            .map(|capacity| capacity.with_acquired_request());
                        let timing =
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                        return Ok((
                            account.clone(),
                            lease,
                            ProviderSelectionObservation::new(timing, capacity),
                        ));
                    }
                    ProviderLeaseAcquisition::Busy { .. } => {
                        selection.busy_accounts.insert(account.id().clone());
                    }
                }
            }
            let waiting_for = AccountSelector.wait_candidates(&candidates, &selection);
            if !context.is_diagnostic_required_account()
                && queue.max_waiting > 0
                && !waiting_for.is_empty()
            {
                AccountSelector
                    .wait_for_capacity(&mut waiting, &waiting_for, &candidates, &selection)
                    .await
                    .map_err(|reason| {
                        ProviderError::new(
                            match reason {
                                QueueRejection::Full => ProviderErrorKind::ConcurrencyQueueFull,
                                QueueRejection::Timeout => {
                                    ProviderErrorKind::ConcurrencyQueueTimeout
                                }
                            },
                            UpstreamSendState::NotSent,
                        )
                    })?;
                continue;
            }
            let kind = if !waiting_for.is_empty() || !selection.busy_accounts.is_empty() {
                ProviderErrorKind::AccountCapacityUnavailable
            } else if earliest_cooldown.is_some() {
                ProviderErrorKind::RateLimited
            } else {
                ProviderErrorKind::NoEligibleAccount
            };
            let mut error = ProviderError::new(kind, UpstreamSendState::NotSent);
            if let Some(delay) =
                earliest_cooldown.and_then(|until| until.duration_since(SystemTime::now()).ok())
            {
                error = error.with_retry_after(delay);
            }
            return Err(error);
        }
    }
}

#[async_trait]
impl Provider for BpsProvider {
    fn name(&self) -> &str {
        super::config::PROVIDER
    }
    fn is_direct_account(&self, authentication_kind: &str) -> bool {
        authentication_kind == "oauth"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::new(self.generation.load(Ordering::Relaxed))
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        self.discover_models()
            .await
            .map_err(|error| error.execution(UpstreamSendState::NotSent))
    }
    async fn execute(
        self: Arc<Self>,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let model = request
            .candidate()
            .upstream_model()
            .ok_or_else(|| {
                ProviderError::new(ProviderErrorKind::Unsupported, UpstreamSendState::NotSent)
            })?
            .clone();
        let (account, lease, selection) = tokio::select! {
            biased;
            ()=context.cancellation().cancelled()=>return Err(ProviderError::new(ProviderErrorKind::Cancelled,UpstreamSendState::NotSent)),
            ()=context.deadline().wait()=>return Err(ProviderError::new(ProviderErrorKind::Timeout,UpstreamSendState::NotSent)),
            result=self.select(model.as_str(),&context)=>result?,
        };
        let account_id = account.id().clone();
        let terminal_context = context.clone();
        let requirements = request.operation().capability_requirements();
        context
            .execute_middleware(
                request.operation().clone(),
                self.kind.clone(),
                Some(model.as_str().into()),
                account_id,
                Box::new(move |operation, headers| {
                    Box::pin(async move {
                        if operation.capability_requirements() != requirements {
                            return Err(ProviderError::new(
                                ProviderErrorKind::InvalidRequest,
                                UpstreamSendState::NotSent,
                            )
                            .with_retry_prohibited());
                        }
                        self.open_stream(
                            operation,
                            model,
                            account,
                            lease,
                            terminal_context,
                            (headers, selection),
                        )
                        .await
                    })
                }),
            )
            .await
    }
}
