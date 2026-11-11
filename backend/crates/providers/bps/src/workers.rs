//! BPS 目录和到期凭据维护，由 Host 统一调度及取消

use crate::provider::BpsProvider;
use futures::{StreamExt, future::BoxFuture};
use gateway_core::{
    account::ProviderRefreshQuery,
    task::{
        ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerDefinitionError, WorkerId,
        WorkerKind, WorkerLeaseRequest, WorkerRegistration, WorkerRunnable, WorkerSchedule,
        WorkerTaskError,
    },
};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

const REFRESH_BATCH_SIZE: u32 = 100;

pub(crate) fn contributions(
    provider: Arc<BpsProvider>,
) -> Result<Vec<WorkerContribution>, WorkerDefinitionError> {
    let refresh_id = WorkerId::try_new(WorkerKind::OAuthRefresh, "bps")?;
    let catalog_id = WorkerId::try_new(WorkerKind::QuotaCatalogHealth, "bps")?;
    let quota_id = WorkerId::try_new(WorkerKind::QuotaCatalogHealth, "bps-quota")?;
    let quota_schedule = WorkerSchedule::try_new(
        Duration::from_secs(300),
        Duration::from_secs(1),
        Duration::from_secs(60),
        Duration::from_secs(900),
        Duration::from_secs(300),
    )?;
    let quota_lease = WorkerLeaseRequest::try_new(quota_id.clone(), Duration::from_secs(900))?;
    let schedule = WorkerSchedule::try_new(
        Duration::from_secs(30),
        Duration::from_secs(1),
        Duration::from_secs(60),
        Duration::from_secs(900),
        Duration::from_secs(300),
    )?;
    let lease = WorkerLeaseRequest::try_new(refresh_id.clone(), Duration::from_secs(900))?;
    Ok(vec![
        WorkerContribution::Registration(WorkerRegistration::try_new(
            refresh_id,
            WorkerRunnable::Scheduled {
                schedule,
                lease: Some(lease),
                task: Box::new(Refresh(provider.clone())),
            },
        )?),
        // 目录及其 generation 属于本机，不能被跨副本的 OAuth leader lease 阻止刷新
        WorkerContribution::Registration(WorkerRegistration::try_new(
            catalog_id,
            WorkerRunnable::Scheduled {
                schedule,
                lease: None,
                task: Box::new(Catalog(provider.clone())),
            },
        )?),
        WorkerContribution::Registration(WorkerRegistration::try_new(
            quota_id,
            WorkerRunnable::Scheduled {
                schedule: quota_schedule,
                lease: Some(quota_lease),
                task: Box::new(Quota(provider)),
            },
        )?),
    ])
}

struct Refresh(Arc<BpsProvider>);
impl ScheduledTask for Refresh {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let operation = async {
                let policy = self
                    .0
                    .ports
                    .runtime_policy()
                    .load_refresh_policy()
                    .await
                    .map_err(|_| WorkerTaskError::safe("BPS refresh policy unavailable"))?;
                let now = SystemTime::now();
                let mut scanned = Vec::new();
                let mut failed = false;
                loop {
                    if context.cancellation().is_cancelled() {
                        return Ok(());
                    }
                    let accounts = self
                        .0
                        .ports
                        .accounts()
                        .list_refresh_candidates(ProviderRefreshQuery::new(
                            self.0.kind.clone(),
                            now.checked_add(policy.staggered_refresh_bound())
                                .unwrap_or(now),
                            now,
                            now,
                            scanned.clone(),
                            NonZeroU32::new(REFRESH_BATCH_SIZE).expect("positive refresh batch"),
                        ))
                        .await
                        .map_err(|_| WorkerTaskError::safe("BPS accounts unavailable"))?;
                    let last_page = accounts.len() < REFRESH_BATCH_SIZE as usize;
                    for loaded in accounts {
                        // 跳过和失败的账号也必须推进分页，否则会永久占据最早的候选位置
                        scanned.push(loaded.account.id().clone());
                        if loaded
                            .credential
                            .expose_to_provider()
                            .get("refresh_attempted")
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)
                        {
                            continue;
                        }
                        if loaded
                            .account
                            .access_token_expires_at()
                            .is_some_and(|expires| {
                                policy.is_refresh_due_staggered(loaded.account.id(), expires, now)
                            })
                            && self
                                .0
                                .refresh_background(loaded.account.id())
                                .await
                                .is_err()
                        {
                            failed = true;
                        }
                    }
                    if last_page {
                        break;
                    }
                }
                if failed {
                    Err(WorkerTaskError::safe("BPS account refresh failed"))
                } else {
                    Ok(())
                }
            };
            tokio::select! {
                () = context.cancellation().cancelled() => Ok(()),
                result = operation => result,
            }
        })
    }
}

struct Catalog(Arc<BpsProvider>);
impl ScheduledTask for Catalog {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            tokio::select! {
                () = context.cancellation().cancelled() => Ok(()),
                result = self.0.discover_models() => result
                    .map(|_| ())
                    .map_err(|_| WorkerTaskError::safe("BPS catalog maintenance failed")),
            }
        })
    }
}

struct Quota(Arc<BpsProvider>);
impl ScheduledTask for Quota {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let operation = async {
                let accounts = self
                    .0
                    .ports
                    .accounts()
                    .list_for_provider(&self.0.kind)
                    .await
                    .map_err(|_| WorkerTaskError::safe("BPS quota accounts unavailable"))?;
                // 现有账号端口返回完整候选；限制出站并发，不用快照新鲜度跳过周期查询
                let mut refreshes = futures::stream::iter(
                    accounts
                        .into_iter()
                        .filter(|account| {
                            account.enabled() && account.authentication_kind() == "oauth"
                        })
                        .map(|account| async move { self.0.refresh_quota(&account).await }),
                )
                .buffer_unordered(4);
                let mut failed = false;
                while let Some(result) = refreshes.next().await {
                    failed |= result.is_err();
                }
                if failed {
                    Err(WorkerTaskError::safe("BPS quota maintenance failed"))
                } else {
                    Ok(())
                }
            };
            tokio::select! {
                biased;
                () = context.cancellation().cancelled() => Ok(()),
                result = operation => result,
            }
        })
    }
}
