//! 上游认证与限流失败通过已有端口回灌账号事实

use crate::provider::BpsProvider;
use gateway_core::{
    account::{AccountErrorReason, AccountStateChange, CredentialState, ProviderAccount},
    engine::AttemptContext,
    error::ProviderErrorKind,
    provider_ports::ProviderCooldown,
};
use std::time::{Duration, SystemTime};

impl BpsProvider {
    pub(crate) async fn record_failure(
        &self,
        account: &ProviderAccount,
        context: &AttemptContext,
        kind: ProviderErrorKind,
        retry_after: Option<Duration>,
    ) {
        if !context
            .account_eligibility()
            .allows_account_state_mutation(account.enabled())
        {
            return;
        }
        let now = SystemTime::now();
        let result = match kind {
            ProviderErrorKind::Unauthorized => self
                .ports
                .accounts()
                .apply_state_change(AccountStateChange {
                    account_id: account.id().clone(),
                    expected_revision: account.revision(),
                    credential_state: CredentialState::Expired,
                    observed_at: now,
                    error_reason: Some(AccountErrorReason::AccessTokenExpired),
                    message: None,
                })
                .await
                .map_err(|_| ()),
            ProviderErrorKind::RateLimited => {
                // 参考相邻 Provider 的短暂退避；429 不足以证明额度耗尽或凭据失效
                let delay = retry_after.unwrap_or(Duration::from_secs(120));
                let Some(until) = now.checked_add(delay) else {
                    return;
                };
                self.ports
                    .cooldowns()
                    .put_if_later(ProviderCooldown::new(
                        account.id().clone(),
                        account.revision(),
                        until,
                    ))
                    .await
                    .map(|_| ())
                    .map_err(|_| ())
            }
            _ => return,
        };
        if result.is_err() {
            // 反馈写入不能覆盖原始上游错误，也不能输出原始账号或凭据材料
            tracing::warn!(provider = "bps", "BPS account failure feedback unavailable");
        }
    }
}
