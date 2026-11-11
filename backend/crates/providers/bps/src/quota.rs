//! BPS 共享套餐额度的有界查询、版本合并与管理端投影

use crate::{BpsError, credential::Credential, network, provider::BpsProvider};
use chrono::{DateTime, Utc};
use gateway_admin::{
    model::provider_credentials::{
        ProviderQuota, ProviderQuotaCredits, ProviderQuotaReadRequest, ProviderQuotaWindow,
        ProviderQuotaWindowRole, QuotaLocalUsageAttribution,
    },
    ports::provider::ProviderAdminError,
};
use gateway_core::account::{
    OpaqueProviderData, ProviderAccount, ProviderAccountId, QuotaEvidence, QuotaMergeBase,
    QuotaObservation, QuotaPauseDecision, QuotaState, QuotaWriteOutcome, UpstreamConnection,
};
use gateway_protocol::openai::events::{parse_credits_from_object, parse_rate_limit_headers};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

impl BpsProvider {
    pub(crate) async fn refresh_quota(
        &self,
        account: &ProviderAccount,
    ) -> Result<ProviderQuota, BpsError> {
        let observed_at = SystemTime::now();
        let loaded = self
            .ports
            .accounts()
            .load_credential(account.id(), account.revision())
            .await
            .map_err(|_| BpsError::Unavailable)?;
        let credential = Credential::parse(loaded.credential.expose_to_provider())?;
        let response = network::client(account.outbound_proxy())?
            .get(self.config.quota_endpoint("backend-api/wham/usage")?)
            .bearer_auth(credential.access_token())
            .header("chatgpt-account-id", credential.account_id())
            .header("accept", "application/json")
            .header("openai-beta", "codex-1")
            .header("originator", "Codex Desktop")
            .timeout(Duration::from_secs(20))
            .send()
            .await
            .map_err(|_| BpsError::Unavailable)?;
        if !response.status().is_success() {
            return Err(BpsError::QuotaUpstream(response.status().as_u16()));
        }
        let raw = network::json(response).await?;
        if raw
            .get("account_id")
            .and_then(Value::as_str)
            .is_some_and(|id| id != credential.account_id())
        {
            return Err(BpsError::Protocol);
        }
        let mut document = decode_usage(&raw, observed_at)?;
        if document.get("plan_type").is_none()
            && let Some(plan) = credential.plan_type()
        {
            document.insert("plan_type".into(), plan.into());
        }
        self.merge_quota(account, document, observed_at).await
    }

    pub(crate) async fn read_quota(
        &self,
        account: &ProviderAccount,
    ) -> Result<ProviderQuota, BpsError> {
        let previous = self
            .ports
            .accounts()
            .get_quotas(&[account.id().clone()])
            .await
            .map_err(|_| BpsError::Unavailable)?
            .into_iter()
            .find(|value| value.expected_revision == account.revision());
        let mut result = previous
            .map(|value| project(value.quota.expose_to_provider(), Some(value.observed_at)))
            .unwrap_or_default();
        if result.plan_type.is_none() {
            result.plan_type = account.plan_type().map(str::to_owned);
        }
        Ok(result)
    }

    pub(crate) async fn read_account_quotas(
        &self,
        requests: &[ProviderQuotaReadRequest<'_>],
    ) -> Vec<Result<ProviderQuota, ProviderAdminError>> {
        let ids = requests
            .iter()
            .filter_map(|request| ProviderAccountId::new(&request.account.id).ok())
            .collect::<Vec<_>>();
        let observations = match self.ports.accounts().get_quotas(&ids).await {
            Ok(values) => values
                .into_iter()
                .map(|value| (value.account_id.clone(), value))
                .collect::<BTreeMap<_, _>>(),
            Err(_) => {
                return requests
                    .iter()
                    .map(|_| Err(BpsError::Unavailable.admin()))
                    .collect();
            }
        };
        requests
            .iter()
            .map(|request| {
                let account = request.account;
                let observation = ProviderAccountId::new(&account.id)
                    .ok()
                    .and_then(|id| observations.get(&id))
                    .filter(|value| {
                        value.expected_revision.get() == account.credential_revision.get()
                    });
                let mut result = observation
                    .map(|value| project(value.quota.expose_to_provider(), Some(value.observed_at)))
                    .unwrap_or_default();
                if result.plan_type.is_none() {
                    result.plan_type = account.plan_type.clone();
                }
                Ok(result)
            })
            .collect()
    }

    pub(crate) async fn observe_quota_headers(
        &self,
        account: &ProviderAccount,
        headers: &reqwest::header::HeaderMap,
        observed_at: SystemTime,
    ) -> Result<(), BpsError> {
        if !account.enabled() {
            return Ok(());
        }
        let pairs = headers
            .iter()
            .filter_map(|(key, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (key.to_string(), value.to_owned()))
            })
            .collect::<Vec<_>>();
        let Some(parsed) = parse_rate_limit_headers(&pairs) else {
            return Ok(());
        };
        let mut document = Map::new();
        if let Some(plan) = parsed
            .plan_type
            .filter(|plan| !plan.eq_ignore_ascii_case("unknown"))
        {
            document.insert("plan_type".into(), plan.into());
        }
        if let Some(credits) = parsed.credits {
            let mut value =
                json!({"has_credits":credits.has_credits,"unlimited":credits.unlimited});
            if let Some(balance) = credits.balance {
                value["balance"] = balance.into();
            }
            document.insert("credits".into(), value);
        }
        let mut limits = Map::new();
        for (id, details) in parsed.limits {
            let mut item = Map::new();
            if let Some(value) = details.allowed {
                item.insert("allowed".into(), value.into());
            }
            if let Some(value) = details.limit_reached {
                item.insert("limit_reached".into(), value.into());
            }
            for (role, window) in [
                ("primary_window", details.primary),
                ("secondary_window", details.secondary),
            ] {
                if let Some(window) =
                    window.filter(|window| (0.0..=100.0).contains(&window.used_percent))
                {
                    let mut value = Map::new();
                    value.insert("used_percent".into(), json!(window.used_percent));
                    if let Some(seconds) = window
                        .window_minutes
                        .and_then(|minutes| minutes.checked_mul(60))
                    {
                        value.insert("limit_window_seconds".into(), seconds.into());
                    }
                    if let Some(reset) = window.reset_at {
                        value.insert("reset_at".into(), reset.into());
                    }
                    item.insert(role.into(), value.into());
                }
            }
            limits.insert(id, Value::Object(item));
        }
        if !limits.is_empty() {
            document.insert("limits".into(), Value::Object(limits));
        }
        if document.is_empty() {
            return Ok(());
        }
        self.merge_quota(account, document, observed_at)
            .await
            .map(|_| ())
    }

    async fn merge_quota(
        &self,
        account: &ProviderAccount,
        incoming: Map<String, Value>,
        observed_at: SystemTime,
    ) -> Result<ProviderQuota, BpsError> {
        for _ in 0..3 {
            let previous = self
                .ports
                .accounts()
                .get_quotas(&[account.id().clone()])
                .await
                .map_err(|_| BpsError::Unavailable)?
                .into_iter()
                .find(|value| value.expected_revision == account.revision());
            if let Some(previous) = &previous
                && previous.observed_at > observed_at
            {
                return Ok(project(
                    previous.quota.expose_to_provider(),
                    Some(previous.observed_at),
                ));
            }
            let mut merged = previous
                .as_ref()
                .map(|value| value.quota.expose_to_provider().clone())
                .unwrap_or_default();
            merge_document(&mut merged, &incoming);
            let view = project(&merged, Some(observed_at));
            let paused = view
                .windows
                .iter()
                .filter(|window| window.limit_id.as_deref() == Some("codex"))
                .any(|window| {
                    account
                        .quota_limits()
                        .reached(&window.key, window.used_percent)
                });
            let known = view.windows.iter().any(|window| {
                window.limit_id.as_deref() == Some("codex") && window.used_percent.is_some()
            });
            let reset = view
                .windows
                .iter()
                .filter(|window| {
                    window.limit_id.as_deref() == Some("codex") && window.limit_reached
                })
                .filter_map(|window| window.reset_at)
                .max()
                .map(SystemTime::from);
            let old_state = previous
                .as_ref()
                .map(|value| value.state)
                .filter(|state| state.observed_at() >= account.quota().observed_at())
                .unwrap_or(account.quota());
            let fresh = project(&incoming, Some(observed_at));
            let state = if fresh.limit_reached {
                QuotaState::exhausted(QuotaEvidence::UsageLimitReached, observed_at, reset)
            } else if incoming
                .get("limits")
                .and_then(|limits| limits.get("codex"))
                .and_then(|limit| limit.get("allowed"))
                .and_then(Value::as_bool)
                == Some(true)
                && known
                && (!matches!(
                    old_state.access(),
                    gateway_core::account::QuotaAccessState::Exhausted
                ) || complete_allowed(&incoming))
            {
                QuotaState::allowed(observed_at)
            } else {
                old_state
            };
            let observation = QuotaObservation {
                account_id: account.id().clone(),
                expected_revision: account.revision(),
                expected_connection: Some(UpstreamConnection::for_account(account)),
                quota: OpaqueProviderData::new(merged),
                plan_type: view.plan_type.clone(),
                observed_at,
                state,
                quota_pause: Some(QuotaPauseDecision {
                    merge_base: Some(QuotaMergeBase {
                        observed_at: previous.as_ref().map(|value| value.observed_at),
                    }),
                    limits: account.quota_limits().clone(),
                    paused: paused
                        || (account.quota_paused()
                            && !account.quota_limits().keys().all(|key| {
                                view.windows.iter().any(|window| {
                                    window.limit_id.as_deref() == Some("codex")
                                        && window.key == key
                                        && window.used_percent.is_some()
                                })
                            })),
                }),
            };
            match self
                .ports
                .accounts()
                .compare_and_swap_quota(observation)
                .await
                .map_err(|_| BpsError::Unavailable)?
            {
                QuotaWriteOutcome::Updated => return Ok(view),
                QuotaWriteOutcome::Conflict => continue,
            }
        }
        Err(BpsError::Busy)
    }
}

fn decode_usage(raw: &Value, observed_at: SystemTime) -> Result<Map<String, Value>, BpsError> {
    let raw = raw.as_object().ok_or(BpsError::Protocol)?;
    let mut result = Map::new();
    if let Some(plan) = raw
        .get("plan_type")
        .and_then(Value::as_str)
        .filter(|plan| !plan.trim().is_empty() && !plan.eq_ignore_ascii_case("unknown"))
    {
        result.insert("plan_type".into(), plan.into());
    }
    if let Some(credits) = raw.get("credits").and_then(parse_credits_from_object) {
        result.insert("credits".into(),json!({"has_credits":credits.has_credits,"unlimited":credits.unlimited,"balance":credits.balance}));
    }
    let mut limits = Map::new();
    if let Some(limit) = raw.get("rate_limit").filter(|value| value.is_object()) {
        limits.insert("codex".into(), decode_limit(limit, observed_at));
    }
    if let Some(additional) = raw.get("additional_rate_limits").and_then(Value::as_array) {
        for item in additional {
            if let Some(id) = item
                .get("metered_feature")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                && let Some(limit) = item.get("rate_limit").filter(|value| value.is_object())
            {
                limits.insert(id.into(), decode_limit(limit, observed_at));
            }
        }
    }
    if !limits.is_empty() {
        result.insert("limits".into(), Value::Object(limits));
    }
    if result.is_empty() {
        return Err(BpsError::Protocol);
    }
    Ok(result)
}

fn decode_limit(raw: &Value, observed_at: SystemTime) -> Value {
    let mut result = Map::new();
    for key in ["allowed", "limit_reached"] {
        if let Some(value) = raw.get(key).and_then(Value::as_bool) {
            result.insert(key.into(), value.into());
        }
    }
    for role in ["primary_window", "secondary_window"] {
        if let Some(window) = raw.get(role).filter(|value| value.is_object()) {
            let mut normalized = Map::new();
            if let Some(used) = window
                .get("used_percent")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && (0.0..=100.0).contains(value))
            {
                normalized.insert("used_percent".into(), json!(used));
            }
            if let Some(seconds) = window.get("limit_window_seconds").and_then(Value::as_u64) {
                normalized.insert("limit_window_seconds".into(), seconds.into());
            }
            let reset = window
                .get("reset_at")
                .and_then(Value::as_i64)
                .filter(|value| *value > 0)
                .or_else(|| {
                    window
                        .get("reset_after_seconds")
                        .and_then(Value::as_u64)
                        .and_then(|seconds| observed_at.checked_add(Duration::from_secs(seconds)))
                        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
                });
            if let Some(reset) = reset {
                normalized.insert("reset_at".into(), reset.into());
            }
            result.insert(role.into(), normalized.into());
        }
    }
    result.into()
}

fn merge_document(target: &mut Map<String, Value>, incoming: &Map<String, Value>) {
    for (key, value) in incoming {
        if let Some(existing) = target.get_mut(key).and_then(Value::as_object_mut)
            && let Some(value) = value.as_object()
        {
            merge_document(existing, value);
        } else {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn project(document: &Map<String, Value>, observed_at: Option<SystemTime>) -> ProviderQuota {
    let mut quota = ProviderQuota {
        plan_type: document
            .get("plan_type")
            .and_then(Value::as_str)
            .map(str::to_owned),
        observed_at: observed_at.map(DateTime::<Utc>::from),
        ..ProviderQuota::default()
    };
    quota.credits = document
        .get("credits")
        .and_then(parse_credits_from_object)
        .map(|value| ProviderQuotaCredits {
            has_credits: value.has_credits,
            unlimited: value.unlimited,
            balance: value.balance,
        });
    if let Some(limits) = document.get("limits").and_then(Value::as_object) {
        for (id, limit) in limits {
            let reached = limit.get("limit_reached").and_then(Value::as_bool) == Some(true)
                || limit.get("allowed").and_then(Value::as_bool) == Some(false);
            if id == "codex" {
                quota.limit_reached |= reached;
            }
            for (role, window_role) in [
                ("primary_window", ProviderQuotaWindowRole::Primary),
                ("secondary_window", ProviderQuotaWindowRole::Secondary),
            ] {
                if let Some(window) = limit.get(role).and_then(Value::as_object) {
                    let seconds = window.get("limit_window_seconds").and_then(Value::as_u64);
                    let key = match seconds {
                        Some(18000) => format!("{id}_5h"),
                        Some(604800) => format!("{id}_7d"),
                        _ => format!("{id}_{role}"),
                    };
                    let used = window.get("used_percent").and_then(Value::as_f64);
                    quota.windows.push(ProviderQuotaWindow {
                        key,
                        group: id.clone(),
                        label: format!(
                            "{id} {}",
                            seconds.map_or_else(
                                || role.to_owned(),
                                |seconds| format!("{}h", seconds / 3600)
                            )
                        ),
                        limit_id: Some(id.clone()),
                        limit_name: None,
                        role: Some(window_role),
                        local_usage_attribution: QuotaLocalUsageAttribution::Unavailable,
                        window_seconds: seconds,
                        used_percent: used,
                        reset_at: window
                            .get("reset_at")
                            .and_then(Value::as_i64)
                            .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0)),
                        limit_reached: reached || used.is_some_and(|value| value >= 100.0),
                        local_usage: None,
                        provider_data: None,
                        upstream: None,
                    });
                }
            }
        }
    }
    quota.limit_reached |= quota
        .windows
        .iter()
        .any(|window| window.limit_id.as_deref() == Some("codex") && window.limit_reached);
    quota
}

// 恢复拒绝必须使用本次完整窗口，不得由合并遗留字段制造明确放行
fn complete_allowed(document: &Map<String, Value>) -> bool {
    let Some(limit) = document
        .get("limits")
        .and_then(|limits| limits.get("codex"))
    else {
        return false;
    };
    limit.get("allowed").and_then(Value::as_bool) == Some(true)
        && limit.get("limit_reached").and_then(Value::as_bool) == Some(false)
        && ["primary_window", "secondary_window"].iter().all(|role| {
            limit
                .get(role)
                .and_then(|window| window.get("used_percent"))
                .and_then(Value::as_f64)
                .is_some_and(|used| (0.0..100.0).contains(&used))
        })
}
