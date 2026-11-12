//! 从 BPS 账号访问目录编译模型能力，保留每个模型的来源账号

use crate::{BpsError, credential::Credential, network, provider::BpsProvider};
use gateway_core::{
    account::{CredentialRevision, ProviderAccount, ProviderAccountId},
    operation::{Feature, OperationKind},
    routing::{
        ModelCapabilities, ModelPresentation, ProviderModelCapabilities, SupportLevel,
        UpstreamModelId,
    },
};
use gateway_protocol::bps::BpsImagePolicy;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct CatalogEntry {
    pub(crate) revision: CredentialRevision,
    outbound_proxy: Option<gateway_core::account::OutboundProxy>,
    pub(crate) models: Vec<(String, String, Vec<String>)>,
    pub(crate) observed: Instant,
    pub(crate) images: BpsImagePolicy,
}

struct DiscoveredModel {
    label: String,
    efforts: Vec<String>,
    accounts: BTreeSet<ProviderAccountId>,
    images: BpsImagePolicy,
}

pub(crate) fn parse(document: &Value) -> Result<Vec<(String, String, Vec<String>)>, BpsError> {
    let allowed = document
        .get("allowed")
        .and_then(Value::as_bool)
        .ok_or(BpsError::Protocol)?;
    if !allowed {
        return Ok(Vec::new());
    }
    let catalog = document.get("model_catalog").ok_or(BpsError::Protocol)?;
    let models = catalog
        .get("models")
        .and_then(Value::as_array)
        .ok_or(BpsError::Protocol)?;
    let restricted = catalog.get("restricted_models").and_then(Value::as_array);
    let mut result = BTreeMap::new();
    for model in models {
        let Some(id) = model
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        if restricted.is_some_and(|items| items.iter().any(|value| value.as_str() == Some(id)))
            || ["available", "enabled", "model_picker_enabled"]
                .iter()
                .any(|field| model.get(*field).and_then(Value::as_bool) == Some(false))
            || matches!(
                model.pointer("/policy/state").and_then(Value::as_str),
                Some("disabled" | "unconfigured")
            )
        {
            continue;
        }
        let label = model
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or(id)
            .to_owned();
        let mut seen_efforts = BTreeSet::new();
        let efforts = model
            .get("efforts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|effort| effort.get("value").and_then(Value::as_str))
            .filter(|effort| matches!(*effort, "low" | "medium" | "high" | "xhigh"))
            .filter(|effort| seen_efforts.insert(*effort))
            .map(str::to_owned)
            .collect();
        result.insert(id.to_owned(), (label, efforts));
    }
    Ok(result
        .into_iter()
        .map(|(id, (name, efforts))| (id, name, efforts))
        .collect())
}

impl BpsProvider {
    pub(crate) async fn fetch_catalog(
        &self,
        credential: &Credential,
        proxy: Option<&gateway_core::account::OutboundProxy>,
    ) -> Result<Vec<(String, String, Vec<String>)>, BpsError> {
        let request = network::client(proxy)?
            .get(
                self.config
                    .endpoint("basispoints/api/responses/access?include_models=true")?,
            )
            .header(reqwest::header::ACCEPT, "application/json")
            .timeout(Duration::from_secs(30));
        let response = network::authenticate(request, credential)
            .send()
            .await
            .map_err(|_| BpsError::Unavailable)?;
        parse(&network::json(response).await?)
    }

    pub(crate) async fn account_catalog(
        &self,
        account: &ProviderAccount,
        refresh: bool,
    ) -> Result<CatalogEntry, BpsError> {
        if !refresh
            && let Some(entry) = self
                .catalogs
                .read()
                .expect("BPS catalog lock poisoned")
                .get(account.id())
            && entry.revision == account.revision()
            && entry.outbound_proxy.as_ref() == account.outbound_proxy()
            && entry.observed.elapsed() < Duration::from_secs(300)
        {
            return Ok(entry.clone());
        }
        let started = Instant::now();
        let loaded = self
            .ports
            .accounts()
            .load_credential(account.id(), account.revision())
            .await
            .map_err(|_| BpsError::Unavailable)?;
        let credential = Credential::parse(loaded.credential.expose_to_provider())?;
        let models = self
            .fetch_catalog(&credential, account.outbound_proxy())
            .await?;
        // 网络响应不能在账号删除或凭据轮换后重新发布旧权限
        let current = self
            .ports
            .accounts()
            .get_account(account.id())
            .await
            .map_err(|_| BpsError::Unavailable)?
            .ok_or(BpsError::Credential)?;
        if current.revision() != account.revision()
            || current.outbound_proxy() != account.outbound_proxy()
        {
            return Err(BpsError::Busy);
        }
        let entry = CatalogEntry {
            revision: account.revision(),
            outbound_proxy: account.outbound_proxy().cloned(),
            models,
            observed: started,
            images: credential.request_policy(&self.config).images,
        };
        let mut catalogs = self.catalogs.write().expect("BPS catalog lock poisoned");
        // 并发读取按请求开始顺序发布，迟到的旧响应不能覆盖已观察到的权限撤回
        if let Some(current) = catalogs.get(account.id())
            && current.revision == entry.revision
            && current.outbound_proxy == entry.outbound_proxy
            && current.observed > started
        {
            return Ok(current.clone());
        }
        let changed = catalogs.get(account.id()).is_none_or(|previous| {
            previous.models != entry.models
                || previous.revision != entry.revision
                || previous.outbound_proxy != entry.outbound_proxy
                || previous.images != entry.images
        });
        catalogs.insert(account.id().clone(), entry.clone());
        if changed {
            self.generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(entry)
    }

    pub(crate) async fn discover_models(&self) -> Result<Vec<ProviderModelCapabilities>, BpsError> {
        let accounts = self
            .ports
            .accounts()
            .list_for_provider(&self.kind)
            .await
            .map_err(|_| BpsError::Unavailable)?;
        // 删除和停用可能由其他进程完成，本机目录不能永久保留已退出账号的出口信息
        let live = accounts
            .iter()
            .map(ProviderAccount::id)
            .collect::<BTreeSet<_>>();
        self.catalogs
            .write()
            .expect("BPS catalog lock poisoned")
            .retain(|id, _| live.contains(id));
        let mut models: BTreeMap<String, DiscoveredModel> = BTreeMap::new();
        let mut failed = false;
        for account in &accounts {
            match self.account_catalog(account, false).await {
                Ok(catalog) => {
                    for (id, label, efforts) in catalog.models {
                        let entry = models.entry(id).or_insert_with(|| DiscoveredModel {
                            label,
                            efforts,
                            accounts: BTreeSet::new(),
                            images: BpsImagePolicy::Reject,
                        });
                        entry.accounts.insert(account.id().clone());
                        if catalog.images == BpsImagePolicy::Enabled
                            || entry.images == BpsImagePolicy::Reject
                        {
                            entry.images = catalog.images;
                        }
                    }
                }
                Err(_) => failed = true,
            }
        }
        // 部分读取失败属于未知目录，不能把缺失账号的模型当作权威撤回
        if failed {
            return Err(BpsError::Unavailable);
        }
        models
            .into_iter()
            .map(
                |(
                    id,
                    DiscoveredModel {
                        label,
                        efforts,
                        accounts,
                        images,
                    },
                )| {
                    let default_effort = efforts
                        .iter()
                        .find(|effort| effort.as_str() == "medium")
                        .or_else(|| efforts.first())
                        .cloned();
                    let model = UpstreamModelId::new(id).map_err(|_| BpsError::Protocol)?;
                    let mut capabilities =
                        ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None)
                            .with_feature(Feature::Tools, SupportLevel::Emulated)
                            .with_feature(Feature::Reasoning, SupportLevel::Native);
                    if images == BpsImagePolicy::Enabled {
                        capabilities =
                            capabilities.with_feature(Feature::Vision, SupportLevel::Native);
                    } else if images == BpsImagePolicy::Omit {
                        capabilities =
                            capabilities.with_feature(Feature::Vision, SupportLevel::Emulated);
                    }
                    Ok(ProviderModelCapabilities::new(model, capabilities)
                        .with_catalog_accounts(accounts)
                        .with_presentation(
                            ModelPresentation::new(Some(label), None)
                                .with_reasoning(default_effort, efforts)
                                .with_agent_tools(true, true),
                        ))
                },
            )
            .collect()
    }
}
