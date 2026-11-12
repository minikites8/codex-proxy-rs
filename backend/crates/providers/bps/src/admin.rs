//! BPS 管理能力复用宿主账号事务、导入与连接测试合同

use crate::{
    BpsError,
    credential::{Credential, import_documents},
    provider::BpsProvider,
};
use async_trait::async_trait;
use chrono::Utc;
use gateway_admin::{
    model::{
        accounts::{AccountRecord, ProviderAccountCapabilities},
        observability::{CalculatedBillingBreakdown, DashboardWireProfile, ProviderBillingInput},
        provider_credentials::{
            AuthorizationStarted, CompleteAuthorization, PendingAuthorizationMutation,
            PrepareCredentialImport, PrepareCredentialRefresh, PrepareCredentialRotation,
            PreparedAuthorizationCommit, PreparedCredentialImport, PreparedCredentialRotation,
            ProviderDocument, ProviderExport, ProviderExportCredentialInput, ProviderModel,
            ProviderModels, ProviderQuota, ProviderQuotaReadRequest, ProviderQuotaRequest,
        },
    },
    ports::provider::{ProviderAdmin, ProviderAdminError},
};
use gateway_core::{
    account::{OpaqueProviderData, ProviderAccountId},
    operation::{GenerateRequest, Operation, ProtocolPayload},
    routing::{ProviderKind, UpstreamModelId},
};
use serde_json::{Value, json};

#[async_trait]
impl ProviderAdmin for BpsProvider {
    fn provider_kind(&self) -> &ProviderKind {
        &self.kind
    }
    fn is_direct_account(&self, authentication_kind: &str) -> bool {
        authentication_kind == "oauth"
    }
    fn account_capabilities(
        &self,
        _account_id: &ProviderAccountId,
        authentication_kind: &str,
    ) -> ProviderAccountCapabilities {
        let oauth = authentication_kind == "oauth";
        ProviderAccountCapabilities {
            quota: oauth,
            quota_refresh: oauth,
            ..ProviderAccountCapabilities::default()
        }
    }
    fn peer_model_eligible(&self, _account: &AccountRecord, _model: &str) -> Option<bool> {
        None
    }
    async fn account_unavailable(&self, id: &ProviderAccountId) {
        self.catalogs
            .write()
            .expect("BPS catalog lock poisoned")
            .remove(id);
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    async fn account_facts_changed(&self, ids: &[ProviderAccountId]) {
        let mut catalogs = self.catalogs.write().expect("BPS catalog lock poisoned");
        for id in ids {
            catalogs.remove(id);
        }
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    async fn connection_test_operation(
        &self,
        model: &UpstreamModelId,
        input: &str,
    ) -> Result<Operation, ProviderAdminError> {
        let body = json!({"model":model.as_str(),"input":input,"stream":true});
        let payload = ProtocolPayload::json_object(
            gateway_protocol::openai::OPENAI_PROTOCOL,
            body.as_object().expect("JSON object").clone(),
        )
        .map_err(|_| BpsError::Protocol.admin())?;
        Ok(Operation::Generate(GenerateRequest::from_protocol_payload(
            payload,
        )))
    }
    fn dashboard_wire_profile(&self) -> Option<DashboardWireProfile> {
        None
    }
    fn calculated_billing(
        &self,
        _input: &ProviderBillingInput,
    ) -> Result<Option<CalculatedBillingBreakdown>, ProviderAdminError> {
        Ok(None)
    }
    fn evaluate_usage_pricing(
        &self,
        input: &gateway_admin::ports::provider::ProviderUsagePricingInput,
        pricing: Option<&gateway_core::metering::ModelPriceOverride>,
    ) -> Result<Option<gateway_core::metering::CalculatedCostBreakdown>, ProviderAdminError> {
        Ok(crate::pricing::calculate(&input.usage, pricing))
    }
    async fn prepare_import(
        &self,
        command: PrepareCredentialImport,
    ) -> Result<PreparedCredentialImport, ProviderAdminError> {
        let document = command.document.expose_to_provider().expose_to_provider();
        let mut credentials = Vec::new();
        let documents = import_documents(document).map_err(BpsError::admin)?;
        for item in documents {
            let credential = Credential::parse(item).map_err(BpsError::admin)?;
            if credential
                .expires_at()
                .is_some_and(|expires| expires <= Utc::now())
            {
                return Err(BpsError::CredentialExpired.admin());
            }
            self.fetch_catalog(&credential, command.default_outbound_proxy.as_ref())
                .await
                .map_err(BpsError::admin)?;
            let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
            let mut prepared = credential.prepare(name).map_err(BpsError::admin)?;
            prepared.outbound_proxy = command.default_outbound_proxy.clone();
            credentials.push(prepared);
        }
        Ok(PreparedCredentialImport {
            provider_kind: self.kind.clone(),
            credentials,
        })
    }
    async fn start_authorization(
        &self,
        pending: PendingAuthorizationMutation,
    ) -> Result<AuthorizationStarted, ProviderAdminError> {
        self.start_oauth(pending).await.map_err(BpsError::admin)
    }
    async fn complete_authorization(
        &self,
        command: CompleteAuthorization,
    ) -> Result<PreparedAuthorizationCommit, ProviderAdminError> {
        self.complete_oauth(command).await.map_err(BpsError::admin)
    }
    async fn prepare_rotation(
        &self,
        command: PrepareCredentialRotation,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        let credential = Credential::parse(
            command
                .provider_material
                .expose_to_provider()
                .expose_to_provider(),
        )
        .map_err(BpsError::admin)?;
        self.fetch_catalog(&credential, command.account.effective_outbound_proxy())
            .await
            .map_err(BpsError::admin)?;
        self.prepare_replacement(&command.account, &credential, false)
            .await
            .map_err(BpsError::admin)
    }
    async fn prepare_refresh(
        &self,
        command: PrepareCredentialRefresh,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        self.refresh_for_admin(&command.account)
            .await
            .map_err(BpsError::admin)
    }
    async fn quota(
        &self,
        request: ProviderQuotaRequest,
    ) -> Result<ProviderQuota, ProviderAdminError> {
        let account = self
            .ports
            .accounts()
            .get_account(&request.account_id)
            .await
            .map_err(|_| BpsError::Unavailable.admin())?
            .filter(|account| account.provider() == &self.kind)
            .ok_or_else(|| BpsError::Credential.admin())?;
        let mut quota = if request.refresh {
            self.refresh_quota(&account).await
        } else {
            self.read_quota(&account).await
        }
        .map_err(BpsError::admin)?;
        crate::quota::attach_rolling_usage(&mut quota, request.rolling_usage);
        Ok(quota)
    }
    async fn read_quotas(
        &self,
        requests: &[ProviderQuotaReadRequest<'_>],
    ) -> Vec<Result<ProviderQuota, ProviderAdminError>> {
        self.read_account_quotas(requests).await
    }
    async fn models(
        &self,
        id: &ProviderAccountId,
        refresh: bool,
    ) -> Result<ProviderModels, ProviderAdminError> {
        let account = self
            .ports
            .accounts()
            .get_account(id)
            .await
            .map_err(|_| BpsError::Unavailable.admin())?
            .filter(|account| account.provider() == &self.kind)
            .ok_or_else(|| BpsError::Credential.admin())?;
        let catalog = self
            .account_catalog(&account, refresh)
            .await
            .map_err(BpsError::admin)?;
        let models = catalog
            .models
            .into_iter()
            .map(|(id, name, _)| {
                Ok(ProviderModel {
                    id: UpstreamModelId::new(id).map_err(|_| BpsError::Protocol.admin())?,
                    name,
                })
            })
            .collect::<Result<Vec<_>, ProviderAdminError>>()?;
        Ok(ProviderModels {
            models,
            observed_at: Some(Utc::now()),
        })
    }
    async fn export_credentials(
        &self,
        credentials: Vec<ProviderExportCredentialInput>,
    ) -> Result<ProviderExport, ProviderAdminError> {
        let mut ids = Vec::new();
        let mut accounts = Vec::new();
        for entry in credentials {
            if entry.account.provider_kind != self.kind {
                return Err(BpsError::Credential.admin());
            }
            let credential = Credential::parse(
                entry
                    .provider_material
                    .expose_to_provider()
                    .expose_to_provider(),
            )
            .map_err(BpsError::admin)?;
            ids.push(
                ProviderAccountId::new(entry.account.id)
                    .map_err(|_| BpsError::Credential.admin())?,
            );
            accounts.push(json!({"name":entry.account.name,"provider":"bps","credentials":credential.material()}));
        }
        let document = json!({"provider":"bps","accounts":accounts})
            .as_object()
            .expect("JSON object")
            .clone();
        Ok(ProviderExport {
            provider_kind: self.kind.clone(),
            account_ids: ids,
            document: ProviderDocument::new(OpaqueProviderData::new(document)),
        })
    }
}
