//! Excel OAuth 的 PKCE、独占授权流与凭据轮换

use crate::{
    BpsError,
    config::{CLIENT_ID, REDIRECT_URI},
    credential::Credential,
    network,
    provider::BpsProvider,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{TimeDelta, Utc};
use gateway_admin::model::{
    AdminError, Revision,
    accounts::AccountRecord,
    provider_credentials::{
        AuthorizationCommitGuard, AuthorizationMutationTarget, AuthorizationReceiptKey,
        AuthorizationStarted, CompleteAuthorization, CredentialCommitGuard,
        PendingAuthorizationMutation, PreparedAuthorizationCommit, PreparedAuthorizationCredential,
        PreparedCredentialRotation, PreparedCredentialRotationFacts,
    },
};
use gateway_core::{
    account::{
        CredentialCasOutcome, CredentialCasUpdate, OpaqueProviderData, PlaintextCredential,
        ProviderAccount, ProviderAccountId, ProviderAccountIdentity, ProviderAccountUpdate,
    },
    provider_ports::{
        NewOAuthPendingFlow, OAuthPendingBinding, OAuthPendingClaimOutcome,
        OAuthPendingConsumeOutcome, OAuthPendingFlowPort, OAuthPendingPutOutcome,
        OAuthPendingReleaseOutcome, ProviderLeaseAcquisition, ProviderLeaseGuard,
        ProviderLeaseRequest, ProviderRefreshCapacityRequest, ProviderRefreshLeaseRequest,
    },
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

struct RefreshGuard {
    _leases: Vec<Box<dyn ProviderLeaseGuard>>,
}
impl CredentialCommitGuard for RefreshGuard {
    fn finish(self: Box<Self>) {
        drop(self);
    }
}

struct AuthorizationGuard {
    port: Arc<dyn OAuthPendingFlowPort>,
    flow: OAuthPendingBinding,
    owner: OAuthPendingBinding,
    claim: OAuthPendingBinding,
}

#[async_trait]
impl AuthorizationCommitGuard for AuthorizationGuard {
    async fn commit(self: Box<Self>) -> Result<(), AdminError> {
        match self
            .port
            .consume_claim(
                &super::provider::kind(),
                &self.flow,
                &self.owner,
                &self.claim,
            )
            .await
        {
            Ok(OAuthPendingConsumeOutcome::Consumed) => Ok(()),
            _ => Err(AdminError::unavailable("BPS 授权结算状态不可用")),
        }
    }
    async fn abort(self: Box<Self>) -> Result<(), AdminError> {
        match self
            .port
            .release_claim(
                &super::provider::kind(),
                &self.flow,
                &self.owner,
                &self.claim,
            )
            .await
        {
            Ok(OAuthPendingReleaseOutcome::Released) => Ok(()),
            _ => Err(AdminError::unavailable("BPS 授权处理状态暂时无法释放")),
        }
    }
}

impl BpsProvider {
    pub(crate) async fn start_oauth(
        &self,
        mut pending: PendingAuthorizationMutation,
    ) -> Result<AuthorizationStarted, BpsError> {
        if pending.provider_kind() != &self.kind {
            return Err(BpsError::Authorization);
        }
        if let AuthorizationMutationTarget::Reauthorize { account_id } = pending.target() {
            let account = self
                .ports
                .accounts()
                .get_account(account_id)
                .await
                .map_err(|_| BpsError::Unavailable)?
                .filter(|account| account.provider() == &self.kind)
                .ok_or(BpsError::Credential)?;
            pending = pending.with_outbound_proxy(account.outbound_proxy().cloned());
        }
        let mut random = [0u8; 32];
        getrandom::fill(&mut random).map_err(|_| BpsError::Unavailable)?;
        let verifier = URL_SAFE_NO_PAD.encode(random);
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let flow = uuid::Uuid::new_v4().to_string();
        let state = uuid::Uuid::new_v4().to_string();
        let expires_at = Utc::now() + TimeDelta::minutes(30);
        let owner = pending_owner(&pending)?;
        let payload = json!({"mutation":pending.to_storage_v1(),"verifier":verifier,"state":state});
        let flow_record = NewOAuthPendingFlow::try_new(
            self.kind.clone(),
            binding(&flow)?,
            binding(&owner)?,
            Duration::from_secs(1800),
            OpaqueProviderData::new(payload.as_object().expect("JSON object").clone()),
        )
        .map_err(|_| BpsError::Authorization)?;
        if self
            .ports
            .oauth_pending()
            .put_if_absent(flow_record)
            .await
            .map_err(|_| BpsError::Unavailable)?
            != OAuthPendingPutOutcome::Stored
        {
            return Err(BpsError::Busy);
        }
        let mut url = self.config.auth_endpoint("api/accounts/authorize")?;
        url.query_pairs_mut().extend_pairs([
            ("client_id", CLIENT_ID),
            ("redirect_uri", REDIRECT_URI),
            ("response_type", "code"),
            (
                "scope",
                "openid profile email offline_access organization.read",
            ),
            ("state", state.as_str()),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
        ]);
        Ok(AuthorizationStarted {
            flow_id: flow,
            authorization_url: url.into(),
            expires_at,
        })
    }

    pub(crate) async fn complete_oauth(
        &self,
        command: CompleteAuthorization,
    ) -> Result<PreparedAuthorizationCommit, BpsError> {
        let receipt =
            AuthorizationReceiptKey::new(self.kind.clone(), &command.flow_id, &command.context)
                .map_err(|_| BpsError::Authorization)?;
        let guard = AuthorizationGuard {
            port: self.ports.oauth_pending(),
            flow: binding(&command.flow_id)?,
            owner: binding(receipt.owner_digest())?,
            claim: binding(&uuid::Uuid::new_v4().to_string())?,
        };
        let payload = match guard
            .port
            .claim_if_owner(
                &self.kind,
                &guard.flow,
                &guard.owner,
                &guard.claim,
                Duration::from_secs(90),
            )
            .await
            .map_err(|_| BpsError::Unavailable)?
        {
            OAuthPendingClaimOutcome::Claimed(payload) => payload,
            OAuthPendingClaimOutcome::InProgress => return Err(BpsError::Busy),
            _ => return Err(BpsError::Authorization),
        };
        let result = self.exchange_oauth(&command, payload).await;
        match result {
            Ok((pending, credential)) => Ok(PreparedAuthorizationCommit::new(pending, credential)
                .with_authorization_guard(Box::new(guard))),
            Err(error) => {
                // 回调校验尚未消耗授权码，允许修正输入；交换后的失败必须重新授权
                if matches!(error, BpsError::Authorization) {
                    guard
                        .port
                        .release_claim(&self.kind, &guard.flow, &guard.owner, &guard.claim)
                        .await
                        .map_err(|_| BpsError::Unavailable)?;
                } else {
                    Box::new(guard)
                        .commit()
                        .await
                        .map_err(|_| BpsError::Unavailable)?;
                }
                Err(error)
            }
        }
    }

    async fn exchange_oauth(
        &self,
        command: &CompleteAuthorization,
        payload: OpaqueProviderData,
    ) -> Result<
        (
            PendingAuthorizationMutation,
            PreparedAuthorizationCredential,
        ),
        BpsError,
    > {
        let payload = payload.expose_to_provider();
        let pending = PendingAuthorizationMutation::from_storage_v1(
            payload
                .get("mutation")
                .cloned()
                .ok_or(BpsError::Authorization)?,
        )
        .map_err(|_| BpsError::Authorization)?;
        if !pending.owner_binding().matches_context(&command.context)
            || pending.provider_kind() != &self.kind
        {
            return Err(BpsError::Authorization);
        }
        let callback = Url::parse(&command.callback_url).map_err(|_| BpsError::Authorization)?;
        let expected = Url::parse(REDIRECT_URI).map_err(|_| BpsError::Configuration)?;
        if callback.origin() != expected.origin()
            || callback.path() != expected.path()
            || callback.fragment().is_some()
            || !callback.username().is_empty()
            || callback.password().is_some()
        {
            return Err(BpsError::Authorization);
        }
        let query = callback.query_pairs().collect::<Vec<_>>();
        let one = |name: &str| {
            let mut values = query
                .iter()
                .filter(|(key, _)| key == name)
                .map(|(_, value)| value.as_ref());
            let value = values
                .next()
                .filter(|value| !value.is_empty())
                .ok_or(BpsError::Authorization)?;
            if values.next().is_some() {
                return Err(BpsError::Authorization);
            }
            Ok(value)
        };
        if Some(one("state")?) != payload.get("state").and_then(Value::as_str) {
            return Err(BpsError::Authorization);
        }
        let verifier = payload
            .get("verifier")
            .and_then(Value::as_str)
            .ok_or(BpsError::Authorization)?;
        let body = json!({"grant_type":"authorization_code","client_id":CLIENT_ID,"code":one("code")?,"redirect_uri":REDIRECT_URI,"code_verifier":verifier});
        let response = network::client(pending.outbound_proxy())?
            .post(self.config.auth_endpoint("oauth/token?unified=true")?)
            .timeout(Duration::from_secs(30))
            .form(&body)
            .send()
            .await
            .map_err(|_| BpsError::Unavailable)?;
        let mut tokens = network::json(response).await?;
        tokens
            .as_object_mut()
            .ok_or(BpsError::Protocol)?
            .insert("client_id".into(), CLIENT_ID.into());
        let credential = Credential::parse(tokens.as_object().ok_or(BpsError::Protocol)?)?;
        self.fetch_catalog(&credential, pending.outbound_proxy())
            .await?;
        let prepared = match pending.target() {
            AuthorizationMutationTarget::Create { name } => {
                PreparedAuthorizationCredential::Create(Box::new(credential.prepare(name)?))
            }
            AuthorizationMutationTarget::Reauthorize { account_id } => {
                let loaded = self
                    .ports
                    .accounts()
                    .load_current_credential(account_id)
                    .await
                    .map_err(|_| BpsError::Unavailable)?;
                let (facts, guard) = self
                    .replacement(&loaded.account, &credential, false)
                    .await?;
                PreparedAuthorizationCredential::Reauthorize(Box::new(
                    PreparedCredentialRotation::new(facts, guard),
                ))
            }
        };
        Ok((pending, prepared))
    }

    pub(crate) async fn prepare_replacement(
        &self,
        account: &AccountRecord,
        credential: &Credential,
        preserve_profile: bool,
    ) -> Result<PreparedCredentialRotation, BpsError> {
        let id = ProviderAccountId::new(account.id.clone()).map_err(|_| BpsError::Credential)?;
        let loaded = self
            .ports
            .accounts()
            .load_current_credential(&id)
            .await
            .map_err(|_| BpsError::Unavailable)?;
        let (facts, guard) = self
            .replacement(&loaded.account, credential, preserve_profile)
            .await?;
        Ok(PreparedCredentialRotation::new(facts, guard))
    }

    async fn replacement(
        &self,
        account: &ProviderAccount,
        credential: &Credential,
        preserve_profile: bool,
    ) -> Result<
        (
            PreparedCredentialRotationFacts,
            Box<dyn CredentialCommitGuard>,
        ),
        BpsError,
    > {
        if account.provider() != &self.kind
            || account.upstream_user_id() != Some(credential.user_id())
            || account.upstream_account_id() != Some(credential.account_id())
        {
            return Err(BpsError::Credential);
        }
        let created = credential.prepare(account.name())?;
        let facts = PreparedCredentialRotationFacts {
            account_id: account.id().clone(),
            provider_kind: self.kind.clone(),
            expected_credential_revision: Revision::new(account.revision().get())
                .map_err(|_| BpsError::Credential)?,
            replacement_identity: Some(ProviderAccountIdentity::new(
                credential.user_id().into(),
                Some(credential.account_id().into()),
            )),
            name: account.name().into(),
            email: created.email,
            plan_type: credential
                .plan_type()
                .or_else(|| account.plan_type())
                .map(str::to_owned),
            preserve_profile,
            preserve_credential_state: false,
            provider_material: credential.document(),
            has_refresh_token: credential.refresh_token().is_some(),
            access_token_expires_at: credential.expires_at(),
            next_refresh_at: None,
        };
        Ok((
            facts,
            Box::new(RefreshGuard {
                _leases: Vec::new(),
            }),
        ))
    }

    pub(crate) async fn refresh_for_admin(
        &self,
        account: &AccountRecord,
    ) -> Result<PreparedCredentialRotation, BpsError> {
        let id = ProviderAccountId::new(account.id.clone()).map_err(|_| BpsError::Credential)?;
        let (current, credential, guard) = self.refresh_tokens(&id).await?;
        let (facts, _) = self.replacement(&current, &credential, true).await?;
        Ok(PreparedCredentialRotation::new(facts, guard))
    }

    pub(crate) async fn refresh_background(&self, id: &ProviderAccountId) -> Result<(), BpsError> {
        let (account, credential, guard) = self.refresh_tokens(id).await?;
        let update = cas_update(&account, &credential, credential.material().clone())?;
        match self
            .ports
            .accounts()
            .compare_and_swap_credential(update)
            .await
            .map_err(|_| BpsError::Unavailable)?
        {
            CredentialCasOutcome::Updated(_) => {
                guard.finish();
                self.catalogs
                    .write()
                    .expect("BPS catalog lock poisoned")
                    .remove(id);
                Ok(())
            }
            CredentialCasOutcome::Conflict => Err(BpsError::Busy),
        }
    }

    async fn refresh_tokens(
        &self,
        id: &ProviderAccountId,
    ) -> Result<(ProviderAccount, Credential, Box<dyn CredentialCommitGuard>), BpsError> {
        let accounts = self.ports.accounts();
        let loaded = accounts
            .load_current_credential(id)
            .await
            .map_err(|_| BpsError::Unavailable)?;
        if loaded.account.provider() != &self.kind {
            return Err(BpsError::Credential);
        }
        let credential = Credential::parse(loaded.credential.expose_to_provider())?;
        if credential
            .material()
            .get("refresh_attempted")
            .and_then(Value::as_bool)
            == Some(true)
        {
            return Err(BpsError::Authorization);
        }
        let body = credential.refresh_request()?;
        let client = network::client(loaded.account.outbound_proxy())?;
        let token_url = self.config.auth_endpoint("oauth/token?unified=true")?;
        let leases = self.ports.leases();
        let mut guards = Vec::new();
        for request in [
            ProviderLeaseRequest::RefreshCapacity(ProviderRefreshCapacityRequest::new(
                self.ports
                    .runtime_policy()
                    .load_refresh_policy()
                    .await
                    .map_err(|_| BpsError::Unavailable)?
                    .concurrency(),
            )),
            ProviderLeaseRequest::Refresh(ProviderRefreshLeaseRequest::new(
                id.clone(),
                loaded.account.revision(),
            )),
        ] {
            match leases
                .try_acquire(request)
                .await
                .map_err(|_| BpsError::Unavailable)?
            {
                ProviderLeaseAcquisition::Acquired(guard) => guards.push(guard),
                ProviderLeaseAcquisition::Busy { .. } => return Err(BpsError::Busy),
            }
        }
        // 在消耗 RT 前持久标记；崩溃或交换结果不明确时不得再次消耗同一 RT
        let mut material = credential.material().clone();
        material.insert("refresh_attempted".into(), true.into());
        let account = &loaded.account;
        let update = cas_update(account, &credential, material)?;
        let revision = match accounts
            .compare_and_swap_credential(update)
            .await
            .map_err(|_| BpsError::Unavailable)?
        {
            CredentialCasOutcome::Updated(revision) => revision,
            CredentialCasOutcome::Conflict => return Err(BpsError::Busy),
        };
        let response = client
            .post(token_url)
            .timeout(Duration::from_secs(30))
            .form(&body)
            .send()
            .await
            .map_err(|_| BpsError::Unavailable)?;
        let refreshed = credential.refreshed(network::json(response).await?)?;
        let current = accounts
            .load_credential(id, revision)
            .await
            .map_err(|_| BpsError::Busy)?
            .account;
        Ok((
            current,
            refreshed,
            Box::new(RefreshGuard { _leases: guards }),
        ))
    }
}

fn binding(value: &str) -> Result<OAuthPendingBinding, BpsError> {
    OAuthPendingBinding::try_new(value).map_err(|_| BpsError::Authorization)
}

fn pending_owner(pending: &PendingAuthorizationMutation) -> Result<String, BpsError> {
    use gateway_admin::model::provider_credentials::AuthorizationOwner;
    let owner = match pending.owner_binding().owner() {
        AuthorizationOwner::AdminSession { admin_user_id } => {
            format!("admin_session:{admin_user_id}")
        }
        AuthorizationOwner::AdminApiKey => "admin_api_key".into(),
        AuthorizationOwner::System => "system".into(),
    };
    Ok(Sha256::digest(owner.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn cas_update(
    account: &ProviderAccount,
    credential: &Credential,
    material: serde_json::Map<String, Value>,
) -> Result<CredentialCasUpdate, BpsError> {
    CredentialCasUpdate::new(
        account.id().clone(),
        account.revision(),
        ProviderAccountUpdate {
            account_id: account.id().clone(),
            name: account.name().into(),
            email: account.email().map(str::to_owned),
            plan_type: account.plan_type().map(str::to_owned),
        },
        PlaintextCredential::new(material),
        credential.refresh_token().is_some(),
        credential.expires_at().map(Into::into),
        None,
    )
    .map(CredentialCasUpdate::preserving_profile)
    .map_err(|_| BpsError::Credential)
}
