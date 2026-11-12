//! Excel OAuth 凭据导入与身份归一化，不接受 Codex 客户端 Token 配对

use crate::{
    BpsConfig, BpsError, BpsUpstreamTransport,
    config::{CLIENT_ID, PROVIDER},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use gateway_admin::model::{
    accounts::CredentialState,
    provider_credentials::{PreparedCredentialCreate, ProviderDocument},
};
use gateway_core::account::{OpaqueProviderData, ProviderAccountId};
use serde_json::{Map, Value, json};

const CONNECTION_FIELDS: [&str; 5] = [
    "transport",
    "image_input_enabled",
    "omit_images_when_disabled",
    "omit_unsupported_hosted_tools",
    "ignore_encrypted_history",
];

pub(crate) fn inherit_connection(material: &mut Map<String, Value>, previous: &Map<String, Value>) {
    for field in CONNECTION_FIELDS {
        if let Some(value) = previous.get(field) {
            material.entry(field).or_insert_with(|| value.clone());
        }
    }
}

pub(crate) struct Credential {
    material: Map<String, Value>,
    account_id: String,
    user_id: String,
    email: Option<String>,
    plan_type: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    transport: Option<BpsUpstreamTransport>,
}

impl Credential {
    pub(crate) fn parse(document: &Map<String, Value>) -> Result<Self, BpsError> {
        let material = ["tokens", "credentials", "session_info"]
            .iter()
            .find_map(|key| document.get(*key).and_then(Value::as_object))
            .unwrap_or(document);
        let access = material
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .ok_or(BpsError::Credential)?;
        let claims = token_claims(access)?;
        let client = material
            .get("client_id")
            .or_else(|| document.get("client_id"))
            .and_then(Value::as_str)
            .or_else(|| {
                claims
                    .get("client_id")
                    .or_else(|| claims.get("azp"))
                    .and_then(Value::as_str)
            });
        let token_client = claims
            .get("client_id")
            .or_else(|| claims.get("azp"))
            .and_then(Value::as_str);
        if token_client != Some(CLIENT_ID) || client != Some(CLIENT_ID) {
            return Err(BpsError::Credential);
        }
        let identity = material
            .get("id_token")
            .and_then(Value::as_str)
            .map(token_claims)
            .transpose()?;
        if let Some(identity) = &identity {
            let audience = identity.get("aud").ok_or(BpsError::Credential)?;
            if !(audience.as_str() == Some(CLIENT_ID)
                || audience
                    .as_array()
                    .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(CLIENT_ID))))
            {
                return Err(BpsError::Credential);
            }
        }
        let auth = claims
            .get("https://api.openai.com/auth")
            .unwrap_or(&Value::Null);
        let claimed_account = auth
            .get("chatgpt_account_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or(BpsError::Credential)?;
        let account_id = material
            .get("chatgpt_account_id")
            .or_else(|| material.get("account_id"))
            .or_else(|| document.get("chatgpt_account_id"))
            .or_else(|| auth.get("chatgpt_account_id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or(BpsError::Credential)?
            .to_owned();
        if claimed_account != account_id {
            return Err(BpsError::Credential);
        }
        let user_id = auth
            .get("chatgpt_user_id")
            .or_else(|| auth.get("user_id"))
            .or_else(|| claims.get("sub"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or(BpsError::Credential)?
            .to_owned();
        let expires_at = claims
            .get("exp")
            .and_then(Value::as_i64)
            .filter(|timestamp| *timestamp > 0)
            .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0))
            .ok_or(BpsError::Credential)?;
        let profile = claims.get("https://api.openai.com/profile");
        let email = profile
            .and_then(|profile| profile.get("email"))
            .or_else(|| claims.get("email"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        // 套餐是上游 JWT 的明确资料，不能由模型目录、套餐权限或缺失字段推断
        let identity_auth = identity
            .as_ref()
            .and_then(|identity| identity.get("https://api.openai.com/auth"));
        let identity_matches = identity_auth.is_some_and(|identity_auth| {
            identity_auth
                .get("chatgpt_account_id")
                .and_then(Value::as_str)
                == Some(account_id.as_str())
                && identity_auth
                    .get("chatgpt_user_id")
                    .or_else(|| identity_auth.get("user_id"))
                    .and_then(Value::as_str)
                    == Some(user_id.as_str())
        });
        let plan_type = explicit_plan(auth.get("chatgpt_plan_type").and_then(Value::as_str))
            .or_else(|| {
                identity_auth.filter(|_| identity_matches).and_then(|auth| {
                    explicit_plan(auth.get("chatgpt_plan_type").and_then(Value::as_str))
                })
            });
        let mut material = material.clone();
        let transport = material.get("transport").map(parse_transport).transpose()?;
        if CONNECTION_FIELDS[1..].iter().any(|field| {
            material
                .get(*field)
                .is_some_and(|value| !value.is_boolean())
        }) {
            return Err(BpsError::Credential);
        }
        material.insert("client_id".into(), CLIENT_ID.into());
        material.insert("chatgpt_account_id".into(), account_id.clone().into());
        material.insert("schema_version".into(), 1.into());
        Ok(Self {
            material,
            account_id,
            user_id,
            email,
            plan_type,
            expires_at: Some(expires_at),
            transport,
        })
    }
    pub(crate) fn transport(&self, default: BpsUpstreamTransport) -> BpsUpstreamTransport {
        self.transport.unwrap_or(default)
    }
    pub(crate) fn configuration(&self, defaults: &BpsConfig) -> ProviderDocument {
        let transport = match self.transport(defaults.upstream_transport) {
            BpsUpstreamTransport::Ws => "prefer_websocket",
            BpsUpstreamTransport::Sse => "http",
        };
        ProviderDocument::new(OpaqueProviderData::new(
            json!({"transport":transport,
                "image_input_enabled":self.boolean_option("image_input_enabled",defaults.image_input_enabled),
                "omit_images_when_disabled":self.boolean_option("omit_images_when_disabled",defaults.omit_images_when_disabled),
                "omit_unsupported_hosted_tools":self.boolean_option("omit_unsupported_hosted_tools",defaults.omit_unsupported_hosted_tools),
                "ignore_encrypted_history":self.boolean_option("ignore_encrypted_history",defaults.ignore_encrypted_history)})
                .as_object()
                .expect("JSON object")
                .clone(),
        ))
    }
    fn boolean_option(&self, field: &str, default: bool) -> bool {
        self.material
            .get(field)
            .and_then(Value::as_bool)
            .unwrap_or(default)
    }
    pub(crate) fn request_policy(
        &self,
        defaults: &BpsConfig,
    ) -> gateway_protocol::bps::BpsRequestPolicy {
        use gateway_protocol::bps::{BpsImagePolicy, BpsRequestPolicy};
        BpsRequestPolicy {
            images: if self.boolean_option("image_input_enabled", defaults.image_input_enabled) {
                BpsImagePolicy::Enabled
            } else if self.boolean_option(
                "omit_images_when_disabled",
                defaults.omit_images_when_disabled,
            ) {
                BpsImagePolicy::Omit
            } else {
                BpsImagePolicy::Reject
            },
            omit_unsupported_hosted_tools: self.boolean_option(
                "omit_unsupported_hosted_tools",
                defaults.omit_unsupported_hosted_tools,
            ),
            ignore_encrypted_history: self.boolean_option(
                "ignore_encrypted_history",
                defaults.ignore_encrypted_history,
            ),
        }
    }
    pub(crate) fn update_connection(
        &mut self,
        connection: &Map<String, Value>,
    ) -> Result<(), BpsError> {
        if connection.iter().any(|(field, value)| {
            !CONNECTION_FIELDS.contains(&field.as_str())
                || (field != "transport" && !value.is_boolean())
        }) {
            return Err(BpsError::Connection);
        }
        let value = connection.get("transport").ok_or(BpsError::Connection)?;
        self.transport = Some(parse_transport(value).map_err(|_| BpsError::Connection)?);
        self.material.extend(connection.clone());
        Ok(())
    }
    pub(crate) fn access_token(&self) -> &str {
        self.material["access_token"]
            .as_str()
            .expect("credential validated before use")
    }
    pub(crate) fn refresh_token(&self) -> Option<&str> {
        self.material
            .get("refresh_token")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
    }
    pub(crate) fn account_id(&self) -> &str {
        &self.account_id
    }
    pub(crate) fn user_id(&self) -> &str {
        &self.user_id
    }
    pub(crate) fn plan_type(&self) -> Option<&str> {
        self.plan_type.as_deref()
    }
    pub(crate) fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }
    pub(crate) fn document(&self) -> ProviderDocument {
        ProviderDocument::new(OpaqueProviderData::new(self.material.clone()))
    }
    pub(crate) fn material(&self) -> &Map<String, Value> {
        &self.material
    }
    pub(crate) fn prepare(&self, name: &str) -> Result<PreparedCredentialCreate, BpsError> {
        Ok(PreparedCredentialCreate {
            model_access: None,
            outbound_proxy: None,
            account_id: ProviderAccountId::new(format!("acct_{}", uuid::Uuid::now_v7()))
                .map_err(|_| BpsError::Credential)?,
            provider_kind: super::provider::kind(),
            name: if name.trim().is_empty() {
                self.email.clone().unwrap_or_else(|| "BPS".into())
            } else {
                name.trim().into()
            },
            email: self.email.clone(),
            upstream_user_id: Some(self.user_id.clone()),
            upstream_account_id: Some(self.account_id.clone()),
            plan_type: self.plan_type.clone(),
            authentication_kind: "oauth".into(),
            provider_material: self.document(),
            has_refresh_token: self.refresh_token().is_some(),
            access_token_expires_at: self.expires_at,
            next_refresh_at: None,
            enabled: true,
            credential_state: CredentialState::Ready,
            credential_observed_at: Utc::now(),
        })
    }
    pub(crate) fn refresh_request(&self) -> Result<Value, BpsError> {
        Ok(
            json!({"grant_type":"refresh_token","client_id":CLIENT_ID,"refresh_token":self.refresh_token().ok_or(BpsError::Credential)?}),
        )
    }
    pub(crate) fn refreshed(&self, mut response: Value) -> Result<Self, BpsError> {
        let material = response.as_object_mut().ok_or(BpsError::Protocol)?;
        material.insert("client_id".into(), CLIENT_ID.into());
        material.insert("chatgpt_account_id".into(), self.account_id.clone().into());
        inherit_connection(material, &self.material);
        if !material.contains_key("refresh_token")
            && let Some(token) = self.refresh_token()
        {
            material.insert("refresh_token".into(), token.into());
        }
        let refreshed = Self::parse(material)?;
        if refreshed.account_id != self.account_id || refreshed.user_id != self.user_id {
            return Err(BpsError::Credential);
        }
        Ok(refreshed)
    }
}

fn parse_transport(value: &Value) -> Result<BpsUpstreamTransport, BpsError> {
    match value.as_str() {
        Some("http") => Ok(BpsUpstreamTransport::Sse),
        Some("prefer_websocket") => Ok(BpsUpstreamTransport::Ws),
        _ => Err(BpsError::Credential),
    }
}

fn token_claims(token: &str) -> Result<Value, BpsError> {
    let parts: Vec<_> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(BpsError::Credential);
    }
    let payload = parts[1];
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| BpsError::Credential)?;
    serde_json::from_slice(&bytes).map_err(|_| BpsError::Credential)
}

pub(crate) fn import_documents(
    document: &Map<String, Value>,
) -> Result<Vec<&Map<String, Value>>, BpsError> {
    if document
        .get("provider")
        .and_then(Value::as_str)
        .is_some_and(|provider| provider != PROVIDER)
    {
        return Err(BpsError::Credential);
    }
    match document.get("accounts") {
        Some(Value::Array(accounts)) if !accounts.is_empty() && accounts.len() <= 200 => accounts
            .iter()
            .map(|account| account.as_object().ok_or(BpsError::Credential))
            .collect(),
        None => Ok(vec![document]),
        _ => Err(BpsError::Credential),
    }
}

fn explicit_plan(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("unknown"))
        .map(str::to_owned)
}
