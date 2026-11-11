//! BPS 出站连接沿用账号代理，禁止重定向携带凭据

use crate::{BpsError, credential::Credential};
use gateway_core::{
    account::{OutboundProxy, ProviderAccount},
    engine::{
        AttemptContext,
        middleware::{
            MiddlewareHeader,
            outbound::{Request, Target},
        },
    },
    error::ProviderError,
    routing::{ProviderKind, UpstreamModelId},
    upstream::UpstreamSendState,
};
use reqwest::{Client, RequestBuilder, Response};
use serde_json::Value;
use std::time::Duration;

pub(crate) fn client(proxy: Option<&OutboundProxy>) -> Result<Client, BpsError> {
    let mut builder = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(120));
    if let Some(proxy) = proxy {
        builder = builder
            .proxy(reqwest::Proxy::all(proxy.expose_url()).map_err(|_| BpsError::Configuration)?);
    }
    builder.build().map_err(|_| BpsError::Unavailable)
}

pub(crate) fn authenticate(request: RequestBuilder, credential: &Credential) -> RequestBuilder {
    request
        .bearer_auth(credential.access_token())
        .header("Chatgpt-Account-Id", credential.account_id())
        .header("X-Openai-Account-Id", credential.account_id())
        .header("X-Basispoints-Auth-Mode", "chatgpt")
        .header(
            "X-Openai-Internal-Basispoints-Client-Product",
            "basispoints-excel-plugin",
        )
        .header(
            "X-Openai-Internal-Basispoints-Client-Agent-Profile",
            "excel",
        )
        .header("Origin", "https://bps.openai.com")
        .header("User-Agent", "Mozilla/5.0")
}

// 附件与生成共用同一次 attempt 的出站计划，插件看到的是完成认证和序列化后的请求
pub(crate) async fn prepare_outbound_request(
    client: &Client,
    request: reqwest::Request,
    context: &AttemptContext,
    provider: &ProviderKind,
    model: &UpstreamModelId,
    account: &ProviderAccount,
) -> Result<reqwest::Request, ProviderError> {
    let invalid = || BpsError::Protocol.execution(UpstreamSendState::NotSent);
    let Some(pipeline) = context.outbound_middleware(
        provider.clone(),
        Some(model.as_str().into()),
        account.id().clone(),
    ) else {
        return Ok(request);
    };
    let timeout = request.timeout().copied();
    let prepared = pipeline
        .prepare(Request {
            target: Target::Http {
                method: request.method().clone(),
                uri: request.url().as_str().parse().map_err(|_| invalid())?,
            },
            headers: request
                .headers()
                .iter()
                .map(|(name, value)| {
                    MiddlewareHeader::new(
                        name.as_str(),
                        bytes::Bytes::copy_from_slice(value.as_bytes()),
                    )
                })
                .collect(),
            body: bytes::Bytes::copy_from_slice(
                request
                    .body()
                    .and_then(reqwest::Body::as_bytes)
                    .unwrap_or_default(),
            ),
        })
        .await?;
    let headers = prepared.header_map()?;
    let Target::Http { method, uri } = prepared.target else {
        return Err(invalid());
    };
    let mut request = client
        .request(method, uri.to_string())
        .headers(headers)
        .body(prepared.body)
        .build()
        .map_err(|_| invalid())?;
    *request.timeout_mut() = timeout;
    Ok(request)
}

pub(crate) async fn json(response: Response) -> Result<Value, BpsError> {
    let status = response.status();
    if !status.is_success() {
        return Err(BpsError::Upstream(status.as_u16()));
    }
    // 登录和模型目录不是生成正文；沿用 Provider 管理响应的有界读取
    let mut response = response;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| BpsError::Unavailable)? {
        if body.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 {
            return Err(BpsError::Protocol);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| BpsError::Protocol)
}
