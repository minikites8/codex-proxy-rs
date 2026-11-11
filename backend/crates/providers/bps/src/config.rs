//! BPS 固定客户端身份和可配置的服务连接地址

use crate::BpsError;
use serde::Deserialize;
use url::Url;

pub(crate) const PROVIDER: &str = "bps";
pub(crate) const CLIENT_ID: &str = "app_fnr0pYvVwwFDocDumLG3H2Bp";
pub(crate) const REDIRECT_URI: &str = "https://bps.openai.com/basispoints/extension/360590d7-f8f9-4d88-bf75-0edfe0a4b9f3/auth/callback";

/// BPS 生成请求使用的上游传输
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BpsUpstreamTransport {
    /// 每轮建立独立 WebSocket，不自动恢复或重放
    #[default]
    Ws,
    /// HTTP 请求与 SSE 响应
    Sse,
}

/// BPS 服务地址；账号代理由既有账号配置决定
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BpsConfig {
    /// 生成传输，默认使用 WebSocket
    pub upstream_transport: BpsUpstreamTransport,
    /// BPS 服务根地址
    pub base_url: String,
    /// OAuth 服务根地址
    pub auth_base_url: String,
    /// 共享套餐额度查询服务根地址
    pub quota_base_url: String,
    /// 开启原生附件与图片输入
    pub image_input_enabled: bool,
    /// 图片输入关闭时，以明确占位保留历史和工具配对
    pub omit_images_when_disabled: bool,
    /// 自动工具选择时省略 BPS 不支持的托管工具，并告知模型
    pub omit_unsupported_hosted_tools: bool,
    /// 用占位替换无法跨通道解释的加密历史
    pub ignore_encrypted_history: bool,
}

impl Default for BpsConfig {
    fn default() -> Self {
        Self {
            upstream_transport: BpsUpstreamTransport::Ws,
            base_url: "https://bps.openai.com/".into(),
            auth_base_url: "https://auth.openai.com/".into(),
            quota_base_url: "https://chatgpt.com/".into(),
            image_input_enabled: true,
            omit_images_when_disabled: false,
            omit_unsupported_hosted_tools: false,
            ignore_encrypted_history: false,
        }
    }
}

impl BpsConfig {
    pub(crate) fn request_policy(&self) -> gateway_protocol::bps::BpsRequestPolicy {
        use gateway_protocol::bps::{BpsImagePolicy, BpsRequestPolicy};
        BpsRequestPolicy {
            images: if self.image_input_enabled {
                BpsImagePolicy::Enabled
            } else if self.omit_images_when_disabled {
                BpsImagePolicy::Omit
            } else {
                BpsImagePolicy::Reject
            },
            omit_unsupported_hosted_tools: self.omit_unsupported_hosted_tools,
            ignore_encrypted_history: self.ignore_encrypted_history,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), BpsError> {
        for value in [&self.base_url, &self.auth_base_url, &self.quota_base_url] {
            let url = Url::parse(value).map_err(|_| BpsError::Configuration)?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(BpsError::Configuration);
            }
        }
        Ok(())
    }
    pub(crate) fn endpoint(&self, path: &str) -> Result<Url, BpsError> {
        Url::parse(&self.base_url)
            .and_then(|url| url.join(path))
            .map_err(|_| BpsError::Configuration)
    }
    pub(crate) fn auth_endpoint(&self, path: &str) -> Result<Url, BpsError> {
        Url::parse(&self.auth_base_url)
            .and_then(|url| url.join(path))
            .map_err(|_| BpsError::Configuration)
    }
    pub(crate) fn quota_endpoint(&self, path: &str) -> Result<Url, BpsError> {
        Url::parse(&self.quota_base_url)
            .and_then(|url| url.join(path))
            .map_err(|_| BpsError::Configuration)
    }
}
