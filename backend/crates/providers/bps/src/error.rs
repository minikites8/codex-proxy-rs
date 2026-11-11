//! BPS 本地错误与 Core、Admin 的安全分类映射

use gateway_admin::ports::provider::{ProviderAdminError, ProviderAdminErrorKind};
use gateway_core::{
    error::{ClientVisibleUpstreamError, ProviderDiagnostic, ProviderError, ProviderErrorKind},
    upstream::UpstreamSendState,
};

/// 不包含凭据、对话或原始网络地址的错误
#[derive(Debug, thiserror::Error)]
pub enum BpsError {
    #[error("invalid BPS configuration")]
    Configuration,
    #[error("invalid BPS image input")]
    InvalidImage,
    #[error("invalid BPS credential")]
    Credential,
    #[error("BPS imported access token expired")]
    CredentialExpired,
    #[error("BPS authorization expired or does not match")]
    Authorization,
    #[error("BPS resource is busy")]
    Busy,
    #[error("BPS dependency unavailable")]
    Unavailable,
    #[error("BPS upstream rejected the request ({0})")]
    Upstream(u16),
    #[error("BPS quota query rejected ({0})")]
    QuotaUpstream(u16),
    #[error("invalid BPS response")]
    Protocol,
    #[error(transparent)]
    Request(#[from] gateway_protocol::bps::BpsProtocolError),
    #[error(transparent)]
    Worker(#[from] gateway_core::task::WorkerDefinitionError),
}

impl BpsError {
    pub(crate) fn admin(self) -> ProviderAdminError {
        let (kind, message) = match self {
            Self::InvalidImage => (
                ProviderAdminErrorKind::Invalid,
                "BPS 图片格式、引用或大小不受支持",
            ),
            Self::Credential => (
                ProviderAdminErrorKind::Invalid,
                "请导入同一会话的 Excel OAuth 凭据",
            ),
            Self::CredentialExpired => (
                ProviderAdminErrorKind::Invalid,
                "Excel 访问令牌已过期，请重新授权后导入",
            ),
            Self::Authorization => (
                ProviderAdminErrorKind::Invalid,
                "BPS 授权已过期或回调不匹配，请重新授权",
            ),
            Self::Busy => (
                ProviderAdminErrorKind::Conflict,
                "BPS 账号正在处理另一项操作",
            ),
            Self::QuotaUpstream(_) => (ProviderAdminErrorKind::Unavailable, "BPS 额度查询失败"),
            Self::Upstream(401) => (
                ProviderAdminErrorKind::CredentialRefreshRequired,
                "BPS 授权已失效，请重新授权",
            ),
            Self::Upstream(403) => (ProviderAdminErrorKind::Invalid, "当前账号没有 BPS 访问权限"),
            Self::Request(_) => (ProviderAdminErrorKind::Invalid, "BPS 不支持该请求"),
            _ => (ProviderAdminErrorKind::Unavailable, "BPS 服务暂时不可用"),
        };
        ProviderAdminError::new(kind)
            .with_public_message(message)
            .with_source(self)
    }

    pub(crate) fn execution(self, state: UpstreamSendState) -> ProviderError {
        let kind = match self {
            Self::Credential
            | Self::CredentialExpired
            | Self::Authorization
            | Self::Upstream(401) => ProviderErrorKind::Unauthorized,
            Self::Upstream(403) => ProviderErrorKind::PermissionDenied,
            Self::Upstream(429) => ProviderErrorKind::RateLimited,
            Self::InvalidImage | Self::Request(_) | Self::Upstream(400 | 422) => {
                ProviderErrorKind::InvalidRequest
            }
            Self::Protocol => ProviderErrorKind::Protocol,
            _ => ProviderErrorKind::Unavailable,
        };
        let mut error = ProviderError::new(kind, state);
        if let Self::Upstream(status) = self {
            error = error.with_status(status);
        }
        if let Self::Request(ref cause) = self {
            error = error
                .with_diagnostic(
                    ProviderDiagnostic::new(cause.message())
                        .with_classification("bps_request_prepare", "basispoints_request_invalid"),
                )
                .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
                    cause.message(),
                    Some("basispoints_request_invalid".into()),
                    Some("invalid_request_error".into()),
                ))
                .with_retry_prohibited();
        }
        if matches!(self, Self::InvalidImage) {
            const MESSAGE: &str =
                "Invalid or unsupported Basispoints image format, reference, or size.";
            error = error
                .with_diagnostic(
                    ProviderDiagnostic::new(MESSAGE)
                        .with_classification("bps_request_prepare", "basispoints_image_invalid"),
                )
                .with_client_visible_upstream_error(ClientVisibleUpstreamError::new(
                    MESSAGE,
                    Some("basispoints_image_invalid".into()),
                    Some("invalid_request_error".into()),
                ))
                .with_retry_prohibited();
        }
        error
    }
}
