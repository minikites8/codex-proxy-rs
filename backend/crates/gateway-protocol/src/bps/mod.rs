//! Basispoints 与 Responses 的纯转换，不持有账号、连接或跨请求缓存

mod policy;
mod request;
mod stream;
mod tools;

pub use policy::{BpsImagePolicy, BpsRequestPolicy};
pub use request::{BpsRequest, RequestIdentity};
pub use stream::BpsResponse;
pub use tools::ToolReplay;

/// 可安全返回客户端的协议失败，不携带输入正文或工具参数
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BpsProtocolError(&'static str);

impl BpsProtocolError {
    /// 返回固定的客户端提示
    #[must_use]
    pub const fn message(&self) -> &'static str {
        self.0
    }
}
