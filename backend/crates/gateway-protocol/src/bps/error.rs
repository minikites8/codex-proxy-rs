//! BPS 安全错误文案，仅允许固定文本和有界的历史项类型名

use std::borrow::Cow;

/// 可安全返回客户端的协议失败，不携带输入正文或工具参数
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BpsProtocolError(Cow<'static, str>);

// 类型名仅用于诊断，不限制请求正文或工具参数的长度
const MAX_HISTORY_TYPE_BYTES: usize = 64;

impl BpsProtocolError {
    pub(super) const fn new(message: &'static str) -> Self {
        Self(Cow::Borrowed(message))
    }

    pub(super) fn unsupported_history_type(kind: &str) -> Self {
        // 拒绝回显异常类型值，避免控制字符和过长内容进入公共错误
        let kind = if !kind.is_empty()
            && kind.len() <= MAX_HISTORY_TYPE_BYTES
            && kind
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            kind
        } else {
            "<invalid>"
        };
        Self(Cow::Owned(format!(
            "Unsupported Basispoints history item type '{kind}'."
        )))
    }

    /// 返回仅含固定文本或经过校验的类型名的客户端提示
    #[must_use]
    pub fn message(&self) -> &str {
        self.0.as_ref()
    }
}
