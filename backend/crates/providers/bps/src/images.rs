//! 图片只上传到固定 BPS 附件端点；外部 HTTPS 引用交由 BPS 读取，不携带账号认证下载

use crate::{BpsError, credential::Credential, network, provider::BpsProvider};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_core::{
    account::ProviderAccount,
    engine::AttemptContext,
    error::{ProviderError, ProviderErrorKind},
    upstream::UpstreamSendState,
};
use serde_json::Value;
use std::{io::Cursor, time::Duration};

const MAX_IMAGE_BYTES: usize = 20 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 32 * 1024 * 1024;
const MAX_IMAGES: usize = 16;

struct Attachment {
    pointer: String,
    mime: &'static str,
    bytes: Vec<u8>,
}

/// 先用占位附件验证完整协议，再上传并在原始规范请求相同位置替换真实附件 ID
pub(crate) struct ImagePlan {
    source: Value,
    attachments: Vec<Attachment>,
}

impl ImagePlan {
    pub(crate) fn prepare(mut source: Value) -> Result<Self, BpsError> {
        let mut attachments = Vec::new();
        let mut count = 0;
        let mut total = 0usize;
        if let Some(items) = source.get_mut("input").and_then(Value::as_array_mut) {
            for (index, item) in items.iter_mut().enumerate() {
                let kind = item.get("type").and_then(Value::as_str).unwrap_or("");
                let field = match kind {
                    "" | "message" | "agent_message" => "content",
                    "function_call_output" | "custom_tool_call_output" => "output",
                    _ => continue,
                };
                let Some(parts) = item.get_mut(field).and_then(Value::as_array_mut) else {
                    continue;
                };
                for (part_index, part) in parts.iter_mut().enumerate() {
                    if part.get("type").and_then(Value::as_str) != Some("input_image") {
                        continue;
                    }
                    count += 1;
                    if count > MAX_IMAGES {
                        return Err(BpsError::InvalidImage);
                    }
                    if let Some(id) = part.get("file_id") {
                        if part.get("image_url").is_some() || !id.as_str().is_some_and(valid_id) {
                            return Err(BpsError::InvalidImage);
                        }
                        continue;
                    }
                    let raw = part
                        .get("image_url")
                        .and_then(Value::as_str)
                        .ok_or(BpsError::InvalidImage)?;
                    if raw.starts_with("data:") {
                        let (mime, bytes) = inline_image(raw)?;
                        total = total
                            .checked_add(bytes.len())
                            .ok_or(BpsError::InvalidImage)?;
                        if total > MAX_TOTAL_BYTES {
                            return Err(BpsError::InvalidImage);
                        }
                        if field == "content" {
                            attachments.push(Attachment {
                                pointer: format!("/input/{index}/{field}/{part_index}"),
                                mime,
                                bytes,
                            });
                            let part = part.as_object_mut().ok_or(BpsError::InvalidImage)?;
                            part.remove("image_url");
                            part.insert("file_id".into(), "file-preflight".into());
                        }
                    } else {
                        let url = url::Url::parse(raw).map_err(|_| BpsError::InvalidImage)?;
                        if raw.trim() != raw
                            || url.scheme() != "https"
                            || url.host_str().is_none()
                            || !url.username().is_empty()
                            || url.password().is_some()
                            || url.fragment().is_some()
                        {
                            return Err(BpsError::InvalidImage);
                        }
                    }
                }
            }
        }
        Ok(Self {
            source,
            attachments,
        })
    }

    pub(crate) fn source(&self) -> &Value {
        &self.source
    }

    pub(crate) fn has_uploads(&self) -> bool {
        !self.attachments.is_empty()
    }

    pub(crate) async fn upload(
        mut self,
        provider: &BpsProvider,
        account: &ProviderAccount,
        context: &AttemptContext,
        model: &gateway_core::routing::UpstreamModelId,
    ) -> Result<Value, ProviderError> {
        if self.attachments.is_empty() {
            return Ok(self.source);
        }
        let operation = async {
            let local = |error: BpsError| error.execution(UpstreamSendState::NotSent);
            let loaded = provider
                .ports
                .accounts()
                .load_credential(account.id(), account.revision())
                .await
                .map_err(|_| local(BpsError::Unavailable))?;
            let credential =
                Credential::parse(loaded.credential.expose_to_provider()).map_err(local)?;
            let client = network::client(account.outbound_proxy()).map_err(local)?;
            let endpoint = provider
                .config
                .endpoint("basispoints/api/attachments")
                .map_err(local)?;
            for attachment in self.attachments {
                let boundary = format!("bps-{}", uuid::Uuid::new_v4());
                let extension = match attachment.mime {
                    "image/jpeg" => "jpg",
                    "image/gif" => "gif",
                    "image/webp" => "webp",
                    _ => "png",
                };
                let mut body = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"image.{extension}\"\r\nContent-Type: {}\r\n\r\n", attachment.mime).into_bytes();
                body.extend_from_slice(&attachment.bytes);
                body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
                let request = network::authenticate(client.post(endpoint.clone()), &credential)
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .header("accept", "application/json")
                    .timeout(Duration::from_secs(60))
                    .body(body)
                    .build()
                    .map_err(|_| local(BpsError::Protocol))?;
                let request = network::prepare_outbound_request(
                    &client,
                    request,
                    context,
                    &provider.kind,
                    model,
                    account,
                )
                .await?;
                let response = client
                    .execute(request)
                    .await
                    .map_err(|_| local(BpsError::Unavailable))?;
                if !response.status().is_success() {
                    let status = response.status().as_u16();
                    let retry_after = response
                        .headers()
                        .get("retry-after")
                        .and_then(|value| value.to_str().ok())
                        .and_then(gateway_protocol::openai::parse_retry_after_seconds)
                        .map(Duration::from_secs);
                    let error = BpsError::Upstream(status).execution(UpstreamSendState::NotSent);
                    provider
                        .record_failure(account, context, error.kind(), retry_after)
                        .await;
                    return Err(match retry_after {
                        Some(delay) => error.with_retry_after(delay),
                        None => error,
                    });
                }
                let mut response = response;
                let mut raw = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| local(BpsError::Unavailable))?
                {
                    if raw.len().saturating_add(chunk.len()) > 64 * 1024 {
                        return Err(local(BpsError::Protocol));
                    }
                    raw.extend_from_slice(&chunk);
                }
                let result: Value =
                    serde_json::from_slice(&raw).map_err(|_| local(BpsError::Protocol))?;
                let id = result
                    .get("openai_file_id")
                    .and_then(Value::as_str)
                    .filter(|id| valid_id(id))
                    .ok_or_else(|| local(BpsError::Protocol))?;
                let part = self
                    .source
                    .pointer_mut(&attachment.pointer)
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| local(BpsError::Protocol))?;
                part.insert("file_id".into(), id.into());
            }
            Ok(self.source)
        };
        tokio::select! {
            biased;
            () = context.cancellation().cancelled() => Err(ProviderError::new(ProviderErrorKind::Cancelled, UpstreamSendState::NotSent).with_retry_prohibited()),
            () = context.deadline().wait() => Err(ProviderError::new(ProviderErrorKind::Timeout, UpstreamSendState::NotSent).with_retry_prohibited()),
            result = operation => result.map_err(ProviderError::with_retry_prohibited),
        }
    }
}

fn valid_id(id: &str) -> bool {
    id.starts_with("file-")
        && (6..=256).contains(&id.len())
        && id[5..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn inline_image(raw: &str) -> Result<(&'static str, Vec<u8>), BpsError> {
    let (header, payload) = raw.split_once(',').ok_or(BpsError::InvalidImage)?;
    let (mime, format) = match header {
        "data:image/png;base64" => ("image/png", image::ImageFormat::Png),
        "data:image/jpeg;base64" => ("image/jpeg", image::ImageFormat::Jpeg),
        "data:image/gif;base64" => ("image/gif", image::ImageFormat::Gif),
        "data:image/webp;base64" => ("image/webp", image::ImageFormat::WebP),
        _ => return Err(BpsError::InvalidImage),
    };
    if payload.len() > MAX_IMAGE_BYTES.div_ceil(3) * 4 {
        return Err(BpsError::InvalidImage);
    }
    let bytes = STANDARD
        .decode(payload)
        .map_err(|_| BpsError::InvalidImage)?;
    if bytes.is_empty()
        || bytes.len() > MAX_IMAGE_BYTES
        || image::guess_format(&bytes).ok() != Some(format)
    {
        return Err(BpsError::InvalidImage);
    }
    let (width, height) = image::ImageReader::with_format(Cursor::new(&bytes), format)
        .into_dimensions()
        .map_err(|_| BpsError::InvalidImage)?;
    if width == 0 || height == 0 || u64::from(width) * u64::from(height) > 64_000_000 {
        return Err(BpsError::InvalidImage);
    }
    Ok((mime, bytes))
}
