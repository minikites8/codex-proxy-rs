//! BPS HTTP 与 WebSocket 出站准备、握手和单轮事件读取

use crate::{BpsError, BpsUpstreamTransport, credential::Credential, network};
use futures::{SinkExt, StreamExt, stream::BoxStream};
use gateway_core::{
    account::ProviderAccount,
    engine::{
        AttemptContext,
        middleware::{
            MiddlewareHeader,
            outbound::{Request, Target},
        },
    },
    error::{ProviderError, ProviderErrorKind},
    routing::{ProviderKind, UpstreamModelId},
    upstream::UpstreamSendState,
};
use gateway_protocol::openai::sse::{MAX_SSE_EVENT_BUFFER_BYTES, SseEventDecoder};
use reqwest::{Client, Response};
use serde_json::{Value, json};
use std::{future::Future, time::Duration};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        handshake::derive_accept_key,
        protocol::{Role, WebSocketConfig},
    },
};
use url::Url;
use uuid::Uuid;

const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct GenerationRequest<'a> {
    pub(crate) client: &'a Client,
    pub(crate) url: Url,
    pub(crate) transport: BpsUpstreamTransport,
    pub(crate) body: Value,
    pub(crate) headers: &'a [MiddlewareHeader],
    pub(crate) credential: &'a Credential,
    pub(crate) context: &'a AttemptContext,
    pub(crate) provider: &'a ProviderKind,
    pub(crate) model: &'a UpstreamModelId,
    pub(crate) account: &'a ProviderAccount,
}

pub(crate) struct Opening {
    pub(crate) response: Response,
    pub(crate) payload: Option<String>,
}

impl GenerationRequest<'_> {
    pub(crate) async fn open(mut self) -> Result<Opening, ProviderError> {
        let mut request = self
            .client
            .post(self.url.clone())
            .header("Accept", "text/event-stream");
        let payload = if self.transport == BpsUpstreamTransport::Ws {
            let request_id = Uuid::new_v4().to_string();
            let platform = if cfg!(target_os = "macos") {
                "Mac"
            } else {
                "PC"
            };
            // BPS 内部协议参考 bps-local-public@7134964 的 internal/proxy/websocket.go
            let profile = json!({
                "X-Openai-Internal-Basispoints-Client-Product":"basispoints-excel-plugin",
                "X-Openai-Internal-Basispoints-Client-Platform":"excel",
                "X-Openai-Internal-Basispoints-Client-Agent-Profile":"excel",
                "X-Openai-Internal-Basispoints-Client-Editor":"excel",
                "X-Openai-Internal-Basispoints-Client-Host":"office",
                "X-Openai-Internal-Basispoints-Client-Runtime":"desktop",
                "X-Openai-Internal-Basispoints-Client-Platform-Class":platform,
                "X-Openai-Internal-Basispoints-Office-Host":"Excel",
                "X-Openai-Internal-Basispoints-Office-Platform":platform,
                "X-Openai-Internal-Basispoints-Browser-Name":"unknown"
            });
            self.url
                .query_pairs_mut()
                .append_pair("bps_client_info", &profile.to_string())
                .append_pair("bps_auth_mode", "chatgpt")
                .append_pair("bps_ws_affinity", &request_id)
                .append_pair("bps_control_frames", "upstream_sent,heartbeat");
            self.body["type"] = json!("response.create");
            self.body["basispoints_request_id"] = json!(request_id);
            self.body["basispoints_resume_token"] =
                json!(format!("resume_{}", Uuid::new_v4().simple()));
            request = self
                .client
                .get(self.url.clone())
                .version(reqwest::Version::HTTP_11)
                .header("Accept", "*/*");
            for (name, value) in profile
                .as_object()
                .expect("static client profile is an object")
            {
                request = request.header(
                    name,
                    value.as_str().expect("static profile values are strings"),
                );
            }
            request = request.header(
                "Sec-WebSocket-Protocol",
                format!(
                    "responses, openai-bearer.{}",
                    self.credential.access_token()
                ),
            );
            Some(self.body.to_string())
        } else {
            request = request.json(&self.body);
            None
        };
        for header in self.headers {
            if !matches!(
                header.name().to_ascii_lowercase().as_str(),
                "authorization"
                    | "host"
                    | "content-length"
                    | "chatgpt-account-id"
                    | "x-openai-account-id"
                    | "connection"
                    | "upgrade"
                    | "sec-websocket-key"
                    | "sec-websocket-version"
                    | "sec-websocket-protocol"
                    | "sec-websocket-extensions"
            ) {
                request = request.header(header.name(), header.value().as_ref());
            }
        }
        let mut request = network::authenticate(request, self.credential)
            .build()
            .map_err(|_| protocol(UpstreamSendState::NotSent))?;
        let mut payload = payload;
        if let Some(body) = payload.take() {
            let mut url = request.url().clone();
            let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
            url.set_scheme(scheme)
                .map_err(|_| protocol(UpstreamSendState::NotSent))?;
            let handshake = url
                .as_str()
                .into_client_request()
                .map_err(|_| protocol(UpstreamSendState::NotSent))?;
            request.headers_mut().extend(handshake.headers().clone());
            let mut prepared = Request {
                target: Target::WebSocket {
                    uri: url
                        .as_str()
                        .parse()
                        .map_err(|_| protocol(UpstreamSendState::NotSent))?,
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
                body: body.into(),
            };
            if let Some(pipeline) = self.context.outbound_middleware(
                self.provider.clone(),
                Some(self.model.as_str().into()),
                self.account.id().clone(),
            ) {
                prepared = pipeline.prepare(prepared).await?;
            }
            let headers = prepared.header_map()?;
            let Target::WebSocket { uri } = prepared.target else {
                return Err(protocol(UpstreamSendState::NotSent));
            };
            let mut handshake = tokio_tungstenite::tungstenite::handshake::client::Request::new(());
            *handshake.uri_mut() = uri.clone();
            *handshake.headers_mut() = headers.clone();
            tokio_tungstenite::tungstenite::handshake::client::generate_request(handshake)
                .map_err(|_| protocol(UpstreamSendState::NotSent))?;
            if headers.contains_key("sec-websocket-extensions") {
                return Err(protocol(UpstreamSendState::NotSent));
            }
            let mut url =
                Url::parse(&uri.to_string()).map_err(|_| protocol(UpstreamSendState::NotSent))?;
            let scheme = match url.scheme() {
                "ws" => "http",
                "wss" => "https",
                _ => return Err(protocol(UpstreamSendState::NotSent)),
            };
            url.set_scheme(scheme)
                .map_err(|_| protocol(UpstreamSendState::NotSent))?;
            payload = Some(
                String::from_utf8(prepared.body.into())
                    .map_err(|_| protocol(UpstreamSendState::NotSent))?,
            );
            request = self
                .client
                .get(url)
                .version(reqwest::Version::HTTP_11)
                .headers(headers)
                .build()
                .map_err(|_| protocol(UpstreamSendState::NotSent))?;
        } else {
            request = network::prepare_outbound_request(
                self.client,
                request,
                self.context,
                self.provider,
                self.model,
                self.account,
            )
            .await?;
        }
        let opening_headers = request.headers().clone();
        let websocket = payload.is_some();
        // reqwest 升级连接保留账号代理、TLS 和禁止重定向策略，不另建直连拨号旁路
        let response = wait(
            self.context,
            if websocket {
                UpstreamSendState::NotSent
            } else {
                UpstreamSendState::Ambiguous
            },
            if websocket {
                Duration::from_secs(60)
            } else {
                IDLE_TIMEOUT
            },
            async {
                self.client.execute(request).await.map_err(|error| {
                    let state = if websocket || error.is_connect() {
                        UpstreamSendState::NotSent
                    } else {
                        UpstreamSendState::Ambiguous
                    };
                    ProviderError::new(ProviderErrorKind::Transport, state)
                })
            },
        )
        .await?;
        if websocket {
            if response.status() == reqwest::StatusCode::SWITCHING_PROTOCOLS {
                let headers = response.headers();
                let accepted = opening_headers
                    .get("sec-websocket-key")
                    .map(|key| derive_accept_key(key.as_bytes()));
                let upgrade = headers
                    .get("upgrade")
                    .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"));
                let connection = headers
                    .get("connection")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| {
                        value
                            .split(',')
                            .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
                    });
                let protocol_matches =
                    headers
                        .get("sec-websocket-protocol")
                        .is_none_or(|selected| {
                            opening_headers
                                .get("sec-websocket-protocol")
                                .and_then(|value| value.to_str().ok())
                                .is_some_and(|offered| {
                                    offered
                                        .split(',')
                                        .any(|value| value.trim().as_bytes() == selected.as_bytes())
                                })
                        });
                if !upgrade
                    || !connection
                    || !protocol_matches
                    || headers.contains_key("sec-websocket-extensions")
                    || !headers.get("sec-websocket-accept").is_some_and(|value| {
                        Some(value.as_bytes()) == accepted.as_deref().map(str::as_bytes)
                    })
                {
                    return Err(protocol(UpstreamSendState::NotSent));
                }
            } else if response.status().is_success() {
                return Err(protocol(UpstreamSendState::NotSent));
            }
        }
        Ok(Opening { response, payload })
    }
}

pub(crate) async fn events(
    response: Response,
    payload: Option<String>,
    context: AttemptContext,
) -> Result<BoxStream<'static, Result<Value, ProviderError>>, ProviderError> {
    let Some(payload) = payload else {
        return Ok(Box::pin(async_stream::try_stream! {
            let mut bytes = response.bytes_stream();
            let mut decoder = SseEventDecoder::default();
            loop {
                let chunk = wait(&context, UpstreamSendState::Sent, IDLE_TIMEOUT, async {
                    bytes.next().await.transpose().map_err(|_| ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Sent))
                }).await?;
                let drained = chunk.is_none();
                let events = match chunk { Some(chunk) => decoder.push(&chunk), None => decoder.finish() }.map_err(|_| protocol(UpstreamSendState::Sent))?;
                for event in events {
                    if event.data != "[DONE]" { yield serde_json::from_str(&event.data).map_err(|_| protocol(UpstreamSendState::Sent))?; }
                }
                if drained { break; }
            }
        }));
    };
    let upgraded = wait(&context, UpstreamSendState::NotSent, SEND_TIMEOUT, async {
        response
            .upgrade()
            .await
            .map_err(|_| protocol(UpstreamSendState::NotSent))
    })
    .await?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_SSE_EVENT_BUFFER_BYTES))
        .max_frame_size(Some(MAX_SSE_EVENT_BUFFER_BYTES));
    let mut socket = WebSocketStream::from_raw_socket(upgraded, Role::Client, Some(config)).await;
    wait(
        &context,
        UpstreamSendState::Ambiguous,
        SEND_TIMEOUT,
        async {
            socket
                .send(Message::Text(payload.into()))
                .await
                .map_err(|_| {
                    ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Ambiguous)
                        .with_retry_prohibited()
                })
        },
    )
    .await?;
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await;
    // unfold 的每次读取独占连接，错误返回前即释放 socket，不把资源留在已报错的生成器中
    Ok(Box::pin(futures::stream::try_unfold(
        Some((socket, context, ping)),
        |state| async move {
            let Some((mut socket, context, mut ping)) = state else {
                return Ok(None);
            };
            loop {
                let message = wait(&context, UpstreamSendState::Sent, IDLE_TIMEOUT, async {
                loop {
                    tokio::select! {
                        message = socket.next() => return message.transpose().map_err(|_| ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Sent).with_retry_prohibited()),
                        _ = ping.tick() => {
                            tokio::time::timeout(SEND_TIMEOUT, socket.send(Message::Ping(bytes::Bytes::new()))).await
                                .map_err(|_| ProviderError::new(ProviderErrorKind::Timeout, UpstreamSendState::Sent).with_retry_prohibited())?
                                .map_err(|_| ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Sent).with_retry_prohibited())?;
                        }
                    }
                }
            }).await?;
                let Some(message) = message else {
                    return Err(protocol(UpstreamSendState::Sent));
                };
                match message {
                    Message::Text(text) => {
                        let mut event: Value = serde_json::from_str(&text)
                            .map_err(|_| protocol(UpstreamSendState::Sent))?;
                        let kind = event
                            .get("type")
                            .and_then(Value::as_str)
                            .ok_or_else(|| protocol(UpstreamSendState::Sent))?;
                        match kind {
                            "basispoints.response.resume_token"
                            | "basispoints.response.metadata"
                            | "basispoints.response.upstream_sent"
                            | "basispoints.response.heartbeat" => continue,
                            "basispoints.response.server_draining" => Err(ProviderError::new(
                                ProviderErrorKind::Unavailable,
                                UpstreamSendState::Sent,
                            )
                            .with_retry_prohibited())?,
                            "error" => {}
                            kind if kind.starts_with("response.") => {}
                            _ => Err(protocol(UpstreamSendState::Sent))?,
                        }
                        let terminal = matches!(
                            kind,
                            "error"
                                | "response.completed"
                                | "response.failed"
                                | "response.cancelled"
                                | "response.incomplete"
                        );
                        event
                            .as_object_mut()
                            .ok_or_else(|| protocol(UpstreamSendState::Sent))?
                            .remove("basispoints_replay_cursor");
                        let next = if terminal {
                            None
                        } else {
                            Some((socket, context, ping))
                        };
                        return Ok(Some((event, next)));
                    }
                    Message::Ping(_) => {
                        // tungstenite 已排队 Pong，显式 flush 防止业务流空闲时延迟回复
                        wait(&context, UpstreamSendState::Sent, SEND_TIMEOUT, async {
                            socket.flush().await.map_err(|_| {
                                ProviderError::new(
                                    ProviderErrorKind::Transport,
                                    UpstreamSendState::Sent,
                                )
                                .with_retry_prohibited()
                            })
                        })
                        .await?;
                    }
                    Message::Pong(_) => {}
                    _ => Err(protocol(UpstreamSendState::Sent))?,
                }
            }
        },
    )))
}

async fn wait<T>(
    context: &AttemptContext,
    state: UpstreamSendState,
    timeout: Duration,
    future: impl Future<Output = Result<T, ProviderError>>,
) -> Result<T, ProviderError> {
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => Err(ProviderError::new(ProviderErrorKind::Cancelled, state)),
        () = context.deadline().wait() => Err(ProviderError::new(ProviderErrorKind::Timeout, state)),
        result = tokio::time::timeout(timeout, future) => result.unwrap_or_else(|_| Err(ProviderError::new(ProviderErrorKind::Timeout, state))),
    }
}

fn protocol(state: UpstreamSendState) -> ProviderError {
    BpsError::Protocol.execution(state).with_retry_prohibited()
}
