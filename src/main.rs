mod game;
mod lobby;

use tracing_subscriber::prelude::*;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt as _};
use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
};
use hyper_tungstenite::{HyperWebsocket, tungstenite::Message};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use indoc::indoc;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;
use tracing::{error, info, level_filters::LevelFilter};
use tracing_subscriber::EnvFilter;

use crate::{game::RunningGames, lobby::Lobby};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct GameSettings {
    capacity: u8,
    width: u8,
    height: u8,
}

pub trait WsHandler {
    type Serverbound: DeserializeOwned;
    type Clientbound: Serialize;

    fn receive(&mut self, message: Self::Serverbound) -> impl Future<Output = ()> + Send;
    fn close(&mut self) -> impl Future<Output = ()> + Send;
    fn set_send_handler(&mut self, handler: Box<dyn Fn(Self::Clientbound) + Send + Sync>);
}

async fn handle_request(
    mut request: Request<Incoming>,
    lobby: Arc<Lobby>,
    running_games: Arc<RunningGames>,
) -> anyhow::Result<Response<Full<Bytes>>> {
    if hyper_tungstenite::is_upgrade_request(&request) {
        let path = request.uri().path();
        if path == "/ws/lobby" {
            let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)?;

            tokio::spawn(async move {
                if let Err(e) = serve_websocket(websocket, lobby.new_handler()).await {
                    error!("error in websocket connection: {e}");
                }
            });

            Ok(response)
        } else if path == "/ws/game" {
            let Some(id) = request
                .uri()
                .query()
                .filter(|x| x.starts_with("id="))
                .map(|x| &x[3..])
                .map(String::from)
            else {
                return Ok(Response::builder()
                    .status(400)
                    .body(Full::from(r#"{"error": "expected game ID param"}"#))
                    .unwrap());
            };

            let Some(game) = running_games.game(&id) else {
                return Ok(Response::builder()
                    .status(400)
                    .body(Full::from(r#"{"error": "game not found"}"#))
                    .unwrap());
            };
            let handler = game.new_handler();

            let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)?;

            tokio::spawn(async move {
                if let Err(e) = serve_websocket(websocket, handler).await {
                    error!("error in websocket connection: {e}");
                }
            });

            Ok(response)
        } else {
            Ok(Response::builder()
                .status(400)
                .body(Full::from(
                    r#"{"error": "incorrect URL for websocket request"}"#,
                ))
                .unwrap())
        }
    } else {
        const PAGE: &str = indoc! {"
            <!DOCTYPE html>
            <html>
                <head>
                    <title>Hopdot server</title>
                </head>
                <body>
                    <p>Hi there! This server is intended as a WebSocket server. Play Hopdot at <a href=\"https://rdrpenguin.itch.io/hopdot\">https://rdrpenguin.itch.io/hopdot</a>!</p>
                </body>
            </html>
        "};
        Ok(Response::new(Full::from(PAGE)))
    }
}

async fn serve_websocket<T: Serialize + Send + 'static, U: DeserializeOwned + Send>(
    websocket: HyperWebsocket,
    mut handler: impl WsHandler<Serverbound = U, Clientbound = T> + Send,
) -> anyhow::Result<()> {
    let mut websocket = websocket.await?;
    let (tx, mut rx) = mpsc::unbounded_channel::<T>();
    let is_closing = Arc::new(AtomicBool::new(false));
    {
        let is_closing = is_closing.clone();
        handler.set_send_handler(Box::new(move |x| {
            if !is_closing.load(Ordering::Relaxed) {
                tx.send(x).ok();
            }
        }));
    }
    loop {
        futures_util::select! {
            outbound = rx.recv().fuse() => if let Some(x) = outbound {
                websocket.send(Message::Binary(bson::serialize_to_vec(&x)?.into())).await?;
            } else {
                websocket.send(Message::Close(None)).await?;
                break;
            },
            inbound = websocket.next() => if let Some(x) = inbound {
                #[allow(clippy::single_match)] // Will support Message::Text as an alternate channel in the future
                match x? {
                    Message::Binary(x) => {
                        let inbound = bson::deserialize_from_slice(&x);
                        match inbound {
                            Ok(inbound) => handler.receive(inbound).await,
                            Err(err) => websocket.send(Message::Binary(bson::serialize_to_vec(&bson::bson!({
                                "error": "deserialization error",
                                "details": err.to_string(),
                            }))?.into())).await?,
                        }
                    }
                    Message::Close(_) => {
                        is_closing.store(true, Ordering::Relaxed);
                        handler.close().await;
                    }
                    _ => {}
                }
            } else {
                break;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .try_init()?;

    let mut addr: std::net::SocketAddr = "[::1]:8080".parse()?;
    if let Ok(port) = std::env::var("PORT") {
        addr.set_port(port.parse()?);
    }
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("listening on http://{addr}");

    let running_games = Arc::new(RunningGames::new());
    let lobby = Arc::new(Lobby::new(running_games.clone()));

    loop {
        let (stream, _) = listener.accept().await?;

        tokio::spawn({
            let lobby = lobby.clone();
            let running_games = running_games.clone();
            async move {
                let mut http = auto::Builder::new(TokioExecutor::new());
                http.http1().keep_alive(true);
                http.http2()
                    .keep_alive_interval(Some(Duration::from_secs(20)));

                let connection = http.serve_connection_with_upgrades(
                    TokioIo::new(stream),
                    hyper::service::service_fn(|x| {
                        handle_request(x, lobby.clone(), running_games.clone())
                    }),
                );

                if let Err(e) = connection.await {
                    error!("error serving HTTP connection: {e}");
                }
            }
        });
    }
}
