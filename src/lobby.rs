use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Arc, Mutex},
};

use rand::{Rng, distr::Uniform};
use rustrict::CensorStr as _;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::{GameSettings, WsHandler, game::RunningGames};

struct RoomData {
    settings: GameSettings,
    sockets: Vec<Arc<dyn Fn(LobbyClientbound) + Send + Sync>>,
}

#[derive(Default)]
struct LobbyData {
    rooms: HashMap<String, RoomData>,
}

pub struct Lobby {
    data: Arc<Mutex<LobbyData>>,
    running_games: Arc<RunningGames>,
}

impl Lobby {
    pub fn new(running_games: Arc<RunningGames>) -> Self {
        Self {
            data: Arc::default(),
            running_games,
        }
    }

    pub fn new_handler(
        &self,
    ) -> impl WsHandler<Serverbound = LobbyServerbound, Clientbound = LobbyClientbound>
    + Send
    + Sync
    + use<> {
        LobbyHandler::new(self)
    }
}

#[derive(Deserialize)]
#[serde(tag = "ty", rename_all = "snake_case")]
pub enum LobbyServerbound {
    New(GameSettings),
    Join { code: String },
}

#[derive(Serialize)]
#[serde(tag = "ty", rename_all = "snake_case")]
pub enum LobbyClientbound {
    Created {
        code: String,
    },
    Ready {
        code: String,
        settings: GameSettings,
    },
    RoomNotFound {
        code: String,
    },
}

struct LobbyHandler {
    sender: Option<Arc<dyn Fn(LobbyClientbound) + Send + Sync>>,
    lobby_data: Arc<Mutex<LobbyData>>,
    running_games: Arc<RunningGames>,
}

impl LobbyHandler {
    pub fn new(lobby: &Lobby) -> Self {
        Self {
            sender: None,
            lobby_data: lobby.data.clone(),
            running_games: lobby.running_games.clone(),
        }
    }

    fn send(&self, data: LobbyClientbound) {
        (self.sender.as_ref().unwrap())(data);
    }
}

impl WsHandler for LobbyHandler {
    type Serverbound = LobbyServerbound;
    type Clientbound = LobbyClientbound;

    async fn receive(&mut self, message: LobbyServerbound) {
        match message {
            LobbyServerbound::New(settings) => {
                loop {
                    let code = rand::rng()
                        .sample_iter(Uniform::new(0, 16).unwrap())
                        .take(4)
                        .map(|x| {
                            [
                                'A', 'E', 'G', 'I', 'K', 'L', 'N', 'O', 'P', 'S', 'T', 'U', 'V',
                                'X', 'Y', 'Z',
                            ][x]
                        })
                        .collect::<String>();
                    if code.is_inappropriate() {
                        continue;
                    }
                    match self.lobby_data.lock().unwrap().rooms.entry(code.clone()) {
                        Entry::Occupied(_) => {} // try again
                        Entry::Vacant(x) => {
                            x.insert(RoomData {
                                settings,
                                sockets: vec![self.sender.clone().unwrap()],
                            });
                            info!("created room with code {code}");
                            self.send(LobbyClientbound::Created { code });
                            break;
                        }
                    }
                }
            }
            LobbyServerbound::Join { code } => {
                if let Entry::Occupied(mut room) =
                    self.lobby_data.lock().unwrap().rooms.entry(code.clone())
                {
                    info!("new player joining room {code}");
                    let room_inner = room.get_mut();
                    room_inner.sockets.push(self.sender.clone().unwrap());
                    if room_inner.sockets.len() == room_inner.settings.capacity as usize {
                        let room = room.remove();
                        info!("room {code} filled, announcing room settings");
                        for sender in &room.sockets {
                            sender(LobbyClientbound::Ready {
                                code: code.clone(),
                                settings: room.settings,
                            });
                        }
                        self.running_games.clone().new_game(code, room.settings);
                    }
                } else {
                    self.send(LobbyClientbound::RoomNotFound { code });
                }
            }
        }
    }

    // Clippy bug, or at least bad suggestion
    #[allow(clippy::significant_drop_tightening)]
    async fn close(&mut self) {
        info!("player left, clearing any rooms they were in");
        let mut data = self.lobby_data.lock().unwrap();
        let rooms: Vec<_> = data
            .rooms
            .iter()
            .enumerate()
            .filter(|(_, (_, x))| {
                x.sockets
                    .iter()
                    .any(|x| Arc::ptr_eq(x, self.sender.as_ref().unwrap()))
            })
            .map(|(x, (y, _))| (x, y.clone()))
            .collect();
        for (idx, code) in rooms {
            let room = data.rooms.get_mut(&code).unwrap();
            if room.sockets.len() == 1 {
                debug!("player was alone in room {code}, removing the room");
                data.rooms.remove(&code);
            } else {
                debug!("player was not alone in room {code}, removing them");
                room.sockets.remove(idx);
            }
        }
    }

    fn set_send_handler(&mut self, handler: Box<dyn Fn(LobbyClientbound) + Send + Sync>) {
        self.sender = Some(handler.into());
    }
}
