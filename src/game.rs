use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use common::{grid::Grid, proto::CellState};
use dashmap::{DashMap, mapref::one::Ref};
use rand::seq::SliceRandom as _;
use serde::{Deserialize, Serialize};

use crate::{GameSettings, WsHandler};

#[derive(Default)]
pub struct RunningGames {
    games: DashMap<String, Game>,
    gc_running: AtomicBool,
}

impl RunningGames {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn game(&self, game_id: &str) -> Option<Ref<'_, String, Game>> {
        self.games.get(game_id)
    }

    pub fn new_game(self: Arc<Self>, game_id: String, settings: GameSettings) {
        self.games.insert(game_id, Game::new(settings));
        if self.games.len() > 100 && self.gc_running.fetch_or(true, Ordering::Relaxed) {
            std::thread::spawn({
                move || {
                    self.games.retain(|_, x| {
                        x.data
                            .lock()
                            .map(|x| !x.remaining_players.is_empty())
                            .unwrap_or(false)
                    });
                    self.gc_running.store(false, Ordering::Relaxed);
                }
            });
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "ty", rename_all = "snake_case")]
pub enum GameServerbound {
    Move { x: u8, y: u8 },
    Resign,
}

#[derive(Clone, Copy, Serialize)]
pub enum LeaveReason {
    Disconnected,
    NoLegalMoves,
    Resigned,
}

#[derive(Clone, Serialize)]
#[serde(tag = "ty", rename_all = "snake_case")]
pub enum GameClientbound {
    OutOfTurn,
    InvalidMove {
        grid: Vec<u8>, // Vec<CellState>
    },
    Move {
        player: u8,
        x: u8,
        y: u8,
    },
    GameStart {
        me: u8,
    },
    PlayerEliminated {
        player: u8,
        reason: LeaveReason,
    },
    WaitingFor {
        players: u8,
    },
    Turn {
        player: u8,
    },
    GameWin {
        player: u8,
    },
}

pub struct GameData {
    grid: Grid,
    // TODO: simplify this type
    #[allow(clippy::type_complexity)]
    senders: Vec<(Arc<dyn Fn(GameClientbound) + Send + Sync>, u8)>,
    remaining_players: Vec<u8>,
    waiting_count: u8,
    cur_player: u8,
}

impl GameData {
    const GAME_OVER_SENTINEL: u8 = 254;
    const SPECTATOR_SENTINEL: u8 = 255;

    fn play_move(&mut self, player: u8, x: u8, y: u8) -> bool {
        if self.grid[y][x].owner != 0 && self.grid[y][x].owner != player {
            return false;
        }
        self.broadcast(GameClientbound::Move { player, x, y });
        let (new_grid, _) = self.grid.with_move(x, y, player);
        let losers = if let Some(new_grid) = new_grid {
            self.grid = new_grid;
            self.remaining_players
                .iter()
                .copied()
                .filter(|&x| {
                    !self
                        .grid
                        .grid_inner()
                        .iter()
                        .any(|cell| cell.owner == 0 || cell.owner == x)
                })
                .collect::<Vec<_>>()
        } else {
            self.remaining_players
                .iter()
                .copied()
                .filter(|&x| x != player)
                .collect::<Vec<_>>()
        };
        for l in losers {
            self.lose(l, LeaveReason::NoLegalMoves);
        }
        self.advance_turn();
        true
    }

    fn ready(&mut self, player: &GameHandler) {
        self.senders
            .push((player.sender.as_ref().unwrap().clone(), player.me));
        self.waiting_count -= 1;
        if self.waiting_count == 0 {
            for (sender, player) in &self.senders {
                sender(GameClientbound::GameStart { me: *player });
            }
            self.remaining_players = (1..=self.senders.len()).map(|x| x as u8).collect();
            self.broadcast(GameClientbound::Turn { player: 1 });
        } else {
            self.broadcast(GameClientbound::WaitingFor {
                players: self.waiting_count,
            });
        }
    }

    fn broadcast(&self, msg: GameClientbound) {
        for (sender, _) in &self.senders {
            sender(msg.clone());
        }
    }

    fn compressed_grid(&self) -> Vec<u8> {
        self.grid
            .grid_inner()
            .iter()
            .map(CellState::from_grid_cell)
            .map(|x| x.inner())
            .collect()
    }

    fn advance_turn(&mut self) {
        if self.cur_player == Self::GAME_OVER_SENTINEL {
            return;
        }
        let max_player = *self.remaining_players.last().unwrap();
        loop {
            self.cur_player += 1;
            if self.cur_player > max_player {
                self.cur_player = 0;
            }
            if self.remaining_players.contains(&self.cur_player) {
                break;
            }
        }
        self.broadcast(GameClientbound::Turn {
            player: self.cur_player,
        });
    }

    fn lose(&mut self, player: u8, reason: LeaveReason) {
        self.broadcast(GameClientbound::PlayerEliminated { player, reason });
        self.remaining_players.retain(|&x| x != player);
        if self.remaining_players.len() == 1 {
            self.broadcast(GameClientbound::GameWin {
                player: self.remaining_players[0],
            });
            self.cur_player = Self::GAME_OVER_SENTINEL; // Now it's nobody's turn!
        } else if self.cur_player == player {
            self.advance_turn();
        }
    }
}

pub struct Game {
    data: Arc<Mutex<GameData>>,
}

impl Game {
    pub fn new(settings: GameSettings) -> Self {
        let mut grid = Grid::new(settings.width, settings.height, settings.capacity);
        grid.init_capacity();
        let mut remaining_players = (1..=settings.capacity).collect::<Vec<_>>();
        remaining_players.shuffle(&mut rand::rng());
        Self {
            data: Arc::new(Mutex::new(GameData {
                grid,
                senders: Vec::new(),
                remaining_players,
                waiting_count: settings.capacity,
                cur_player: 1,
            })),
        }
    }

    pub fn new_handler(
        &self,
    ) -> impl WsHandler<Serverbound = GameServerbound, Clientbound = GameClientbound> + Send + Sync + use<>
    {
        let mut data = self.data.lock().unwrap();
        GameHandler::new(
            self,
            data.remaining_players
                .pop()
                .unwrap_or(GameData::SPECTATOR_SENTINEL),
        )
    }
}

pub struct GameHandler {
    me: u8,
    sender: Option<Arc<dyn Fn(GameClientbound) + Send + Sync>>,
    game_data: Arc<Mutex<GameData>>,
}

impl GameHandler {
    pub fn new(game: &Game, me: u8) -> Self {
        Self {
            me,
            sender: None,
            game_data: game.data.clone(),
        }
    }

    fn send(&self, data: GameClientbound) {
        (self.sender.as_ref().unwrap())(data);
    }
}

impl WsHandler for GameHandler {
    type Serverbound = GameServerbound;
    type Clientbound = GameClientbound;

    async fn receive(&mut self, message: GameServerbound) {
        let mut data = self.game_data.lock().unwrap();
        match message {
            GameServerbound::Move { x, y } => {
                if self.me != data.cur_player {
                    self.send(GameClientbound::OutOfTurn);
                } else if !data.play_move(self.me, x, y) {
                    self.send(GameClientbound::InvalidMove {
                        grid: data.compressed_grid(),
                    });
                }
            }
            GameServerbound::Resign => {
                data.lose(self.me, LeaveReason::Resigned);
            }
        }
    }

    async fn close(&mut self) {
        self.game_data
            .lock()
            .unwrap()
            .lose(self.me, LeaveReason::Disconnected);
    }

    fn set_send_handler(&mut self, handler: Box<dyn Fn(GameClientbound) + Send + Sync>) {
        self.sender = Some(handler.into());
        self.game_data.lock().unwrap().ready(self);
    }
}
