//! rustygrator desktop app (section 9): browse PostgreSQL, MongoDB, SQLite and ScyllaDB
//! databases, run read-only queries, start migrations and live transfers.
//!
//! The UI follows the Elm architecture of iced (APP-5): state -> `view` -> `Message` -> `update`.
//! Database work runs on a separate tokio runtime of `core`; `update` only starts it and receives
//! its results as messages, so the UI thread never waits for a database.

mod connections;
mod inspector;
mod jobs;
mod migration;
mod platform;
mod pool;
mod results;
mod theme;
mod view;
mod widgets;
#[cfg(test)]
mod ui_bench;
#[cfg(test)]
mod ui_tests;

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use iced::{keyboard, widget::text_editor, window, Color, Element, Size, Subscription, Task, Theme};
use tokio::sync::{mpsc, watch};

use rustygrator_core::{
    browse::{Canceller, Query, QueryRunner, Sort},
    dbc::batch::ColumnBatch,
    migrator::{batching::BatchSizer, migrator::Migrator, migrator_steps::DataProvider},
    models::{data_definitions::DataDefinition, migration_execution_settings::ContainerFilter},
};

use connections::{Profile, SshTunnel, Store, Tls};
use jobs::Job;
use migration::Migration;
use platform::Command;
use pool::{Pool, Status};
use results::ResultWindow;
use theme::Tokens;
use widgets::tree::TreeRow;

/// Process start, for the startup probe (APERF-1).
static STARTED: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn main() -> iced::Result {
    STARTED.get_or_init(Instant::now);
    env_logger::init();
    iced::daemon(App::boot, App::update, App::view)
        .title(App::title)
        .theme(App::theme)
        .subscription(App::subscription)
        .scale_factor(App::scale_factor)
        .antialiasing(true)
        .run()
}

/// What the database stream of a tab sends (VIEW-2).
#[derive(Debug, Clone)]
pub enum StreamMsg {
    Columns(Arc<DataDefinition>),
    Batch(Arc<ColumnBatch>),
    Done(Result<u64, String>),
}

pub struct Stream {
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<StreamMsg>>>,
    abort: tokio::task::AbortHandle,
    canceller: Arc<Mutex<Option<Canceller>>>,
    pulling: bool,
    generation: u64,
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TabKind {
    Table,
    Query,
    Migration,
}

pub struct Tab {
    pub id: u64,
    pub conn: String,
    pub title: String,
    pub kind: TabKind,
    pub def: Option<Arc<DataDefinition>>,
    pub query: Option<Query>,
    pub results: Option<ResultWindow>,
    pub stream: Option<Stream>,
    pub error: Option<String>,
    pub selected: Option<(u64, usize)>,
    pub sort: Option<(usize, bool)>,
    pub filter: String,
    pub view: (u64, u64),
    pub started: Option<Instant>,
    pub elapsed: Option<Duration>,
    pub editor: text_editor::Content,
    pub generation: u64,
    pub migration: Option<Migration>,
    pub window: Option<window::Id>,
}

impl Tab {
    fn new(id: u64, conn: String, title: String, kind: TabKind) -> Self {
        Tab {
            id,
            conn,
            title,
            kind,
            def: None,
            query: None,
            results: None,
            stream: None,
            error: None,
            selected: None,
            sort: None,
            filter: String::new(),
            view: (0, 0),
            started: None,
            elapsed: None,
            editor: text_editor::Content::new(),
            generation: 0,
            migration: None,
            window: None,
        }
    }

    pub fn running(&self) -> bool {
        self.stream.is_some() && self.results.as_ref().is_none_or(|r| !r.complete) && self.error.is_none()
            || self.migration.as_ref().is_some_and(|m| m.running.is_some())
    }
}

pub struct Conn {
    pub id: String,
    pub pool: Option<Arc<Pool>>,
    pub status: Option<Status>,
    pub expanded: bool,
    pub defs: Option<Vec<Arc<DataDefinition>>>,
    pub error: Option<String>,
    /// Expanded nodes below the connection: "s:<schema>", "t:<table>", "c:<table>:<section>".
    pub open: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Conn(String),
    Schema(String, String),
    Table(String, String),
    Section(String, String, &'static str),
    Item,
}

/// Connection editor (CONN-1, CONN-3).
#[derive(Debug, Clone, Default)]
pub struct ConnForm {
    pub editing: Option<String>,
    pub name: String,
    pub url: String,
    pub password: String,
    pub tls: Tls,
    pub ssh_on: bool,
    pub ssh_host: String,
    pub ssh_port: String,
    pub ssh_user: String,
    pub ssh_key: String,
    pub testing: bool,
    pub test: Option<Result<String, String>>,
}

impl ConnForm {
    fn profile(&self) -> Result<Profile, String> {
        if self.name.trim().is_empty() {
            return Err("Enter a name".into());
        }
        let (url, inline_pw) = connections::split_password(self.url.trim());
        if inline_pw.is_some() && !self.password.is_empty() {
            return Err("Enter the password either in the connection string or in the password field".into());
        }
        rustygrator_core::models::connection::MigrationConnection::new(&url).map_err(|e| format!("Connection string: {e}"))?;
        let ssh = if self.ssh_on {
            Some(SshTunnel {
                host: self.ssh_host.trim().to_string(),
                port: self.ssh_port.trim().parse().map_err(|_| "SSH port must be a number".to_string())?,
                user: self.ssh_user.trim().to_string(),
                key_file: Some(PathBuf::from(self.ssh_key.trim())).filter(|p| !p.as_os_str().is_empty()),
            })
            .filter(|s| !s.host.is_empty())
            .ok_or("Enter the SSH host")?
            .into()
        } else {
            None
        };
        Ok(Profile { id: self.editing.clone().unwrap_or_else(connections::new_id), name: self.name.trim().to_string(), url, tls: self.tls.clone(), ssh })
    }

    fn password(&self) -> Option<String> {
        let inline = connections::split_password(self.url.trim()).1;
        inline.or_else(|| Some(self.password.clone()).filter(|p| !p.is_empty()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Confirm {
    DeleteConnection(String),
    StartMigration(u64, Vec<String>),
    CloseRunningTab(u64),
}

pub enum Sheet {
    Connection(Box<ConnForm>),
    Confirm(Confirm),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileTarget {
    SqliteFile,
    Ca,
    ClientCert,
    ClientKey,
    SshKey,
    JobKey(u64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FormField {
    Name(String),
    Url(String),
    Password(String),
    Tls(connections::TlsMode),
    SshOn(bool),
    SshHost(String),
    SshPort(String),
    SshUser(String),
    SshKey(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum JobField {
    Source(String),
    Target(String),
    Strategy(String),
    Transfer(String),
    Batch(String),
    Compression(String),
    KeyFile(String),
    Include(String),
    Exclude(String),
    Live(bool),
    Name(String),
}

#[derive(Debug, Clone)]
pub enum Message {
    WindowOpened(window::Id),
    WindowClosed(window::Id),
    ThemeMode(iced::theme::Mode),
    Accent(Option<Color>),
    Key(keyboard::Event),
    Command(Command),
    MenuOpen(Option<&'static str>),
    // sidebar
    SideSelect(usize),
    SideToggle(usize),
    SideActivate(usize),
    PoolReady(String, Result<Arc<Pool>, String>),
    DefsLoaded(String, Result<Vec<Arc<DataDefinition>>, String>),
    Reconnect,
    // connection sheet
    EditConnection(Option<String>),
    Form(FormField),
    PickFile(FileTarget),
    FilePicked(FileTarget, Option<PathBuf>),
    TestConnection,
    TestResult(Result<String, String>),
    SaveConnection,
    AskDeleteConnection(String),
    Confirmed,
    CloseSheet,
    // tabs
    SelectTab(u64),
    CloseTab(u64),
    Stream(u64, u64, Option<StreamMsg>),
    GridView(u64, u64, u64),
    GridSelect(u64, u64, usize),
    GridSort(u64, usize),
    FilterChanged(u64, String),
    ApplyFilter(u64),
    ClearFilter(u64),
    Edit(u64, text_editor::Action),
    Run(u64),
    Cancel(u64),
    // inspector
    InspectorToggle(usize),
    // migration
    Job(u64, JobField),
    StartMigration(u64),
    OverwriteCheck(u64, Result<Vec<String>, String>),
    MigrationStarted(u64, Result<Arc<rustygrator_core::migrator::migrator_steps::MigrationStats>, String>),
    MigrationDone(u64, Result<String, String>),
    StopMigration(u64),
    SaveJob(u64),
    OpenJob(usize),
    CopyCommand(u64),
    Tick(Instant),
    /// First frame drawn with `DM_STARTUP_PROBE` set (APERF-1)
    FirstFrame,
    DismissBanner,
}

pub struct App {
    rt: Arc<tokio::runtime::Runtime>,
    pub store: Store,
    store_path: PathBuf,
    pub dark: bool,
    pub accent: Option<Color>,
    pub scale: f32,
    pub main: Option<window::Id>,
    pub conns: Vec<Conn>,
    pub side: Vec<TreeRow>,
    pub side_nodes: Vec<Node>,
    pub side_sel: Option<usize>,
    pub tabs: Vec<Tab>,
    pub active: Option<u64>,
    next_id: u64,
    pub inspector: bool,
    pub inspector_open: HashSet<String>,
    /// JSON tree of the selected value (VIEW-7): rows and node paths, rebuilt when the selection changes.
    pub inspector_tree: Option<(Vec<TreeRow>, Vec<String>)>,
    pub sheet: Option<Sheet>,
    pub banner: Option<String>,
    pub menu_open: Option<&'static str>,
    #[cfg(target_os = "macos")]
    menu: Option<muda::Menu>,
}

fn main_window_settings() -> window::Settings {
    window::Settings {
        size: Size::new(1280.0, 800.0),
        min_size: Some(Size::new(900.0, 560.0)),
        #[cfg(target_os = "macos")]
        platform_specific: window::settings::PlatformSpecific { title_hidden: true, titlebar_transparent: true, fullsize_content_view: true },
        ..window::Settings::default()
    }
}

impl App {
    fn boot() -> (App, Task<Message>) {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().thread_name("rustygrator-core").build().expect("tokio runtime");
        let store_path = Store::path();
        let store = Store::load(&store_path);
        let conns = store.profiles.iter().map(|p| Conn { id: p.id.clone(), pool: None, status: None, expanded: false, defs: None, error: None, open: HashSet::new() }).collect();
        let (_, open) = window::open(main_window_settings());
        let mut app = App {
            rt: Arc::new(rt),
            store,
            store_path,
            dark: false,
            accent: None,
            scale: 1.0,
            main: None,
            conns,
            side: Vec::new(),
            side_nodes: Vec::new(),
            side_sel: None,
            tabs: Vec::new(),
            active: None,
            next_id: 1,
            inspector: true,
            inspector_open: HashSet::new(),
            inspector_tree: None,
            sheet: None,
            banner: None,
            menu_open: None,
            #[cfg(target_os = "macos")]
            menu: None,
        };
        app.rebuild_sidebar();
        // the accent color query starts a process: off the UI thread and after the window opens
        let accent = Task::perform(async { tokio::task::spawn_blocking(platform::accent_color).await.ok().flatten() }, Message::Accent);
        (app, Task::batch([open.map(Message::WindowOpened), iced::system::theme().map(Message::ThemeMode), accent]))
    }

    pub fn tokens(&self) -> Tokens {
        Tokens::for_dark(self.dark).with_accent(self.accent)
    }

    fn title(&self, window: window::Id) -> String {
        match self.tabs.iter().find(|t| t.window == Some(window)) {
            Some(t) => format!("{} — rustygrator", t.title),
            None => "rustygrator".to_string(),
        }
    }

    fn theme(&self, _window: window::Id) -> Theme {
        self.tokens().theme()
    }

    fn scale_factor(&self, _window: window::Id) -> f32 {
        self.scale
    }

    fn subscription(&self) -> Subscription<Message> {
        let mut subs = vec![
            window::close_events().map(Message::WindowClosed),
            iced::system::theme_changes().map(Message::ThemeMode),
            keyboard::listen().map(Message::Key),
        ];
        if std::env::var_os("DM_STARTUP_PROBE").is_some() {
            subs.push(window::frames().map(|_| Message::FirstFrame));
        }
        if self.tabs.iter().any(|t| t.migration.as_ref().is_some_and(|m| m.running.is_some())) {
            subs.push(iced::time::every(migration::TICK).map(Message::Tick));
        }
        if self.conns.iter().any(|c| matches!(c.status, Some(Status::Failed(_))) && c.pool.is_some()) {
            subs.push(iced::time::every(Duration::from_secs(5)).map(|_| Message::Reconnect));
        }
        #[cfg(target_os = "macos")]
        subs.push(Subscription::run(menu_events));
        Subscription::batch(subs)
    }

    pub fn profile(&self, id: &str) -> Option<&Profile> {
        self.store.profiles.iter().find(|p| p.id == id)
    }

    fn conn_mut(&mut self, id: &str) -> Option<&mut Conn> {
        self.conns.iter_mut().find(|c| c.id == id)
    }

    pub fn tab(&self, id: u64) -> Option<&Tab> {
        self.tabs.iter().find(|t| t.id == id)
    }

    fn tab_mut(&mut self, id: u64) -> Option<&mut Tab> {
        self.tabs.iter_mut().find(|t| t.id == id)
    }

    pub fn active_tab(&self) -> Option<&Tab> {
        self.active.and_then(|id| self.tab(id))
    }

    fn save_store(&mut self) {
        if let Err(e) = self.store.save(&self.store_path) {
            self.banner = Some(format!("Could not save connections to {}: {e}", self.store_path.display()));
        }
    }

    fn new_tab(&mut self, conn: String, title: String, kind: TabKind) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.tabs.push(Tab::new(id, conn, title, kind));
        self.active = Some(id);
        id
    }

    /// Rows of the sidebar tree from the open nodes (VIEW-1).
    pub fn rebuild_sidebar(&mut self) {
        let t = self.tokens();
        let mut rows = Vec::new();
        let mut nodes = Vec::new();
        for (p, c) in self.store.profiles.iter().zip(&self.conns) {
            let badge = match &c.status {
                Some(Status::Connected) => Some(t.success),
                Some(Status::Failed(_)) => Some(t.danger),
                Some(Status::Connecting) => Some(t.warning),
                None => Some(t.separator),
            };
            let detail = match (&c.status, &c.defs, &c.error) {
                (_, _, Some(_)) => "error".to_string(),
                (Some(Status::Connecting), _, _) => "connecting…".to_string(),
                (_, None, _) if c.expanded => "loading…".to_string(),
                _ => scheme_label(&p.url).to_string(),
            };
            rows.push(TreeRow { depth: 0, label: p.name.clone(), detail, expandable: true, expanded: c.expanded, badge });
            nodes.push(Node::Conn(c.id.clone()));
            let (true, Some(defs)) = (c.expanded, &c.defs) else { continue };
            let mut schemas: Vec<(&str, Vec<&Arc<DataDefinition>>)> = Vec::new();
            for d in defs {
                let schema = d.container_name.split_once('.').map(|s| s.0).unwrap_or("");
                match schemas.iter_mut().find(|s| s.0 == schema) {
                    Some(s) => s.1.push(d),
                    None => schemas.push((schema, vec![d])),
                }
            }
            for (schema, tables) in schemas {
                let depth = if schema.is_empty() {
                    1
                } else {
                    let open = c.open.contains(&format!("s:{schema}"));
                    rows.push(TreeRow { depth: 1, label: schema.to_string(), detail: tables.len().to_string(), expandable: true, expanded: open, badge: None });
                    nodes.push(Node::Schema(c.id.clone(), schema.to_string()));
                    if !open {
                        continue;
                    }
                    2
                };
                for d in tables {
                    let name = &d.container_name;
                    let open = c.open.contains(&format!("t:{name}"));
                    let short = name.split_once('.').map(|s| s.1).unwrap_or(name);
                    rows.push(TreeRow { depth, label: short.to_string(), detail: String::new(), expandable: true, expanded: open, badge: None });
                    nodes.push(Node::Table(c.id.clone(), name.clone()));
                    if !open {
                        continue;
                    }
                    let sections: [(&'static str, Vec<(String, String)>); 3] = [
                        ("Columns", d.properties_info.iter().map(|p| (p.property_name.clone(), format!("{}{}", p.source_type.clone().unwrap_or_else(|| format!("{:?}", p.data_type)), if p.is_identifier { " · key" } else { "" }))).collect()),
                        ("Indexes", d.indexes.iter().map(|i| (i.name.clone(), format!("{}{}", i.columns.join(", "), if i.unique { " · unique" } else { "" }))).collect()),
                        ("Foreign keys", d.foreign_keys.iter().map(|f| (f.columns.join(", "), format!("→ {}", f.reference_container_name))).collect()),
                    ];
                    for (section, items) in sections {
                        if items.is_empty() {
                            continue;
                        }
                        let key = format!("c:{name}:{section}");
                        let open = c.open.contains(&key);
                        rows.push(TreeRow { depth: depth + 1, label: section.to_string(), detail: items.len().to_string(), expandable: true, expanded: open, badge: None });
                        nodes.push(Node::Section(c.id.clone(), name.clone(), section));
                        if open {
                            for (label, detail) in items {
                                rows.push(TreeRow { depth: depth + 2, label, detail, expandable: false, expanded: false, badge: None });
                                nodes.push(Node::Item);
                            }
                        }
                    }
                }
            }
        }
        self.side = rows;
        self.side_nodes = nodes;
        if self.side_sel.is_some_and(|s| s >= self.side.len()) {
            self.side_sel = None;
        }
    }

    /// Opens the pool of a connection (tunnel, keychain) and then loads its tables.
    fn open_connection(&mut self, id: &str) -> Task<Message> {
        let Some(profile) = self.profile(id).cloned() else { return Task::none() };
        if let Some(c) = self.conn_mut(id) {
            if let Some(pool) = c.pool.clone() {
                return load_defs(id.to_string(), pool, &self.rt);
            }
            c.status = Some(Status::Connecting);
            c.error = None;
        }
        let id = id.to_string();
        let rt = self.rt.clone();
        Task::perform(
            async move {
                rt.spawn_blocking(move || {
                    let pw = connections::load_password(&profile.id)?;
                    Pool::new(&profile, pw.as_deref()).map(Arc::new)
                })
                .await
                .map_err(|e| e.to_string())?
            },
            move |r| Message::PoolReady(id.clone(), r),
        )
    }

    fn command(&mut self, c: Command) -> Task<Message> {
        self.menu_open = None;
        match c {
            Command::NewConnection => {
                self.sheet = Some(Sheet::Connection(Box::new(ConnForm { ssh_port: "22".into(), ..ConnForm::default() })));
                Task::none()
            }
            Command::NewQuery => self.new_query(),
            Command::NewMigration => {
                let id = self.new_tab(String::new(), "Migration".into(), TabKind::Migration);
                let mut job = Job::default();
                if let Some(p) = self.store.profiles.first() {
                    job.source = p.id.clone();
                }
                if let Some(p) = self.store.profiles.get(1) {
                    job.target = p.id.clone();
                }
                if let Some(t) = self.tab_mut(id) {
                    t.migration = Some(Migration::new(job));
                }
                Task::none()
            }
            Command::CloseTab => self.active.map(|id| self.update(Message::CloseTab(id))).unwrap_or_else(Task::none),
            Command::OpenInWindow => {
                let Some(id) = self.active.filter(|id| self.tab(*id).is_some_and(|t| t.window.is_none())) else { return Task::none() };
                let (wid, open) = window::open(window::Settings { size: Size::new(960.0, 640.0), ..main_window_settings() });
                if let Some(t) = self.tab_mut(id) {
                    t.window = Some(wid);
                }
                self.active = self.tabs.iter().find(|t| t.window.is_none()).map(|t| t.id);
                open.discard()
            }
            Command::Run => self.active.map(|id| self.update(Message::Run(id))).unwrap_or_else(Task::none),
            Command::Cancel => self.active.map(|id| self.update(Message::Cancel(id))).unwrap_or_else(Task::none),
            Command::ToggleInspector => {
                self.inspector = !self.inspector;
                Task::none()
            }
            Command::ZoomIn => {
                self.scale = (self.scale + 0.1).min(2.0);
                Task::none()
            }
            Command::ZoomOut => {
                self.scale = (self.scale - 0.1).max(0.7);
                Task::none()
            }
            Command::ZoomReset => {
                self.scale = 1.0;
                Task::none()
            }
            Command::Quit => iced::exit(),
        }
    }

    fn new_query(&mut self) -> Task<Message> {
        let conn = self
            .side_sel
            .and_then(|i| self.side_nodes.get(i))
            .and_then(|n| match n {
                Node::Conn(c) | Node::Schema(c, _) | Node::Table(c, _) | Node::Section(c, _, _) => Some(c.clone()),
                Node::Item => None,
            })
            .or_else(|| self.active_tab().map(|t| t.conn.clone()).filter(|c| !c.is_empty()))
            .or_else(|| self.store.profiles.first().map(|p| p.id.clone()));
        let Some(conn) = conn else {
            self.banner = Some("Add a connection first".into());
            return Task::none();
        };
        let name = self.profile(&conn).map(|p| p.name.clone()).unwrap_or_default();
        let n = self.tabs.iter().filter(|t| t.kind == TabKind::Query).count() + 1;
        let id = self.new_tab(conn.clone(), format!("Query {n} · {name}"), TabKind::Query);
        let template = match self.profile(&conn).map(|p| scheme_label(&p.url)) {
            Some("MongoDB") => "db.collection.find({}).limit(100)",
            _ => "SELECT * FROM ",
        };
        if let Some(t) = self.tab_mut(id) {
            t.editor = text_editor::Content::with_text(template);
        }
        let opening = self.conns.iter().find(|c| c.id == conn).is_some_and(|c| c.pool.is_none());
        if opening {
            if let Some(c) = self.conn_mut(&conn) {
                c.expanded = true;
            }
            self.rebuild_sidebar();
            return self.open_connection(&conn);
        }
        Task::none()
    }

    /// Opens a tab with the table (or focuses the open one).
    fn open_table(&mut self, conn: &str, table: &str) -> Task<Message> {
        if let Some(t) = self.tabs.iter().find(|t| t.kind == TabKind::Table && t.conn == conn && t.def.as_ref().is_some_and(|d| d.container_name == table)) {
            self.active = Some(t.id);
            return Task::none();
        }
        let Some(def) = self.conns.iter().find(|c| c.id == conn).and_then(|c| c.defs.as_ref()).and_then(|d| d.iter().find(|d| d.container_name == table)).cloned() else {
            return Task::none();
        };
        let short = table.split_once('.').map(|s| s.1).unwrap_or(table).to_string();
        let id = self.new_tab(conn.to_string(), short, TabKind::Table);
        if let Some(t) = self.tab_mut(id) {
            t.def = Some(def.clone());
            t.query = Some(Query::Table { def: (*def).clone(), filter: None, sort: None });
        }
        self.start_stream(id, 0)
    }

    /// Starts (or restarts) the database stream of a tab (VIEW-2).
    fn start_stream(&mut self, tab_id: u64, skip_to: u64) -> Task<Message> {
        let rt = self.rt.clone();
        let Some(tab) = self.tabs.iter().find(|t| t.id == tab_id) else { return Task::none() };
        let Some(query) = tab.query.clone() else { return Task::none() };
        let Some(pool) = self.conns.iter().find(|c| c.id == tab.conn).and_then(|c| c.pool.clone()) else {
            if let Some(t) = self.tab_mut(tab_id) {
                t.error = Some("Not connected".into());
            }
            return Task::none();
        };
        let tab = self.tab_mut(tab_id).expect("found above");
        tab.generation += 1;
        let generation = tab.generation;
        let restart = skip_to > 0 && tab.results.is_some();
        if restart {
            if let Some(r) = tab.results.as_mut() {
                r.restart(skip_to);
            }
        } else {
            tab.results = None;
            tab.selected = None;
            tab.started = Some(Instant::now());
            tab.elapsed = None;
        }
        tab.error = None;
        // capacity 1: the database cursor only moves when the grid takes the next batch
        let (tx, rx) = mpsc::channel::<StreamMsg>(1);
        let canceller: Arc<Mutex<Option<Canceller>>> = Arc::new(Mutex::new(None));
        let c2 = canceller.clone();
        let handle = rt.spawn(async move {
            let result = async {
                let mut conn = pool.get().await?;
                *c2.lock().unwrap_or_else(|e| e.into_inner()) = Some(conn.canceller());
                let def = match conn.describe(&query).await {
                    Ok(d) => Arc::new(d),
                    Err(e) => {
                        conn.broken = true;
                        return Err(e.to_string());
                    }
                };
                if tx.send(StreamMsg::Columns(def.clone())).await.is_err() {
                    return Ok(0);
                }
                let (btx, mut brx) = mpsc::channel::<ColumnBatch>(1);
                let fetch = conn.fetch(&query, &def, Arc::new(BatchSizer::fixed(results::BATCH_ROWS)), btx);
                let forward = async {
                    while let Some(b) = brx.recv().await {
                        if tx.send(StreamMsg::Batch(Arc::new(b))).await.is_err() {
                            break;
                        }
                    }
                };
                let (n, ()) = tokio::join!(fetch, forward);
                n.map_err(|e| e.to_string())
            }
            .await;
            let _ = tx.send(StreamMsg::Done(result)).await;
        });
        tab.stream = Some(Stream { rx: Arc::new(tokio::sync::Mutex::new(rx)), abort: handle.abort_handle(), canceller, pulling: false, generation });
        self.pull(tab_id)
    }

    /// Takes the next message of the tab's stream.
    fn pull(&mut self, tab_id: u64) -> Task<Message> {
        let Some(s) = self.tab_mut(tab_id).and_then(|t| t.stream.as_mut()) else { return Task::none() };
        if s.pulling {
            return Task::none();
        }
        s.pulling = true;
        let rx = s.rx.clone();
        let generation = s.generation;
        Task::perform(async move { rx.lock().await.recv().await }, move |m| Message::Stream(tab_id, generation, m))
    }

    fn table_query(&self, tab: &Tab) -> Option<Query> {
        let def = tab.def.as_ref()?;
        let filter = Some(tab.filter.trim().to_string()).filter(|f| !f.is_empty());
        let sort = tab.sort.and_then(|(c, desc)| def.properties_info.get(c).map(|p| Sort { column: p.property_name.clone(), descending: desc }));
        Some(Query::Table { def: (**def).clone(), filter, sort })
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::WindowOpened(id) => {
                if self.main.is_none() {
                    self.main = Some(id);
                    #[cfg(target_os = "macos")]
                    {
                        match platform::menu_bar::install() {
                            Ok(m) => self.menu = Some(m),
                            Err(e) => log::error!("menu bar: {e}"),
                        }
                    }
                }
                Task::none()
            }
            Message::WindowClosed(id) => {
                if Some(id) == self.main {
                    return iced::exit();
                }
                if let Some(t) = self.tabs.iter_mut().find(|t| t.window == Some(id)) {
                    t.window = None;
                    self.active = Some(t.id);
                }
                Task::none()
            }
            Message::ThemeMode(mode) => {
                self.dark = mode == iced::theme::Mode::Dark;
                self.rebuild_sidebar();
                Task::none()
            }
            Message::Accent(c) => {
                self.accent = c;
                Task::none()
            }
            Message::Key(keyboard::Event::KeyPressed { key, modifiers, .. }) => {
                use keyboard::{key::Named, Key};
                match key {
                    Key::Named(Named::Tab) => {
                        return if modifiers.shift() { iced::widget::operation::focus_previous() } else { iced::widget::operation::focus_next() };
                    }
                    Key::Named(Named::Escape) => {
                        self.menu_open = None;
                        self.sheet = None;
                        return Task::none();
                    }
                    _ => {}
                }
                // on macOS the menu bar handles its key equivalents
                if cfg!(target_os = "macos") {
                    return Task::none();
                }
                match Command::for_key(&key, modifiers) {
                    Some(c) => self.command(c),
                    None => Task::none(),
                }
            }
            Message::Key(_) => Task::none(),
            Message::Command(c) => self.command(c),
            Message::MenuOpen(m) => {
                self.menu_open = if self.menu_open == m { None } else { m };
                Task::none()
            }
            Message::DismissBanner => {
                self.banner = None;
                Task::none()
            }

            // ------------------------------------------------------------ sidebar
            Message::SideSelect(i) => {
                self.side_sel = Some(i);
                Task::none()
            }
            Message::SideToggle(i) => {
                let Some(node) = self.side_nodes.get(i).cloned() else { return Task::none() };
                self.side_sel = Some(i);
                let task = match &node {
                    Node::Conn(id) => {
                        let Some(c) = self.conn_mut(id) else { return Task::none() };
                        c.expanded = !c.expanded;
                        if c.expanded && c.defs.is_none() {
                            let id = id.clone();
                            self.open_connection(&id)
                        } else {
                            Task::none()
                        }
                    }
                    Node::Schema(id, s) => {
                        toggle(self.conn_mut(id), format!("s:{s}"));
                        Task::none()
                    }
                    Node::Table(id, t) => {
                        toggle(self.conn_mut(id), format!("t:{t}"));
                        Task::none()
                    }
                    Node::Section(id, t, s) => {
                        toggle(self.conn_mut(id), format!("c:{t}:{s}"));
                        Task::none()
                    }
                    Node::Item => Task::none(),
                };
                self.rebuild_sidebar();
                task
            }
            Message::SideActivate(i) => match self.side_nodes.get(i).cloned() {
                Some(Node::Table(c, t)) => self.open_table(&c, &t),
                Some(Node::Conn(id)) => self.update(Message::EditConnection(Some(id))),
                _ => Task::none(),
            },
            Message::PoolReady(id, r) => {
                let rt = self.rt.clone();
                let Some(c) = self.conn_mut(&id) else { return Task::none() };
                let task = match r {
                    Ok(pool) => {
                        c.pool = Some(pool.clone());
                        load_defs(id.clone(), pool, &rt)
                    }
                    Err(e) => {
                        c.status = Some(Status::Failed(e.clone()));
                        c.error = Some(e);
                        Task::none()
                    }
                };
                self.rebuild_sidebar();
                task
            }
            Message::DefsLoaded(id, r) => {
                if let Some(c) = self.conn_mut(&id) {
                    c.status = c.pool.as_ref().map(|p| p.status());
                    match r {
                        Ok(defs) => {
                            c.defs = Some(defs);
                            c.error = None;
                        }
                        Err(e) => {
                            c.error = Some(e.clone());
                            c.expanded = false;
                            self.banner = Some(format!("{}: {e}", self.store.profiles.iter().find(|p| p.id == id).map(|p| p.name.as_str()).unwrap_or("")));
                        }
                    }
                }
                self.rebuild_sidebar();
                Task::none()
            }
            Message::Reconnect => {
                let failed: Vec<(String, Arc<Pool>)> =
                    self.conns.iter().filter(|c| matches!(c.status, Some(Status::Failed(_)))).filter_map(|c| c.pool.clone().map(|p| (c.id.clone(), p))).collect();
                let rt = self.rt.clone();
                Task::batch(failed.into_iter().map(|(id, pool)| load_defs(id, pool, &rt)))
            }

            // ------------------------------------------------------------ connection sheet
            Message::EditConnection(id) => {
                let form = match id.as_deref().and_then(|id| self.profile(id)) {
                    Some(p) => ConnForm {
                        editing: Some(p.id.clone()),
                        name: p.name.clone(),
                        url: p.url.clone(),
                        password: connections::load_password(&p.id).ok().flatten().unwrap_or_default(),
                        tls: p.tls.clone(),
                        ssh_on: p.ssh.is_some(),
                        ssh_host: p.ssh.as_ref().map(|s| s.host.clone()).unwrap_or_default(),
                        ssh_port: p.ssh.as_ref().map(|s| s.port.to_string()).unwrap_or_else(|| "22".into()),
                        ssh_user: p.ssh.as_ref().map(|s| s.user.clone()).unwrap_or_default(),
                        ssh_key: p.ssh.as_ref().and_then(|s| s.key_file.as_ref()).map(|k| k.display().to_string()).unwrap_or_default(),
                        testing: false,
                        test: None,
                    },
                    None => ConnForm { ssh_port: "22".into(), ..ConnForm::default() },
                };
                self.sheet = Some(Sheet::Connection(Box::new(form)));
                Task::none()
            }
            Message::Form(field) => {
                if let Some(Sheet::Connection(f)) = &mut self.sheet {
                    f.test = None;
                    match field {
                        FormField::Name(v) => f.name = v,
                        FormField::Url(v) => f.url = v,
                        FormField::Password(v) => f.password = v,
                        FormField::Tls(m) => f.tls.mode = m,
                        FormField::SshOn(v) => f.ssh_on = v,
                        FormField::SshHost(v) => f.ssh_host = v,
                        FormField::SshPort(v) => f.ssh_port = v,
                        FormField::SshUser(v) => f.ssh_user = v,
                        FormField::SshKey(v) => f.ssh_key = v,
                    }
                }
                Task::none()
            }
            Message::PickFile(target) => {
                let dialog = rfd::AsyncFileDialog::new();
                let dialog = match target {
                    FileTarget::SqliteFile => dialog.set_title("Choose SQLite database").add_filter("SQLite", &["db", "sqlite", "sqlite3"]).add_filter("All files", &["*"]),
                    _ => dialog.set_title("Choose file"),
                };
                Task::perform(async move { dialog.pick_file().await.map(|f| f.path().to_path_buf()) }, move |p| Message::FilePicked(target, p))
            }
            Message::FilePicked(target, Some(path)) => {
                if let FileTarget::JobKey(tab) = target {
                    if let Some(m) = self.tab_mut(tab).and_then(|t| t.migration.as_mut()) {
                        m.job.key_file = Some(path.display().to_string());
                    }
                } else if let Some(Sheet::Connection(f)) = &mut self.sheet {
                    match target {
                        FileTarget::SqliteFile => {
                            f.url = format!("sqlite://{}", path.display().to_string().replace('\\', "/"));
                            if f.name.is_empty() {
                                f.name = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
                            }
                        }
                        FileTarget::Ca => f.tls.ca_file = Some(path),
                        FileTarget::ClientCert => f.tls.client_cert = Some(path),
                        FileTarget::ClientKey => f.tls.client_key = Some(path),
                        FileTarget::SshKey => f.ssh_key = path.display().to_string(),
                        FileTarget::JobKey(_) => {}
                    }
                }
                Task::none()
            }
            Message::FilePicked(_, None) => Task::none(),
            Message::TestConnection => {
                let Some(Sheet::Connection(f)) = &mut self.sheet else { return Task::none() };
                let profile = match f.profile() {
                    Ok(p) => p,
                    Err(e) => {
                        f.test = Some(Err(e));
                        return Task::none();
                    }
                };
                f.testing = true;
                let pw = f.password();
                let rt = self.rt.clone();
                Task::perform(
                    async move {
                        rt.spawn(async move {
                            let started = Instant::now();
                            let pool = Arc::new(tokio::task::spawn_blocking(move || Pool::new(&profile, pw.as_deref())).await.map_err(|e| e.to_string())??);
                            let mut c = pool.get().await?;
                            let n = c.get_definitions(&ContainerFilter::default()).await.map_err(|e| e.to_string())?.len();
                            Ok(format!("Connected in {} ms · {n} tables / collections", started.elapsed().as_millis()))
                        })
                        .await
                        .map_err(|e| e.to_string())?
                    },
                    Message::TestResult,
                )
            }
            Message::TestResult(r) => {
                if let Some(Sheet::Connection(f)) = &mut self.sheet {
                    f.testing = false;
                    f.test = Some(r);
                }
                Task::none()
            }
            Message::SaveConnection => {
                let Some(Sheet::Connection(f)) = &mut self.sheet else { return Task::none() };
                let profile = match f.profile() {
                    Ok(p) => p,
                    Err(e) => {
                        f.test = Some(Err(e));
                        return Task::none();
                    }
                };
                if let Err(e) = connections::store_password(&profile.id, f.password().as_deref()) {
                    f.test = Some(Err(e));
                    return Task::none();
                }
                let id = profile.id.clone();
                match self.store.profiles.iter_mut().find(|p| p.id == id) {
                    Some(p) => *p = profile,
                    None => {
                        self.store.profiles.push(profile);
                        self.conns.push(Conn { id: id.clone(), pool: None, status: None, expanded: false, defs: None, error: None, open: HashSet::new() });
                    }
                }
                // settings changed: reconnect with the new ones
                if let Some(c) = self.conn_mut(&id) {
                    let was_open = c.expanded;
                    *c = Conn { id: id.clone(), pool: None, status: None, expanded: was_open, defs: None, error: None, open: std::mem::take(&mut c.open) };
                }
                self.sheet = None;
                self.save_store();
                self.rebuild_sidebar();
                if self.conns.iter().any(|c| c.id == id && c.expanded) {
                    return self.open_connection(&id);
                }
                Task::none()
            }
            Message::AskDeleteConnection(id) => {
                self.sheet = Some(Sheet::Confirm(Confirm::DeleteConnection(id)));
                Task::none()
            }
            Message::Confirmed => match self.sheet.take() {
                Some(Sheet::Confirm(Confirm::DeleteConnection(id))) => {
                    self.store.profiles.retain(|p| p.id != id);
                    self.conns.retain(|c| c.id != id);
                    self.tabs.retain(|t| t.conn != id || t.kind == TabKind::Migration);
                    if self.active.is_some_and(|a| self.tab(a).is_none()) {
                        self.active = self.tabs.last().map(|t| t.id);
                    }
                    if let Err(e) = connections::store_password(&id, None) {
                        self.banner = Some(e);
                    }
                    self.save_store();
                    self.rebuild_sidebar();
                    Task::none()
                }
                Some(Sheet::Confirm(Confirm::StartMigration(tab, _))) => self.start_migration(tab),
                Some(Sheet::Confirm(Confirm::CloseRunningTab(tab))) => {
                    if let Some(r) = self.tab(tab).and_then(|t| t.migration.as_ref()).and_then(|m| m.running.as_ref()) {
                        let _ = r.shutdown.send(true);
                    }
                    self.remove_tab(tab);
                    Task::none()
                }
                other => {
                    self.sheet = other;
                    Task::none()
                }
            },
            Message::CloseSheet => {
                self.sheet = None;
                Task::none()
            }

            // ------------------------------------------------------------ tabs
            Message::SelectTab(id) => {
                self.active = Some(id);
                self.refresh_inspector();
                Task::none()
            }
            Message::CloseTab(id) => {
                if self.tab(id).is_some_and(|t| t.migration.as_ref().is_some_and(|m| m.running.is_some())) {
                    self.sheet = Some(Sheet::Confirm(Confirm::CloseRunningTab(id)));
                    return Task::none();
                }
                let window = self.tab(id).and_then(|t| t.window);
                self.remove_tab(id);
                window.map(window::close).unwrap_or_else(Task::none)
            }
            Message::Stream(tab_id, generation, msg) => {
                let Some(tab) = self.tab_mut(tab_id) else { return Task::none() };
                let Some(stream) = tab.stream.as_mut().filter(|s| s.generation == generation) else { return Task::none() };
                stream.pulling = false;
                match msg {
                    Some(StreamMsg::Columns(def)) => {
                        if tab.results.as_ref().is_none_or(|r| r.def.properties_info.len() != def.properties_info.len()) {
                            tab.results = Some(ResultWindow::new(def));
                        }
                        self.pull(tab_id)
                    }
                    Some(StreamMsg::Batch(b)) => {
                        let keep_from = tab.view.0;
                        let last = tab.view.1;
                        let results = tab.results.as_mut().expect("columns come first");
                        results.push(Arc::try_unwrap(b).unwrap_or_else(|b| (*b).clone()), keep_from);
                        if results.wants_more(last) {
                            self.pull(tab_id)
                        } else {
                            Task::none()
                        }
                    }
                    Some(StreamMsg::Done(r)) => {
                        tab.elapsed = tab.started.map(|s| s.elapsed());
                        match r {
                            Ok(_) => {
                                if let Some(res) = tab.results.as_mut() {
                                    res.finish();
                                }
                            }
                            Err(e) => tab.error = Some(e),
                        }
                        let conn = tab.conn.clone();
                        if let Some(c) = self.conn_mut(&conn) {
                            c.status = c.pool.as_ref().map(|p| p.status());
                        }
                        self.rebuild_sidebar();
                        Task::none()
                    }
                    None => Task::none(),
                }
            }
            Message::GridView(tab_id, first, last) => {
                let Some(tab) = self.tab_mut(tab_id) else { return Task::none() };
                tab.view = (first, last);
                let Some(r) = tab.results.as_ref() else { return Task::none() };
                if r.needs_restart(first) {
                    return self.start_stream(tab_id, first);
                }
                if r.wants_more(last) && tab.stream.is_some() {
                    return self.pull(tab_id);
                }
                Task::none()
            }
            Message::GridSelect(tab_id, row, col) => {
                if let Some(t) = self.tab_mut(tab_id) {
                    t.selected = Some((row, col));
                }
                self.inspector_open.clear();
                self.refresh_inspector();
                Task::none()
            }
            Message::GridSort(tab_id, col) => {
                let Some(tab) = self.tab_mut(tab_id) else { return Task::none() };
                if tab.kind != TabKind::Table {
                    return Task::none();
                }
                tab.sort = match tab.sort {
                    Some((c, false)) if c == col => Some((col, true)),
                    Some((c, true)) if c == col => None,
                    _ => Some((col, false)),
                };
                let q = self.tab(tab_id).and_then(|t| self.table_query(t));
                if let Some(t) = self.tab_mut(tab_id) {
                    t.query = q;
                }
                self.start_stream(tab_id, 0)
            }
            Message::FilterChanged(tab_id, f) => {
                if let Some(t) = self.tab_mut(tab_id) {
                    t.filter = f;
                }
                Task::none()
            }
            Message::ApplyFilter(tab_id) => {
                let q = self.tab(tab_id).and_then(|t| self.table_query(t));
                if let Some(t) = self.tab_mut(tab_id) {
                    t.query = q;
                }
                self.start_stream(tab_id, 0)
            }
            Message::ClearFilter(tab_id) => {
                if let Some(t) = self.tab_mut(tab_id) {
                    t.filter.clear();
                }
                self.update(Message::ApplyFilter(tab_id))
            }
            Message::Edit(tab_id, action) => {
                if let Some(t) = self.tab_mut(tab_id) {
                    t.editor.perform(action);
                }
                Task::none()
            }
            Message::Run(tab_id) => {
                let Some(tab) = self.tab_mut(tab_id) else { return Task::none() };
                match tab.kind {
                    TabKind::Query => {
                        let text = tab.editor.text();
                        if text.trim().is_empty() {
                            return Task::none();
                        }
                        tab.query = Some(Query::Text(text));
                        tab.results = None;
                        let conn = tab.conn.clone();
                        if self.conns.iter().any(|c| c.id == conn && c.pool.is_none()) {
                            if let Some(t) = self.tab_mut(tab_id) {
                                t.error = Some("Connecting… run the query again when the connection is open".into());
                            }
                            return self.open_connection(&conn);
                        }
                        self.start_stream(tab_id, 0)
                    }
                    TabKind::Table => self.update(Message::ApplyFilter(tab_id)),
                    TabKind::Migration => self.update(Message::StartMigration(tab_id)),
                }
            }
            Message::Cancel(tab_id) => {
                let Some(tab) = self.tab_mut(tab_id) else { return Task::none() };
                if let Some(m) = tab.migration.as_ref() {
                    if m.running.is_some() {
                        return self.update(Message::StopMigration(tab_id));
                    }
                }
                let Some(stream) = tab.stream.take() else { return Task::none() };
                tab.error = Some("Cancelled".into());
                tab.elapsed = tab.started.map(|s| s.elapsed());
                let canceller = stream.canceller.lock().unwrap_or_else(|e| e.into_inner()).take();
                drop(stream);
                match canceller {
                    Some(c) => {
                        let rt = self.rt.clone();
                        Task::perform(async move { rt.spawn(async move { c.cancel().await }).await.ok() }, |_| Message::DismissBanner).discard()
                    }
                    None => Task::none(),
                }
            }
            Message::InspectorToggle(i) => {
                if let Some(p) = self.inspector_tree.as_ref().and_then(|(_, paths)| paths.get(i)).cloned() {
                    if !self.inspector_open.remove(&p) {
                        self.inspector_open.insert(p);
                    }
                    self.refresh_inspector();
                }
                Task::none()
            }

            // ------------------------------------------------------------ migration
            Message::Job(tab_id, field) => {
                let Some(m) = self.tab_mut(tab_id).and_then(|t| t.migration.as_mut()) else { return Task::none() };
                let j = &mut m.job;
                match field {
                    JobField::Source(v) => j.source = v,
                    JobField::Target(v) => j.target = v,
                    JobField::Strategy(v) => j.strategy = v,
                    JobField::Transfer(v) => j.transfer = v,
                    JobField::Batch(v) => {
                        if let Ok(n) = v.trim().parse::<u32>() {
                            j.batch = n.max(1);
                        } else if v.trim().is_empty() {
                            j.batch = 1;
                        }
                    }
                    JobField::Compression(v) => j.compression = v,
                    JobField::KeyFile(v) => j.key_file = Some(v).filter(|k| !k.is_empty()),
                    JobField::Include(v) => j.include = v,
                    JobField::Exclude(v) => j.exclude = v,
                    JobField::Live(v) => j.live = v,
                    JobField::Name(v) => j.name = v,
                }
                Task::none()
            }
            Message::StartMigration(tab_id) => {
                let Some(m) = self.tab(tab_id).and_then(|t| t.migration.as_ref()) else { return Task::none() };
                if m.running.is_some() {
                    return Task::none();
                }
                let job = m.job.clone();
                let (Some(src), Some(dst)) = (self.pool_of(&job.source), self.pool_of(&job.target)) else {
                    let missing: Vec<String> = [&job.source, &job.target].into_iter().filter(|id| self.conns.iter().all(|c| &&c.id != id || c.pool.is_none())).cloned().collect();
                    if missing.iter().any(|id| id.is_empty()) {
                        self.set_migration_result(tab_id, Err("Choose a source and a target".into()));
                        return Task::none();
                    }
                    self.set_migration_result(tab_id, Err("Connecting… start again when both connections are open".into()));
                    return Task::batch(missing.iter().map(|id| self.open_connection(id)).collect::<Vec<_>>());
                };
                if job.source == job.target {
                    self.set_migration_result(tab_id, Err("Source and target must be different connections".into()));
                    return Task::none();
                }
                if let Err(e) = job.settings(&src.url, &dst.url) {
                    self.set_migration_result(tab_id, Err(e));
                    return Task::none();
                }
                // UI-7: overwriting containers in the target needs a confirmation
                let rt = self.rt.clone();
                Task::perform(
                    async move {
                        rt.spawn(async move {
                            let filter = job.settings(&src.url, &dst.url)?.execution_settings().filter;
                            let names = |defs: Vec<DataDefinition>| defs.into_iter().map(|d| rustygrator_core::models::data_definitions::short_name(&d.container_name).to_string()).collect::<HashSet<_>>();
                            let source = names(src.get().await?.get_definitions(&filter).await.map_err(|e| e.to_string())?);
                            let target = names(dst.get().await?.get_definitions(&ContainerFilter::default()).await.map_err(|e| e.to_string())?);
                            let mut both: Vec<String> = source.intersection(&target).cloned().collect();
                            both.sort();
                            Ok(both)
                        })
                        .await
                        .map_err(|e| e.to_string())?
                    },
                    move |r| Message::OverwriteCheck(tab_id, r),
                )
            }
            Message::OverwriteCheck(tab_id, r) => match r {
                Ok(existing) if existing.is_empty() => self.start_migration(tab_id),
                Ok(existing) => {
                    self.sheet = Some(Sheet::Confirm(Confirm::StartMigration(tab_id, existing)));
                    Task::none()
                }
                Err(e) => {
                    self.set_migration_result(tab_id, Err(e));
                    Task::none()
                }
            },
            Message::MigrationStarted(tab_id, r) => {
                match r {
                    Ok(stats) => {
                        if let Some(run) = self.tab_mut(tab_id).and_then(|t| t.migration.as_mut()).and_then(|m| m.running.as_mut()) {
                            run.stats = stats;
                        }
                    }
                    Err(e) => self.set_migration_result(tab_id, Err(e)),
                }
                Task::none()
            }
            Message::MigrationDone(tab_id, r) => {
                self.set_migration_result(tab_id, r);
                Task::none()
            }
            Message::StopMigration(tab_id) => {
                if let Some(r) = self.tab_mut(tab_id).and_then(|t| t.migration.as_mut()).and_then(|m| m.running.as_mut()) {
                    let _ = r.shutdown.send(true);
                    r.stopping = true;
                }
                Task::none()
            }
            Message::SaveJob(tab_id) => {
                let Some(mut job) = self.tab(tab_id).and_then(|t| t.migration.as_ref()).map(|m| m.job.clone()) else { return Task::none() };
                if job.name.trim().is_empty() {
                    job.name = format!("{} → {}", self.profile(&job.source).map(|p| p.name.as_str()).unwrap_or("?"), self.profile(&job.target).map(|p| p.name.as_str()).unwrap_or("?"));
                }
                match self.store.jobs.iter_mut().find(|j| j.name == job.name) {
                    Some(j) => *j = job.clone(),
                    None => self.store.jobs.push(job.clone()),
                }
                if let Some(m) = self.tab_mut(tab_id).and_then(|t| t.migration.as_mut()) {
                    m.job.name = job.name.clone();
                }
                if let Some(t) = self.tab_mut(tab_id) {
                    t.title = job.name;
                }
                self.save_store();
                Task::none()
            }
            Message::OpenJob(i) => {
                let Some(job) = self.store.jobs.get(i).cloned() else { return Task::none() };
                let id = self.new_tab(String::new(), job.name.clone(), TabKind::Migration);
                if let Some(t) = self.tab_mut(id) {
                    t.migration = Some(Migration::new(job));
                }
                Task::none()
            }
            Message::CopyCommand(tab_id) => {
                let Some(job) = self.tab(tab_id).and_then(|t| t.migration.as_ref()).map(|m| m.job.clone()) else { return Task::none() };
                // MAPP-4: connection strings from the profiles, without passwords
                let url = |id: &str| self.profile(id).map(|p| connections::with_tls(&p.url, &p.tls)).unwrap_or_default();
                let cmd = job.command_line(&url(&job.source), &url(&job.target));
                self.banner = Some("Command copied. Passwords are not included; add them to the connection strings or use a key file.".into());
                iced::clipboard::write(cmd)
            }
            Message::FirstFrame => {
                println!("first-frame-ms {:.1}", STARTED.get().map(|s| s.elapsed().as_secs_f64() * 1000.0).unwrap_or_default());
                iced::exit()
            }
            Message::Tick(now) => {
                for t in &mut self.tabs {
                    if let Some(m) = t.migration.as_mut() {
                        m.tick(now);
                    }
                }
                Task::none()
            }
        }
    }

    fn pool_of(&self, id: &str) -> Option<Arc<Pool>> {
        self.conns.iter().find(|c| c.id == id).and_then(|c| c.pool.clone())
    }

    fn set_migration_result(&mut self, tab_id: u64, r: Result<String, String>) {
        if let Some(m) = self.tab_mut(tab_id).and_then(|t| t.migration.as_mut()) {
            m.finished(r);
        }
    }

    fn remove_tab(&mut self, id: u64) {
        self.tabs.retain(|t| t.id != id);
        if self.active == Some(id) {
            self.active = self.tabs.iter().rfind(|t| t.window.is_none()).map(|t| t.id);
        }
    }

    /// Runs the migration of a tab on the core runtime with connections from the pools.
    fn start_migration(&mut self, tab_id: u64) -> Task<Message> {
        let Some(job) = self.tab(tab_id).and_then(|t| t.migration.as_ref()).map(|m| m.job.clone()) else { return Task::none() };
        let (Some(src), Some(dst)) = (self.pool_of(&job.source), self.pool_of(&job.target)) else { return Task::none() };
        let settings = match job.settings(&src.url, &dst.url) {
            Ok(s) => s.execution_settings(),
            Err(e) => {
                self.set_migration_result(tab_id, Err(e));
                return Task::none();
            }
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (stats_tx, stats_rx) = tokio::sync::oneshot::channel();
        let live = job.live;
        let handle = self.rt.spawn(async move {
            let provider = src.detached().await?;
            let receiver = dst.detached().await?;
            let mut m = Migrator::with_settings(Some(provider), Some(receiver), settings).with_workers(Some(src.connection()), Some(dst.connection()));
            let _ = stats_tx.send(m.stats());
            let s = m.run_with_shutdown(shutdown_rx).await.map_err(|e| e.to_string())?;
            Ok::<String, String>(match s.live {
                Some(l) if live => format!("Stopped. {} rows copied, {} live events applied, {} errors", s.rows, l.events, l.errors),
                _ => format!("Done. {} tables, {} rows", s.containers, s.rows),
            })
        });
        if let Some(m) = self.tab_mut(tab_id).and_then(|t| t.migration.as_mut()) {
            m.result = None;
            m.samples.clear();
            // counters arrive once both connections are open (MigrationStarted)
            m.running = Some(migration::Running { stats: Arc::new(rustygrator_core::migrator::migrator_steps::MigrationStats::new()), shutdown: shutdown_tx, started: Instant::now(), stopping: false });
        }
        Task::batch([
            Task::perform(async move { stats_rx.await.map_err(|_| "could not open the connections".to_string()) }, move |r| Message::MigrationStarted(tab_id, r)),
            Task::perform(async move { handle.await.map_err(|e| e.to_string()).and_then(|r| r) }, move |r| Message::MigrationDone(tab_id, r)),
        ])
    }

    fn refresh_inspector(&mut self) {
        self.inspector_tree = self.inspector_rows();
    }

    /// Inspector tree of the selected JSON value: rows and node paths.
    fn inspector_rows(&self) -> Option<(Vec<TreeRow>, Vec<String>)> {
        let tab = self.active_tab()?;
        let (row, col) = tab.selected?;
        let r = tab.results.as_ref()?;
        let (b, i) = r.row(row)?;
        let json = match b.cell(i, col) {
            rustygrator_core::dbc::batch::Cell::Json(s) => serde_json::from_str(s).ok()?,
            rustygrator_core::dbc::batch::Cell::JsonValue(v) => v.clone(),
            _ => return None,
        };
        Some(inspector::json_rows(&json, &self.inspector_open))
    }
}

fn toggle(conn: Option<&mut Conn>, key: String) {
    if let Some(c) = conn {
        if !c.open.remove(&key) {
            c.open.insert(key);
        }
    }
}

pub fn scheme_label(url: &str) -> &'static str {
    let scheme = url.split(':').next().unwrap_or("");
    match scheme {
        "postgresql" | "postgres" => "PostgreSQL",
        "mongodb" | "mongodb+srv" => "MongoDB",
        "sqlite" => "SQLite",
        "scylla" | "cassandra" => "ScyllaDB",
        "mysql" | "mariadb" => "MySQL",
        "oracle" => "Oracle",
        _ => "",
    }
}

fn load_defs(id: String, pool: Arc<Pool>, rt: &Arc<tokio::runtime::Runtime>) -> Task<Message> {
    let rt = rt.clone();
    Task::perform(
        async move {
            rt.spawn(async move {
                let mut c = pool.get().await?;
                match c.get_definitions(&ContainerFilter::default()).await {
                    Ok(defs) => Ok(defs.into_iter().map(Arc::new).collect()),
                    Err(e) => {
                        c.broken = true;
                        Err(e.to_string())
                    }
                }
            })
            .await
            .map_err(|e| e.to_string())?
        },
        move |r| Message::DefsLoaded(id.clone(), r),
    )
}

#[cfg(target_os = "macos")]
fn menu_events() -> impl iced::futures::Stream<Item = Message> {
    iced::stream::channel(16, async |mut out: iced::futures::channel::mpsc::Sender<Message>| {
        use iced::futures::SinkExt;
        loop {
            for c in platform::menu_bar::take_commands() {
                let _ = out.send(Message::Command(c)).await;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
}

impl App {
    fn view(&self, window: window::Id) -> Element<'_, Message> {
        view::view(self, window)
    }
}
