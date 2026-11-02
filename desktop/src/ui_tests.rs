//! Headless UI tests: the real views of the app over a real SQLite database, rendered with the
//! software renderer. Screenshots are written to `target/ui/` for review.

use std::{path::PathBuf, sync::Arc};

use iced::{window, Size};
use iced_test::Simulator;

use rustygrator_core::{
    browse::{Query, QueryRunner},
    migrator::{batching::BatchSizer, migrator_steps::DataProvider},
    models::migration_execution_settings::ContainerFilter,
};

use crate::{
    connections::{Profile, Tls},
    jobs::Job,
    migration::Migration,
    pool::Pool,
    results::ResultWindow,
    App, Message, TabKind,
};

pub fn screenshots() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/ui")
}

const SEED: &str = "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, profile JSON, created_at DATETIME);
CREATE INDEX customers_email ON customers(email);
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id), amount DECIMAL(10,2), note TEXT);";

/// App with one SQLite connection (opened and loaded), one open table tab with `rows` rows and
/// the connection expanded in the sidebar.
pub fn demo_app(dir: &std::path::Path, rows: u32) -> App {
    let db = dir.join("shop.db");
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute_batch(SEED).unwrap();
    c.execute(
        "WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < ?1)
         INSERT INTO customers SELECT n, 'Customer ' || n, 'c' || n || '@example.com',
             json_object('age', 20 + n % 50, 'tags', json_array('a', 'b'), 'address', json_object('city', 'Gdańsk', 'zip', '80-001')),
             datetime('2024-01-01', '+' || n || ' minutes') FROM g",
        [rows],
    )
    .unwrap();
    c.execute_batch("INSERT INTO orders VALUES (1, 1, 1.25, 'first'), (2, 2, 2.50, NULL);").unwrap();
    drop(c);
    let (mut app, _) = App::boot();
    let url = format!("sqlite://{}", db.display().to_string().replace('\\', "/"));
    let profile = Profile { id: "demo".into(), name: "Shop (SQLite)".into(), url, tls: Tls::default(), ssh: None };
    app.store.profiles = vec![profile.clone()];
    app.store.jobs = vec![Job { name: "Shop → Warehouse".into(), ..Job::default() }];
    let open = ["t:customers".to_string(), "c:customers:Columns".to_string()].into();
    app.conns = vec![crate::Conn { id: "demo".into(), pool: None, status: None, expanded: true, defs: None, error: None, open }];
    let pool = Arc::new(Pool::new(&profile, None).unwrap());
    let rt = app.rt.clone();
    let defs: Vec<_> = rt
        .block_on(async { pool.get().await.unwrap().get_definitions(&ContainerFilter::default()).await.unwrap() })
        .into_iter()
        .map(Arc::new)
        .collect();
    app.conns[0].status = Some(pool.status());
    app.conns[0].pool = Some(pool.clone());
    app.conns[0].defs = Some(defs.clone());
    app.rebuild_sidebar();
    app.side_sel = Some(1);

    let id = app.new_tab("demo".into(), "customers".into(), TabKind::Table);
    let def = defs.iter().find(|d| d.container_name == "customers").unwrap().clone();
    let q = Query::Table { def: (*def).clone(), filter: None, sort: None };
    let mut window = ResultWindow::new(def.clone());
    rt.block_on(async {
        let mut c = pool.get().await.unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let fetch = c.fetch(&q, &def, Arc::new(BatchSizer::fixed(1000)), tx);
        let take = async {
            while let Some(b) = rx.recv().await {
                window.push(b, 0);
            }
        };
        let (n, ()) = tokio::join!(fetch, take);
        n.unwrap();
    });
    window.finish();
    let tab = app.tab_mut(id).unwrap();
    tab.def = Some(def);
    tab.query = Some(q);
    tab.results = Some(window);
    tab.selected = Some((2, 3));
    app.main = Some(window::Id::unique());
    app.refresh_inspector();
    app
}

pub fn simulator(app: &App) -> Simulator<'_, Message> {
    Simulator::with_size(iced::Settings::default(), Size::new(1280.0, 800.0), app.view(app.main.unwrap()))
}

fn shot(app: &App, name: &str) {
    let path = screenshots().join(name);
    // the renderer name is added to the file name (main_light-wgpu.png): remove older shots
    for e in std::fs::read_dir(screenshots()).into_iter().flatten().flatten() {
        if e.file_name().to_string_lossy().starts_with(&format!("{name}-")) {
            let _ = std::fs::remove_file(e.path());
        }
    }
    let snap = simulator(app).snapshot(&app.tokens().theme()).unwrap();
    assert!(snap.matches_image(&path).unwrap());
}

fn has_text(app: &App, texts: &[&str]) {
    let mut sim = simulator(app);
    for t in texts {
        assert!(sim.find(*t).is_ok(), "missing text {t:?}");
    }
}

/// UI-3, VIEW-1, VIEW-7: sidebar tree, table grid and inspector, light and dark (UI-5).
#[test]
fn ui3_main_window_renders_light_and_dark() {
    let dir = tempfile::tempdir().unwrap();
    let mut app = demo_app(dir.path(), 500);
    has_text(&app, &["CONNECTIONS", "INSPECTOR", "profile", "Filter (WHERE), e.g. created_at > '2024-01-01'", "500 rows · row 3, profile"]);
    shot(&app, "main_light");
    app.dark = true;
    app.rebuild_sidebar();
    shot(&app, "main_dark");
}

/// CONN-1 / CONN-3: the connection sheet with TLS and SSH options.
#[test]
fn conn1_connection_sheet() {
    let dir = tempfile::tempdir().unwrap();
    let mut app = demo_app(dir.path(), 10);
    let _ = app.update(Message::EditConnection(None));
    let _ = app.update(Message::Form(crate::FormField::Url("postgresql://app@db.internal:5432/shop".into())));
    let _ = app.update(Message::Form(crate::FormField::Tls(crate::connections::TlsMode::VerifyFull)));
    let _ = app.update(Message::Form(crate::FormField::SshOn(true)));
    has_text(&app, &["New Connection", "Password", "CA certificate", "SSH server", "Test", "Save"]);
    shot(&app, "connection_sheet");
    // UI-7: saving without a name says what is missing
    let _ = app.update(Message::SaveConnection);
    let Some(crate::Sheet::Connection(f)) = &app.sheet else { panic!("sheet closed") };
    assert_eq!(f.test, Some(Err("Enter a name".into())));
}

/// MAPP-1: the migration form with the same options as the CLI.
#[test]
fn mapp1_migration_tab() {
    let dir = tempfile::tempdir().unwrap();
    let mut app = demo_app(dir.path(), 10);
    let _ = app.update(Message::Command(crate::platform::Command::NewMigration));
    let id = app.active.unwrap();
    app.tab_mut(id).unwrap().migration = Some(Migration::new(Job { source: "demo".into(), live: true, ..Job::default() }));
    has_text(&app, &["SQL → NoSQL", "Compression", "Live transfer", "Copy CLI Command", "Start"]);
    shot(&app, "migration");
}

/// VIEW-6: query tab with the editor.
#[test]
fn view6_query_tab() {
    let dir = tempfile::tempdir().unwrap();
    let mut app = demo_app(dir.path(), 50);
    let _ = app.update(Message::Command(crate::platform::Command::NewQuery));
    let id = app.active.unwrap();
    assert_eq!(app.tab(id).unwrap().kind, TabKind::Query);
    assert_eq!(app.tab(id).unwrap().editor.text(), "SELECT * FROM ");
    has_text(&app, &["Run", "Shop (SQLite) · read-only"]);
    shot(&app, "query");
}

/// UI-7: empty state when there are no connections.
#[test]
fn ui7_empty_state() {
    let (mut app, _) = App::boot();
    app.store.profiles.clear();
    app.conns.clear();
    app.rebuild_sidebar();
    app.main = Some(window::Id::unique());
    has_text(&app, &["No connections yet", "Nothing open", "Add Connection…"]);
}
