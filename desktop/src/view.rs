//! Views (UI-3, UI-4): a macOS style window with a sidebar of connections, a toolbar, tabs in
//! the content area and an inspector on the right. Linux and Windows keep the layout and get the
//! menu inside the window.

use iced::{
    widget::{
        button, canvas, checkbox, column, container, image, mouse_area, opaque, pick_list, row, rule, scrollable, stack, text, text_editor, text_input,
        space, space as spacer,
        tooltip,
    },
    window, Alignment, Element, Fill, Font, Length, Padding, Shrink,
};

use rustygrator_core::dbc::batch::Cell;

use crate::{
    connections::TlsMode,
    inspector,
    jobs::{COMPRESSIONS, STRATEGIES, TRANSFERS},
    migration::{format_rate, LagChart},
    platform::{Command, MENUS},
    scheme_label,
    theme::{self, ButtonKind, Tokens, BODY, INSPECTOR_WIDTH, LARGE_TITLE, S1, S2, S3, S4, SIDEBAR_WIDTH, SMALL, TITLE},
    widgets::{grid::Grid, tree::TreeView},
    App, Confirm, ConnForm, FileTarget, FormField, JobField, Message, Sheet, Tab, TabKind,
};

type E<'a> = Element<'a, Message>;

fn label<'a>(s: impl text::IntoFragment<'a>, t: Tokens) -> text::Text<'a> {
    text(s).size(BODY).color(t.text)
}

fn secondary<'a>(s: impl text::IntoFragment<'a>, t: Tokens) -> text::Text<'a> {
    text(s).size(SMALL).color(t.text_secondary)
}

fn btn<'a>(s: &'a str, t: Tokens, kind: ButtonKind, msg: Option<Message>) -> button::Button<'a, Message> {
    button(text(s).size(BODY)).padding([S1, S3]).style(theme::button(t, kind)).on_press_maybe(msg)
}

/// Toolbar button with a tooltip naming its shortcut (UI-6: discoverable without a mouse).
fn tool<'a>(s: &'a str, t: Tokens, msg: Option<Message>, command: Option<Command>) -> E<'a> {
    let b = btn(s, t, ButtonKind::Plain, msg);
    match command {
        Some(c) => tooltip(b, container(secondary(format!("{} ({})", c.title(), c.shortcut_label()), t)).padding(S1).style(theme::sheet(t)), tooltip::Position::Bottom).into(),
        None => b.into(),
    }
}

pub fn view(app: &App, window: window::Id) -> E<'_> {
    let t = app.tokens();
    if let Some(tab) = app.tabs.iter().find(|tab| tab.window == Some(window)) {
        // APP-10: a tab opened in its own window
        return container(column![tab_body(app, tab, t), status_bar(tab, t)]).style(theme::panel(t, t.content)).width(Fill).height(Fill).into();
    }
    let mut body = row![
        container(sidebar(app, t)).width(SIDEBAR_WIDTH).height(Fill).style(theme::panel(t, t.sidebar)),
        rule::vertical(1).style(move |_| rule::Style { color: t.separator, radius: 0.0.into(), fill_mode: rule::FillMode::Full, snap: true }),
        content(app, t),
    ];
    if app.inspector {
        body = body.push(row![
            rule::vertical(1).style(move |_| rule::Style { color: t.separator, radius: 0.0.into(), fill_mode: rule::FillMode::Full, snap: true }),
            container(inspector_panel(app, t)).width(INSPECTOR_WIDTH).height(Fill).style(theme::panel(t, t.window))
        ]);
    }
    let mut main = column![];
    if !cfg!(target_os = "macos") {
        main = main.push(menu_bar(app, t));
    }
    main = main.push(toolbar(app, t)).push(rule::horizontal(1).style(move |_| rule::Style { color: t.separator, radius: 0.0.into(), fill_mode: rule::FillMode::Full, snap: true }));
    if let Some(b) = &app.banner {
        main = main.push(
            container(row![label(b.as_str(), t).width(Fill), btn("Dismiss", t, ButtonKind::Plain, Some(Message::DismissBanner))].align_y(Alignment::Center).spacing(S2))
                .padding([S1, S3])
                .style(theme::panel(t, t.selection_inactive)),
        );
    }
    let main = container(main.push(body)).style(theme::panel(t, t.window)).width(Fill).height(Fill);
    let mut layers = stack![main];
    if let Some(m) = app.menu_open {
        layers = layers.push(menu_dropdown(m, t));
    }
    if let Some(s) = &app.sheet {
        layers = layers.push(sheet(app, s, t));
    }
    layers.into()
}

// ------------------------------------------------------------------ chrome

fn toolbar(app: &App, t: Tokens) -> E<'_> {
    let active = app.active_tab();
    let running = active.is_some_and(Tab::running);
    let can_run = active.is_some_and(|tab| tab.kind != TabKind::Table || tab.def.is_some());
    // macOS: room for the window buttons in the full size content view
    let inset = if cfg!(target_os = "macos") { 78.0 } else { S2 };
    let title = active.map(|t| t.title.as_str()).unwrap_or("rustygrator");
    container(
        row![
            spacer().width(inset),
            tool("＋ Connection", t, Some(Message::Command(Command::NewConnection)), Some(Command::NewConnection)),
            tool("New Query", t, Some(Message::Command(Command::NewQuery)), Some(Command::NewQuery)),
            tool("Migrate…", t, Some(Message::Command(Command::NewMigration)), Some(Command::NewMigration)),
            text(title).size(TITLE).font(Font { weight: iced::font::Weight::Semibold, ..Font::DEFAULT }).color(t.text).width(Fill).center(),
            tool("Run", t, (can_run && !running).then_some(Message::Command(Command::Run)), Some(Command::Run)),
            tool("Cancel", t, running.then_some(Message::Command(Command::Cancel)), Some(Command::Cancel)),
            tool("Inspector", t, Some(Message::Command(Command::ToggleInspector)), Some(Command::ToggleInspector)),
        ]
        .spacing(S1)
        .align_y(Alignment::Center),
    )
    .padding([S2, S2])
    .height(Length::Fixed(44.0))
    .style(theme::panel(t, t.window))
    .into()
}

/// UI-4: the menu inside the window on Linux and Windows.
fn menu_bar(app: &App, t: Tokens) -> E<'_> {
    let mut r = row![].spacing(2).padding([2.0, S1]);
    for (title, items) in MENUS {
        if items.is_empty() {
            continue;
        }
        r = r.push(button(text(title).size(BODY)).padding([2.0, S2]).style(theme::row_button(t, app.menu_open == Some(title))).on_press(Message::MenuOpen(Some(title))));
    }
    container(r).width(Fill).style(theme::panel(t, t.window)).into()
}

fn menu_dropdown(title: &'static str, t: Tokens) -> E<'static> {
    let index = MENUS.iter().position(|m| m.0 == title).unwrap_or(0);
    let items = MENUS[index].1;
    let mut list = column![].spacing(1).padding(S1).width(260);
    for it in items {
        list = list.push(match it {
            Some(c) => button(row![text(c.title()).size(BODY).width(Fill), secondary(c.shortcut_label(), t)].spacing(S3))
                .width(Fill)
                .padding([S1, S2])
                .style(theme::row_button(t, false))
                .on_press(Message::Command(*c))
                .into(),
            None => E::from(rule::horizontal(1).style(move |_| rule::Style { color: t.separator, radius: 0.0.into(), fill_mode: rule::FillMode::Full, snap: true })),
        });
    }
    let x = 4.0 + MENUS[..index].iter().filter(|m| !m.1.is_empty()).map(|m| m.0.len() as f32 * 7.5 + 2.0 * S2 + 2.0).sum::<f32>();
    mouse_area(container(column![spacer().height(26), row![spacer().width(x), opaque(container(list).style(theme::sheet(t)))]]).width(Fill).height(Fill))
        .on_press(Message::MenuOpen(None))
        .into()
}

fn sidebar(app: &App, t: Tokens) -> E<'_> {
    let header = row![
        secondary("CONNECTIONS", t).width(Fill),
        button(text("＋").size(BODY)).padding([0.0, S2]).style(theme::button(t, ButtonKind::Plain)).on_press(Message::Command(Command::NewConnection)),
    ]
    .align_y(Alignment::Center)
    .padding(Padding { top: S2, bottom: S1, left: S3, right: S1 });
    let body: E<'_> = if app.store.profiles.is_empty() {
        // UI-7: empty state with the next step
        container(
            column![
                label("No connections yet", t),
                secondary("Add PostgreSQL, MongoDB, SQLite or ScyllaDB.", t),
                btn("Add Connection…", t, ButtonKind::Primary, Some(Message::Command(Command::NewConnection))),
            ]
            .spacing(S2)
            .align_x(Alignment::Center),
        )
        .center(Fill)
        .padding(S4)
        .into()
    } else {
        TreeView::new(&app.side, t, t.sidebar, Message::SideSelect, Message::SideToggle).selected(app.side_sel).on_activate(Message::SideActivate).into()
    };
    let mut col = column![header, body];
    if let Some(crate::Node::Conn(id)) = app.side_sel.and_then(|i| app.side_nodes.get(i)) {
            let c = app.conns.iter().find(|c| &c.id == id);
            let actions = row![
                btn("Edit…", t, ButtonKind::Secondary, Some(Message::EditConnection(Some(id.clone())))),
                btn("Delete…", t, ButtonKind::Secondary, Some(Message::AskDeleteConnection(id.clone()))),
            ]
            .spacing(S2);
            if let Some(err) = c.and_then(|c| c.error.as_ref()) {
                col = col.push(container(text(err.as_str()).size(SMALL).color(t.danger)).padding([S1, S3]));
            }
            col = col.push(container(actions).padding(S2));
    }
    if !app.store.jobs.is_empty() {
        col = col.push(container(secondary("SAVED MIGRATIONS", t)).padding(Padding { top: S2, bottom: S1, left: S3, right: S1 }));
        for (i, j) in app.store.jobs.iter().enumerate() {
            col = col.push(button(text(j.name.as_str()).size(BODY)).width(Fill).padding([S1, S3]).style(theme::row_button(t, false)).on_press(Message::OpenJob(i)));
        }
    }
    col.into()
}

fn content(app: &App, t: Tokens) -> E<'_> {
    let tabs: Vec<&Tab> = app.tabs.iter().filter(|t| t.window.is_none()).collect();
    if tabs.is_empty() {
        return container(
            column![
                text("Nothing open").size(LARGE_TITLE).color(t.text),
                secondary("Open a table from the sidebar (double click or Enter), or start a query.", t),
                row![
                    btn("New Query", t, ButtonKind::Secondary, Some(Message::Command(Command::NewQuery))),
                    btn("New Migration…", t, ButtonKind::Secondary, Some(Message::Command(Command::NewMigration))),
                ]
                .spacing(S2),
            ]
            .spacing(S3)
            .align_x(Alignment::Center),
        )
        .center(Fill)
        .style(theme::panel(t, t.content))
        .into();
    }
    let mut bar = row![].spacing(2).padding(Padding { top: S1, bottom: 0.0, left: S2, right: S2 });
    for tab in &tabs {
        let active = app.active == Some(tab.id);
        bar = bar.push(
            row![
                button(text(tab.title.as_str()).size(SMALL + 1.0)).padding([S1, S3]).style(theme::tab_button(t, active)).on_press(Message::SelectTab(tab.id)),
                button(text("×").size(BODY)).padding([S1, 6.0]).style(theme::button(t, ButtonKind::Plain)).on_press(Message::CloseTab(tab.id)),
            ]
            .align_y(Alignment::Center),
        );
    }
    let Some(tab) = app.active_tab().filter(|t| t.window.is_none()) else {
        return column![scrollable(bar).direction(scrollable::Direction::Horizontal(scrollable::Scrollbar::new().width(4).scroller_width(4)))].into();
    };
    column![
        container(scrollable(bar).direction(scrollable::Direction::Horizontal(scrollable::Scrollbar::new().width(4).scroller_width(4)))).style(theme::panel(t, t.window)),
        container(tab_body(app, tab, t)).width(Fill).height(Fill).style(theme::panel(t, t.content)),
        status_bar(tab, t),
    ]
    .width(Fill)
    .into()
}

fn status_bar(tab: &Tab, t: Tokens) -> E<'_> {
    let mut s = String::new();
    if let Some(r) = &tab.results {
        if r.complete {
            s = format!("{} rows", r.known_rows);
        } else {
            s = format!("{} rows loaded…", r.known_rows);
        }
        if let Some(e) = tab.elapsed {
            s.push_str(&format!(" · {} ms", e.as_millis()));
        }
        if let Some((row, col)) = tab.selected {
            s.push_str(&format!(" · row {}, {}", row + 1, r.def.properties_info.get(col).map(|p| p.property_name.as_str()).unwrap_or("")));
        }
    } else if tab.running() {
        s = "Running…".into();
    }
    container(secondary(s, t)).padding([2.0, S3]).width(Fill).style(theme::panel(t, t.window)).into()
}

fn error_box<'a>(e: &'a str, t: Tokens) -> E<'a> {
    container(text(e).size(BODY).color(t.danger)).padding(S3).width(Fill).into()
}

fn tab_body<'a>(app: &'a App, tab: &'a Tab, t: Tokens) -> E<'a> {
    match tab.kind {
        TabKind::Table => {
            let mongo = app.profile(&tab.conn).is_some_and(|p| scheme_label(&p.url) == "MongoDB");
            let placeholder = if mongo { r#"Filter, e.g. {"age": {"$gt": 30}}"# } else { "Filter (WHERE), e.g. created_at > '2024-01-01'" };
            let id = tab.id;
            let filter = row![
                text_input(placeholder, &tab.filter).on_input(move |s| Message::FilterChanged(id, s)).on_submit(Message::ApplyFilter(id)).size(BODY).padding([S1, S2]).style(theme::text_input(t)),
                btn("Apply", t, ButtonKind::Secondary, Some(Message::ApplyFilter(id))),
                btn("Clear", t, ButtonKind::Plain, (!tab.filter.is_empty()).then_some(Message::ClearFilter(id))),
            ]
            .spacing(S2)
            .padding(S2)
            .align_y(Alignment::Center);
            column![filter, results(tab, t)].into()
        }
        TabKind::Query => {
            let id = tab.id;
            let syntax = match app.profile(&tab.conn).map(|p| scheme_label(&p.url)) {
                Some("MongoDB") => "js",
                _ => "sql",
            };
            let hl = if t.dark { iced::highlighter::Theme::Base16Ocean } else { iced::highlighter::Theme::InspiredGitHub };
            let editor = text_editor(&tab.editor)
                .on_action(move |a| Message::Edit(id, a))
                .font(Font::MONOSPACE)
                .size(BODY)
                .height(160)
                .padding(S2)
                .style(theme::text_editor(t))
                .highlight(syntax, hl);
            let actions = row![
                secondary(format!("{} · read-only", app.profile(&tab.conn).map(|p| p.name.as_str()).unwrap_or("")), t).width(Fill),
                btn("Cancel", t, ButtonKind::Secondary, tab.running().then_some(Message::Cancel(id))),
                btn("Run", t, ButtonKind::Primary, (!tab.running()).then_some(Message::Run(id))),
            ]
            .spacing(S2)
            .align_y(Alignment::Center);
            column![column![editor, actions].spacing(S2).padding(S2), results(tab, t)].into()
        }
        TabKind::Migration => migration_view(app, tab, t),
    }
}

fn results(tab: &Tab, t: Tokens) -> E<'_> {
    let id = tab.id;
    let mut col = column![];
    if let Some(e) = &tab.error {
        col = col.push(error_box(e, t));
    }
    match &tab.results {
        Some(r) if r.complete && r.known_rows == 0 => col.push(container(secondary("No rows", t)).center(Fill)).into(),
        Some(r) => {
            let mut g = Grid::new(r, t, move |a, b| Message::GridView(id, a, b), move |row, c| Message::GridSelect(id, row, c)).selected(tab.selected);
            if tab.kind == TabKind::Table {
                g = g.sort(tab.sort, move |c| Message::GridSort(id, c));
            }
            col.push(g).into()
        }
        None if tab.running() => col.push(container(secondary("Running…", t)).center(Fill)).into(),
        None => col.push(container(secondary("Run a query to see rows here (⌘↩ / Ctrl+Enter).", t)).center(Fill)).into(),
    }
}

// ------------------------------------------------------------------ inspector

fn inspector_panel(app: &App, t: Tokens) -> E<'_> {
    let head = container(secondary("INSPECTOR", t)).padding(Padding { top: S2, bottom: S1, left: S3, right: S3 });
    let Some(tab) = app.active_tab() else { return column![head, container(secondary("Nothing selected", t)).center(Fill)].into() };
    let (Some((row, col)), Some(r)) = (tab.selected, tab.results.as_ref()) else {
        return column![head, container(secondary("Select a cell to see its value", t)).center(Fill)].into();
    };
    let Some(prop) = r.def.properties_info.get(col) else { return head.into() };
    let ty = prop.source_type.clone().unwrap_or_else(|| format!("{:?}", prop.data_type));
    let info = column![
        text(prop.property_name.as_str()).size(TITLE).font(Font { weight: iced::font::Weight::Semibold, ..Font::DEFAULT }).color(t.text),
        secondary(format!("{ty} · row {}", row + 1), t),
    ]
    .spacing(2)
    .padding(Padding { top: 0.0, bottom: S2, left: S3, right: S3 });
    let Some((b, i)) = r.row(row) else { return column![head, info, container(secondary("Row is not loaded", t)).padding(S3)].into() };
    let value: E<'_> = match b.cell(i, col) {
        Cell::Null => container(secondary("NULL", t)).padding(S3).into(),
        Cell::Json(_) | Cell::JsonValue(_) => match &app.inspector_tree {
            Some((rows, _)) => TreeView::new(rows, t, t.window, |_| Message::DismissBanner, Message::InspectorToggle).into(),
            None => text_value(b.cell(i, col).to_text().into_owned(), t),
        },
        Cell::Bytes(bytes) => {
            let mut c = column![secondary(format!("{} bytes", bytes.len()), t)].spacing(S2).padding(S3);
            if inspector::is_image(bytes) {
                c = c.push(image(image::Handle::from_bytes(bytes.to_vec())).width(Fill));
            }
            c = c.push(text(inspector::hex_dump(bytes)).font(Font::MONOSPACE).size(SMALL).color(t.text));
            scrollable(c).height(Fill).into()
        }
        other => text_value(other.to_text().into_owned(), t),
    };
    column![head, info, rule::horizontal(1).style(move |_| rule::Style { color: t.separator, radius: 0.0.into(), fill_mode: rule::FillMode::Full, snap: true }), value].into()
}

fn text_value<'a>(s: String, t: Tokens) -> E<'a> {
    scrollable(container(text(inspector::cut_text(&s)).size(BODY).color(t.text).font(Font::MONOSPACE)).padding(S3)).height(Fill).into()
}

// ------------------------------------------------------------------ migration tab

fn field<'a>(name: &'a str, t: Tokens, input: impl Into<E<'a>>) -> E<'a> {
    row![text(name).size(BODY).color(t.text_secondary).width(130).align_x(iced::alignment::Horizontal::Right), input.into()].spacing(S3).align_y(Alignment::Center).into()
}

#[derive(Debug, Clone, PartialEq)]
struct ProfileChoice {
    id: String,
    name: String,
}

impl std::fmt::Display for ProfileChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

fn migration_view<'a>(app: &'a App, tab: &'a Tab, t: Tokens) -> E<'a> {
    let Some(m) = tab.migration.as_ref() else { return spacer().into() };
    let id = tab.id;
    let j = &m.job;
    let busy = m.running.is_some();
    let choices: Vec<ProfileChoice> = app.store.profiles.iter().map(|p| ProfileChoice { id: p.id.clone(), name: format!("{} ({})", p.name, scheme_label(&p.url)) }).collect();
    let chosen = |pid: &str| choices.iter().find(|c| c.id == pid).cloned();
    let pick = |pid: &str, f: fn(String) -> JobField| -> E<'a> {
        let p = pick_list(choices.clone(), chosen(pid), move |c: ProfileChoice| Message::Job(id, f(c.id))).placeholder("Choose a connection").text_size(BODY).style(theme::pick_list(t)).width(Fill);
        if busy {
            E::from(text(chosen(pid).map(|c| c.name).unwrap_or_default()).size(BODY).color(t.text))
        } else {
            p.into()
        }
    };
    let strs = |opts: &'static [&'static str], cur: &str, f: fn(String) -> JobField| -> E<'a> {
        pick_list(opts.iter().map(|s| s.to_string()).collect::<Vec<_>>(), Some(cur.to_string()), move |s| Message::Job(id, f(s))).text_size(BODY).style(theme::pick_list(t)).into()
    };
    let input = |placeholder: &'a str, value: &str, f: fn(String) -> JobField| -> E<'a> {
        text_input(placeholder, value).on_input_maybe((!busy).then_some(move |s| Message::Job(id, f(s)))).size(BODY).padding([S1, S2]).style(theme::text_input(t)).into()
    };
    let form = column![
        text("Migration").size(LARGE_TITLE).color(t.text),
        field("Name", t, input("Saved under this name", &j.name, JobField::Name)),
        field("Source", t, pick(&j.source, JobField::Source)),
        field("Target", t, pick(&j.target, JobField::Target)),
        field("SQL → NoSQL", t, strs(&STRATEGIES, &j.strategy, JobField::Strategy)),
        field("Transfer", t, strs(&TRANSFERS, &j.transfer, JobField::Transfer)),
        field("Batch rows", t, input("4096", &j.batch.to_string(), JobField::Batch)),
        field("Compression", t, strs(&COMPRESSIONS, &j.compression, JobField::Compression)),
        field(
            "Key file",
            t,
            row![
                text_input("one-time key in memory", j.key_file.as_deref().unwrap_or("")).on_input_maybe((!busy).then_some(move |s| Message::Job(id, JobField::KeyFile(s)))).size(BODY).padding([S1, S2]).style(theme::text_input(t)),
                btn("Choose…", t, ButtonKind::Secondary, (!busy).then_some(Message::PickFile(FileTarget::JobKey(id)))),
            ]
            .spacing(S2)
        ),
        field("Only", t, input("all tables (comma separated names)", &j.include, JobField::Include)),
        field("Skip", t, input("none", &j.exclude, JobField::Exclude)),
        field("Live transfer", t, checkbox(j.live).label("Keep streaming changes after the copy").on_toggle_maybe((!busy).then_some(move |v| Message::Job(id, JobField::Live(v)))).text_size(BODY).style(theme::checkbox(t))),
        row![
            spacer().width(Fill),
            btn("Copy CLI Command", t, ButtonKind::Plain, Some(Message::CopyCommand(id))),
            btn("Save", t, ButtonKind::Secondary, Some(Message::SaveJob(id))),
            if busy {
                btn(if m.running.as_ref().is_some_and(|r| r.stopping) { "Stopping…" } else { "Stop" }, t, ButtonKind::Destructive, m.running.as_ref().is_some_and(|r| !r.stopping).then_some(Message::StopMigration(id)))
            } else {
                btn("Start", t, ButtonKind::Primary, Some(Message::StartMigration(id)))
            },
        ]
        .spacing(S2),
    ]
    .spacing(S2)
    .padding(S4)
    .max_width(560);

    let mut progress = column![].spacing(S2).padding(S4).width(Fill);
    if let Some(r) = &m.result {
        progress = progress.push(match r {
            Ok(s) => text(s.as_str()).size(BODY).color(t.success),
            Err(e) => text(e.as_str()).size(BODY).color(t.danger),
        });
    }
    if let Some(run) = &m.running {
        progress = progress.push(text(format!("Running for {} s", run.started.elapsed().as_secs())).size(TITLE).color(t.text));
        progress = progress.push(secondary(format!("{} rows/s · {:.1} MB/s", format_rate(m.rows_per_s), m.mb_per_s), t));
    }
    if !m.tables.is_empty() {
        progress = progress.push(secondary("MIGRATION ORDER", t));
        let mut list = column![].spacing(2);
        for (n, (name, p)) in m.tables.iter().enumerate() {
            let state = if p.done {
                format!("✓ {} rows", p.rows)
            } else if p.phase.is_empty() {
                "waiting".to_string()
            } else {
                format!("{} · {} rows", p.phase, p.rows)
            };
            list = list.push(row![secondary(format!("{}.", n + 1), t).width(28), label(name.as_str(), t).width(Fill), text(state).size(SMALL).color(if p.done { t.success } else { t.text_secondary })].spacing(S2));
        }
        progress = progress.push(scrollable(list).height(Length::FillPortion(1)));
    }
    if m.job.live && (m.running.is_some() || !m.samples.is_empty()) {
        progress = progress.push(secondary("LIVE TRANSFER", t));
        progress = progress.push(label(
            format!(
                "{} events/s · {} events · lag {} · {} errors",
                format_rate(m.events_per_s),
                m.live_events,
                m.lag_ms.map(|l| format!("{l} ms")).unwrap_or_else(|| "–".into()),
                m.live_errors
            ),
            t,
        ));
        progress = progress.push(canvas(LagChart { samples: &m.samples, t }).width(Fill).height(140));
        progress = progress.push(secondary(format!("Checkpoint: {}", m.checkpoint.as_deref().unwrap_or("–")), t));
    }
    if m.result.is_none() && m.running.is_none() {
        progress = progress.push(secondary("Choose source and target, then Start. The target is checked first: existing tables are only overwritten after you confirm.", t));
    }
    row![scrollable(form).height(Fill), progress].height(Fill).into()
}

// ------------------------------------------------------------------ sheets

fn sheet<'a>(app: &'a App, s: &'a Sheet, t: Tokens) -> E<'a> {
    let card: E<'a> = match s {
        Sheet::Connection(f) => connection_sheet(f, t),
        Sheet::Confirm(c) => confirm_sheet(app, c, t),
    };
    // dimmed background; clicks outside the card do nothing (sheets are modal)
    opaque(container(container(card).style(theme::sheet(t)).max_width(620)).center(Fill).style(theme::container_bg(t.overlay)))
}

fn connection_sheet(f: &ConnForm, t: Tokens) -> E<'_> {
    let input = |placeholder: &'static str, value: &str, m: fn(String) -> FormField| -> E<'static> {
        text_input(placeholder, value).on_input(move |s| Message::Form(m(s))).size(BODY).padding([S1, S2]).style(theme::text_input(t)).into()
    };
    let file = |p: &Option<std::path::PathBuf>, target: FileTarget| -> E<'static> {
        row![
            text(p.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "none".into())).size(SMALL).color(t.text_secondary).width(Fill),
            btn("Choose…", t, ButtonKind::Secondary, Some(Message::PickFile(target))),
        ]
        .spacing(S2)
        .align_y(Alignment::Center)
        .into()
    };
    let sqlite = f.url.starts_with("sqlite:");
    let mut c = column![
        text(if f.editing.is_some() { "Edit Connection" } else { "New Connection" }).size(TITLE).font(Font { weight: iced::font::Weight::Semibold, ..Font::DEFAULT }).color(t.text),
        field("Name", t, input("e.g. Production orders", &f.name, FormField::Name)),
        field(
            "Connection",
            t,
            column![
                row![
                    text_input("postgresql://user@host:5432/db · mysql://user@host/db · oracle://user@host:1521/service · mongodb://… · sqlite://path · scylla://host/keyspace", &f.url).on_input(|s| Message::Form(FormField::Url(s))).size(BODY).padding([S1, S2]).style(theme::text_input(t)),
                    btn("SQLite File…", t, ButtonKind::Secondary, Some(Message::PickFile(FileTarget::SqliteFile))),
                ]
                .spacing(S2),
                secondary("Same connection strings as the command line.", t),
            ]
            .spacing(2)
        ),
    ]
    .spacing(S2)
    .padding(S4);
    if !sqlite {
        c = c.push(field("Password", t, text_input("stored in the system keychain", &f.password).secure(true).on_input(|s| Message::Form(FormField::Password(s))).size(BODY).padding([S1, S2]).style(theme::text_input(t))));
        c = c.push(field("TLS", t, pick_list(&TlsMode::ALL[..], Some(f.tls.mode), |m| Message::Form(FormField::Tls(m))).text_size(BODY).style(theme::pick_list(t))));
        if f.tls.mode != TlsMode::Off {
            c = c.push(field("CA certificate", t, file(&f.tls.ca_file, FileTarget::Ca)));
            c = c.push(field("Client certificate", t, file(&f.tls.client_cert, FileTarget::ClientCert)));
            c = c.push(field("Client key", t, file(&f.tls.client_key, FileTarget::ClientKey)));
        }
        c = c.push(field("SSH tunnel", t, checkbox(f.ssh_on).label("Connect through an SSH server").on_toggle(|v| Message::Form(FormField::SshOn(v))).text_size(BODY).style(theme::checkbox(t))));
        if f.ssh_on {
            c = c.push(field("SSH server", t, row![input("host", &f.ssh_host, FormField::SshHost), container(input("22", &f.ssh_port, FormField::SshPort)).width(70)].spacing(S2)));
            c = c.push(field("SSH user", t, input("user", &f.ssh_user, FormField::SshUser)));
            c = c.push(field(
                "SSH key",
                t,
                row![input("ssh agent", &f.ssh_key, FormField::SshKey), btn("Choose…", t, ButtonKind::Secondary, Some(Message::PickFile(FileTarget::SshKey)))].spacing(S2),
            ));
        }
    }
    if let Some(r) = &f.test {
        c = c.push(match r {
            Ok(s) => text(s.as_str()).size(BODY).color(t.success),
            Err(e) => text(e.as_str()).size(BODY).color(t.danger),
        });
    }
    let mut buttons = row![].spacing(S2).align_y(Alignment::Center);
    if let Some(id) = &f.editing {
        buttons = buttons.push(btn("Delete…", t, ButtonKind::Plain, Some(Message::AskDeleteConnection(id.clone()))));
    }
    buttons = buttons
        .push(space::horizontal())
        .push(btn(if f.testing { "Testing…" } else { "Test" }, t, ButtonKind::Secondary, (!f.testing).then_some(Message::TestConnection)))
        .push(btn("Cancel", t, ButtonKind::Secondary, Some(Message::CloseSheet)))
        .push(btn("Save", t, ButtonKind::Primary, Some(Message::SaveConnection)));
    scrollable(c.push(buttons)).into()
}

fn confirm_sheet<'a>(app: &'a App, c: &'a Confirm, t: Tokens) -> E<'a> {
    let (title, body, action): (String, String, &str) = match c {
        Confirm::DeleteConnection(id) => (
            format!("Delete “{}”?", app.profile(id).map(|p| p.name.as_str()).unwrap_or("")),
            "The connection and its saved password are removed. The database is not changed.".into(),
            "Delete",
        ),
        Confirm::StartMigration(_, existing) => (
            "Overwrite tables in the target?".into(),
            format!(
                "The target already has {}: {}. Rows with the same key are replaced and new rows are added.",
                if existing.len() == 1 { "this table" } else { "these tables" },
                existing.join(", ")
            ),
            "Overwrite and Start",
        ),
        Confirm::CloseRunningTab(_) => ("Stop the running migration?".into(), "Closing the tab stops the migration. A live transfer saves its checkpoint first.".into(), "Stop and Close"),
    };
    column![
        text(title).size(TITLE).font(Font { weight: iced::font::Weight::Semibold, ..Font::DEFAULT }).color(t.text),
        text(body).size(BODY).color(t.text),
        row![space::horizontal(), btn("Cancel", t, ButtonKind::Secondary, Some(Message::CloseSheet)), btn(action, t, ButtonKind::Destructive, Some(Message::Confirmed))].spacing(S2),
    ]
    .spacing(S3)
    .padding(S4)
    .width(Shrink)
    .max_width(460)
    .into()
}
