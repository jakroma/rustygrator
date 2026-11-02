//! Virtualized data grid (VIEW-2..VIEW-4): draws only the rows and columns on screen straight
//! from the column batches of a [`ResultWindow`], formats a value to text only when its cell is
//! drawn, scrolls with the wheel, the scroll bar and the keyboard (UI-6), sorts on a header click
//! (VIEW-5) and resizes columns by dragging the header edge.

use std::time::{Duration, Instant};

use iced::{
    advanced::{
        clipboard::{self, Clipboard},
        layout::{self, Layout},
        mouse,
        renderer,
        text,
        widget::{self, tree, Tree, Widget},
        Shell,
    },
    keyboard::{self, key::Named, Key},
    Element, Event, Length, Point, Rectangle, Renderer, Size, Theme, Vector,
};

use rustygrator_core::models::value::LogicalType;

use super::{fill, line, outline, preview, scrollbar, wheel_pixels, Look};
use crate::{results::ResultWindow, theme::{Tokens, BODY, ROW_HEIGHT, SMALL}};

const HEADER: f32 = ROW_HEIGHT + 4.0;
const PAD: f32 = 6.0;
const MIN_WIDTH: f32 = 48.0;
const EDGE: f32 = 4.0;
const DOUBLE_CLICK: Duration = Duration::from_millis(400);

type OnView<'a, M> = Box<dyn Fn(u64, u64) -> M + 'a>;
type OnCell<'a, M> = Box<dyn Fn(u64, usize) -> M + 'a>;

pub struct Grid<'a, Message> {
    data: &'a ResultWindow,
    t: Tokens,
    selected: Option<(u64, usize)>,
    sort: Option<(usize, bool)>,
    on_view: OnView<'a, Message>,
    on_select: OnCell<'a, Message>,
    on_activate: Option<OnCell<'a, Message>>,
    on_sort: Option<Box<dyn Fn(usize) -> Message + 'a>>,
}

impl<'a, Message> Grid<'a, Message> {
    pub fn new(data: &'a ResultWindow, t: Tokens, on_view: impl Fn(u64, u64) -> Message + 'a, on_select: impl Fn(u64, usize) -> Message + 'a) -> Self {
        Grid { data, t, selected: None, sort: None, on_view: Box::new(on_view), on_select: Box::new(on_select), on_activate: None, on_sort: None }
    }

    pub fn selected(mut self, cell: Option<(u64, usize)>) -> Self {
        self.selected = cell;
        self
    }

    pub fn sort(mut self, sort: Option<(usize, bool)>, on_sort: impl Fn(usize) -> Message + 'a) -> Self {
        self.sort = sort;
        self.on_sort = Some(Box::new(on_sort));
        self
    }

    pub fn on_activate(mut self, f: impl Fn(u64, usize) -> Message + 'a) -> Self {
        self.on_activate = Some(Box::new(f));
        self
    }

    fn rows(&self) -> u64 {
        // one placeholder row while more rows are coming
        self.data.known_rows + u64::from(!self.data.complete)
    }
}

#[derive(Default)]
enum Drag {
    #[default]
    None,
    Thumb { grab: f32 },
    Resize { col: usize, start_x: f32, start_w: f32 },
}

#[derive(Default)]
pub struct State {
    offset: Vector,
    widths: Vec<f32>,
    focused: bool,
    drag: Drag,
    view: Option<(u64, u64)>,
    last_click: Option<(Instant, u64, usize)>,
}

impl widget::operation::Focusable for State {
    fn is_focused(&self) -> bool {
        self.focused
    }
    fn focus(&mut self) {
        self.focused = true;
    }
    fn unfocus(&mut self) {
        self.focused = false;
    }
}

/// Starting width of a column from its name and type.
pub fn initial_width(name: &str, kind: LogicalType) -> f32 {
    let by_type = match kind {
        LogicalType::Bool => 60.0,
        LogicalType::Int16 | LogicalType::Int32 => 80.0,
        LogicalType::Int64 | LogicalType::Float32 | LogicalType::Float64 | LogicalType::Date | LogicalType::Time => 110.0,
        LogicalType::Decimal => 130.0,
        LogicalType::DateTime | LogicalType::DateTimeTz => 190.0,
        LogicalType::Uuid => 290.0,
        LogicalType::ObjectId => 200.0,
        LogicalType::String | LogicalType::Json | LogicalType::Bytes => 220.0,
    };
    (name.chars().count() as f32 * 7.5 + 2.0 * PAD + 14.0).max(by_type).min(320.0)
}

fn right_aligned(kind: LogicalType) -> bool {
    matches!(kind, LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64 | LogicalType::Float32 | LogicalType::Float64 | LogicalType::Decimal)
}

impl<Message> Grid<'_, Message> {
    fn widths<'s>(&self, state: &'s mut State) -> &'s [f32] {
        let props = &self.data.def.properties_info;
        if state.widths.len() != props.len() {
            state.widths = props.iter().map(|p| initial_width(&p.property_name, p.data_type)).collect();
        }
        &state.widths
    }

    fn content_size(&self, state: &State) -> Size {
        Size::new(state.widths.iter().sum(), self.rows() as f32 * ROW_HEIGHT)
    }

    fn clamp(&self, state: &mut State, bounds: Rectangle) {
        let content = self.content_size(state);
        let max_x = (content.width - (bounds.width - super::SCROLLBAR)).max(0.0);
        let max_y = (content.height - (bounds.height - HEADER)).max(0.0);
        state.offset.x = state.offset.x.clamp(0.0, max_x);
        state.offset.y = state.offset.y.clamp(0.0, max_y);
    }

    fn visible(&self, state: &State, bounds: Rectangle) -> (u64, u64) {
        let first = (state.offset.y / ROW_HEIGHT) as u64;
        let count = ((bounds.height - HEADER) / ROW_HEIGHT).ceil().max(1.0) as u64;
        (first, (first + count).min(self.rows().saturating_sub(1)).max(first))
    }

    fn publish_view(&self, state: &mut State, bounds: Rectangle, shell: &mut Shell<'_, Message>) {
        let v = self.visible(state, bounds);
        if state.view != Some(v) {
            state.view = Some(v);
            shell.publish((self.on_view)(v.0, v.1));
        }
    }

    /// Column under an x position relative to the content.
    fn column_at(state: &State, x: f32) -> Option<usize> {
        let mut acc = 0.0;
        for (i, w) in state.widths.iter().enumerate() {
            if x < acc + w {
                return Some(i);
            }
            acc += w;
        }
        None
    }

    /// Column whose right edge is under an x position relative to the content.
    fn edge_at(state: &State, x: f32) -> Option<usize> {
        let mut acc = 0.0;
        for (i, w) in state.widths.iter().enumerate() {
            acc += w;
            if (x - acc).abs() <= EDGE {
                return Some(i);
            }
        }
        None
    }

    fn scroll_into_view(&self, state: &mut State, bounds: Rectangle, row: u64, col: usize) {
        let top = row as f32 * ROW_HEIGHT;
        let view_h = bounds.height - HEADER;
        if top < state.offset.y {
            state.offset.y = top;
        } else if top + ROW_HEIGHT > state.offset.y + view_h {
            state.offset.y = top + ROW_HEIGHT - view_h;
        }
        let left: f32 = state.widths[..col].iter().sum();
        let right = left + state.widths[col];
        let view_w = bounds.width - super::SCROLLBAR;
        if left < state.offset.x {
            state.offset.x = left;
        } else if right > state.offset.x + view_w {
            state.offset.x = right - view_w;
        }
        self.clamp(state, bounds);
    }
}

impl<Message> Widget<Message, Theme, Renderer> for Grid<'_, Message> {
    fn tag(&self) -> tree::Tag {
        tree::Tag::of::<State>()
    }

    fn state(&self) -> tree::State {
        tree::State::new(State::default())
    }

    fn size(&self) -> Size<Length> {
        Size::new(Length::Fill, Length::Fill)
    }

    fn layout(&mut self, _tree: &mut Tree, _renderer: &Renderer, limits: &layout::Limits) -> layout::Node {
        layout::Node::new(limits.max())
    }

    fn operate(&mut self, tree: &mut Tree, layout: Layout<'_>, _renderer: &Renderer, operation: &mut dyn widget::Operation) {
        operation.focusable(None, layout.bounds(), tree.state.downcast_mut::<State>());
    }

    fn update(
        &mut self,
        tree: &mut Tree,
        event: &Event,
        layout: Layout<'_>,
        cursor: mouse::Cursor,
        _renderer: &Renderer,
        clipboard: &mut dyn Clipboard,
        shell: &mut Shell<'_, Message>,
        _viewport: &Rectangle,
    ) {
        let bounds = layout.bounds();
        let state = tree.state.downcast_mut::<State>();
        self.widths(state);
        let cols = state.widths.len();
        let rows = self.rows();
        match event {
            Event::Mouse(mouse::Event::WheelScrolled { delta }) if cursor.is_over(bounds) => {
                let (dx, dy) = wheel_pixels(*delta, ROW_HEIGHT);
                state.offset.x += dx;
                state.offset.y += dy;
                self.clamp(state, bounds);
                shell.capture_event();
                shell.request_redraw();
            }
            Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)) => {
                let Some(p) = cursor.position_in(bounds) else {
                    state.focused = false;
                    return;
                };
                state.focused = true;
                shell.capture_event();
                if let Some((track, thumb)) = scrollbar(bounds, HEADER, self.content_size(state).height, state.offset.y) {
                    let abs = Point::new(p.x + bounds.x, p.y + bounds.y);
                    if track.contains(abs) {
                        let grab = if thumb.contains(abs) { abs.y - thumb.y } else { thumb.height / 2.0 };
                        state.drag = Drag::Thumb { grab };
                        drag_thumb(self, state, bounds, abs.y, grab);
                        return;
                    }
                }
                let x = p.x + state.offset.x;
                if p.y < HEADER {
                    if let Some(col) = Self::edge_at(state, x) {
                        state.drag = Drag::Resize { col, start_x: p.x, start_w: state.widths[col] };
                    } else if let (Some(col), Some(on_sort)) = (Self::column_at(state, x), &self.on_sort) {
                        shell.publish(on_sort(col));
                    }
                    return;
                }
                let row = ((p.y - HEADER + state.offset.y) / ROW_HEIGHT) as u64;
                if let Some(col) = Self::column_at(state, x).filter(|_| row < self.data.known_rows) {
                    let now = Instant::now();
                    let double = state.last_click.is_some_and(|(t, r, c)| r == row && c == col && now - t < DOUBLE_CLICK);
                    state.last_click = Some((now, row, col));
                    shell.publish((self.on_select)(row, col));
                    if let (true, Some(f)) = (double, &self.on_activate) {
                        shell.publish(f(row, col));
                    }
                }
            }
            Event::Mouse(mouse::Event::CursorMoved { position }) => match state.drag {
                Drag::Thumb { grab } => {
                    drag_thumb(self, state, bounds, position.y, grab);
                    shell.request_redraw();
                }
                Drag::Resize { col, start_x, start_w } => {
                    state.widths[col] = (start_w + position.x - bounds.x - start_x).max(MIN_WIDTH);
                    self.clamp(state, bounds);
                    shell.request_redraw();
                }
                Drag::None => {}
            },
            Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)) => state.drag = Drag::None,
            Event::Keyboard(keyboard::Event::KeyPressed { key, modifiers, .. }) if state.focused && cols > 0 => {
                let page = ((bounds.height - HEADER) / ROW_HEIGHT).floor().max(1.0) as u64;
                let (row, col) = self.selected.unwrap_or((0, 0));
                let last_row = self.data.known_rows.saturating_sub(1);
                let target = match key {
                    Key::Named(Named::ArrowDown) => Some((row.saturating_add(1).min(last_row), col)),
                    Key::Named(Named::ArrowUp) => Some((row.saturating_sub(1), col)),
                    Key::Named(Named::ArrowRight) => Some((row, (col + 1).min(cols - 1))),
                    Key::Named(Named::ArrowLeft) => Some((row, col.saturating_sub(1))),
                    Key::Named(Named::PageDown) => Some(((row + page).min(last_row), col)),
                    Key::Named(Named::PageUp) => Some((row.saturating_sub(page), col)),
                    Key::Named(Named::Home) => Some((0, if modifiers.command() { 0 } else { col })),
                    Key::Named(Named::End) => Some((last_row, if modifiers.command() { cols - 1 } else { col })),
                    Key::Named(Named::Enter) => {
                        if let (Some(f), Some((r, c))) = (&self.on_activate, self.selected) {
                            shell.publish(f(r, c));
                        }
                        None
                    }
                    Key::Character(c) if c.as_str() == "c" && modifiers.command() => {
                        if let Some(cell) = self.selected.and_then(|(r, c)| self.data.row(r).map(|(b, i)| b.cell(i, c))) {
                            clipboard.write(clipboard::Kind::Standard, cell.to_text().into_owned());
                        }
                        None
                    }
                    _ => return,
                };
                shell.capture_event();
                if let Some((r, c)) = target.filter(|_| rows > 0 && self.data.known_rows > 0) {
                    self.scroll_into_view(state, bounds, r, c);
                    shell.publish((self.on_select)(r, c));
                }
            }
            _ => {}
        }
        self.publish_view(state, bounds, shell);
    }

    fn mouse_interaction(&self, tree: &Tree, layout: Layout<'_>, cursor: mouse::Cursor, _viewport: &Rectangle, _renderer: &Renderer) -> mouse::Interaction {
        let state = tree.state.downcast_ref::<State>();
        if matches!(state.drag, Drag::Resize { .. }) {
            return mouse::Interaction::ResizingColumn;
        }
        match cursor.position_in(layout.bounds()) {
            Some(p) if p.y < HEADER && Self::edge_at(state, p.x + state.offset.x).is_some() => mouse::Interaction::ResizingColumn,
            Some(p) if p.y < HEADER && self.on_sort.is_some() => mouse::Interaction::Pointer,
            _ => mouse::Interaction::default(),
        }
    }

    fn draw(&self, tree: &Tree, renderer: &mut Renderer, _theme: &Theme, _style: &renderer::Style, layout: Layout<'_>, _cursor: mouse::Cursor, _viewport: &Rectangle) {
        use renderer::Renderer as _;
        let t = self.t;
        let bounds = layout.bounds();
        let state = tree.state.downcast_ref::<State>();
        let props = &self.data.def.properties_info;
        let widths: Vec<f32> = if state.widths.len() == props.len() {
            state.widths.clone()
        } else {
            props.iter().map(|p| initial_width(&p.property_name, p.data_type)).collect()
        };
        fill(renderer, bounds, t.content);
        let body = Rectangle { y: bounds.y + HEADER, height: bounds.height - HEADER, width: bounds.width - super::SCROLLBAR, ..bounds };
        let (first, last) = self.visible(state, bounds);

        renderer.with_layer(body, |renderer| {
            for row in first..=last {
                let y = body.y + row as f32 * ROW_HEIGHT - state.offset.y;
                let row_rect = Rectangle { x: body.x, y, width: body.width, height: ROW_HEIGHT };
                if row % 2 == 1 {
                    fill(renderer, row_rect, t.row_alt);
                }
                let data = self.data.row(row);
                let mut x = body.x - state.offset.x;
                for (col, (p, w)) in props.iter().zip(&widths).enumerate() {
                    let cell_rect = Rectangle { x, y, width: *w, height: ROW_HEIGHT };
                    x += w;
                    if cell_rect.x + cell_rect.width < body.x || cell_rect.x > body.x + body.width {
                        continue;
                    }
                    let selected = self.selected == Some((row, col));
                    if selected {
                        fill(renderer, cell_rect, if state.focused { t.selection } else { t.selection_inactive });
                    }
                    let inner = Rectangle { x: cell_rect.x + PAD, width: cell_rect.width - 2.0 * PAD, ..cell_rect };
                    let clip = inner.intersection(&body).unwrap_or(inner);
                    let fg = if selected && state.focused { t.text_on_accent } else { t.text };
                    match data {
                        Some((b, r)) => {
                            let cell = b.cell(r, col);
                            let (content, color) = if cell.is_null() { ("NULL".to_string(), if selected && state.focused { fg } else { t.text_secondary }) } else { (preview(cell), fg) };
                            let align = if right_aligned(p.data_type) { text::Alignment::Right } else { text::Alignment::Left };
                            line(renderer, content, inner, clip, Look::new(color, BODY).align(align));
                        }
                        None if col == 0 => line(renderer, "Loading…".into(), inner, clip, Look::new(t.text_secondary, BODY)),
                        None => {}
                    }
                }
            }
        });

        // header (stays on top while scrolling)
        let header = Rectangle { height: HEADER, width: bounds.width, ..bounds };
        fill(renderer, header, t.window);
        fill(renderer, Rectangle { y: header.y + HEADER - 1.0, height: 1.0, ..header }, t.separator);
        renderer.with_layer(Rectangle { width: body.width, ..header }, |renderer| {
            let mut x = bounds.x - state.offset.x;
            for (col, (p, w)) in props.iter().zip(&widths).enumerate() {
                let r = Rectangle { x: x + PAD, y: header.y, width: w - 2.0 * PAD - 10.0, height: HEADER };
                let arrow = match self.sort {
                    Some((c, desc)) if c == col => if desc { " ▼" } else { " ▲" },
                    _ => "",
                };
                line(renderer, format!("{}{arrow}", p.property_name), r, Rectangle { x: x + PAD, width: w - 2.0 * PAD, ..header }, Look::new(t.text, SMALL + 1.0).semibold());
                x += w;
                fill(renderer, Rectangle { x: x - 1.0, y: header.y + 4.0, width: 1.0, height: HEADER - 8.0 }, t.separator);
            }
        });

        if let Some((track, thumb)) = scrollbar(bounds, HEADER, self.content_size_from(&widths).height, state.offset.y) {
            fill(renderer, track, t.content);
            super::fill_rounded(renderer, thumb, iced::Color { a: 0.45, ..t.text_secondary }, thumb.width / 2.0);
        }
        if state.focused {
            outline(renderer, bounds, t.focus, 2.0, 0.0);
        }
    }
}

impl<Message> Grid<'_, Message> {
    fn content_size_from(&self, widths: &[f32]) -> Size {
        Size::new(widths.iter().sum(), self.rows() as f32 * ROW_HEIGHT)
    }
}

fn drag_thumb<Message>(grid: &Grid<'_, Message>, state: &mut State, bounds: Rectangle, y: f32, grab: f32) {
    let content = grid.content_size(state).height;
    if let Some((track, thumb)) = scrollbar(bounds, HEADER, content, state.offset.y) {
        let travel = (track.height - thumb.height).max(1.0);
        let view = bounds.height - HEADER;
        state.offset.y = ((y - grab - track.y) / travel) * (content - view);
        grid.clamp(state, bounds);
    }
}

impl<'a, Message: 'a> From<Grid<'a, Message>> for Element<'a, Message> {
    fn from(g: Grid<'a, Message>) -> Self {
        Element::new(g)
    }
}
