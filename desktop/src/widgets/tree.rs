//! Tree list for the sidebar (VIEW-1) and the inspector (VIEW-7): the caller passes the rows of
//! the open nodes only (children are loaded when a node is opened); the widget draws the visible
//! rows and handles mouse and keyboard (UI-6: arrows move, Right / Left open / close, Enter opens).

use iced::{
    advanced::{
        layout::{self, Layout},
        mouse,
        renderer,
        text,
        widget::{self, tree as wtree, Tree, Widget},
        Clipboard, Shell,
    },
    keyboard::{self, key::Named, Key},
    Color, Element, Event, Length, Rectangle, Renderer, Size, Theme,
};

use super::{fill, fill_rounded, line, outline, scrollbar, wheel_pixels, Look};
use crate::theme::{Tokens, BODY, RADIUS, ROW_HEIGHT, SMALL};

const INDENT: f32 = 14.0;

#[derive(Debug, Clone, PartialEq)]
pub struct TreeRow {
    pub depth: u16,
    pub label: String,
    /// Shown dimmed after the label (type, count, ...).
    pub detail: String,
    pub expandable: bool,
    pub expanded: bool,
    /// Status dot before the label (connection state).
    pub badge: Option<Color>,
}

type OnRow<'a, M> = Box<dyn Fn(usize) -> M + 'a>;

pub struct TreeView<'a, Message> {
    rows: &'a [TreeRow],
    t: Tokens,
    background: Color,
    selected: Option<usize>,
    on_select: OnRow<'a, Message>,
    on_toggle: OnRow<'a, Message>,
    on_activate: Option<OnRow<'a, Message>>,
}

impl<'a, Message> TreeView<'a, Message> {
    pub fn new(rows: &'a [TreeRow], t: Tokens, background: Color, on_select: impl Fn(usize) -> Message + 'a, on_toggle: impl Fn(usize) -> Message + 'a) -> Self {
        TreeView { rows, t, background, selected: None, on_select: Box::new(on_select), on_toggle: Box::new(on_toggle), on_activate: None }
    }

    pub fn selected(mut self, i: Option<usize>) -> Self {
        self.selected = i;
        self
    }

    pub fn on_activate(mut self, f: impl Fn(usize) -> Message + 'a) -> Self {
        self.on_activate = Some(Box::new(f));
        self
    }
}

#[derive(Default)]
pub struct State {
    offset: f32,
    focused: bool,
    last_click: Option<(std::time::Instant, usize)>,
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

/// Index of the parent row of `i` (the closest row above with a smaller depth).
pub fn parent_of(rows: &[TreeRow], i: usize) -> Option<usize> {
    let d = rows.get(i)?.depth;
    rows[..i].iter().rposition(|r| r.depth < d)
}

impl<Message> TreeView<'_, Message> {
    fn clamp(&self, state: &mut State, bounds: Rectangle) {
        let max = (self.rows.len() as f32 * ROW_HEIGHT - bounds.height).max(0.0);
        state.offset = state.offset.clamp(0.0, max);
    }

    fn reveal(&self, state: &mut State, bounds: Rectangle, i: usize) {
        let top = i as f32 * ROW_HEIGHT;
        if top < state.offset {
            state.offset = top;
        } else if top + ROW_HEIGHT > state.offset + bounds.height {
            state.offset = top + ROW_HEIGHT - bounds.height;
        }
        self.clamp(state, bounds);
    }
}

impl<Message> Widget<Message, Theme, Renderer> for TreeView<'_, Message> {
    fn tag(&self) -> wtree::Tag {
        wtree::Tag::of::<State>()
    }

    fn state(&self) -> wtree::State {
        wtree::State::new(State::default())
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
        _clipboard: &mut dyn Clipboard,
        shell: &mut Shell<'_, Message>,
        _viewport: &Rectangle,
    ) {
        let bounds = layout.bounds();
        let state = tree.state.downcast_mut::<State>();
        match event {
            Event::Mouse(mouse::Event::WheelScrolled { delta }) if cursor.is_over(bounds) => {
                state.offset += wheel_pixels(*delta, ROW_HEIGHT).1;
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
                let i = ((p.y + state.offset) / ROW_HEIGHT) as usize;
                let Some(row) = self.rows.get(i) else { return };
                let disclosure_end = 8.0 + row.depth as f32 * INDENT + INDENT;
                let now = std::time::Instant::now();
                let double = state.last_click.is_some_and(|(t, r)| r == i && now - t < std::time::Duration::from_millis(400));
                state.last_click = Some((now, i));
                shell.publish((self.on_select)(i));
                if row.expandable && (p.x < disclosure_end || double) {
                    shell.publish((self.on_toggle)(i));
                } else if double {
                    if let Some(f) = &self.on_activate {
                        shell.publish(f(i));
                    }
                }
            }
            Event::Keyboard(keyboard::Event::KeyPressed { key, .. }) if state.focused && !self.rows.is_empty() => {
                let cur = self.selected.unwrap_or(0).min(self.rows.len() - 1);
                let row = &self.rows[cur];
                let target = match key {
                    Key::Named(Named::ArrowDown) => Some((cur + 1).min(self.rows.len() - 1)),
                    Key::Named(Named::ArrowUp) => Some(cur.saturating_sub(1)),
                    Key::Named(Named::Home) => Some(0),
                    Key::Named(Named::End) => Some(self.rows.len() - 1),
                    Key::Named(Named::ArrowRight) if row.expandable && !row.expanded => {
                        shell.publish((self.on_toggle)(cur));
                        None
                    }
                    Key::Named(Named::ArrowRight) if row.expanded => Some((cur + 1).min(self.rows.len() - 1)),
                    Key::Named(Named::ArrowLeft) if row.expanded => {
                        shell.publish((self.on_toggle)(cur));
                        None
                    }
                    Key::Named(Named::ArrowLeft) => parent_of(self.rows, cur),
                    Key::Named(Named::Enter) => {
                        match (&self.on_activate, row.expandable) {
                            (Some(f), false) => shell.publish(f(cur)),
                            _ => shell.publish((self.on_toggle)(cur)),
                        }
                        None
                    }
                    Key::Named(Named::Space) if row.expandable => {
                        shell.publish((self.on_toggle)(cur));
                        None
                    }
                    _ => return,
                };
                shell.capture_event();
                if let Some(i) = target {
                    self.reveal(state, bounds, i);
                    shell.publish((self.on_select)(i));
                }
            }
            _ => {}
        }
    }

    fn draw(&self, tree: &Tree, renderer: &mut Renderer, _theme: &Theme, _style: &renderer::Style, layout: Layout<'_>, _cursor: mouse::Cursor, _viewport: &Rectangle) {
        use renderer::Renderer as _;
        let t = self.t;
        let bounds = layout.bounds();
        let state = tree.state.downcast_ref::<State>();
        fill(renderer, bounds, self.background);
        let first = (state.offset / ROW_HEIGHT) as usize;
        let count = (bounds.height / ROW_HEIGHT).ceil() as usize + 1;
        let width = bounds.width - if scrollbar(bounds, 0.0, self.rows.len() as f32 * ROW_HEIGHT, state.offset).is_some() { super::SCROLLBAR } else { 0.0 };
        renderer.with_layer(bounds, |renderer| {
            for (i, row) in self.rows.iter().enumerate().skip(first).take(count) {
                let y = bounds.y + i as f32 * ROW_HEIGHT - state.offset;
                let r = Rectangle { x: bounds.x + 4.0, y, width: width - 8.0, height: ROW_HEIGHT };
                let selected = self.selected == Some(i);
                if selected {
                    fill_rounded(renderer, r, if state.focused { t.selection } else { t.selection_inactive }, RADIUS);
                }
                let fg = if selected && state.focused { t.text_on_accent } else { t.text };
                let dim = if selected && state.focused { t.text_on_accent } else { t.text_secondary };
                let mut x = r.x + 4.0 + row.depth as f32 * INDENT;
                if row.expandable {
                    let chevron = if row.expanded { "▾" } else { "▸" };
                    line(renderer, chevron.into(), Rectangle { x, width: INDENT, ..r }, r, Look::new(dim, BODY).align(text::Alignment::Center));
                }
                x += INDENT;
                if let Some(c) = row.badge {
                    fill_rounded(renderer, Rectangle { x: x + 1.0, y: r.center_y() - 4.0, width: 8.0, height: 8.0 }, c, 4.0);
                    x += 14.0;
                }
                let label_w = r.x + r.width - x - 4.0;
                line(renderer, row.label.clone(), Rectangle { x, width: label_w, ..r }, r, Look::new(fg, BODY));
                if !row.detail.is_empty() {
                    line(renderer, row.detail.clone(), Rectangle { x, width: label_w, ..r }, r, Look::new(dim, SMALL).align(text::Alignment::Right));
                }
            }
        });
        if let Some((_, thumb)) = scrollbar(bounds, 0.0, self.rows.len() as f32 * ROW_HEIGHT, state.offset) {
            fill_rounded(renderer, thumb, Color { a: 0.45, ..t.text_secondary }, thumb.width / 2.0);
        }
        if state.focused {
            outline(renderer, bounds, t.focus, 2.0, 0.0);
        }
    }
}

impl<'a, Message: 'a> From<TreeView<'a, Message>> for Element<'a, Message> {
    fn from(t: TreeView<'a, Message>) -> Self {
        Element::new(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(depth: u16) -> TreeRow {
        TreeRow { depth, label: String::new(), detail: String::new(), expandable: false, expanded: false, badge: None }
    }

    #[test]
    fn parent_is_the_closest_shallower_row_above() {
        let rows = [row(0), row(1), row(2), row(2), row(1), row(0)];
        assert_eq!(parent_of(&rows, 3), Some(1));
        assert_eq!(parent_of(&rows, 4), Some(0));
        assert_eq!(parent_of(&rows, 0), None);
        assert_eq!(parent_of(&rows, 5), None);
    }
}
