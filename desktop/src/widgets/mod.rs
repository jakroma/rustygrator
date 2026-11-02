//! Custom widgets on iced's low-level `Widget` trait (APP-7): they lay out and draw only the
//! visible rows themselves instead of nesting one built-in widget per row or cell.

pub mod grid;
pub mod tree;

use iced::{
    advanced::{
        renderer::{self, Quad},
        text::{self, Renderer as _, Text},
    },
    alignment, border, Background, Color, Font, Pixels, Point, Rectangle, Renderer, Size,
};

use rustygrator_core::dbc::batch::Cell;

/// Characters of a value shown in a cell; the inspector shows the whole value (VIEW-8).
pub const PREVIEW_CHARS: usize = 200;

/// Text of a cell as shown in a list: one line, at most [`PREVIEW_CHARS`] characters, bytes as
/// the first bytes in hex. Only called for visible cells (VIEW-4).
pub fn preview(cell: Cell<'_>) -> String {
    match cell {
        Cell::Null => "NULL".to_string(),
        Cell::Bytes(b) => {
            let mut s = String::with_capacity(2 + 32 + 16);
            s.push_str("0x");
            for byte in b.iter().take(16) {
                s.push_str(&format!("{byte:02x}"));
            }
            if b.len() > 16 {
                s.push_str(&format!("… ({} bytes)", b.len()));
            }
            s
        }
        other => {
            let full = other.to_text();
            let mut s: String = full.chars().take(PREVIEW_CHARS).map(|c| if c == '\n' || c == '\r' { '⏎' } else { c }).collect();
            if full.chars().nth(PREVIEW_CHARS).is_some() {
                s.push('…');
            }
            s
        }
    }
}

pub fn fill(renderer: &mut Renderer, bounds: Rectangle, color: Color) {
    use renderer::Renderer as _;
    renderer.fill_quad(Quad { bounds, ..Quad::default() }, Background::Color(color));
}

pub fn fill_rounded(renderer: &mut Renderer, bounds: Rectangle, color: Color, radius: f32) {
    use renderer::Renderer as _;
    renderer.fill_quad(Quad { bounds, border: border::rounded(radius), ..Quad::default() }, Background::Color(color));
}

pub fn outline(renderer: &mut Renderer, bounds: Rectangle, color: Color, width: f32, radius: f32) {
    use renderer::Renderer as _;
    renderer.fill_quad(
        Quad { bounds, border: iced::Border { color, width, radius: radius.into() }, ..Quad::default() },
        Background::Color(Color::TRANSPARENT),
    );
}

/// Look of a line of text.
#[derive(Clone, Copy)]
pub struct Look {
    pub color: Color,
    pub size: f32,
    pub font: Font,
    pub align: text::Alignment,
}

impl Look {
    pub fn new(color: Color, size: f32) -> Self {
        Look { color, size, font: Font::DEFAULT, align: text::Alignment::Left }
    }

    pub fn align(self, align: text::Alignment) -> Self {
        Look { align, ..self }
    }

    pub fn semibold(self) -> Self {
        Look { font: Font { weight: iced::font::Weight::Semibold, ..self.font }, ..self }
    }
}

/// One line of text, vertically centered in `bounds` and clipped to `clip`.
pub fn line(renderer: &mut Renderer, content: String, bounds: Rectangle, clip: Rectangle, look: Look) {
    let Look { color, size, font, align } = look;
    let x = match align {
        text::Alignment::Right => bounds.x + bounds.width,
        text::Alignment::Center => bounds.center_x(),
        _ => bounds.x,
    };
    renderer.fill_text(
        Text {
            content,
            // unbounded width: one line, cut by `clip` instead of wrapped
            bounds: Size::new(if align == text::Alignment::Left { f32::INFINITY } else { bounds.width }, bounds.height),
            size: Pixels(size),
            line_height: text::LineHeight::default(),
            font,
            align_x: align,
            align_y: alignment::Vertical::Center,
            shaping: text::Shaping::Auto,
            wrapping: text::Wrapping::None,
        },
        Point::new(x, bounds.center_y()),
        color,
        clip,
    );
}

/// Vertical scroll bar geometry: (track, thumb) for `content` pixels shown in `view` pixels.
pub fn scrollbar(bounds: Rectangle, top: f32, content: f32, offset: f32) -> Option<(Rectangle, Rectangle)> {
    let view = bounds.height - top;
    if content <= view || view <= 0.0 {
        return None;
    }
    let track = Rectangle { x: bounds.x + bounds.width - SCROLLBAR, y: bounds.y + top, width: SCROLLBAR, height: view };
    let h = (view * view / content).max(24.0).min(view);
    let y = track.y + (offset / (content - view)) * (view - h);
    Some((track, Rectangle { x: track.x + 2.0, y, width: SCROLLBAR - 4.0, height: h }))
}

pub const SCROLLBAR: f32 = 11.0;

/// Pixels to scroll for a wheel event.
pub fn wheel_pixels(delta: iced::mouse::ScrollDelta, row: f32) -> (f32, f32) {
    match delta {
        iced::mouse::ScrollDelta::Lines { x, y } => (-x * row * 3.0, -y * row * 3.0),
        iced::mouse::ScrollDelta::Pixels { x, y } => (-x, -y),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view4_preview_is_one_short_line() {
        let long = "a\nb".repeat(500);
        let p = preview(Cell::Str(&long));
        assert_eq!(p.chars().count(), PREVIEW_CHARS + 1);
        assert!(!p.contains('\n'));
        assert_eq!(preview(Cell::Bytes(&[0, 255, 30])), "0x00ff1e");
        assert_eq!(preview(Cell::Bytes(&[7; 20])), "0x07070707070707070707070707070707… (20 bytes)");
        assert_eq!(preview(Cell::Null), "NULL");
        assert_eq!(preview(Cell::I64(-5)), "-5");
    }

    #[test]
    fn scrollbar_thumb_follows_the_offset() {
        let b = Rectangle { x: 0.0, y: 0.0, width: 100.0, height: 110.0 };
        assert!(scrollbar(b, 10.0, 50.0, 0.0).is_none());
        let (track, top) = scrollbar(b, 10.0, 1000.0, 0.0).unwrap();
        let (_, bottom) = scrollbar(b, 10.0, 1000.0, 900.0).unwrap();
        assert_eq!(top.y, track.y);
        assert!((bottom.y + bottom.height - (track.y + track.height)).abs() < 0.01);
    }
}
