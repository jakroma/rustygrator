//! macOS look as design tokens (APP-8, UI-1, UI-2): colors after the macOS system colors in
//! light and dark mode (UI-5), the macOS type scale and an 4 pt spacing grid. Every widget style
//! in the app is built from these tokens.

use iced::{border, widget, Background, Border, Color, Shadow, Theme};

/// Color and size tokens of one appearance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tokens {
    pub dark: bool,
    pub window: Color,
    pub sidebar: Color,
    pub content: Color,
    pub row_alt: Color,
    pub control: Color,
    pub control_border: Color,
    pub separator: Color,
    pub text: Color,
    pub text_secondary: Color,
    pub text_on_accent: Color,
    pub accent: Color,
    pub selection: Color,
    pub selection_inactive: Color,
    pub focus: Color,
    pub danger: Color,
    pub success: Color,
    pub warning: Color,
    pub overlay: Color,
}

/// Type scale of macOS (points): body 13, small 11, title 15 / 17.
pub const BODY: f32 = 13.0;
pub const SMALL: f32 = 11.0;
pub const TITLE: f32 = 15.0;
pub const LARGE_TITLE: f32 = 17.0;
/// Spacing grid.
pub const S1: f32 = 4.0;
pub const S2: f32 = 8.0;
pub const S3: f32 = 12.0;
pub const S4: f32 = 16.0;
pub const RADIUS: f32 = 6.0;
pub const ROW_HEIGHT: f32 = 22.0;
pub const SIDEBAR_WIDTH: f32 = 240.0;
pub const INSPECTOR_WIDTH: f32 = 280.0;

const fn rgb(r: u8, g: u8, b: u8) -> Color {
    Color { r: r as f32 / 255.0, g: g as f32 / 255.0, b: b as f32 / 255.0, a: 1.0 }
}

impl Tokens {
    pub const LIGHT: Tokens = Tokens {
        dark: false,
        window: rgb(0xEC, 0xEC, 0xEC),
        sidebar: rgb(0xE3, 0xE3, 0xE6),
        content: rgb(0xFF, 0xFF, 0xFF),
        row_alt: rgb(0xF4, 0xF5, 0xF5),
        control: rgb(0xFF, 0xFF, 0xFF),
        control_border: rgb(0xC4, 0xC4, 0xC6),
        separator: rgb(0xD9, 0xD9, 0xDB),
        text: rgb(0x1D, 0x1D, 0x1F),
        text_secondary: rgb(0x5E, 0x5E, 0x63),
        text_on_accent: rgb(0xFF, 0xFF, 0xFF),
        // systemBlue darkened enough for white text at WCAG AA
        accent: rgb(0x00, 0x66, 0xD6),
        selection: rgb(0x00, 0x66, 0xD6),
        selection_inactive: rgb(0xDC, 0xDC, 0xE0),
        focus: rgb(0x00, 0x66, 0xD6),
        danger: rgb(0xC4, 0x1E, 0x14),
        success: rgb(0x1B, 0x7F, 0x37),
        warning: rgb(0x9A, 0x5B, 0x00),
        overlay: Color { r: 0.0, g: 0.0, b: 0.0, a: 0.25 },
    };

    pub const DARK: Tokens = Tokens {
        dark: true,
        window: rgb(0x26, 0x26, 0x28),
        sidebar: rgb(0x2D, 0x2D, 0x30),
        content: rgb(0x1E, 0x1E, 0x1E),
        row_alt: rgb(0x25, 0x25, 0x27),
        control: rgb(0x3A, 0x3A, 0x3C),
        control_border: rgb(0x55, 0x55, 0x58),
        separator: rgb(0x3D, 0x3D, 0x40),
        text: rgb(0xF2, 0xF2, 0xF7),
        text_secondary: rgb(0xC0, 0xC0, 0xC5),
        text_on_accent: rgb(0xFF, 0xFF, 0xFF),
        accent: rgb(0x0A, 0x6E, 0xE0),
        selection: rgb(0x0A, 0x5F, 0xC8),
        selection_inactive: rgb(0x46, 0x46, 0x4A),
        focus: rgb(0x40, 0x9C, 0xFF),
        danger: rgb(0xFF, 0x8A, 0x80),
        success: rgb(0x4C, 0xD9, 0x64),
        warning: rgb(0xFF, 0xC2, 0x4D),
        overlay: Color { r: 0.0, g: 0.0, b: 0.0, a: 0.45 },
    };

    pub fn for_dark(dark: bool) -> Tokens {
        if dark {
            Self::DARK
        } else {
            Self::LIGHT
        }
    }

    /// Same tokens with the system accent color, if it keeps WCAG AA contrast with white text.
    pub fn with_accent(mut self, accent: Option<Color>) -> Tokens {
        if let Some(a) = accent.filter(|a| contrast(self.text_on_accent, *a) >= 4.5) {
            self.accent = a;
            self.selection = a;
            self.focus = a;
        }
        self
    }

    pub fn theme(&self) -> Theme {
        Theme::custom(
            if self.dark { "rustygrator dark" } else { "rustygrator light" },
            iced::theme::Palette {
                background: self.content,
                text: self.text,
                primary: self.accent,
                success: self.success,
                warning: self.warning,
                danger: self.danger,
            },
        )
    }
}

/// Relative luminance (WCAG 2.1).
pub fn luminance(c: Color) -> f32 {
    let ch = |v: f32| if v <= 0.04045 { v / 12.92 } else { ((v + 0.055) / 1.055).powf(2.4) };
    0.2126 * ch(c.r) + 0.7152 * ch(c.g) + 0.0722 * ch(c.b)
}

/// Contrast ratio between two opaque colors (WCAG 2.1).
pub fn contrast(a: Color, b: Color) -> f32 {
    let (la, lb) = (luminance(a), luminance(b));
    (la.max(lb) + 0.05) / (la.min(lb) + 0.05)
}

// ------------------------------------------------------------------ widget styles

pub fn container_bg(color: Color) -> impl Fn(&Theme) -> widget::container::Style {
    move |_| widget::container::Style { background: Some(Background::Color(color)), ..Default::default() }
}

pub fn panel(t: Tokens, color: Color) -> impl Fn(&Theme) -> widget::container::Style {
    move |_| widget::container::Style {
        background: Some(Background::Color(color)),
        text_color: Some(t.text),
        ..Default::default()
    }
}

/// Sheet (modal dialog) card.
pub fn sheet(t: Tokens) -> impl Fn(&Theme) -> widget::container::Style {
    move |_| widget::container::Style {
        background: Some(Background::Color(t.window)),
        text_color: Some(t.text),
        border: Border { color: t.control_border, width: 1.0, radius: (RADIUS * 2.0).into() },
        shadow: Shadow { color: Color { a: 0.3, ..Color::BLACK }, offset: iced::Vector::new(0.0, 8.0), blur_radius: 24.0 },
        snap: true,
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum ButtonKind {
    /// Default action of a sheet (accent filled)
    Primary,
    /// Push button
    Secondary,
    /// Toolbar / borderless
    Plain,
    Destructive,
}

pub fn button(t: Tokens, kind: ButtonKind) -> impl Fn(&Theme, widget::button::Status) -> widget::button::Style {
    move |_, status| {
        use widget::button::Status;
        let (bg, fg, border_color) = match kind {
            ButtonKind::Primary => (t.accent, t.text_on_accent, t.accent),
            ButtonKind::Destructive => (t.danger, if t.dark { Color::BLACK } else { Color::WHITE }, t.danger),
            ButtonKind::Secondary => (t.control, t.text, t.control_border),
            ButtonKind::Plain => (Color::TRANSPARENT, t.text, Color::TRANSPARENT),
        };
        let bg = match status {
            Status::Hovered if kind == ButtonKind::Plain => t.selection_inactive,
            Status::Pressed => mix(bg, if t.dark { Color::WHITE } else { Color::BLACK }, 0.12),
            Status::Hovered => mix(bg, if t.dark { Color::WHITE } else { Color::BLACK }, 0.05),
            _ => bg,
        };
        let disabled = status == Status::Disabled;
        widget::button::Style {
            background: Some(Background::Color(if disabled { Color { a: bg.a * 0.5, ..bg } } else { bg })),
            text_color: if disabled { t.text_secondary } else { fg },
            border: Border { color: border_color, width: if kind == ButtonKind::Plain { 0.0 } else { 1.0 }, radius: RADIUS.into() },
            shadow: Shadow::default(),
            snap: true,
        }
    }
}

/// Sidebar / list row; `selected` rows use the accent like a focused macOS source list.
pub fn row_button(t: Tokens, selected: bool) -> impl Fn(&Theme, widget::button::Status) -> widget::button::Style {
    move |_, status| {
        let bg = match (selected, status) {
            (true, _) => t.selection,
            (false, widget::button::Status::Hovered) => t.selection_inactive,
            _ => Color::TRANSPARENT,
        };
        widget::button::Style {
            background: Some(Background::Color(bg)),
            text_color: if selected { t.text_on_accent } else { t.text },
            border: border::rounded(RADIUS),
            shadow: Shadow::default(),
            snap: true,
        }
    }
}

pub fn tab_button(t: Tokens, active: bool) -> impl Fn(&Theme, widget::button::Status) -> widget::button::Style {
    move |_, status| widget::button::Style {
        background: Some(Background::Color(match (active, status) {
            (true, _) => t.content,
            (false, widget::button::Status::Hovered) => t.selection_inactive,
            _ => t.window,
        })),
        text_color: if active { t.text } else { t.text_secondary },
        border: Border { color: t.separator, width: if active { 1.0 } else { 0.0 }, radius: RADIUS.into() },
        shadow: Shadow::default(),
        snap: true,
    }
}

pub fn text_input(t: Tokens) -> impl Fn(&Theme, widget::text_input::Status) -> widget::text_input::Style {
    move |_, status| {
        let focused = matches!(status, widget::text_input::Status::Focused { .. });
        widget::text_input::Style {
            background: Background::Color(t.control),
            // UI-6: visible focus ring
            border: Border { color: if focused { t.focus } else { t.control_border }, width: if focused { 2.0 } else { 1.0 }, radius: RADIUS.into() },
            icon: t.text_secondary,
            placeholder: t.text_secondary,
            value: t.text,
            selection: Color { a: 0.35, ..t.accent },
        }
    }
}

pub fn text_editor(t: Tokens) -> impl Fn(&Theme, widget::text_editor::Status) -> widget::text_editor::Style {
    move |_, status| {
        let focused = matches!(status, widget::text_editor::Status::Focused { .. });
        widget::text_editor::Style {
            background: Background::Color(t.content),
            border: Border { color: if focused { t.focus } else { t.separator }, width: if focused { 2.0 } else { 1.0 }, radius: RADIUS.into() },
            placeholder: t.text_secondary,
            value: t.text,
            selection: Color { a: 0.35, ..t.accent },
        }
    }
}

pub fn pick_list(t: Tokens) -> impl Fn(&Theme, widget::pick_list::Status) -> widget::pick_list::Style {
    move |_, status| widget::pick_list::Style {
        text_color: t.text,
        placeholder_color: t.text_secondary,
        handle_color: t.text_secondary,
        background: Background::Color(t.control),
        border: Border {
            color: if matches!(status, widget::pick_list::Status::Opened { .. }) { t.focus } else { t.control_border },
            width: 1.0,
            radius: RADIUS.into(),
        },
    }
}

pub fn checkbox(t: Tokens) -> impl Fn(&Theme, widget::checkbox::Status) -> widget::checkbox::Style {
    move |_, status| {
        let checked = match status {
            widget::checkbox::Status::Active { is_checked } | widget::checkbox::Status::Hovered { is_checked } | widget::checkbox::Status::Disabled { is_checked } => is_checked,
        };
        widget::checkbox::Style {
            background: Background::Color(if checked { t.accent } else { t.control }),
            icon_color: t.text_on_accent,
            border: Border { color: if checked { t.accent } else { t.control_border }, width: 1.0, radius: 4.0.into() },
            text_color: Some(t.text),
        }
    }
}

pub fn mix(a: Color, b: Color, f: f32) -> Color {
    Color { r: a.r + (b.r - a.r) * f, g: a.g + (b.g - a.g) * f, b: a.b + (b.b - a.b) * f, a: a.a }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// UI-6: text tokens reach WCAG AA (4.5:1) on every background they are drawn on; UI
    /// component colors (focus ring, control borders of the accent) reach 3:1.
    #[test]
    fn ui6_tokens_meet_wcag_aa() {
        for t in [Tokens::LIGHT, Tokens::DARK] {
            for bg in [t.window, t.sidebar, t.content, t.row_alt, t.control, t.selection_inactive] {
                assert!(contrast(t.text, bg) >= 4.5, "text on {bg:?} (dark={})", t.dark);
                assert!(contrast(t.text_secondary, bg) >= 4.5, "secondary text on {bg:?} (dark={}): {}", t.dark, contrast(t.text_secondary, bg));
            }
            assert!(contrast(t.text_on_accent, t.accent) >= 4.5, "text on accent (dark={})", t.dark);
            assert!(contrast(t.text_on_accent, t.selection) >= 4.5, "text on selection (dark={})", t.dark);
            for bg in [t.content, t.control, t.window] {
                assert!(contrast(t.focus, bg) >= 3.0, "focus ring on {bg:?} (dark={})", t.dark);
                assert!(contrast(t.danger, bg) >= 4.5, "danger text on {bg:?} (dark={})", t.dark);
            }
        }
    }

    #[test]
    fn ui5_accent_is_used_only_when_readable() {
        let yellow = Color::from_rgb(1.0, 0.85, 0.0);
        assert_eq!(Tokens::LIGHT.with_accent(Some(yellow)).accent, Tokens::LIGHT.accent);
        let purple = Color::from_rgb8(0x6E, 0x2C, 0xC8);
        assert_eq!(Tokens::LIGHT.with_accent(Some(purple)).accent, purple);
        assert!((contrast(Color::WHITE, Color::BLACK) - 21.0).abs() < 0.01);
    }
}
