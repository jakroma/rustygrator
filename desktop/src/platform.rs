//! Platform specific parts (APP-3, APP-9): system accent color (UI-5) and the native macOS menu
//! bar. Linux and Windows draw their menu inside the window (UI-4, see `view::menu_bar`).

use iced::Color;

/// Actions reachable from menus and keyboard shortcuts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    NewConnection,
    NewQuery,
    NewMigration,
    CloseTab,
    OpenInWindow,
    Run,
    Cancel,
    ToggleInspector,
    ZoomIn,
    ZoomOut,
    ZoomReset,
    Quit,
}

impl Command {
    pub const ALL: [Command; 12] = [
        Command::NewConnection,
        Command::NewQuery,
        Command::NewMigration,
        Command::CloseTab,
        Command::OpenInWindow,
        Command::Run,
        Command::Cancel,
        Command::ToggleInspector,
        Command::ZoomIn,
        Command::ZoomOut,
        Command::ZoomReset,
        Command::Quit,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Command::NewConnection => "new-connection",
            Command::NewQuery => "new-query",
            Command::NewMigration => "new-migration",
            Command::CloseTab => "close-tab",
            Command::OpenInWindow => "open-in-window",
            Command::Run => "run",
            Command::Cancel => "cancel",
            Command::ToggleInspector => "toggle-inspector",
            Command::ZoomIn => "zoom-in",
            Command::ZoomOut => "zoom-out",
            Command::ZoomReset => "zoom-reset",
            Command::Quit => "quit",
        }
    }

    pub fn from_id(id: &str) -> Option<Command> {
        Self::ALL.into_iter().find(|c| c.id() == id)
    }

    /// Menu title (macOS title case).
    pub fn title(self) -> &'static str {
        match self {
            Command::NewConnection => "New Connection…",
            Command::NewQuery => "New Query",
            Command::NewMigration => "New Migration…",
            Command::CloseTab => "Close Tab",
            Command::OpenInWindow => "Open Tab in New Window",
            Command::Run => "Run Query",
            Command::Cancel => "Cancel Query",
            Command::ToggleInspector => "Show or Hide Inspector",
            Command::ZoomIn => "Zoom In",
            Command::ZoomOut => "Zoom Out",
            Command::ZoomReset => "Actual Size",
            Command::Quit => "Quit rustygrator",
        }
    }

    /// Shortcut: (command / ctrl, shift, alt, key). `⌘` on macOS, `Ctrl` elsewhere (UI-4).
    pub fn shortcut(self) -> Option<(bool, bool, &'static str)> {
        Some(match self {
            Command::NewConnection => (false, false, "n"),
            Command::NewQuery => (false, false, "t"),
            Command::NewMigration => (true, false, "m"),
            Command::CloseTab => (false, false, "w"),
            Command::OpenInWindow => (true, false, "n"),
            Command::Run => (false, false, "Enter"),
            Command::Cancel => (false, false, "."),
            Command::ToggleInspector => (false, true, "i"),
            Command::ZoomIn => (false, false, "="),
            Command::ZoomOut => (false, false, "-"),
            Command::ZoomReset => (false, false, "0"),
            Command::Quit => (false, false, "q"),
        })
    }

    /// Shortcut text shown next to a menu item.
    pub fn shortcut_label(self) -> String {
        let Some((shift, alt, key)) = self.shortcut() else { return String::new() };
        let mac = cfg!(target_os = "macos");
        let key = match key {
            "Enter" => if mac { "↩" } else { "Enter" },
            "=" => "+",
            k => k,
        }
        .to_uppercase();
        let mut s = String::new();
        if mac {
            if alt {
                s.push('⌥');
            }
            if shift {
                s.push('⇧');
            }
            s.push('⌘');
            s.push_str(&key);
        } else {
            s.push_str("Ctrl+");
            if shift {
                s.push_str("Shift+");
            }
            if alt {
                s.push_str("Alt+");
            }
            s.push_str(&key);
        }
        s
    }

    /// Command for a key press (`command` is ⌘ on macOS and Ctrl elsewhere).
    pub fn for_key(key: &iced::keyboard::Key, m: iced::keyboard::Modifiers) -> Option<Command> {
        use iced::keyboard::{key::Named, Key};
        if !m.command() {
            return None;
        }
        let name = match key {
            Key::Named(Named::Enter) => "Enter".to_string(),
            Key::Character(c) => match c.as_str() {
                "+" => "=".to_string(),
                c => c.to_lowercase(),
            },
            _ => return None,
        };
        Self::ALL.into_iter().find(|c| c.shortcut().is_some_and(|(shift, alt, k)| k == name && shift == m.shift() && alt == m.alt()))
    }
}

/// Menus and their items: (menu title, items; `None` = separator).
pub const MENUS: [(&str, &[Option<Command>]); 4] = [
    ("File", &[Some(Command::NewConnection), Some(Command::NewQuery), Some(Command::NewMigration), None, Some(Command::OpenInWindow), Some(Command::CloseTab)]),
    ("View", &[Some(Command::ToggleInspector), None, Some(Command::ZoomIn), Some(Command::ZoomOut), Some(Command::ZoomReset)]),
    ("Query", &[Some(Command::Run), Some(Command::Cancel)]),
    ("Window", &[]),
];

/// System accent color, if the platform has one (UI-5). Blocking: runs a short system query.
pub fn accent_color() -> Option<Color> {
    fn run(cmd: &str, args: &[&str]) -> Option<String> {
        let out = std::process::Command::new(cmd).args(args).output().ok()?;
        out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
    if cfg!(target_os = "macos") {
        // no value = blue; -1 = graphite; 0..6 = red, orange, yellow, green, blue, purple, pink
        let v = run("defaults", &["read", "-g", "AppleAccentColor"]).and_then(|s| s.parse::<i32>().ok()).unwrap_or(4);
        Some(match v {
            -1 => Color::from_rgb8(0x6E, 0x6E, 0x73),
            0 => Color::from_rgb8(0xD7, 0x0A, 0x16),
            1 => Color::from_rgb8(0xC2, 0x4E, 0x00),
            2 => Color::from_rgb8(0xA6, 0x7C, 0x00),
            3 => Color::from_rgb8(0x24, 0x8A, 0x3D),
            5 => Color::from_rgb8(0x8E, 0x3B, 0xC8),
            6 => Color::from_rgb8(0xC4, 0x2A, 0x6B),
            _ => Color::from_rgb8(0x00, 0x66, 0xD6),
        })
    } else if cfg!(windows) {
        // DWORD 0xAABBGGRR
        let out = run("reg", &["query", r"HKCU\Software\Microsoft\Windows\DWM", "/v", "AccentColor"])?;
        let hex = out.split_whitespace().last()?.trim_start_matches("0x");
        let v = u32::from_str_radix(hex, 16).ok()?;
        Some(Color::from_rgb8((v & 0xFF) as u8, ((v >> 8) & 0xFF) as u8, ((v >> 16) & 0xFF) as u8))
    } else {
        let name = run("gsettings", &["get", "org.gnome.desktop.interface", "accent-color"])?;
        Some(match name.trim_matches('\'') {
            "red" => Color::from_rgb8(0xC0, 0x1C, 0x28),
            "orange" => Color::from_rgb8(0xB3, 0x4A, 0x00),
            "yellow" => Color::from_rgb8(0x93, 0x6F, 0x00),
            "green" => Color::from_rgb8(0x26, 0x80, 0x4A),
            "teal" => Color::from_rgb8(0x13, 0x7E, 0x80),
            "purple" => Color::from_rgb8(0x91, 0x41, 0xAC),
            "pink" => Color::from_rgb8(0xC2, 0x30, 0x7A),
            "slate" => Color::from_rgb8(0x58, 0x65, 0x76),
            _ => Color::from_rgb8(0x1C, 0x63, 0xC4),
        })
    }
}

/// Native macOS menu bar (APP-9).
#[cfg(target_os = "macos")]
pub mod menu_bar {
    use muda::{
        accelerator::{Accelerator, Code, Modifiers},
        Menu, MenuItem, PredefinedMenuItem, Submenu,
    };

    use super::{Command, MENUS};

    fn code(key: &str) -> Option<Code> {
        Some(match key {
            "n" => Code::KeyN,
            "t" => Code::KeyT,
            "m" => Code::KeyM,
            "w" => Code::KeyW,
            "i" => Code::KeyI,
            "q" => Code::KeyQ,
            "Enter" => Code::Enter,
            "." => Code::Period,
            "=" => Code::Equal,
            "-" => Code::Minus,
            "0" => Code::Digit0,
            _ => return None,
        })
    }

    fn item(c: Command) -> MenuItem {
        let accel = c.shortcut().and_then(|(shift, alt, key)| {
            let mut m = Modifiers::SUPER;
            if shift {
                m |= Modifiers::SHIFT;
            }
            if alt {
                m |= Modifiers::ALT;
            }
            code(key).map(|k| Accelerator::new(Some(m), k))
        });
        MenuItem::with_id(c.id(), c.title(), true, accel)
    }

    /// Builds the menu bar and attaches it to the app; the returned menu must be kept alive.
    pub fn install() -> Result<Menu, muda::Error> {
        let menu = Menu::new();
        let app = Submenu::new("rustygrator", true);
        app.append_items(&[
            &PredefinedMenuItem::about(None, None),
            &PredefinedMenuItem::separator(),
            &PredefinedMenuItem::services(None),
            &PredefinedMenuItem::separator(),
            &PredefinedMenuItem::hide(None),
            &PredefinedMenuItem::hide_others(None),
            &PredefinedMenuItem::show_all(None),
            &PredefinedMenuItem::separator(),
            &item(Command::Quit),
        ])?;
        menu.append(&app)?;
        for (title, items) in MENUS {
            let sub = Submenu::new(title, true);
            if title == "View" {
                sub.append(&PredefinedMenuItem::fullscreen(None))?;
                sub.append(&PredefinedMenuItem::separator())?;
            }
            if title == "Window" {
                sub.append_items(&[&PredefinedMenuItem::minimize(None), &PredefinedMenuItem::maximize(None), &PredefinedMenuItem::close_window(None)])?;
                sub.set_as_windows_menu_for_nsapp();
            }
            for it in items.iter() {
                match it {
                    Some(c) => sub.append(&item(*c))?,
                    None => sub.append(&PredefinedMenuItem::separator())?,
                }
            }
            menu.append(&sub)?;
            if title == "File" {
                // Edit menu with the standard items, so ⌘C / ⌘V work in text fields
                let edit = Submenu::new("Edit", true);
                edit.append_items(&[
                    &PredefinedMenuItem::undo(None),
                    &PredefinedMenuItem::redo(None),
                    &PredefinedMenuItem::separator(),
                    &PredefinedMenuItem::cut(None),
                    &PredefinedMenuItem::copy(None),
                    &PredefinedMenuItem::paste(None),
                    &PredefinedMenuItem::select_all(None),
                ])?;
                menu.append(&edit)?;
            }
        }
        menu.init_for_nsapp();
        Ok(menu)
    }

    /// Commands chosen in the menu bar since the last call.
    pub fn take_commands() -> Vec<Command> {
        let mut out = Vec::new();
        while let Ok(e) = muda::MenuEvent::receiver().try_recv() {
            if let Some(c) = Command::from_id(e.id().as_ref()) {
                out.push(c);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iced::keyboard::{key::Named, Key, Modifiers};

    #[test]
    fn ui4_shortcuts_use_command_or_ctrl_and_are_unique() {
        let ctrl = if cfg!(target_os = "macos") { Modifiers::LOGO } else { Modifiers::CTRL };
        assert_eq!(Command::for_key(&Key::Character("t".into()), ctrl), Some(Command::NewQuery));
        assert_eq!(Command::for_key(&Key::Character("T".into()), ctrl), Some(Command::NewQuery));
        assert_eq!(Command::for_key(&Key::Named(Named::Enter), ctrl), Some(Command::Run));
        assert_eq!(Command::for_key(&Key::Character("i".into()), ctrl | Modifiers::ALT), Some(Command::ToggleInspector));
        assert_eq!(Command::for_key(&Key::Character("n".into()), ctrl | Modifiers::SHIFT), Some(Command::OpenInWindow));
        assert_eq!(Command::for_key(&Key::Character("t".into()), Modifiers::empty()), None);
        let mut seen = std::collections::HashSet::new();
        for c in Command::ALL {
            assert!(seen.insert(c.shortcut()), "duplicate shortcut for {c:?}");
            assert_eq!(Command::from_id(c.id()), Some(c));
        }
        if !cfg!(target_os = "macos") {
            assert_eq!(Command::ToggleInspector.shortcut_label(), "Ctrl+Alt+I");
        }
    }
}
