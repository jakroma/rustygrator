//! Inspector content for the selected value (VIEW-7, VIEW-8): JSON documents, arrays, UDTs and
//! collections as an expandable tree, bytes as a hex dump, images previewed. The value is
//! formatted only when it is opened here.

use std::collections::HashSet;

use crate::widgets::tree::TreeRow;

/// Bytes shown in the hex dump; longer values are cut.
pub const HEX_LIMIT: usize = 64 * 1024;
/// Characters of text shown; longer values are cut.
pub const TEXT_LIMIT: usize = 1 << 20;

/// Rows of the open nodes of a JSON value; `open` holds the paths of expanded nodes.
pub fn json_rows(value: &serde_json::Value, open: &HashSet<String>) -> (Vec<TreeRow>, Vec<String>) {
    let mut rows = Vec::new();
    let mut paths = Vec::new();
    walk("$".to_string(), "value".to_string(), value, 0, open, &mut rows, &mut paths);
    (rows, paths)
}

fn walk(path: String, label: String, v: &serde_json::Value, depth: u16, open: &HashSet<String>, rows: &mut Vec<TreeRow>, paths: &mut Vec<String>) {
    use serde_json::Value as J;
    let (detail, children): (String, Vec<(String, &J)>) = match v {
        J::Object(m) => (format!("{{{}}}", m.len()), m.iter().map(|(k, v)| (k.clone(), v)).collect()),
        J::Array(a) => (format!("[{}]", a.len()), a.iter().enumerate().map(|(i, v)| (i.to_string(), v)).collect()),
        J::String(s) => (format!("\"{s}\""), vec![]),
        other => (other.to_string(), vec![]),
    };
    let expandable = !children.is_empty();
    let expanded = expandable && (depth == 0 || open.contains(&path));
    rows.push(TreeRow { depth, label, detail, expandable, expanded, badge: None });
    paths.push(path.clone());
    if expanded {
        for (k, child) in children {
            walk(format!("{path}/{k}"), k, child, depth + 1, open, rows, paths);
        }
    }
}

/// `offset  hex bytes  ascii` lines, 16 bytes per line.
pub fn hex_dump(bytes: &[u8]) -> String {
    let shown = &bytes[..bytes.len().min(HEX_LIMIT)];
    let mut out = String::with_capacity(shown.len() * 4 + 64);
    for (i, chunk) in shown.chunks(16).enumerate() {
        out.push_str(&format!("{:08x}  ", i * 16));
        for j in 0..16 {
            match chunk.get(j) {
                Some(b) => out.push_str(&format!("{b:02x} ")),
                None => out.push_str("   "),
            }
        }
        out.push(' ');
        out.extend(chunk.iter().map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' }));
        out.push('\n');
    }
    if bytes.len() > shown.len() {
        out.push_str(&format!("… {} more bytes\n", bytes.len() - shown.len()));
    }
    out
}

/// Image formats the preview can show, by their file signature.
pub fn is_image(bytes: &[u8]) -> bool {
    bytes.starts_with(b"\x89PNG\r\n\x1a\n") || bytes.starts_with(&[0xFF, 0xD8, 0xFF]) || bytes.starts_with(b"GIF8") || (bytes.len() > 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP")
}

pub fn cut_text(s: &str) -> String {
    match s.char_indices().nth(TEXT_LIMIT) {
        Some((i, _)) => format!("{}\n… cut, {} characters in total", &s[..i], s.chars().count()),
        None => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view7_json_tree_opens_nodes_on_demand() {
        let v: serde_json::Value = serde_json::from_str(r#"{"a": {"b": [1, 2]}, "c": "x"}"#).unwrap();
        let (rows, paths) = json_rows(&v, &HashSet::new());
        assert_eq!(rows.iter().map(|r| r.label.as_str()).collect::<Vec<_>>(), ["value", "a", "c"]);
        assert!(rows[1].expandable && !rows[1].expanded);
        assert_eq!(rows[2].detail, "\"x\"");
        let open: HashSet<String> = [paths[1].clone(), "$/a/b".to_string()].into();
        let (rows, _) = json_rows(&v, &open);
        assert_eq!(rows.iter().map(|r| (r.depth, r.label.as_str())).collect::<Vec<_>>(), [(0, "value"), (1, "a"), (2, "b"), (3, "0"), (3, "1"), (1, "c")]);
        assert_eq!(rows[2].detail, "[2]");
    }

    #[test]
    fn view8_hex_dump_and_image_detection() {
        let d = hex_dump(b"Hello\x00\xff");
        assert!(d.starts_with("00000000  48 65 6c 6c 6f 00 ff    "));
        // the text column starts after 16 hex columns, so short lines line up with full ones
        assert_eq!(d.find("Hello.."), Some(10 + 16 * 3 + 1));
        assert!(d.ends_with("Hello..\n"));
        assert!(hex_dump(&vec![0u8; HEX_LIMIT + 5]).ends_with("… 5 more bytes\n"));
        assert!(is_image(b"\x89PNG\r\n\x1a\n...."));
        assert!(!is_image(b"plain"));
        assert!(cut_text(&"x".repeat(TEXT_LIMIT + 3)).ends_with(&format!("{} characters in total", TEXT_LIMIT + 3)));
    }
}
