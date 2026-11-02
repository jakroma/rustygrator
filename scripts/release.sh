#!/usr/bin/env bash
# Builds the release archive of the desktop app for this machine's platform.
#   scripts/release.sh [--skip-tests]
# Output: target/dist/rustygrator-desktop-<version>-<target>.tar.gz and its .sha256 file
# (macOS: the archive holds Rustygrator.app). Signed installers (APP-4) need signing
# certificates and are not made here.
set -euo pipefail
cd "$(dirname "$0")/.."

skip_tests=false
for arg in "$@"; do
    case "$arg" in
        --skip-tests) skip_tests=true ;;
        -h|--help) sed -n '2,6p' "$0"; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

version=$(sed -n 's/^version = "\(.*\)"/\1/p' desktop/Cargo.toml | head -n1)
target=$(rustc -vV | sed -n 's/^host: //p')
name="rustygrator-desktop-${version}-${target}"
dist="target/dist"
exe=""
[[ "$target" == *windows* ]] && exe=".exe"

if [[ -n "$(git status --porcelain --untracked-files=no 2>/dev/null)" ]]; then
    echo "warning: the working tree has uncommitted changes; the archive will contain them" >&2
fi

if ! $skip_tests; then
    echo "==> tests"
    cargo test -p rustygrator-desktop
fi

echo "==> release build ($target)"
cargo build --release -p rustygrator-desktop

echo "==> archive"
stage="$dist/$name"
rm -rf "$stage" "$dist/$name.tar.gz" "$dist/$name.tar.gz.sha256"
mkdir -p "$stage"
if [[ "$target" == *apple-darwin ]]; then
    # a bundle gives the app its Dock icon, menu bar and the system dark mode / accent color
    app="$stage/Rustygrator.app/Contents"
    mkdir -p "$app/MacOS"
    cp "target/release/rustygrator-desktop" "$app/MacOS/"
    cat > "$app/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key><string>Rustygrator</string>
    <key>CFBundleDisplayName</key><string>Rustygrator</string>
    <key>CFBundleIdentifier</key><string>dev.rustygrator.desktop</string>
    <key>CFBundleExecutable</key><string>rustygrator-desktop</string>
    <key>CFBundlePackageType</key><string>APPL</string>
    <key>CFBundleShortVersionString</key><string>${version}</string>
    <key>CFBundleVersion</key><string>${version}</string>
    <key>LSMinimumSystemVersion</key><string>11.0</string>
    <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST
else
    cp "target/release/rustygrator-desktop$exe" "$stage/"
fi
cp README.md "$stage/"
tar -czf "$dist/$name.tar.gz" -C "$dist" "$name"
rm -rf "$stage"

if command -v sha256sum >/dev/null; then
    (cd "$dist" && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
else
    (cd "$dist" && shasum -a 256 "$name.tar.gz" > "$name.tar.gz.sha256")
fi

echo "==> $dist/$name.tar.gz"
cat "$dist/$name.tar.gz.sha256"
