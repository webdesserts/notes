# Icons

The webdesserts icon suite's **notes** glyph (an open book with a magenta
ribbon) in the Neapolitan palette: purple-15 plate, sepia-85 left page,
sepia-60 right page, magenta ribbon, 0.14 magenta sheen.

Sources live in `source/`:

- `icon.svg`: the app icon. The plate is inset to 412/480 of the canvas, which
  is the margin macOS expects around app icons.
- `tray.svg`: the same glyph in black, cropped to its bounds, for the menu bar.
  The page gutters are wider (38 vs 24) and the ribbon's notch deeper, because
  only the gaps separate the shapes once colour is gone at 18pt.

Regenerate:

```sh
cargo tauri icon icons/source/icon.svg -o icons
magick -background none -density 600 icons/source/tray.svg -resize 44x44 PNG32:icons/tray-template.png
```

`tray-template.png` is loaded with `icon_as_template(true)` (src/main.rs), so
macOS tints it to match the menu bar.
