#!/usr/bin/env python3
"""
Tkinter xterm layout tool (dark theme) + INI persistence:
- Drag 4 rectangles to choose top-left positions.
- Launch 4 xterms at those coordinates.
- Saves/loads settings to an INI file automatically.
"""

import os
import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import shlex
import shutil
import configparser

DEFAULT_COLS = 100
DEFAULT_ROWS = 28

# Rough pixel size per character for preview only (doesn't affect actual xterm sizing)
DEFAULT_CHAR_W = 8
DEFAULT_CHAR_H = 16

# --- Dark theme palette (UI only) ---
DARK_BG = "#1e1e1e"
DARK_BG_2 = "#252526"
DARK_BG_3 = "#2d2d2d"
DARK_FG = "#d4d4d4"
MUTED_FG = "#a9a9a9"
ACCENT = "#4fc3f7"
OUTLINE = "#3c3c3c"


def get_config_path() -> str:
    xdg = os.environ.get("XDG_CONFIG_HOME", "").strip()
    base = xdg if xdg else os.path.join(os.path.expanduser("~"), ".config")
    folder = os.path.join(base, "xterm_layout_tool")
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, "config.ini")


def _safe_int(value, default):
    try:
        return int(value)
    except Exception:
        return default


class XTermLayoutApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("4 xterms layout")
        self.root.configure(bg=DARK_BG)

        self.config_path = get_config_path()

        # Use a ttk theme that responds well to custom colors
        self.style = ttk.Style(self.root)
        try:
            self.style.theme_use("clam")
        except tk.TclError:
            pass
        self._apply_dark_theme()

        # Screen size (used for placement)
        self.screen_w = root.winfo_screenwidth()
        self.screen_h = root.winfo_screenheight()

        # Processes for launched xterms
        self.procs = [None, None, None, None]

        # Variables (defaults)
        self.cmd_var = tk.StringVar(value=self._default_xterm_cmd())
        self.extra_args_var = tk.StringVar(value="-fa Monospace -fs 12")
        self.cols_var = tk.IntVar(value=DEFAULT_COLS)
        self.rows_var = tk.IntVar(value=DEFAULT_ROWS)
        self.char_w_var = tk.IntVar(value=DEFAULT_CHAR_W)
        self.char_h_var = tk.IntVar(value=DEFAULT_CHAR_H)
        self.margin_var = tk.IntVar(value=20)

        self.titles = [tk.StringVar(value=f"Term {i+1}") for i in range(4)]
        self.x_vars = [tk.IntVar(value=0) for _ in range(4)]
        self.y_vars = [tk.IntVar(value=0) for _ in range(4)]

        # Load saved settings if present (overrides defaults)
        self.load_settings()

        # Layout data for canvas items
        self.canvas_items = [None] * 4
        self.dragging = {"idx": None, "dx": 0, "dy": 0}

        # Build UI
        self._build_ui()

        # If no saved positions (all zero), set quadrants as a reasonable starting point
        if all(v.get() == 0 for v in self.x_vars) and all(v.get() == 0 for v in self.y_vars):
            self.set_quadrants()

        self.redraw_preview()

        # Cleanup on exit
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _apply_dark_theme(self):
        self.style.configure(".", background=DARK_BG, foreground=DARK_FG)

        self.style.configure("TFrame", background=DARK_BG)
        self.style.configure("TLabel", background=DARK_BG, foreground=DARK_FG)

        self.style.configure("TLabelframe", background=DARK_BG, foreground=DARK_FG)
        self.style.configure("TLabelframe.Label", background=DARK_BG, foreground=DARK_FG)

        self.style.configure(
            "TEntry",
            fieldbackground=DARK_BG_3,
            background=DARK_BG_3,
            foreground=DARK_FG,
        )
        self.style.configure(
            "TSpinbox",
            fieldbackground=DARK_BG_3,
            background=DARK_BG_3,
            foreground=DARK_FG,
        )

        self.style.configure(
            "TButton",
            background=DARK_BG_2,
            foreground=DARK_FG,
            padding=6,
        )
        self.style.map(
            "TButton",
            background=[("active", DARK_BG_3), ("pressed", DARK_BG_3)],
            foreground=[("disabled", MUTED_FG)],
        )

        self.style.configure("Hint.TLabel", background=DARK_BG, foreground=MUTED_FG)

    def _default_xterm_cmd(self) -> str:
        for c in ("xterm", "uxterm", "x-terminal-emulator"):
            if shutil.which(c):
                return c
        return "xterm"

    # -------------------- INI persistence --------------------

    def load_settings(self):
        cfg = configparser.ConfigParser()
        if not os.path.exists(self.config_path):
            return

        try:
            cfg.read(self.config_path)
        except Exception:
            return

        app = cfg["app"] if "app" in cfg else {}
        preview = cfg["preview"] if "preview" in cfg else {}

        # Global settings
        if "command" in app:
            self.cmd_var.set(app.get("command", self.cmd_var.get()))
        if "extra_args" in app:
            self.extra_args_var.set(app.get("extra_args", self.extra_args_var.get()))

        self.cols_var.set(_safe_int(app.get("cols", self.cols_var.get()), self.cols_var.get()))
        self.rows_var.set(_safe_int(app.get("rows", self.rows_var.get()), self.rows_var.get()))
        self.margin_var.set(_safe_int(app.get("margin", self.margin_var.get()), self.margin_var.get()))

        self.char_w_var.set(_safe_int(preview.get("char_w", self.char_w_var.get()), self.char_w_var.get()))
        self.char_h_var.set(_safe_int(preview.get("char_h", self.char_h_var.get()), self.char_h_var.get()))

        # Per-term settings
        for i in range(4):
            section = f"term{i+1}"
            if section not in cfg:
                continue
            tsec = cfg[section]
            self.titles[i].set(tsec.get("title", self.titles[i].get()))

            x = _safe_int(tsec.get("x", self.x_vars[i].get()), self.x_vars[i].get())
            y = _safe_int(tsec.get("y", self.y_vars[i].get()), self.y_vars[i].get())

            # clamp to screen bounds
            x = max(0, min(x, self.screen_w))
            y = max(0, min(y, self.screen_h))

            self.x_vars[i].set(x)
            self.y_vars[i].set(y)

        # Optional: restore window size/position
        if "window_geometry" in app:
            geom = app.get("window_geometry", "").strip()
            if geom:
                try:
                    self.root.geometry(geom)
                except Exception:
                    pass

    def save_settings(self, show_message: bool = False):
        cfg = configparser.ConfigParser()

        cfg["app"] = {
            "command": self.cmd_var.get().strip(),
            "extra_args": self.extra_args_var.get().strip(),
            "cols": str(int(self.cols_var.get())),
            "rows": str(int(self.rows_var.get())),
            "margin": str(int(self.margin_var.get())),
            "window_geometry": self.root.winfo_geometry(),
        }
        cfg["preview"] = {
            "char_w": str(int(self.char_w_var.get())),
            "char_h": str(int(self.char_h_var.get())),
        }

        for i in range(4):
            cfg[f"term{i+1}"] = {
                "title": self.titles[i].get().strip(),
                "x": str(int(self.x_vars[i].get())),
                "y": str(int(self.y_vars[i].get())),
            }

        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                cfg.write(f)
            if show_message:
                messagebox.showinfo("Saved", f"Settings saved to:\n{self.config_path}")
        except Exception as e:
            if show_message:
                messagebox.showerror("Save failed", f"Could not write INI:\n{e}")

    # -------------------- UI --------------------

    def _build_ui(self):
        main = ttk.Frame(self.root, padding=10)
        main.grid(row=0, column=0, sticky="nsew")
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        # Left: preview
        left = ttk.Frame(main)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        main.columnconfigure(0, weight=1)
        main.rowconfigure(0, weight=1)

        preview_lbl = ttk.Label(
            left,
            text=f"Screen preview ({self.screen_w}×{self.screen_h}). Drag rectangles, then Launch.",
        )
        preview_lbl.grid(row=0, column=0, sticky="w")

        self.canvas_w = 760
        self.canvas_h = 430
        self.canvas = tk.Canvas(
            left,
            width=self.canvas_w,
            height=self.canvas_h,
            bg="#141414",
            highlightthickness=1,
            highlightbackground=OUTLINE,
        )
        self.canvas.grid(row=1, column=0, sticky="nsew", pady=8)
        left.rowconfigure(1, weight=1)
        left.columnconfigure(0, weight=1)

        # Drag bindings
        self.canvas.bind("<ButtonPress-1>", self._on_press)
        self.canvas.bind("<B1-Motion>", self._on_drag)
        self.canvas.bind("<ButtonRelease-1>", self._on_release)

        # Right: controls
        right = ttk.Frame(main)
        right.grid(row=0, column=1, sticky="ns")

        # Global settings
        g = ttk.LabelFrame(right, text="xterm settings", padding=8)
        g.grid(row=0, column=0, sticky="ew")
        right.columnconfigure(0, weight=1)

        def row(widget_label, widget, r):
            ttk.Label(g, text=widget_label).grid(row=r, column=0, sticky="w", pady=2)
            widget.grid(row=r, column=1, sticky="ew", pady=2)
            g.columnconfigure(1, weight=1)

        row("Command", ttk.Entry(g, textvariable=self.cmd_var, width=26), 0)
        row("Extra args", ttk.Entry(g, textvariable=self.extra_args_var, width=26), 1)
        row("Cols", ttk.Spinbox(g, from_=20, to=400, textvariable=self.cols_var, width=8), 2)
        row("Rows", ttk.Spinbox(g, from_=10, to=200, textvariable=self.rows_var, width=8), 3)
        row("Margin (px)", ttk.Spinbox(g, from_=0, to=200, textvariable=self.margin_var, width=8), 4)

        # Preview-only sizing (for rectangles)
        p = ttk.LabelFrame(right, text="preview sizing (only)", padding=8)
        p.grid(row=1, column=0, sticky="ew", pady=(8, 0))

        ttk.Label(p, text="Char width px").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Spinbox(p, from_=4, to=20, textvariable=self.char_w_var, width=8).grid(row=0, column=1, sticky="w", pady=2)

        ttk.Label(p, text="Char height px").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Spinbox(p, from_=8, to=40, textvariable=self.char_h_var, width=8).grid(row=1, column=1, sticky="w", pady=2)

        # Per-terminal positions
        t = ttk.LabelFrame(right, text="term positions (top-left)", padding=8)
        t.grid(row=2, column=0, sticky="ew", pady=(8, 0))

        ttk.Label(t, text="Title").grid(row=0, column=0, sticky="w")
        ttk.Label(t, text="X").grid(row=0, column=1, sticky="w")
        ttk.Label(t, text="Y").grid(row=0, column=2, sticky="w")

        for i in range(4):
            ttk.Entry(t, textvariable=self.titles[i], width=14).grid(
                row=i + 1, column=0, sticky="ew", padx=(0, 6), pady=2
            )
            ttk.Spinbox(t, from_=0, to=self.screen_w, textvariable=self.x_vars[i], width=7).grid(
                row=i + 1, column=1, sticky="w", padx=(0, 6), pady=2
            )
            ttk.Spinbox(t, from_=0, to=self.screen_h, textvariable=self.y_vars[i], width=7).grid(
                row=i + 1, column=2, sticky="w", pady=2
            )

        t.columnconfigure(0, weight=1)

        # Buttons
        b = ttk.Frame(right)
        b.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        for c in range(2):
            b.columnconfigure(c, weight=1)

        ttk.Button(b, text="Quadrants", command=self._on_quadrants).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(b, text="Update preview", command=self.redraw_preview).grid(row=0, column=1, sticky="ew")

        ttk.Button(b, text="Launch / Relaunch", command=self.launch).grid(
            row=1, column=0, sticky="ew", padx=(0, 6), pady=(6, 0)
        )
        ttk.Button(b, text="Close xterms", command=self.close_xterms).grid(
            row=1, column=1, sticky="ew", pady=(6, 0)
        )

        ttk.Button(b, text="Save settings", command=lambda: self.save_settings(show_message=True)).grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0)
        )

        hint = ttk.Label(
            right,
            text="Tip: drag rectangles on the left.\nSave writes an INI file; closing also saves.",
            style="Hint.TLabel",
        )
        hint.grid(row=4, column=0, sticky="w", pady=(10, 0))

    # -------------------- Preview/drag --------------------

    def _scale(self):
        sx = self.canvas_w / self.screen_w
        sy = self.canvas_h / self.screen_h
        s = min(sx, sy)
        offx = (self.canvas_w - self.screen_w * s) / 2
        offy = (self.canvas_h - self.screen_h * s) / 2
        return s, offx, offy

    def _term_pixel_size_preview(self):
        cols = max(1, int(self.cols_var.get()))
        rows = max(1, int(self.rows_var.get()))
        cw = max(1, int(self.char_w_var.get()))
        ch = max(1, int(self.char_h_var.get()))
        return cols * cw, rows * ch  # preview-only

    def redraw_preview(self):
        self.canvas.delete("all")
        s, offx, offy = self._scale()

        # Screen outline
        x0 = offx
        y0 = offy
        x1 = offx + self.screen_w * s
        y1 = offy + self.screen_h * s
        self.canvas.create_rectangle(x0, y0, x1, y1, outline=OUTLINE, width=2)
        self.canvas.create_text(x0 + 6, y0 + 6, anchor="nw", text="screen", fill=MUTED_FG)

        # Term rectangles
        wpx, hpx = self._term_pixel_size_preview()
        w = wpx * s
        h = hpx * s

        for i in range(4):
            x = int(self.x_vars[i].get())
            y = int(self.y_vars[i].get())

            cx = offx + x * s
            cy = offy + y * s

            cx = max(x0, min(cx, x1 - 10))
            cy = max(y0, min(cy, y1 - 10))

            rect = self.canvas.create_rectangle(cx, cy, cx + w, cy + h, outline=ACCENT, width=2, fill="")
            label = self.canvas.create_text(cx + 6, cy + 6, anchor="nw", text=f"{i+1}", fill=ACCENT)

            self.canvas_items[i] = (rect, label)
            self.canvas.addtag_withtag(f"term{i}", rect)
            self.canvas.addtag_withtag(f"term{i}", label)

    def _hit_test_term(self, event):
        items = self.canvas.find_overlapping(event.x, event.y, event.x, event.y)
        for item in reversed(items):
            for t in self.canvas.gettags(item):
                if t.startswith("term"):
                    try:
                        idx = int(t.replace("term", ""))
                        if 0 <= idx < 4:
                            return idx
                    except ValueError:
                        pass
        return None

    def _on_press(self, event):
        idx = self._hit_test_term(event)
        if idx is None:
            return

        rect_id, _ = self.canvas_items[idx]
        x0, y0, *_ = self.canvas.coords(rect_id)
        self.dragging["idx"] = idx
        self.dragging["dx"] = event.x - x0
        self.dragging["dy"] = event.y - y0

    def _on_drag(self, event):
        idx = self.dragging["idx"]
        if idx is None:
            return

        s, offx, offy = self._scale()
        rect_id, text_id = self.canvas_items[idx]

        wpx, hpx = self._term_pixel_size_preview()
        w = wpx * s
        h = hpx * s

        x0 = offx
        y0 = offy
        x1 = offx + self.screen_w * s
        y1 = offy + self.screen_h * s

        nx = event.x - self.dragging["dx"]
        ny = event.y - self.dragging["dy"]
        nx = max(x0, min(nx, x1 - w))
        ny = max(y0, min(ny, y1 - h))

        self.canvas.coords(rect_id, nx, ny, nx + w, ny + h)
        self.canvas.coords(text_id, nx + 6, ny + 6)

        sx = int(round((nx - offx) / s))
        sy = int(round((ny - offy) / s))
        self.x_vars[idx].set(max(0, min(sx, self.screen_w)))
        self.y_vars[idx].set(max(0, min(sy, self.screen_h)))

    def _on_release(self, _event):
        self.dragging["idx"] = None

    # -------------------- Actions --------------------

    def set_quadrants(self):
        m = int(self.margin_var.get())
        half_w = self.screen_w // 2
        half_h = self.screen_h // 2

        self.x_vars[0].set(m)
        self.y_vars[0].set(m)

        self.x_vars[1].set(half_w + m)
        self.y_vars[1].set(m)

        self.x_vars[2].set(m)
        self.y_vars[2].set(half_h + m)

        self.x_vars[3].set(half_w + m)
        self.y_vars[3].set(half_h + m)

    def _on_quadrants(self):
        self.set_quadrants()
        self.redraw_preview()

    def launch(self):
        cmd = self.cmd_var.get().strip()
        if not cmd:
            messagebox.showerror("Error", "Command is empty (expected xterm).")
            return

        if shutil.which(cmd) is None:
            messagebox.showerror(
                "Error",
                f"'{cmd}' not found in PATH.\nInstall xterm (or set command to uxterm / x-terminal-emulator).",
            )
            return

        cols = int(self.cols_var.get())
        rows = int(self.rows_var.get())
        extra = self.extra_args_var.get().strip()

        # Relaunch: close any existing xterms first
        self.close_xterms()

        for i in range(4):
            x = int(self.x_vars[i].get())
            y = int(self.y_vars[i].get())
            title = self.titles[i].get().strip() or f"Term {i+1}"
            geom = f"{cols}x{rows}+{x}+{y}"

            args = [cmd]
            if extra:
                args += shlex.split(extra)
            args += ["-title", title, "-geometry", geom]

            try:
                self.procs[i] = subprocess.Popen(args)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to launch terminal {i+1}:\n{e}")
                self.procs[i] = None

        # Convenient: save after launching too (so you don't lose changes)
        self.save_settings(show_message=False)

    def close_xterms(self):
        for i, p in enumerate(self.procs):
            if p is None:
                continue
            try:
                p.terminate()
            except Exception:
                pass
            self.procs[i] = None

    def on_close(self):
        # Save layout/settings on exit
        self.save_settings(show_message=False)
        self.close_xterms()
        self.root.destroy()


def main():
    root = tk.Tk()
    app = XTermLayoutApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()

