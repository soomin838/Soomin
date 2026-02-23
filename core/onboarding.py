from __future__ import annotations

import shutil
from pathlib import Path

import yaml


REQUIRED_PATHS = {
    "blogger.blog_id": "Blogger Blog ID",
}

PLACEHOLDER_VALUES = {
    "GEMINI_API_KEY",
    "BLOGGER_BLOG_ID",
    "",
}


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _save(path: Path, data: dict) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")


def _get(data: dict, dotted: str) -> str:
    cur = data
    parts = dotted.split(".")
    for p in parts[:-1]:
        node = cur.get(p)
        if not isinstance(node, dict):
            return ""
        cur = node
    return str(cur.get(parts[-1], "")).strip()


def _set(data: dict, dotted: str, value: str) -> None:
    cur = data
    parts = dotted.split(".")
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


def _is_missing(v: str) -> bool:
    return v.strip() in PLACEHOLDER_VALUES


def has_missing_required(path: Path) -> bool:
    data = _load(path)
    free_mode = _get(data, "budget.free_mode").lower() in {"1", "true", "yes", "on"}
    enable_img = _get(data, "visual.enable_gemini_image_generation").lower() in {"1", "true", "yes", "on"}
    gemini_required = (not free_mode) or enable_img
    if gemini_required and _is_missing(_get(data, "gemini.api_key")):
        return True
    return any(_is_missing(_get(data, key)) for key in REQUIRED_PATHS)


def interactive_setup(path: Path, force: bool = False) -> bool:
    data = _load(path)
    changed = False

    print("\n[RezeroAgent 설정 온보딩]")
    print("필수값을 1회 입력하면 settings.yaml에 저장됩니다.\n")

    free_mode = _get(data, "budget.free_mode").lower() in {"1", "true", "yes", "on"}
    enable_img = _get(data, "visual.enable_gemini_image_generation").lower() in {"1", "true", "yes", "on"}
    required_paths = dict(REQUIRED_PATHS)
    if (not free_mode) or enable_img:
        required_paths["gemini.api_key"] = "Gemini API Key"

    for dotted, label in required_paths.items():
        cur = _get(data, dotted)
        if not force and not _is_missing(cur):
            continue
        while True:
            value = input(f"{label} 입력: ").strip()
            if value:
                _set(data, dotted, value)
                changed = True
                break
            print("값이 비어 있습니다. 다시 입력해주세요.")

    if changed:
        _save(path, data)
        print("\n설정 저장 완료: config/settings.yaml\n")

    return not has_missing_required(path)


def gui_setup(path: Path, force: bool = False) -> bool:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
    except Exception:
        return interactive_setup(path, force=force)

    data = _load(path)
    free_mode = _get(data, "budget.free_mode").lower() in {"1", "true", "yes", "on"}
    enable_img = _get(data, "visual.enable_gemini_image_generation").lower() in {"1", "true", "yes", "on"}
    required_paths = dict(REQUIRED_PATHS)
    if (not free_mode) or enable_img:
        required_paths["gemini.api_key"] = "Gemini API Key"

    fields: list[tuple[str, str]] = []
    for dotted, label in required_paths.items():
        cur = _get(data, dotted)
        if force or _is_missing(cur):
            fields.append((dotted, label))

    if not fields:
        return True

    root = tk.Tk()
    root.title("RezeroAgent 시작 설정")
    root.geometry("780x520")
    root.resizable(False, False)
    root.configure(bg="#e8eef6")

    bg = tk.Canvas(root, highlightthickness=0, bd=0, relief="flat")
    bg.place(relx=0.0, rely=0.0, relwidth=1.0, relheight=1.0)
    bg.create_rectangle(0, 0, 900, 700, fill="#e8eef6", outline="")
    bg.create_oval(-180, -120, 280, 260, fill="#cfe6ff", outline="")
    bg.create_oval(520, -140, 980, 260, fill="#d8f2ff", outline="")
    bg.create_oval(560, 300, 980, 700, fill="#e6dcff", outline="")
    bg.create_oval(-160, 320, 240, 700, fill="#d7f3ea", outline="")

    style = ttk.Style(root)
    style.theme_use("clam")
    style.configure("Page.TFrame", background="#e8eef6")
    style.configure("Card.TFrame", background="#f8fbff", borderwidth=1, relief="solid")
    style.configure("Title.TLabel", background="#f8fbff", foreground="#0f172a", font=("Segoe UI", 16, "bold"))
    style.configure("Sub.TLabel", background="#f8fbff", foreground="#51627b", font=("Segoe UI", 9))
    style.configure("Field.TLabel", background="#f8fbff", foreground="#364152", font=("Segoe UI", 10))

    wrap = ttk.Frame(root, style="Page.TFrame", padding=16)
    wrap.pack(fill="both", expand=True)

    header = ttk.Frame(wrap, style="Card.TFrame", padding=14)
    header.pack(fill="x", pady=(0, 10))
    ttk.Label(header, text="초기 연결 설정", style="Title.TLabel").pack(anchor="w")
    ttk.Label(header, text="iOS 느낌의 간단한 흐름: 방식 선택 -> 로그인/파일연결 -> 저장", style="Sub.TLabel").pack(anchor="w", pady=(3, 0))

    auth_card = ttk.Frame(wrap, style="Card.TFrame", padding=14)
    auth_card.pack(fill="x", pady=(0, 10))
    ttk.Label(auth_card, text="Blogger 연결 방식", style="Title.TLabel").grid(row=0, column=0, sticky="w")
    ttk.Label(auth_card, text="JSON 업로드 / Google 로그인 / 토큰 직접 연결 중 선택", style="Sub.TLabel").grid(
        row=1, column=0, columnspan=3, sticky="w", pady=(3, 10)
    )

    form = ttk.Frame(wrap, style="Card.TFrame", padding=14)
    form.pack(fill="both", expand=True)

    entries: dict[str, tk.Entry] = {}
    row = 0
    for dotted, label in fields:
        ttk.Label(form, text=label, style="Field.TLabel").grid(row=row, column=0, sticky="w", pady=8)
        entry = ttk.Entry(form, width=62)
        entry.grid(row=row, column=1, sticky="we", padx=(12, 0), pady=8)
        cur = _get(data, dotted)
        if cur and not _is_missing(cur):
            entry.insert(0, cur)
        entries[dotted] = entry
        row += 1

    token_var = tk.StringVar(value=_get(data, "blogger.credentials_path") or "config/blogger_token.json")
    ttk.Label(form, text="blogger_token.json", style="Field.TLabel").grid(row=row, column=0, sticky="w", pady=8)
    ttk.Entry(form, textvariable=token_var, width=62).grid(row=row, column=1, sticky="we", padx=(12, 0), pady=8)
    form.grid_columnconfigure(1, weight=1)

    def set_blog_id(blog_id: str) -> None:
        entry = entries.get("blogger.blog_id")
        if entry is not None:
            entry.delete(0, tk.END)
            entry.insert(0, blog_id)
        _set(data, "blogger.blog_id", blog_id)

    def upload_client_secret() -> None:
        selected = filedialog.askopenfilename(
            title="Select client_secret JSON",
            filetypes=[("JSON", "*.json"), ("All files", "*.*")],
        )
        if not selected:
            return
        client_secret = path.parent.parent / "config" / "client_secrets.json"
        try:
            client_secret.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(selected, client_secret)
            messagebox.showinfo("완료", "client_secrets.json 업로드 완료")
        except Exception as exc:
            messagebox.showerror("오류", f"업로드 실패\n{exc}")

    def link_token_direct() -> None:
        selected = filedialog.askopenfilename(
            title="Select blogger_token.json",
            filetypes=[("JSON", "*.json"), ("All files", "*.*")],
        )
        if not selected:
            return
        try:
            rel = str(Path(selected).resolve().relative_to(path.parent.parent)).replace("\\", "/")
        except Exception:
            rel = str(Path(selected).resolve())
        token_var.set(rel)
        _set(data, "blogger.credentials_path", rel)
        messagebox.showinfo("완료", "토큰 파일이 연결되었습니다.")

    def login_google() -> None:
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow
            from googleapiclient.discovery import build
        except Exception as exc:
            messagebox.showerror("모듈 오류", f"Google 인증 모듈 로드 실패\n{exc}")
            return

        client_secret = path.parent.parent / "config" / "client_secrets.json"
        token_path = path.parent.parent / "config" / "blogger_token.json"
        if not client_secret.exists():
            messagebox.showinfo("파일 필요", "먼저 JSON 업로드를 실행해 client_secrets.json을 연결하세요.")
            return

        try:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret),
                scopes=[
                    "https://www.googleapis.com/auth/blogger",
                    "https://www.googleapis.com/auth/drive.file",
                    "https://www.googleapis.com/auth/indexing",
                    "https://www.googleapis.com/auth/adsense.readonly",
                    "https://www.googleapis.com/auth/analytics.readonly",
                    "https://www.googleapis.com/auth/webmasters.readonly",
                ],
            )
            creds = flow.run_local_server(port=0)
            token_path.write_text(creds.to_json(), encoding="utf-8")
            token_var.set("config/blogger_token.json")
            _set(data, "blogger.credentials_path", "config/blogger_token.json")

            service = build("blogger", "v3", credentials=creds)
            blogs = (service.blogs().listByUser(userId="self").execute().get("items", []) or [])
        except Exception as exc:
            messagebox.showerror("로그인 실패", str(exc))
            return

        if not blogs:
            messagebox.showwarning("블로그 없음", "계정에 Blogger 블로그가 없습니다.")
            return

        if len(blogs) == 1:
            set_blog_id(str(blogs[0].get("id", "")))
            messagebox.showinfo("완료", "로그인/블로그 연결이 완료되었습니다.")
            return

        picker = tk.Toplevel(root)
        picker.title("블로그 선택")
        picker.geometry("560x300")
        picker.resizable(False, False)
        picker.transient(root)
        picker.grab_set()

        ttk.Label(picker, text="연결할 블로그를 선택하세요.").pack(anchor="w", padx=12, pady=(10, 8))
        lb = tk.Listbox(picker, height=12)
        lb.pack(fill="both", expand=True, padx=12, pady=(0, 8))
        for blog in blogs:
            lb.insert("end", f"{blog.get('name', 'Untitled')} ({blog.get('id', '')})")

        def choose() -> None:
            idx = lb.curselection()
            if not idx:
                messagebox.showwarning("선택 필요", "블로그를 선택하세요.")
                return
            set_blog_id(str(blogs[idx[0]].get("id", "")))
            picker.destroy()
            messagebox.showinfo("완료", "블로그 연결이 완료되었습니다.")

        ttk.Button(picker, text="선택", command=choose).pack(side="right", padx=12, pady=(0, 12))
        ttk.Button(picker, text="취소", command=picker.destroy).pack(side="right", padx=(0, 8), pady=(0, 12))

    ttk.Button(auth_card, text="JSON 업로드", command=upload_client_secret).grid(row=2, column=0, sticky="w")
    ttk.Button(auth_card, text="Google 로그인", command=login_google).grid(row=2, column=1, sticky="w", padx=(8, 0))
    ttk.Button(auth_card, text="토큰 직접 연결", command=link_token_direct).grid(row=2, column=2, sticky="w", padx=(8, 0))

    canceled = {"value": True}

    def on_save() -> None:
        for dotted, label in fields:
            val = entries[dotted].get().strip()
            if not val:
                messagebox.showerror("입력 오류", f"{label} 값을 입력하세요.")
                return
            _set(data, dotted, val)
        _set(data, "blogger.credentials_path", token_var.get().strip())
        _save(path, data)
        canceled["value"] = False
        root.destroy()

    def on_cancel() -> None:
        root.destroy()

    btns = ttk.Frame(wrap, style="Page.TFrame")
    btns.pack(fill="x", pady=(10, 0))
    ttk.Button(btns, text="저장", command=on_save).pack(side="right")
    ttk.Button(btns, text="취소", command=on_cancel).pack(side="right", padx=(0, 8))

    root.mainloop()
    return not canceled["value"] and not has_missing_required(path)
