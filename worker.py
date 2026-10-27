#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
F4CK CHEAT — GitHub Actions 10-Gbps + TeleCloud-Go 9-AuthKey Work-Stealing Worker
- Minimalist 3-line output format
- Uses /usr/local/bin/f4ck-telecloud-up (Go gotd/td 9-AuthKey Work-Stealing Pool) over Azure 10-Gbps NIC
"""

from __future__ import annotations

import sys
sys.path.insert(0, "/usr/local/lib/f4ck_pydeps")

import asyncio
import base64
import html
import json
import os
import re
import secrets
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

IPATOOL_BIN = "/usr/local/bin/ipatool"
UNFAIRPLAY_BIN = "/usr/local/bin/unfairplay"
TELECLOUD_UP_BIN = "/usr/local/bin/f4ck-telecloud-up"
KEYCHAIN_PASSPHRASE = "f4ck_vps_keychain"
MODEL_DIR = Path("/root/.config/unfairplay/models")
GOTD_POOL_DIR = Path("/root/.cache/f4ck_telethon/gotd_pool")
GH_REPO = os.getenv("GITHUB_REPOSITORY", "")
RELEASE_TAG = "v1.0.0"
VPS_BASE = "https://160-191-242-70.sslip.io"
DL_SECRET = "f4ck_unfairplay_dl_secret_2026"
VPS_POP_URL = f"{VPS_BASE}/dl_dec/gh_warm_pop/{DL_SECRET}/__gh_next_job__.json"

MAX_PART_SIZE = 1950 * 1024 * 1024
TG_API_ID = 2040
TG_API_HASH = "b18441a1ff607e10a989891a5462e627"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")
_http_sess = requests.Session()


def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", str(text or ""))


def format_bytes(num: int) -> str:
    if num <= 0:
        return "0 B"
    if num < 1024 * 1024:
        return f"{num / 1024:.1f} KB"
    if num < 1024 * 1024 * 1024:
        return f"{num / (1024 * 1024):.2f} MB"
    return f"{num / (1024 * 1024 * 1024):.2f} GB"


def sanitize_filename(name: str, fallback: str = "Game") -> str:
    s = re.sub(r"[^\w\-.]+", "_", str(name or "").strip(), flags=re.UNICODE).strip("._")
    return s[:80] or fallback


def _notify_vps_job_done(payload: Dict[str, Any]) -> None:
    try:
        b64 = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
        _http_sess.get(f"{VPS_BASE}/dl_dec/telecloud_reg/{DL_SECRET}/{b64}", timeout=10)
    except Exception:
        pass


def _tg_edit(bot_token: str, chat_id: Any, message_id: Optional[int], text: str, reply_markup: Optional[dict] = None) -> None:
    if not message_id:
        return
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": int(message_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        _http_sess.post(f"https://api.telegram.org/bot{bot_token}/editMessageText", json=payload, timeout=10)
    except Exception:
        pass


def _tg_delete(bot_token: str, chat_id: Any, message_id: Optional[int]) -> None:
    if not message_id:
        return
    try:
        _http_sess.post(
            f"https://api.telegram.org/bot{bot_token}/deleteMessage",
            json={"chat_id": chat_id, "message_id": int(message_id)},
            timeout=8,
        )
    except Exception:
        pass


def _load_first_authkey() -> Optional[Tuple[int, str, int, bytes]]:
    c_path = Path("/root/.cache/f4ck_telethon/pool/worker_0.session")
    if not c_path.is_file():
        return None
    conn = sqlite3.connect(str(c_path))
    row = conn.execute("SELECT dc_id, server_address, port, auth_key FROM sessions").fetchone()
    conn.close()
    if row and row[3] and len(row[3]) == 256:
        return (int(row[0]), str(row[1]), int(row[2]), bytes(row[3]))
    return None


def upload_via_telecloud_go_10gbps(
    bot_token: str,
    chat_id: int,
    ipa_path: Path,
    caption_base: str,
    status_mid: Optional[int],
    reply_to_mid: Optional[int],
) -> bool:
    from telethon import TelegramClient
    from telethon.crypto import AuthKey
    from telethon.network import ConnectionTcpAbridged
    from telethon.sessions import MemorySession
    from telethon.tl.types import DocumentAttributeFilename, InputFileBig

    ak_row = _load_first_authkey()
    if not ak_row or not os.path.isfile(TELECLOUD_UP_BIN):
        return False

    total_sz = ipa_path.stat().st_size
    chunk_size = 512 * 1024
    num_parts = max(1, (total_sz + MAX_PART_SIZE - 1) // MAX_PART_SIZE)
    manifest_parts: List[Dict[str, Any]] = []
    sent_bytes_completed = 0
    start_ts = time.time()

    for part_idx in range(num_parts):
        part_offset = part_idx * MAX_PART_SIZE
        part_sz = min(MAX_PART_SIZE, total_sz - part_offset)
        part_chunks = (part_sz + chunk_size - 1) // chunk_size
        file_id = secrets.randbits(63)
        part_name = ipa_path.name if num_parts == 1 else f"{ipa_path.name}.part{part_idx + 1}"

        proc = subprocess.Popen(
            [
                TELECLOUD_UP_BIN,
                "--pool",
                str(GOTD_POOL_DIR),
                "--file",
                str(ipa_path),
                "--offset",
                str(part_offset),
                "--size",
                str(part_sz),
                "--file-id",
                str(file_id),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        last_edit = time.time()
        if proc.stdout:
            for line in proc.stdout:
                s_line = line.strip()
                if s_line.startswith("PROGRESS "):
                    try:
                        cur_part_bytes = int(s_line.split()[1])
                    except Exception:
                        continue
                    now = time.time()
                    if status_mid and (now - last_edit >= 1.8):
                        last_edit = now
                        cur_total = sent_bytes_completed + cur_part_bytes
                        elapsed = max(0.1, now - start_ts)
                        speed_mb = (cur_total / (1024 * 1024)) / elapsed
                        pct = min(99.9, (cur_total * 100.0 / total_sz) if total_sz > 0 else 0.0)
                        part_label = f" ({part_idx + 1}/{num_parts})" if num_parts > 1 else ""
                        msg_txt = (
                            caption_base
                            + f"\nUploading{part_label}: <code>{pct:.0f}% ({speed_mb:.1f} MB/s)</code>"
                        )
                        threading.Thread(
                            target=_tg_edit,
                            args=(bot_token, chat_id, status_mid, msg_txt, None),
                            daemon=True,
                        ).start()

        proc.wait(timeout=1200)
        if proc.returncode != 0:
            return False
        sent_bytes_completed += part_sz

        part_caption = f"Part {part_idx + 1}/{num_parts}\n" + caption_base if num_parts > 1 else caption_base

        async def _finalize_msg() -> int:
            dc_id, srv_ip, srv_port, ak_bytes = ak_row
            ms = MemorySession()
            ms.set_dc(dc_id, srv_ip, srv_port)
            ms.auth_key = AuthKey(data=ak_bytes)
            c = TelegramClient(ms, TG_API_ID, TG_API_HASH, receive_updates=False, connection=ConnectionTcpAbridged)
            await c.connect()
            try:
                inp = InputFileBig(file_id, part_chunks, part_name)
                try:
                    sent = await c.send_file(
                        int(chat_id),
                        file=inp,
                        caption=part_caption,
                        parse_mode="html",
                        force_document=True,
                        attributes=[DocumentAttributeFilename(file_name=part_name)],
                        reply_to=int(reply_to_mid) if reply_to_mid else None,
                    )
                except Exception:
                    sent = await c.send_file(
                        int(chat_id),
                        file=inp,
                        caption=part_caption,
                        parse_mode="html",
                        force_document=True,
                        attributes=[DocumentAttributeFilename(file_name=part_name)],
                    )
                return int(getattr(sent, "id", 0) or 0)
            finally:
                await c.disconnect()

        mid_val = asyncio.run(_finalize_msg())
        manifest_parts.append({"index": part_idx, "size": part_sz, "tg_msg_id": mid_val})

    _notify_vps_job_done({
        "filename": ipa_path.name,
        "total_size": total_sz,
        "chat_id": int(chat_id),
        "parts": manifest_parts,
    })
    return True


def process_one_job(bot_token: str, job: Dict[str, Any]) -> None:
    chat_id = int(job["chat_id"])
    status_mid = int(job["status_mid"]) if job.get("status_mid") else None
    reply_to_mid = int(job["reply_to_mid"]) if job.get("reply_to_mid") else None
    bundle_id = str(job.get("bundle_id") or "")
    item_id = str(job.get("item_id") or "")
    title = str(job.get("title") or bundle_id or item_id or "Game")
    version = str(job.get("version") or "1.0")
    country = str(job.get("country") or "VN").upper()
    est_size = int(job.get("est_size") or 0)

    safe_title = sanitize_filename(title, bundle_id or item_id or "Game")
    safe_ver = sanitize_filename(version, "1.0")
    out_filename = f"{safe_title}_v{safe_ver}_Decrypted.ipa"
    store_link = f"https://apps.apple.com/app/id{item_id}" if item_id.isdigit() else "https://apps.apple.com/"
    caption_base = (
        f"<code>{html.escape(bundle_id or item_id)}</code>\n"
        f"Version: <code>{html.escape(version)}</code>\n"
        f"<a href=\"{html.escape(store_link)}\">App Store Link</a>"
    )
    notified_vps = False

    shm_free = shutil.disk_usage("/dev/shm").free if os.path.isdir("/dev/shm") else 0
    tmp_dir = Path(tempfile.mkdtemp(prefix="f4ck_gh_", dir="/dev/shm" if shm_free > 2 * 1024**3 else "/tmp"))
    try:
        out_ipa = tmp_dir / out_filename
        enc_ipa = tmp_dir / "encrypted.ipa"

        dl_cmd = [
            IPATOOL_BIN,
            "download",
            "--purchase",
            "-o",
            str(enc_ipa),
            "--keychain-passphrase",
            KEYCHAIN_PASSPHRASE,
            "--non-interactive",
            "--format",
            "json",
        ]
        if item_id and item_id.isdigit():
            dl_cmd.extend(["-i", item_id])
        elif bundle_id:
            dl_cmd.extend(["-b", bundle_id])

        env = os.environ.copy()
        env["F4CK_COUNTRY_HINT"] = country
        t0 = time.time()
        proc = subprocess.Popen(dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        last_dl_edit = time.time()
        dl_start_ts = None

        while proc.poll() is None:
            time.sleep(0.35)
            cur_sz = 0
            enc_tmp = Path(f"{enc_ipa}.tmp")
            f_chk = enc_ipa if enc_ipa.is_file() else (enc_tmp if enc_tmp.is_file() else None)
            if f_chk is not None:
                try:
                    st_f = f_chk.stat()
                    phys = getattr(st_f, "st_blocks", 0) * 512
                    cur_sz = min(st_f.st_size, phys) if phys > 0 else st_f.st_size
                except Exception:
                    cur_sz = 0
            if cur_sz > 0 and dl_start_ts is None:
                dl_start_ts = time.time()
            now = time.time()
            if cur_sz > 0 and status_mid and (now - last_dl_edit >= 1.8):
                last_dl_edit = now
                el = max(0.1, now - (dl_start_ts or t0))
                spd = (cur_sz / (1024 * 1024)) / el
                pct_val = min(99.9, cur_sz * 100.0 / est_size) if est_size > 0 else 0.0
                pct_s = f"{pct_val:.0f}% " if est_size > 0 else f"{format_bytes(cur_sz)} "
                threading.Thread(
                    target=_tg_edit,
                    args=(
                        bot_token,
                        chat_id,
                        status_mid,
                        caption_base + f"\nDownloading: <code>{pct_s}({spd:.1f} MB/s)</code>",
                    ),
                    daemon=True,
                ).start()

        out_s, err_s = proc.communicate(timeout=15)
        if proc.returncode != 0 or not enc_ipa.is_file():
            err_txt = strip_ansi(((out_s or "") + "\n" + (err_s or "")).strip())[-500:]
            _tg_edit(
                bot_token,
                chat_id,
                status_mid,
                caption_base + f"\nError: <code>{html.escape(err_txt or 'Download failed')}</code>",
            )
            return

        _tg_edit(
            bot_token,
            chat_id,
            status_mid,
            caption_base + "\nDecrypting...",
        )
        dec_proc = subprocess.run(
            [UNFAIRPLAY_BIN, "decrypt", "--model", str(MODEL_DIR), "--input", str(enc_ipa), "--output", str(out_ipa), "--force"],
            capture_output=True,
            text=True,
            timeout=1200,
        )
        enc_ipa.unlink(missing_ok=True)
        Path(f"{enc_ipa}.sinfs.json").unlink(missing_ok=True)
        if dec_proc.returncode != 0 or not out_ipa.is_file():
            return

        ok = upload_via_telecloud_go_10gbps(
            bot_token=bot_token,
            chat_id=chat_id,
            ipa_path=out_ipa,
            caption_base=caption_base,
            status_mid=status_mid,
            reply_to_mid=reply_to_mid,
        )
        if ok:
            notified_vps = True
            _tg_delete(bot_token, chat_id, status_mid)
        else:
            _tg_edit(bot_token, chat_id, status_mid, caption_base)
    finally:
        if not notified_vps:
            _notify_vps_job_done({"filename": out_filename, "done": True})
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main() -> None:
    bot_token = os.environ["F4CK_BOT_TOKEN"].strip()
    idle_since = time.time()
    while time.time() - idle_since < 180.0:
        try:
            r = _http_sess.get(VPS_POP_URL, timeout=6)
            if r.status_code == 200:
                job_data = r.json()
                if job_data and job_data.get("chat_id"):
                    process_one_job(bot_token, job_data)
                    idle_since = time.time()
                    continue
        except Exception:
            pass
        time.sleep(0.4)


if __name__ == "__main__":
    main()
