#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
F4CK CHEAT — GitHub Actions 10-Gbps + TeleCloud-Go Multi-TCP Work-Stealing Worker
- Multi-Region Asspp Keychain Pool (/root/.ipatool_pool/<CC>/.ipatool) + 34306 Multi-Region Cloud Fallback
- 32-Stream Parallel HTTP Range Download into 12GB /dev/shm RAM Disk
- 54 TCP Connections x 6 Workers (324 concurrent 512KB chunks = 162MB in-flight BDP window)
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
IPATOOL_POOL_DIR = Path("/root/.ipatool_pool")
MODEL_DIR = Path("/root/.config/unfairplay/models")
GOTD_POOL_DIR = Path("/root/.cache/f4ck_telethon/gotd_pool")
GOTD_PREMIUM_POOL_DIR = Path("/root/.cache/f4ck_telethon/gotd_premium")
PREMIUM_TELETHON_SESSION = Path("/root/.cache/f4ck_telethon/premium_user.session")
VAULT_STORAGE_CHAT_ID = -1003882219288

GH_REPO = os.getenv("GITHUB_REPOSITORY", "")
RELEASE_TAG = "v1.0.0"
VPS_BASE = "https://160-191-242-70.sslip.io"
DL_SECRET = "f4ck_unfairplay_dl_secret_2026"
VPS_POP_URL = f"{VPS_BASE}/dl_dec/gh_warm_pop/{DL_SECRET}/__gh_next_job__.json"

# Bot limit: 3,996 chunks * 512 KB = 2,095,054,848 bytes (~2.00 GB)
MAX_BOT_PART_SIZE = 3996 * 512 * 1024
# Telegram Premium limit: 7,992 chunks * 512 KB = 4,190,109,696 bytes (~4.00 GB)
MAX_PREMIUM_PART_SIZE = 7992 * 512 * 1024

TG_API_ID = 2040
TG_API_HASH = "b18441a1ff607e10a989891a5462e627"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")
_http_sess = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=64)
_http_sess.mount("https://", _adapter)
_http_sess.mount("http://", _adapter)


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


def _tg_send(
    bot_token: str,
    chat_id: Any,
    text: str,
    reply_to_mid: Optional[int] = None,
    reply_markup: Optional[dict] = None,
) -> Optional[int]:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_to_mid:
        payload["reply_to_message_id"] = int(reply_to_mid)
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = _http_sess.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload, timeout=10).json()
        if r.get("ok"):
            return int(r["result"]["message_id"])
    except Exception:
        pass
    return None


def _tg_copy_message(
    bot_token: str,
    from_chat_id: Any,
    message_id: int,
    to_chat_id: Any,
    caption: str,
    reply_to_mid: Optional[int] = None,
) -> Optional[int]:
    payload: Dict[str, Any] = {
        "chat_id": to_chat_id,
        "from_chat_id": from_chat_id,
        "message_id": int(message_id),
        "caption": caption,
        "parse_mode": "HTML",
    }
    if reply_to_mid:
        payload["reply_to_message_id"] = int(reply_to_mid)
    try:
        r = _http_sess.post(f"https://api.telegram.org/bot{bot_token}/copyMessage", json=payload, timeout=15).json()
        if r.get("ok"):
            return int(r["result"]["message_id"])
    except Exception:
        pass
    return None


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


def _load_session_authkey(session_path: Path) -> Optional[Tuple[int, str, int, bytes]]:
    if not session_path.is_file():
        return None
    conn = sqlite3.connect(str(session_path))
    row = conn.execute("SELECT dc_id, server_address, port, auth_key FROM sessions").fetchone()
    conn.close()
    if row and row[3] and len(row[3]) == 256:
        return (int(row[0]), str(row[1]), int(row[2]), bytes(row[3]))
    return None


def _has_premium_4gb_pool() -> bool:
    return (
        (GOTD_PREMIUM_POOL_DIR / "session_0.json").is_file()
        and PREMIUM_TELETHON_SESSION.is_file()
        and _load_session_authkey(PREMIUM_TELETHON_SESSION) is not None
    )


def get_ordered_ipatool_homes(preferred_country: str = "") -> List[Tuple[str, str]]:
    pref = (preferred_country or "").strip().upper()
    homes: List[Tuple[str, str]] = []
    seen_dirs = set()

    def _add(cc_label: str, home_path: str) -> None:
        p = str(home_path)
        if p not in seen_dirs and os.path.isfile(os.path.join(p, ".ipatool", "account")):
            seen_dirs.add(p)
            homes.append((cc_label.upper(), p))

    if pref and IPATOOL_POOL_DIR.is_dir():
        _add(pref, str(IPATOOL_POOL_DIR / pref))
    if IPATOOL_POOL_DIR.is_dir():
        _add("VN", str(IPATOOL_POOL_DIR / "VN"))
    _add("DEFAULT", "/root")
    if IPATOOL_POOL_DIR.is_dir():
        for child in sorted(IPATOOL_POOL_DIR.iterdir()):
            if child.is_dir():
                _add(child.name, str(child))
    return homes


def try_download_from_34306_cloud(
    bundle_id: str,
    country_hint: str,
    out_ipa: Path,
    bot_token: str,
    chat_id: int,
    status_mid: Optional[int],
    caption_base: str,
) -> bool:
    if not bundle_id:
        return False
    base_url = "https://decrypt.34306.lol"
    try:
        c_sess = requests.Session()
        c_sess.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
        r_hp = c_sess.post(f"{base_url}/api/v1/captcha/verify", json={"token": "1x00000000000000000000AA"}, timeout=10)
        hp_cookie = r_hp.cookies.get("hp", "")
        if not hp_cookie:
            return False

        r_cv = c_sess.get(f"{base_url}/api/v1/cached-versions", params={"bundle_id": bundle_id}, timeout=10)
        versions = []
        if r_cv.status_code == 200:
            data = r_cv.json()
            versions = data.get("versions", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

        target_reg = ""
        pref_reg = (country_hint or "us").strip().lower()
        if pref_reg not in ("vn", "us", "cn", "jp"):
            pref_reg = "us"

        if versions:
            target_reg = str(versions[0].get("region") or pref_reg).lower()
            for v in versions:
                if str(v.get("region") or "").lower() == pref_reg:
                    target_reg = pref_reg
                    break
        else:
            for reg_cand in [pref_reg, "us", "cn", "jp", "vn"]:
                r_lk = c_sess.get(f"{base_url}/api/v1/lookup", params={"id": bundle_id, "region": reg_cand, "platform": "iphone"}, timeout=8)
                if r_lk.status_code == 200:
                    res_list = (r_lk.json() or {}).get("results") or []
                    if res_list and res_list[0].get("is_cached"):
                        target_reg = reg_cand
                        break

        if not target_reg:
            return False

        dl_url = f"{base_url}/api/v1/download/{bundle_id}?region={target_reg}"
        with c_sess.get(dl_url, stream=True, timeout=600) as resp:
            if resp.status_code not in (200, 206):
                return False
            total = int(resp.headers.get("Content-Length") or 0)
            written = 0
            last_edit = 0.0
            t0 = time.time()
            with open(out_ipa, "wb") as fp:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    fp.write(chunk)
                    written += len(chunk)
                    now = time.time()
                    if status_mid and (now - last_edit >= 1.5) and total > 0:
                        last_edit = now
                        pct = min(99.9, written * 100.0 / total)
                        spd = (written / (1024 * 1024)) / max(0.1, now - t0)
                        threading.Thread(
                            target=_tg_edit,
                            args=(
                                bot_token,
                                chat_id,
                                status_mid,
                                caption_base + f"\nDownloading ({target_reg.upper()} Cloud): <code>{pct:.0f}% ({spd:.1f} MB/s)</code>",
                            ),
                            daemon=True,
                        ).start()
        return out_ipa.is_file() and out_ipa.stat().st_size > 1024 * 100
    except Exception:
        return False


def upload_via_telecloud_go_10gbps(
    bot_token: str,
    chat_id: int,
    ipa_path: Path,
    caption_base: str,
    status_mid: Optional[int],
    reply_to_mid: Optional[int],
    dl_url: str = "",
) -> bool:
    from telethon import TelegramClient
    from telethon.crypto import AuthKey
    from telethon.network import ConnectionTcpAbridged
    from telethon.sessions import MemorySession
    from telethon.tl.types import DocumentAttributeFilename, InputFileBig

    total_sz = ipa_path.stat().st_size
    chunk_size = 512 * 1024

    use_premium_4gb = total_sz > MAX_BOT_PART_SIZE and _has_premium_4gb_pool()
    if use_premium_4gb:
        active_pool_dir = GOTD_PREMIUM_POOL_DIR
        ak_row = _load_session_authkey(PREMIUM_TELETHON_SESSION)
        max_part_sz = MAX_PREMIUM_PART_SIZE
        conns_per_key = "8"
        workers_per_conn = "6"
    else:
        active_pool_dir = GOTD_POOL_DIR
        ak_row = _load_session_authkey(Path("/root/.cache/f4ck_telethon/pool/worker_0.session"))
        max_part_sz = MAX_BOT_PART_SIZE
        conns_per_key = "6"
        workers_per_conn = "6"

    if not ak_row or not os.path.isfile(TELECLOUD_UP_BIN):
        return False

    num_parts = max(1, (total_sz + max_part_sz - 1) // max_part_sz)
    manifest_parts: List[Dict[str, Any]] = []
    sent_bytes_completed = 0
    start_ts = time.time()

    target_storage_chat = int(chat_id) if (num_parts == 1 and not use_premium_4gb) else VAULT_STORAGE_CHAT_ID

    for part_idx in range(num_parts):
        part_offset = part_idx * max_part_sz
        part_sz = min(max_part_sz, total_sz - part_offset)
        part_chunks = (part_sz + chunk_size - 1) // chunk_size
        file_id = secrets.randbits(63)
        part_name = ipa_path.name if num_parts == 1 else f"{ipa_path.name}.part{part_idx + 1}"

        proc = subprocess.Popen(
            [
                TELECLOUD_UP_BIN,
                "--pool",
                str(active_pool_dir),
                "--file",
                str(ipa_path),
                "--offset",
                str(part_offset),
                "--size",
                str(part_sz),
                "--file-id",
                str(file_id),
                "--conns",
                conns_per_key,
                "--workers",
                workers_per_conn,
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
                    if status_mid and (now - last_edit >= 1.5):
                        last_edit = now
                        cur_total = sent_bytes_completed + cur_part_bytes
                        elapsed = max(0.1, now - start_ts)
                        speed_mb = (cur_total / (1024 * 1024)) / elapsed
                        pct = min(99.9, (cur_total * 100.0 / total_sz) if total_sz > 0 else 0.0)
                        msg_txt = (
                            caption_base
                            + f"\nUploading: <code>{pct:.0f}% ({speed_mb:.1f} MB/s)</code>"
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

        async def _finalize_msg(dest_chat: int) -> int:
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
                        int(dest_chat),
                        file=inp,
                        caption=part_caption,
                        parse_mode="html",
                        force_document=True,
                        attributes=[DocumentAttributeFilename(file_name=part_name)],
                        reply_to=int(reply_to_mid) if (reply_to_mid and dest_chat == int(chat_id)) else None,
                    )
                except Exception:
                    sent = await c.send_file(
                        int(dest_chat),
                        file=inp,
                        caption=part_caption,
                        parse_mode="html",
                        force_document=True,
                        attributes=[DocumentAttributeFilename(file_name=part_name)],
                    )
                return int(getattr(sent, "id", 0) or 0)
            finally:
                await c.disconnect()

        try:
            mid_val = asyncio.run(_finalize_msg(target_storage_chat))
        except Exception:
            if target_storage_chat != int(chat_id):
                target_storage_chat = int(chat_id)
                mid_val = asyncio.run(_finalize_msg(target_storage_chat))
            else:
                raise
        manifest_parts.append({"index": part_idx, "size": part_sz, "tg_msg_id": mid_val})

    _notify_vps_job_done({
        "filename": ipa_path.name,
        "total_size": total_sz,
        "chat_id": int(target_storage_chat),
        "parts": manifest_parts,
    })

    if use_premium_4gb and num_parts == 1 and manifest_parts:
        vault_mid = int(manifest_parts[0]["tg_msg_id"])
        if vault_mid > 0 and int(target_storage_chat) != int(chat_id):
            _tg_copy_message(bot_token, target_storage_chat, vault_mid, chat_id, caption_base, reply_to_mid=reply_to_mid)
        return True

    if num_parts > 1 and dl_url:
        kb = {
            "inline_keyboard": [
                [{"text": f"Tải 1 File .IPA Nguyên Bản ({format_bytes(total_sz)})", "url": dl_url}]
            ]
        }
        _tg_send(bot_token, chat_id, caption_base, reply_to_mid=reply_to_mid, reply_markup=kb)

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
    dl_url = str(job.get("dl_url") or "")

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

    # With 12GB /dev/shm remount, games up to 3.8 GB execute 100% in RAM disk!
    shm_free = shutil.disk_usage("/dev/shm").free if os.path.isdir("/dev/shm") else 0
    use_shm = (0 < est_size < 3800 * 1024 * 1024) and (shm_free > int(est_size * 2.2))
    base_tmp = "/dev/shm" if use_shm else ("/mnt" if os.path.isdir("/mnt") and shutil.disk_usage("/mnt").free > 12 * 1024**3 else "/tmp")
    tmp_dir = Path(tempfile.mkdtemp(prefix="f4ck_gh_", dir=base_tmp))
    try:
        out_ipa = tmp_dir / out_filename
        enc_ipa = tmp_dir / "encrypted.ipa"

        homes = get_ordered_ipatool_homes(country)
        if not homes:
            homes = [("DEFAULT", "/root")]

        downloaded_ok = False
        err_txt = ""
        for acc_cc, home_dir in homes:
            try:
                enc_ipa.unlink(missing_ok=True)
                Path(f"{enc_ipa}.tmp").unlink(missing_ok=True)
            except Exception:
                pass

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
            env["HOME"] = home_dir
            env["F4CK_COUNTRY_HINT"] = country
            env["F4CK_FAST_DOWNLOAD"] = "1"
            env["F4CK_FAST_DECRYPT"] = "1"
            env["TMPDIR"] = str(tmp_dir)
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
                if cur_sz > 0 and status_mid and (now - last_dl_edit >= 1.5):
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
            if proc.returncode == 0 and enc_ipa.is_file():
                downloaded_ok = True
                break
            err_txt = strip_ansi(((out_s or "") + "\n" + (err_s or "")).strip())[-500:]

        if downloaded_ok:
            _tg_edit(
                bot_token,
                chat_id,
                status_mid,
                caption_base + "\nDecrypting...",
            )
            dec_env = os.environ.copy()
            dec_env["F4CK_FAST_DECRYPT"] = "1"
            dec_proc = subprocess.run(
                [UNFAIRPLAY_BIN, "decrypt", "--model", str(MODEL_DIR), "--input", str(enc_ipa), "--output", str(out_ipa), "--force"],
                capture_output=True,
                text=True,
                timeout=1200,
                env=dec_env,
            )
            enc_ipa.unlink(missing_ok=True)
            Path(f"{enc_ipa}.sinfs.json").unlink(missing_ok=True)
            if dec_proc.returncode != 0 or not out_ipa.is_file():
                return
        else:
            # Cross-Region Cloud Fallback on 10-Gbps worker (decrypt.34306.lol multi-region cache)
            if not try_download_from_34306_cloud(bundle_id, country, out_ipa, bot_token, chat_id, status_mid, caption_base):
                _notify_vps_job_done({
                    "filename": out_filename,
                    "fallback_needed": True,
                    "error": err_txt or "Download failed",
                })
                notified_vps = True
                return

        ok = upload_via_telecloud_go_10gbps(
            bot_token=bot_token,
            chat_id=chat_id,
            ipa_path=out_ipa,
            caption_base=caption_base,
            status_mid=status_mid,
            reply_to_mid=reply_to_mid,
            dl_url=dl_url,
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
    while time.time() - idle_since < 120.0:
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
        time.sleep(0.15)


if __name__ == "__main__":
    main()
