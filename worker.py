#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
F4CK CHEAT — GitHub Actions 10-Gbps Zero-Wait CDN + TeleCloud-Go Worker
- Stage 1: Native Go 32-Stream Lock-Free Apple CDN Download into 14GB /dev/shm RAM Disk (200-600+ MB/s)
- Stage 2: RAM-Disk UnfairPlay Decrypt (F4CK_FAST_DECRYPT=1)
- Stage 3: Native Go Multi-Stream 10-Gbps Upload to GitHub Releases / Fastly 100-Gbps CDN (150-350+ MB/s)
- Stage 4: Instant Zero-Wait Delivery + Queue Unblock
- Stage 5: Non-Blocking Background Native Telegram .IPA Upgrade (<= 2.0 GB)
"""

from __future__ import annotations

import sys
sys.path.insert(0, "/usr/local/lib/f4ck_pydeps")

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
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

GH_REPO = os.getenv("GITHUB_REPOSITORY", "").strip()
VPS_BASE = os.getenv("F4CK_VPS_BASE", "https://160-191-242-70.sslip.io").rstrip("/")
DL_SECRET = os.getenv("F4CK_DEC_DL_SECRET", "f4ck_unfairplay_dl_secret_2026")
VPS_POP_URL = f"{VPS_BASE}/dl_dec/gh_warm_pop/{DL_SECRET}/__gh_next_job__.json"
VPS_REG_POST_URL = f"{VPS_BASE}/dl_dec/telecloud_reg/{DL_SECRET}/__post__"

# Bot single-document limit: 3,996 chunks * 512 KB = 2,095,054,848 bytes (~2.00 GB)
MAX_BOT_PART_SIZE = 3996 * 512 * 1024
WARM_IDLE_SECONDS = 240.0

TG_API_ID = 2040
TG_API_HASH = "b18441a1ff607e10a989891a5462e627"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")
_http_sess = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=64)
_http_sess.mount("https://", _adapter)
_http_sess.mount("http://", _adapter)

_bg_executor = ThreadPoolExecutor(max_workers=1)


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
        r = _http_sess.post(VPS_REG_POST_URL, json=payload, timeout=12)
        if r.status_code == 200:
            return
    except Exception:
        pass
    try:
        b64 = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
        _http_sess.get(f"{VPS_BASE}/dl_dec/telecloud_reg/{DL_SECRET}/{b64}", timeout=12)
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
        payload["allow_sending_without_reply"] = True
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = _http_sess.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload, timeout=10).json()
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


def _write_sinf_sidecar_from_meta(enc_ipa: Path, sinfs_list: list) -> bool:
    import plistlib
    import zipfile
    if not sinfs_list or not enc_ipa.is_file():
        return False
    try:
        with zipfile.ZipFile(enc_ipa, "r") as zf:
            names = zf.namelist()
            bundle_name = ""
            info_data = None
            manifest_data = None
            for name in names:
                if ".app/Info.plist" in name and "/Watch/" not in name:
                    parts = name.split(".app/Info.plist")[0].split("/")
                    bundle_name = parts[-1]
                    if info_data is None:
                        info_data = plistlib.loads(zf.read(name))
                    break
            if not bundle_name:
                return False
            manifest_member = f"Payload/{bundle_name}.app/SC_Info/Manifest.plist"
            if manifest_member in names:
                manifest_data = plistlib.loads(zf.read(manifest_member))
            else:
                for name in names:
                    if name.endswith(".app/SC_Info/Manifest.plist"):
                        manifest_data = plistlib.loads(zf.read(name))
                        break
            sinf_map: Dict[str, str] = {}
            sinf_paths = (manifest_data or {}).get("SinfPaths") or []
            if sinf_paths:
                for s_item, sp in zip(sinfs_list, sinf_paths):
                    b64_data = (s_item.get("Data") or s_item.get("sinf")) if isinstance(s_item, dict) else None
                    if b64_data:
                        sinf_map[f"Payload/{bundle_name}.app/{sp}"] = str(b64_data)
            elif info_data and info_data.get("CFBundleExecutable"):
                exe = info_data["CFBundleExecutable"]
                b64_data = (sinfs_list[0].get("Data") or sinfs_list[0].get("sinf")) if isinstance(sinfs_list[0], dict) else None
                if b64_data:
                    sinf_map[f"Payload/{bundle_name}.app/SC_Info/{exe}.sinf"] = str(b64_data)
            if sinf_map:
                Path(f"{enc_ipa}.sinfs.json").write_text(json.dumps(sinf_map), encoding="utf-8")
                return True
    except Exception:
        pass
    return False


def download_cdn_via_go_10gbps(
    cdn_url: str,
    dest_ipa: Path,
    sinfs_list: list,
    bot_token: str,
    chat_id: int,
    status_mid: Optional[int],
    caption_base: str,
    acc_cc: str = "VN",
) -> bool:
    if not os.path.isfile(TELECLOUD_UP_BIN):
        return False
    dest_ipa.unlink(missing_ok=True)
    Path(f"{dest_ipa}.tmp").unlink(missing_ok=True)

    t0 = time.time()
    last_edit = t0
    proc = subprocess.Popen(
        [
            TELECLOUD_UP_BIN,
            "--mode",
            "dl",
            "--url",
            cdn_url,
            "--file",
            str(dest_ipa),
            "--streams",
            "32",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    if proc.stdout:
        for line in proc.stdout:
            s_line = line.strip()
            if s_line.startswith("PROGRESS "):
                parts = s_line.split()
                if len(parts) >= 3:
                    try:
                        done_b = int(parts[1])
                        tot_b = int(parts[2])
                    except Exception:
                        continue
                    now = time.time()
                    if done_b > 0 and status_mid and (now - last_edit >= 1.2):
                        last_edit = now
                        spd = (done_b / (1024 * 1024)) / max(0.08, now - t0)
                        pct_s = f"{min(99.9, done_b * 100.0 / tot_b):.0f}% " if tot_b > 0 else f"{format_bytes(done_b)} "
                        threading.Thread(
                            target=_tg_edit,
                            args=(
                                bot_token,
                                chat_id,
                                status_mid,
                                caption_base + f"\nDownloading ({html.escape(acc_cc)}): <code>{pct_s}({spd:.1f} MB/s)</code>",
                            ),
                            daemon=True,
                        ).start()

    proc.wait(timeout=900)
    if proc.returncode == 0 and dest_ipa.is_file() and dest_ipa.stat().st_size > 100 * 1024:
        _write_sinf_sidecar_from_meta(dest_ipa, sinfs_list)
        return True
    dest_ipa.unlink(missing_ok=True)
    Path(f"{dest_ipa}.tmp").unlink(missing_ok=True)
    return False


def upload_via_gh_releases_10gbps(
    bot_token: str,
    chat_id: int,
    ipa_path: Path,
    caption_base: str,
    status_mid: Optional[int],
    reply_markup: Optional[dict] = None,
    streams: int = 0,
) -> Optional[Dict[str, Any]]:
    if not os.path.isfile(TELECLOUD_UP_BIN) or not GH_REPO:
        return None
    total_sz = ipa_path.stat().st_size
    cmd = [
        TELECLOUD_UP_BIN,
        "--mode",
        "gh",
        "--file",
        str(ipa_path),
        "--repo",
        GH_REPO,
        "--tag",
        "auto",
        "--name",
        ipa_path.name,
    ]
    if streams > 0:
        cmd.extend(["--streams", str(streams)])

    t0 = time.time()
    last_edit = 0.0
    gh_mf: Optional[Dict[str, Any]] = None
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    if proc.stdout:
        for line in proc.stdout:
            s_line = line.strip()
            if s_line.startswith("PROGRESS "):
                parts = s_line.split()
                if len(parts) >= 3:
                    try:
                        sent_b = int(parts[1])
                        tot_b = int(parts[2]) or total_sz
                    except Exception:
                        continue
                    now = time.time()
                    if sent_b > 0 and status_mid and (now - last_edit >= 1.1):
                        last_edit = now
                        spd = (sent_b / (1024 * 1024)) / max(0.08, now - t0)
                        pct = min(99.9, (sent_b * 100.0 / tot_b) if tot_b > 0 else 0.0)
                        threading.Thread(
                            target=_tg_edit,
                            args=(
                                bot_token,
                                chat_id,
                                status_mid,
                                caption_base + f"\nUploading: <code>{pct:.0f}% ({spd:.1f} MB/s)</code>",
                                reply_markup,
                            ),
                            daemon=True,
                        ).start()
            elif s_line.startswith("GH_MANIFEST "):
                try:
                    gh_mf = json.loads(s_line[len("GH_MANIFEST "):])
                except Exception:
                    gh_mf = None

    _, err_out = proc.communicate(timeout=900)
    if proc.returncode != 0 or not gh_mf or not gh_mf.get("parts"):
        sys.stderr.write(f"[gh-upload-err] rc={proc.returncode} err={err_out}\n")
        return None
    el = max(0.05, time.time() - t0)
    print(f"[gh-10gbps] Uploaded {ipa_path.name} ({total_sz / (1024*1024):.1f} MB) in {el:.2f}s = {(total_sz/(1024*1024))/el:.1f} MB/s")
    return gh_mf


def _bg_telegram_native_upgrade(
    bot_token: str,
    chat_id: int,
    staged_ipa: Path,
    out_filename: str,
    caption_base: str,
    reply_to_mid: Optional[int],
    reply_markup: Optional[dict],
    temp_card_mid: Optional[int],
) -> None:
    try:
        from telethon import TelegramClient
        from telethon.crypto import AuthKey
        from telethon.network import ConnectionTcpAbridged
        from telethon.sessions import MemorySession
        from telethon.tl.types import DocumentAttributeFilename, InputFileBig

        ak_row = _load_session_authkey(Path("/root/.cache/f4ck_telethon/pool/worker_0.session"))
        if not ak_row or not os.path.isfile(TELECLOUD_UP_BIN) or not staged_ipa.is_file():
            return

        total_sz = staged_ipa.stat().st_size
        if total_sz <= 0 or total_sz > MAX_BOT_PART_SIZE:
            return
        chunk_size = 512 * 1024
        part_chunks = (total_sz + chunk_size - 1) // chunk_size
        file_id = secrets.randbits(63)

        proc = subprocess.run(
            [
                TELECLOUD_UP_BIN,
                "--mode",
                "tg",
                "--pool",
                str(GOTD_POOL_DIR),
                "--file",
                str(staged_ipa),
                "--offset",
                "0",
                "--size",
                str(total_sz),
                "--file-id",
                str(file_id),
                "--conns",
                "1",
                "--workers",
                "3",
            ],
            capture_output=True,
            text=True,
            timeout=1200,
        )
        if proc.returncode != 0:
            return

        async def _finalize_doc() -> int:
            dc_id, srv_ip, srv_port, ak_bytes = ak_row
            ms = MemorySession()
            ms.set_dc(dc_id, srv_ip, srv_port)
            ms.auth_key = AuthKey(data=ak_bytes)
            c = TelegramClient(ms, TG_API_ID, TG_API_HASH, receive_updates=False, connection=ConnectionTcpAbridged)
            await c.connect()
            try:
                inp = InputFileBig(file_id, part_chunks, out_filename)
                try:
                    sent = await c.send_file(
                        int(chat_id),
                        file=inp,
                        caption=caption_base,
                        parse_mode="html",
                        force_document=True,
                        attributes=[DocumentAttributeFilename(file_name=out_filename)],
                        reply_to=int(reply_to_mid) if reply_to_mid else None,
                    )
                except Exception:
                    sent = await c.send_file(
                        int(chat_id),
                        file=inp,
                        caption=caption_base,
                        parse_mode="html",
                        force_document=True,
                        attributes=[DocumentAttributeFilename(file_name=out_filename)],
                    )
                return int(getattr(sent, "id", 0) or 0)
            finally:
                await c.disconnect()

        mid_val = asyncio.run(_finalize_doc())
        if mid_val > 0:
            if reply_markup:
                try:
                    _http_sess.post(
                        f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup",
                        json={"chat_id": chat_id, "message_id": mid_val, "reply_markup": reply_markup},
                        timeout=8,
                    )
                except Exception:
                    pass
            if temp_card_mid:
                _tg_delete(bot_token, chat_id, temp_card_mid)
            _notify_vps_job_done({
                "filename": out_filename,
                "total_size": total_sz,
                "chat_id": int(chat_id),
                "parts": [{"index": 0, "size": total_sz, "tg_msg_id": mid_val}],
                "bg_archive_only": True,
            })
    except Exception as exc:
        sys.stderr.write(f"[bg-tg-upgrade] {exc}\n")
    finally:
        try:
            staged_ipa.unlink(missing_ok=True)
        except Exception:
            pass


def upload_via_telecloud_go_fallback(
    bot_token: str,
    chat_id: int,
    ipa_path: Path,
    caption_base: str,
    status_mid: Optional[int],
    reply_to_mid: Optional[int],
    reply_markup: Optional[dict],
) -> bool:
    """Emergency fallback if GitHub Releases CDN is ever unreachable."""
    from telethon import TelegramClient
    from telethon.crypto import AuthKey
    from telethon.network import ConnectionTcpAbridged
    from telethon.sessions import MemorySession
    from telethon.tl.types import DocumentAttributeFilename, InputFileBig

    ak_row = _load_session_authkey(Path("/root/.cache/f4ck_telethon/pool/worker_0.session"))
    if not ak_row or not os.path.isfile(TELECLOUD_UP_BIN):
        return False

    total_sz = ipa_path.stat().st_size
    if total_sz > MAX_BOT_PART_SIZE:
        return False
    chunk_size = 512 * 1024
    part_chunks = (total_sz + chunk_size - 1) // chunk_size
    file_id = secrets.randbits(63)
    start_ts = time.time()
    last_edit = start_ts

    proc = subprocess.Popen(
        [
            TELECLOUD_UP_BIN,
            "--mode",
            "tg",
            "--pool",
            str(GOTD_POOL_DIR),
            "--file",
            str(ipa_path),
            "--offset",
            "0",
            "--size",
            str(total_sz),
            "--file-id",
            str(file_id),
            "--conns",
            "1",
            "--workers",
            "3",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    if proc.stdout:
        for line in proc.stdout:
            s_line = line.strip()
            if s_line.startswith("PROGRESS "):
                try:
                    cur_b = int(s_line.split()[1])
                except Exception:
                    continue
                now = time.time()
                if status_mid and (now - last_edit >= 1.5):
                    last_edit = now
                    spd = (cur_b / (1024 * 1024)) / max(0.1, now - start_ts)
                    pct = min(99.9, cur_b * 100.0 / total_sz)
                    threading.Thread(
                        target=_tg_edit,
                        args=(
                            bot_token,
                            chat_id,
                            status_mid,
                            caption_base + f"\nUploading: <code>{pct:.0f}% ({spd:.1f} MB/s)</code>",
                            reply_markup,
                        ),
                        daemon=True,
                    ).start()

    proc.wait(timeout=1200)
    if proc.returncode != 0:
        return False

    async def _finalize() -> int:
        dc_id, srv_ip, srv_port, ak_bytes = ak_row
        ms = MemorySession()
        ms.set_dc(dc_id, srv_ip, srv_port)
        ms.auth_key = AuthKey(data=ak_bytes)
        c = TelegramClient(ms, TG_API_ID, TG_API_HASH, receive_updates=False, connection=ConnectionTcpAbridged)
        await c.connect()
        try:
            inp = InputFileBig(file_id, part_chunks, ipa_path.name)
            sent = await c.send_file(
                int(chat_id),
                file=inp,
                caption=caption_base,
                parse_mode="html",
                force_document=True,
                attributes=[DocumentAttributeFilename(file_name=ipa_path.name)],
                reply_to=int(reply_to_mid) if reply_to_mid else None,
            )
            return int(getattr(sent, "id", 0) or 0)
        finally:
            await c.disconnect()

    mid_val = asyncio.run(_finalize())
    if mid_val > 0 and reply_markup:
        try:
            _http_sess.post(
                f"https://api.telegram.org/bot{bot_token}/editMessageReplyMarkup",
                json={"chat_id": chat_id, "message_id": mid_val, "reply_markup": reply_markup},
                timeout=8,
            )
        except Exception:
            pass
    _notify_vps_job_done({
        "filename": ipa_path.name,
        "total_size": total_sz,
        "chat_id": int(chat_id),
        "parts": [{"index": 0, "size": total_sz, "tg_msg_id": mid_val}],
    })
    return True


def _run_bench_job(job: Dict[str, Any]) -> None:
    """Zero-Telegram live 10-Gbps NIC benchmark on GitHub Actions runner."""
    size_mb = int(job.get("size_mb") or 512)
    stream_counts = job.get("stream_counts") or [1, 4, 6]
    bench_name = str(job.get("filename") or "__gh_10gbps_bench__.bin")
    results: Dict[str, Any] = {}
    base_dir = "/dev/shm" if os.path.isdir("/dev/shm") else "/tmp"
    test_file = Path(base_dir) / f"bench_{secrets.token_hex(4)}.bin"
    try:
        with open(test_file, "wb") as f_out:
            chunk = os.urandom(4 * 1024 * 1024)
            rem = size_mb * 1024 * 1024
            while rem > 0:
                w = min(len(chunk), rem)
                f_out.write(chunk[:w])
                rem -= w
        tok = (os.getenv("GH_TOKEN") or os.getenv("F4CK_GH_TOKEN") or "").strip()
        for sc in stream_counts:
            t0 = time.time()
            p = subprocess.run(
                [
                    TELECLOUD_UP_BIN,
                    "--mode",
                    "gh",
                    "--file",
                    str(test_file),
                    "--repo",
                    GH_REPO,
                    "--tag",
                    "v1.0.0",
                    "--name",
                    f"__bench_s{sc}.bin",
                    "--streams",
                    str(sc),
                ],
                capture_output=True,
                text=True,
                timeout=180,
            )
            dt = max(0.05, time.time() - t0)
            mbps = size_mb / dt
            print(f"[BENCH] size={size_mb}MB streams={sc} elapsed={dt:.2f}s speed={mbps:.1f} MB/s rc={p.returncode}")
            results[str(sc)] = {"elapsed_s": round(dt, 3), "speed_mbs": round(mbps, 2), "rc": p.returncode}
            for ln in (p.stdout or "").splitlines():
                if ln.startswith("GH_MANIFEST "):
                    try:
                        mf = json.loads(ln[len("GH_MANIFEST "):])
                        for pt in mf.get("parts") or []:
                            aid = pt.get("asset_id")
                            if aid and tok:
                                _http_sess.delete(
                                    f"https://api.github.com/repos/{GH_REPO}/releases/assets/{aid}",
                                    headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"},
                                    timeout=10,
                                )
                    except Exception:
                        pass
    finally:
        test_file.unlink(missing_ok=True)
        _notify_vps_job_done({"filename": bench_name, "bench_results": results, "done": True})


def _choose_stage_dir(file_size: int) -> Path:
    if os.path.isdir("/dev/shm") and shutil.disk_usage("/dev/shm").free > file_size + 4 * 1024**3:
        d = Path("/dev/shm/f4ck_bg_stage")
    elif os.path.isdir("/mnt") and shutil.disk_usage("/mnt").free > file_size + 4 * 1024**3:
        d = Path("/mnt/f4ck_bg_stage")
    else:
        d = Path("/tmp/f4ck_bg_stage")
    d.mkdir(parents=True, exist_ok=True)
    return d


def process_one_job(bot_token: str, job: Dict[str, Any]) -> None:
    if job.get("bench_only"):
        _run_bench_job(job)
        return

    chat_id = int(job["chat_id"])
    status_mid = int(job["status_mid"]) if job.get("status_mid") else None
    reply_to_mid = int(job["reply_to_mid"]) if job.get("reply_to_mid") else None
    bundle_id = str(job.get("bundle_id") or "")
    item_id = str(job.get("item_id") or "")
    title = str(job.get("title") or bundle_id or item_id or "Game")
    version = str(job.get("version") or "1.0")
    country = str(job.get("country") or "VN").upper()
    acc_cc = str(job.get("acc_cc") or country).upper()
    est_size = int(job.get("est_size") or 0)
    dl_url = str(job.get("dl_url") or "")
    gh_streams = int(job.get("gh_streams") or 0)

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

    # With 14GB /dev/shm remount on ubuntu-24.04, games up to 5.4 GB execute 100% in RAM disk!
    shm_free = shutil.disk_usage("/dev/shm").free if os.path.isdir("/dev/shm") else 0
    use_shm = (0 <= est_size < 5400 * 1024 * 1024) and (shm_free > max(3 * 1024**3, int(est_size * 2.15)))
    base_tmp = "/dev/shm" if use_shm else ("/mnt" if os.path.isdir("/mnt") and shutil.disk_usage("/mnt").free > 12 * 1024**3 else "/tmp")
    tmp_dir = Path(tempfile.mkdtemp(prefix="f4ck_gh_", dir=base_tmp))
    try:
        out_ipa = tmp_dir / out_filename
        enc_ipa = tmp_dir / "encrypted.ipa"

        cdn_url_pre = str(job.get("cdn_url") or "")
        sinfs_pre = job.get("sinfs") or []

        downloaded_ok = False
        err_txt = ""
        if cdn_url_pre and sinfs_pre:
            downloaded_ok = download_cdn_via_go_10gbps(
                cdn_url=cdn_url_pre,
                dest_ipa=enc_ipa,
                sinfs_list=list(sinfs_pre),
                bot_token=bot_token,
                chat_id=chat_id,
                status_mid=status_mid,
                caption_base=caption_base,
                acc_cc=acc_cc,
            )

        if not downloaded_ok:
            _notify_vps_job_done({
                "filename": out_filename,
                "fallback_needed": True,
                "error": err_txt or "CDN download failed on worker",
            })
            notified_vps = True
            return

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
            dec_err = strip_ansi(((dec_proc.stdout or "") + "\n" + (dec_proc.stderr or "")).strip())[-400:]
            _notify_vps_job_done({
                "filename": out_filename,
                "fallback_needed": True,
                "error": dec_err or "Decrypt failed on worker",
            })
            notified_vps = True
            return

        dec_size = out_ipa.stat().st_size
        sz_label = (
            f"{dec_size / (1024 ** 3):.2f} GB"
            if dec_size >= 1024 ** 3
            else f"{dec_size / (1024 * 1024):.1f} MB"
        )
        kb = (
            {"inline_keyboard": [[{"text": f"[Tải IPA ({sz_label}) — Max Speed]", "url": dl_url}]]}
            if dl_url
            else None
        )

        # Stage 3: 10-Gbps Upload to GitHub Releases / Fastly 100-Gbps CDN (150-350+ MB/s)
        gh_mf = upload_via_gh_releases_10gbps(
            bot_token=bot_token,
            chat_id=chat_id,
            ipa_path=out_ipa,
            caption_base=caption_base,
            status_mid=status_mid,
            reply_markup=kb,
            streams=gh_streams,
        )

        if gh_mf and gh_mf.get("parts"):
            # Stage 4: Instant Zero-Wait Delivery & Queue Unblock!
            _notify_vps_job_done({
                "filename": out_filename,
                "total_size": dec_size,
                "gh_repo": gh_mf.get("repo") or GH_REPO,
                "gh_tag": gh_mf.get("tag") or "v1.0.0",
                "gh_release_id": gh_mf.get("release_id"),
                "gh_parts": gh_mf["parts"],
            })
            notified_vps = True

            # Fast-path for <= 19 MB: Telegram pulls directly from our 100-Gbps HTTPS URL!
            if dec_size <= 19 * 1024 * 1024 and dl_url:
                doc_payload: Dict[str, Any] = {
                    "chat_id": chat_id,
                    "document": dl_url,
                    "caption": caption_base,
                    "parse_mode": "HTML",
                }
                if kb:
                    doc_payload["reply_markup"] = kb
                if reply_to_mid:
                    doc_payload["reply_to_message_id"] = int(reply_to_mid)
                    doc_payload["allow_sending_without_reply"] = True
                try:
                    dr = _http_sess.post(
                        f"https://api.telegram.org/bot{bot_token}/sendDocument",
                        json=doc_payload,
                        timeout=35,
                    ).json()
                    if dr.get("ok"):
                        _tg_delete(bot_token, chat_id, status_mid)
                        return
                except Exception:
                    pass

            temp_card_mid = _tg_send(
                bot_token,
                chat_id,
                caption_base,
                reply_to_mid=reply_to_mid,
                reply_markup=kb,
            )
            _tg_delete(bot_token, chat_id, status_mid)

            # Stage 5: Non-Blocking Background Native Telegram .IPA Upgrade (<= 2.0 GB)
            if dec_size <= MAX_BOT_PART_SIZE and not job.get("skip_bg_tg"):
                stage_dir = _choose_stage_dir(dec_size)
                staged_ipa = stage_dir / f"{int(time.time())}_{secrets.token_hex(3)}_{out_filename}"
                try:
                    shutil.move(str(out_ipa), str(staged_ipa))
                    _bg_executor.submit(
                        _bg_telegram_native_upgrade,
                        bot_token,
                        chat_id,
                        staged_ipa,
                        out_filename,
                        caption_base,
                        reply_to_mid,
                        kb,
                        temp_card_mid,
                    )
                except Exception:
                    staged_ipa.unlink(missing_ok=True)
            return

        # Fallback if GitHub Releases CDN was unavailable
        ok_fb = upload_via_telecloud_go_fallback(
            bot_token=bot_token,
            chat_id=chat_id,
            ipa_path=out_ipa,
            caption_base=caption_base,
            status_mid=status_mid,
            reply_to_mid=reply_to_mid,
            reply_markup=kb,
        )
        if ok_fb:
            notified_vps = True
            _tg_delete(bot_token, chat_id, status_mid)
        else:
            _tg_edit(bot_token, chat_id, status_mid, caption_base, reply_markup=kb)
    finally:
        if not notified_vps:
            _notify_vps_job_done({"filename": out_filename, "done": True})
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main() -> None:
    bot_token = os.environ["F4CK_BOT_TOKEN"].strip()
    idle_since = time.time()
    try:
        while time.time() - idle_since < WARM_IDLE_SECONDS:
            try:
                r = _http_sess.get(VPS_POP_URL, timeout=6)
                if r.status_code == 200:
                    job_data = r.json()
                    if job_data and (job_data.get("chat_id") or job_data.get("bench_only")):
                        process_one_job(bot_token, job_data)
                        idle_since = time.time()
                        continue
            except Exception:
                pass
            time.sleep(0.15)
    finally:
        _bg_executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
