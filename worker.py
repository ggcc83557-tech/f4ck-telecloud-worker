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
VPS_BASE = (os.getenv("F4CK_VPS_BASE") or "https://f4ckdecrypt.duckdns.org").strip().rstrip("/")
DL_SECRET = (os.getenv("F4CK_DEC_DL_SECRET") or "f4ck_unfairplay_dl_secret_2026").strip()
VPS_POP_URL = f"{VPS_BASE}/dl_dec/gh_warm_pop/{DL_SECRET}/__gh_next_job__.json"
VPS_REG_POST_URL = f"{VPS_BASE}/dl_dec/telecloud_reg/{DL_SECRET}/__post__"
VPS_HEARTBEAT_URL = f"{VPS_BASE}/dl_dec/gh_warm_heartbeat/{DL_SECRET}/__hb__"

# Bot single-document limit: 3,996 chunks * 512 KB = 2,095,054,848 bytes (~2.00 GB)
MAX_BOT_PART_SIZE = 3996 * 512 * 1024
MAX_GH_SINGLE_ASSET_SIZE = 1900 * 1024 * 1024
WARM_IDLE_SECONDS = 240.0
MAX_WORKER_LIFETIME_SECONDS = 1800.0

TG_API_ID = 2040
TG_API_HASH = "b18441a1ff607e10a989891a5462e627"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")
_http_sess = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=64)
_http_sess.mount("https://", _adapter)
_http_sess.mount("http://", _adapter)

_bg_cdn_executor = ThreadPoolExecutor(max_workers=1)
_bg_tg_executor = ThreadPoolExecutor(max_workers=1)

_worker_boot_ts = time.time()
_worker_state_lock = threading.Lock()
_worker_busy = False
_worker_active_file = ""
_worker_stage = "idle"
_worker_pct = 0.0
_worker_speed_mbs = 0.0
_worker_done_bytes = 0
_worker_total_bytes = 0
_worker_job_id = ""
_last_fast_hb_ts = 0.0
_worker_stop_evt = threading.Event()


def _build_heartbeat_payload() -> Dict[str, Any]:
    with _worker_state_lock:
        return {
            "busy": _worker_busy,
            "active_file": _worker_active_file,
            "stage": _worker_stage,
            "pct": round(_worker_pct, 1),
            "speed_mbs": round(_worker_speed_mbs, 2),
            "done_bytes": int(_worker_done_bytes),
            "total_bytes": int(_worker_total_bytes),
            "job_id": _worker_job_id,
            "uptime_s": round(time.time() - _worker_boot_ts, 1),
        }


def _push_heartbeat_async(force: bool = False) -> None:
    global _last_fast_hb_ts
    now = time.time()
    with _worker_state_lock:
        if not force and (now - _last_fast_hb_ts) < 0.85:
            return
        _last_fast_hb_ts = now
    payload = _build_heartbeat_payload()

    def _post() -> None:
        try:
            _http_sess.post(VPS_HEARTBEAT_URL, json=payload, timeout=3)
        except Exception:
            pass

    threading.Thread(target=_post, daemon=True).start()


def _set_worker_state(
    busy: bool,
    active_file: str = "",
    stage: str = "idle",
    pct: float = 0.0,
    speed_mbs: float = 0.0,
    done_bytes: int = 0,
    total_bytes: int = 0,
    job_id: Optional[str] = None,
    push_now: bool = False,
) -> None:
    global _worker_busy, _worker_active_file, _worker_stage
    global _worker_pct, _worker_speed_mbs, _worker_done_bytes, _worker_total_bytes, _worker_job_id
    with _worker_state_lock:
        _worker_busy = busy
        _worker_active_file = active_file
        _worker_stage = stage
        _worker_pct = float(pct)
        _worker_speed_mbs = float(speed_mbs)
        _worker_done_bytes = int(done_bytes)
        _worker_total_bytes = int(total_bytes)
        if job_id is not None:
            _worker_job_id = str(job_id)
        elif not busy:
            _worker_job_id = ""
    if push_now:
        _push_heartbeat_async(force=True)


def _heartbeat_loop() -> None:
    while not _worker_stop_evt.is_set():
        payload = _build_heartbeat_payload()
        try:
            _http_sess.post(VPS_HEARTBEAT_URL, json=payload, timeout=4)
        except Exception:
            pass
        _worker_stop_evt.wait(2.2)


class _StagedFileRef:
    def __init__(self, path: Path, refs: int) -> None:
        self.path = path
        self._refs = max(1, refs)
        self._lock = threading.Lock()

    def release(self) -> None:
        should_unlink = False
        with self._lock:
            self._refs -= 1
            if self._refs <= 0:
                should_unlink = True
        if should_unlink:
            try:
                self.path.unlink(missing_ok=True)
            except Exception:
                pass


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


def _notify_vps_job_done(payload: Dict[str, Any]) -> bool:
    for attempt in range(3):
        try:
            r = _http_sess.post(VPS_REG_POST_URL, json=payload, timeout=12)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.25 * (attempt + 1))
    try:
        b64 = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
        r2 = _http_sess.get(f"{VPS_BASE}/dl_dec/telecloud_reg/{DL_SECRET}/{b64}", timeout=12)
        return r2.status_code == 200
    except Exception:
        return False


def _tg_send(
    bot_token: str,
    chat_id: Any,
    text: str,
    reply_to_mid: Optional[int] = None,
    reply_markup: Optional[dict] = None,
) -> Optional[int]:
    if not bot_token or not chat_id:
        return None
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
    if not bot_token or not chat_id or not message_id:
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
    if not bot_token or not chat_id or not message_id:
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


def _write_sinf_sidecar_from_meta(enc_ipa: Path, sinfs_list: Any) -> bool:
    import plistlib
    import posixpath
    import zipfile

    explicit_sinf_paths: List[str] = []
    if isinstance(sinfs_list, dict):
        raw_sp = sinfs_list.get("sinf_paths") or sinfs_list.get("sinfPaths") or []
        if isinstance(raw_sp, list):
            explicit_sinf_paths = [str(p).strip() for p in raw_sp if str(p).strip()]
        sinfs_list = sinfs_list.get("sinfs") or []
    if not isinstance(sinfs_list, list) or not sinfs_list or not enc_ipa.is_file():
        return False
    b64_blobs = [
        str(s.get("Data") or s.get("sinf"))
        for s in sinfs_list
        if isinstance(s, dict) and (s.get("Data") or s.get("sinf"))
    ]
    if not b64_blobs:
        return False
    try:
        with zipfile.ZipFile(enc_ipa, "r") as zf:
            names = zf.namelist()
            name_set = set(names)
            root_app_dir = ""
            root_info_data = None
            for name in names:
                parts = name.split("/")
                if len(parts) == 3 and parts[0] == "Payload" and parts[1].endswith(".app") and parts[2] == "Info.plist":
                    root_app_dir = f"{parts[0]}/{parts[1]}"
                    try:
                        root_info_data = plistlib.loads(zf.read(name))
                    except Exception:
                        root_info_data = None
                    break
            if not root_app_dir:
                for name in names:
                    if name.startswith("Payload/") and ".app/" in name:
                        head = name.split(".app/", 1)[0].split("/")
                        if len(head) == 2 and head[0] == "Payload":
                            root_app_dir = f"Payload/{head[1]}.app"
                            break
            if not root_app_dir:
                return False

            manifest_names = sorted(
                [n for n in names if n.startswith("Payload/") and n.endswith("/SC_Info/Manifest.plist")],
                key=lambda n: (0 if n == f"{root_app_dir}/SC_Info/Manifest.plist" else 1, n.count("/"), n),
            )
            declared_targets: List[str] = []
            for esp in explicit_sinf_paths:
                norm_esp = (
                    posixpath.normpath(esp)
                    if esp.startswith("Payload/")
                    else posixpath.normpath(f"{root_app_dir}/{esp.lstrip('/')}")
                )
                if norm_esp.startswith("Payload/") and norm_esp.endswith(".sinf") and norm_esp not in declared_targets:
                    declared_targets.append(norm_esp)
            replicated_targets = set()
            for m_name in manifest_names:
                b_root = m_name[: -len("/SC_Info/Manifest.plist")]
                try:
                    m_data = plistlib.loads(zf.read(m_name))
                except Exception:
                    continue
                if not isinstance(m_data, dict):
                    continue
                for sp in m_data.get("SinfPaths") or []:
                    norm_sp = posixpath.normpath(f"{b_root}/{str(sp).lstrip('/')}")
                    if norm_sp.startswith("Payload/") and norm_sp.endswith(".sinf") and norm_sp not in declared_targets:
                        declared_targets.append(norm_sp)
                for rp in m_data.get("SinfReplicationPaths") or []:
                    norm_rp = posixpath.normpath(f"{b_root}/{str(rp).lstrip('/')}")
                    if norm_rp.startswith("Payload/") and norm_rp.endswith(".sinf"):
                        replicated_targets.add(norm_rp)

            sup_sinf_candidates: List[str] = []
            for name in names:
                if not name.startswith("Payload/") or "/SC_Info/" not in name:
                    continue
                cand = ""
                if name.endswith(".v5.supf"):
                    cand = name[: -len(".v5.supf")] + ".sinf"
                elif name.endswith(".supf"):
                    cand = name[: -len(".supf")] + ".sinf"
                elif name.endswith(".supp"):
                    cand = name[: -len(".supp")] + ".sinf"
                if cand and cand not in sup_sinf_candidates:
                    sup_sinf_candidates.append(cand)
            sup_sinf_candidates.sort(
                key=lambda c: (0 if c.startswith(f"{root_app_dir}/SC_Info/") else 1, c.count("/"), c)
            )

            sinf_map: Dict[str, str] = {}
            blob_idx = 0
            for target in declared_targets:
                if blob_idx < len(b64_blobs):
                    sinf_map[target] = b64_blobs[blob_idx]
                    blob_idx += 1
                else:
                    sinf_map[target] = b64_blobs[0]

            if not sinf_map:
                if sup_sinf_candidates:
                    for cand in sup_sinf_candidates:
                        if cand in name_set:
                            continue
                        if blob_idx < len(b64_blobs):
                            sinf_map[cand] = b64_blobs[blob_idx]
                            blob_idx += 1
                        else:
                            sinf_map[cand] = b64_blobs[0]
                elif root_info_data and root_info_data.get("CFBundleExecutable"):
                    exe = str(root_info_data["CFBundleExecutable"])
                    sinf_map[f"{root_app_dir}/SC_Info/{exe}.sinf"] = b64_blobs[0]
            else:
                for cand in sup_sinf_candidates:
                    if cand in sinf_map or cand in replicated_targets or cand in name_set:
                        continue
                    if blob_idx < len(b64_blobs):
                        sinf_map[cand] = b64_blobs[blob_idx]
                        blob_idx += 1
                    else:
                        sinf_map[cand] = b64_blobs[0]

            if sinf_map:
                sidecar_p = Path(f"{enc_ipa}.sinfs.json")
                sidecar_p.write_text(json.dumps(sinf_map), encoding="utf-8")
                try:
                    os.chmod(sidecar_p, 0o600)
                except Exception:
                    pass
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
) -> Tuple[bool, float]:
    if not os.path.isfile(TELECLOUD_UP_BIN):
        return False, 0.0
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
                    if done_b > 0:
                        spd = (done_b / (1024 * 1024)) / max(0.08, now - t0)
                        pct_val = min(99.9, done_b * 100.0 / tot_b) if tot_b > 0 else 0.0
                        with _worker_state_lock:
                            act_f = _worker_active_file
                        _set_worker_state(
                            True,
                            act_f,
                            "downloading",
                            pct=pct_val,
                            speed_mbs=spd,
                            done_bytes=done_b,
                            total_bytes=tot_b,
                        )
                        _push_heartbeat_async(force=False)
                        if status_mid and (now - last_edit >= 1.2):
                            last_edit = now
                            pct_s = f"{pct_val:.0f}% " if tot_b > 0 else f"{format_bytes(done_b)} "
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
        el = max(0.05, time.time() - t0)
        dl_spd = round((dest_ipa.stat().st_size / (1024 * 1024)) / el, 2)
        _write_sinf_sidecar_from_meta(dest_ipa, sinfs_list)
        return True, dl_spd
    dest_ipa.unlink(missing_ok=True)
    Path(f"{dest_ipa}.tmp").unlink(missing_ok=True)
    return False, 0.0


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
    t_wire = t0
    last_edit = t_wire - 0.55
    eof_wire_ts: Optional[float] = None
    final_edit_sent = False
    gh_mf: Optional[Dict[str, Any]] = None
    edit_lock = threading.Lock()
    latest_edit_seq = [0]
    edit_threads: List[threading.Thread] = []

    def _dispatch_ordered_edit(msg_html: str) -> None:
        latest_edit_seq[0] += 1
        my_seq = latest_edit_seq[0]

        def _runner() -> None:
            with edit_lock:
                if my_seq < latest_edit_seq[0]:
                    return
                _tg_edit(bot_token, chat_id, status_mid, msg_html, reply_markup)

        th = threading.Thread(target=_runner, daemon=True)
        edit_threads.append(th)
        th.start()

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
            if s_line.startswith("UPLOAD_START "):
                t_wire = time.time()
                last_edit = t_wire - 0.55
                eof_wire_ts = None
                final_edit_sent = False
            elif s_line.startswith("PROGRESS "):
                parts = s_line.split()
                if len(parts) >= 3:
                    try:
                        sent_b = int(parts[1])
                        tot_b = int(parts[2]) or total_sz
                    except Exception:
                        continue
                    now = time.time()
                    if tot_b > 0 and sent_b >= tot_b:
                        if eof_wire_ts is None:
                            eof_wire_ts = now
                        el_wire = max(0.05, eof_wire_ts - t_wire)
                        spd = (tot_b / (1024 * 1024)) / el_wire
                        _set_worker_state(
                            True,
                            ipa_path.name,
                            "uploading",
                            pct=99.0,
                            speed_mbs=spd,
                            done_bytes=tot_b,
                            total_bytes=tot_b,
                        )
                        _push_heartbeat_async(force=False)
                        if status_mid and (not final_edit_sent) and (now - last_edit >= 0.45):
                            last_edit = now
                            final_edit_sent = True
                            _dispatch_ordered_edit(
                                caption_base + f"\nUploading: <code>99% ({spd:.1f} MB/s)</code>"
                            )
                    else:
                        eof_wire_ts = None
                        final_edit_sent = False
                        if sent_b > 0:
                            spd = (sent_b / (1024 * 1024)) / max(0.08, now - t_wire)
                            pct = min(99.0, (sent_b * 100.0 / tot_b) if tot_b > 0 else 0.0)
                            _set_worker_state(
                                True,
                                ipa_path.name,
                                "uploading",
                                pct=pct,
                                speed_mbs=spd,
                                done_bytes=sent_b,
                                total_bytes=tot_b,
                            )
                            _push_heartbeat_async(force=False)
                            if status_mid and (now - last_edit >= 1.1):
                                last_edit = now
                                _dispatch_ordered_edit(
                                    caption_base + f"\nUploading: <code>{pct:.0f}% ({spd:.1f} MB/s)</code>"
                                )
            elif s_line.startswith("GH_MANIFEST "):
                try:
                    gh_mf = json.loads(s_line[len("GH_MANIFEST "):])
                except Exception:
                    gh_mf = None

    _, err_out = proc.communicate(timeout=900)
    for th in edit_threads[-2:]:
        th.join(timeout=0.25)
    if proc.returncode != 0 or not gh_mf or not gh_mf.get("parts"):
        sys.stderr.write(f"[gh-upload-err] rc={proc.returncode} err={err_out}\n")
        return None
    total_el = max(0.05, time.time() - t0)
    wire_ms = int(gh_mf.get("wire_elapsed_ms") or 0)
    pure_wire_ms = int(gh_mf.get("pure_wire_ms") or 0)
    commit_el = max(0.05, (wire_ms / 1000.0) if wire_ms > 0 else (time.time() - t_wire))
    pure_wire_el = max(
        0.02,
        (pure_wire_ms / 1000.0)
        if pure_wire_ms > 0
        else ((eof_wire_ts - t_wire) if eof_wire_ts is not None else commit_el),
    )
    size_mb_f = total_sz / (1024 * 1024)
    pure_wire_spd = round(size_mb_f / pure_wire_el, 2)
    commit_spd = round(size_mb_f / commit_el, 2)
    gh_mf["upload_speed_mbs"] = pure_wire_spd
    gh_mf["e2e_commit_speed_mbs"] = commit_spd
    gh_mf["pure_wire_s"] = round(pure_wire_el, 3)
    gh_mf["e2e_commit_s"] = round(commit_el, 3)
    gh_mf["total_elapsed_s"] = round(total_el, 2)
    print(
        f"[gh-10gbps] Uploaded {ipa_path.name} ({size_mb_f:.1f} MB) "
        f"pure_wire={pure_wire_el:.2f}s ({pure_wire_spd:.1f} MB/s) "
        f"e2e_commit={commit_el:.2f}s ({commit_spd:.1f} MB/s) total={total_el:.2f}s"
    )
    return gh_mf


def _delayed_delete_superseded_gh_assets(
    repo: str,
    token: str,
    old_parts: List[Dict[str, Any]],
    new_aid: Any,
    grace_sec: float = 75.0,
) -> None:
    if grace_sec > 0:
        time.sleep(grace_sec)
    for pt in old_parts:
        aid = pt.get("asset_id") if isinstance(pt, dict) else None
        if aid and aid != new_aid:
            try:
                _http_sess.delete(
                    f"https://api.github.com/repos/{repo}/releases/assets/{aid}",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                    timeout=12,
                )
            except Exception:
                pass


def _bg_promote_single_gh_asset(
    staged_ref: _StagedFileRef,
    out_filename: str,
    dec_size: int,
    old_parts: List[Dict[str, Any]],
) -> None:
    """
    Stage 5a: Zero-downtime background consolidation of multi-part <=1.90 GB assets into a
    single unified GitHub Release asset so user downloads HTTP 302 redirect straight to
    Fastly's 100-Gbps edge POP (0 bytes proxied via VPS!).
    """
    ref_released = False
    try:
        staged_ipa = staged_ref.path
        if not os.path.isfile(TELECLOUD_UP_BIN) or not GH_REPO or not staged_ipa.is_file():
            return
        if dec_size <= 0 or dec_size > MAX_GH_SINGLE_ASSET_SIZE:
            return

        proc = subprocess.run(
            [
                TELECLOUD_UP_BIN,
                "--mode",
                "gh",
                "--file",
                str(staged_ipa),
                "--repo",
                GH_REPO,
                "--tag",
                "v1.0.0",
                "--name",
                out_filename,
                "--streams",
                "1",
                "--keep-siblings",
            ],
            capture_output=True,
            text=True,
            timeout=900,
        )
        staged_ref.release()
        ref_released = True
        if proc.returncode != 0:
            sys.stderr.write(f"[bg-gh-promote] rc={proc.returncode} err={proc.stderr}\n")
            return

        new_mf: Optional[Dict[str, Any]] = None
        for ln in (proc.stdout or "").splitlines():
            s_ln = ln.strip()
            if s_ln.startswith("GH_MANIFEST "):
                try:
                    new_mf = json.loads(s_ln[len("GH_MANIFEST "):])
                except Exception:
                    new_mf = None
        if not new_mf or len(new_mf.get("parts") or []) != 1:
            return

        new_url = str((new_mf["parts"][0] or {}).get("url") or "").strip()
        if new_url.startswith("https://"):
            for _ in range(10):
                try:
                    with _http_sess.get(
                        new_url,
                        headers={"Range": "bytes=0-0"},
                        stream=True,
                        allow_redirects=True,
                        timeout=6,
                    ) as rp:
                        if rp.status_code in (200, 206):
                            break
                except Exception:
                    pass
                time.sleep(0.4)

        ok_reg = _notify_vps_job_done({
            "filename": out_filename,
            "total_size": dec_size,
            "gh_repo": new_mf.get("repo") or GH_REPO,
            "gh_tag": new_mf.get("tag") or "v1.0.0",
            "gh_release_id": new_mf.get("release_id"),
            "gh_parts": new_mf["parts"],
            "bg_archive_only": True,
        })
        if not ok_reg:
            return

        new_aid = new_mf["parts"][0].get("asset_id")
        tok = (os.getenv("GH_TOKEN") or os.getenv("F4CK_GH_TOKEN") or "").strip()
        if tok and old_parts:
            threading.Thread(
                target=_delayed_delete_superseded_gh_assets,
                args=(GH_REPO, tok, list(old_parts), new_aid, 75.0),
                daemon=True,
            ).start()
        print(f"[bg-gh-promote] Consolidated {out_filename} into 1-part HTTP 302 Fastly asset (id={new_aid})")
    except Exception as exc:
        sys.stderr.write(f"[bg-gh-promote] {exc}\n")
    finally:
        if not ref_released:
            staged_ref.release()


def _bg_telegram_native_upgrade(
    bot_token: str,
    chat_id: int,
    staged_ref: _StagedFileRef,
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

        staged_ipa = staged_ref.path
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
        staged_ref.release()


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
    if mid_val <= 0:
        return False
    if reply_markup:
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


def _run_bench_job(bot_token: str, job: Dict[str, Any]) -> None:
    """Live 10-Gbps NIC benchmark on GitHub Actions runner with automatic Telegram callback."""
    size_mb = int(job.get("size_mb") or 512)
    stream_counts = job.get("stream_counts") or [1, 12, 16]
    bench_name = str(job.get("filename") or "__gh_10gbps_bench__.bin")
    notify_chat_id = job.get("notify_chat_id")
    notify_mid = job.get("notify_mid")
    _set_worker_state(True, f"Benchmark ({size_mb} MB)", "benchmarking")
    results: Dict[str, Any] = {}
    base_dir = "/dev/shm" if os.path.isdir("/dev/shm") else "/tmp"
    test_file = Path(base_dir) / f"bench_{secrets.token_hex(4)}.bin"
    try:
        if notify_chat_id and notify_mid and bot_token:
            _tg_edit(
                bot_token,
                notify_chat_id,
                int(notify_mid),
                f"<b>10-GBPS GITHUB BOOST BENCHMARK</b>\n"
                f"• <b>Trạng thái:</b> Đang tạo payload {size_mb} MB trên 14GB /dev/shm RAM Disk...",
            )
        with open(test_file, "wb") as f_out:
            chunk = os.urandom(4 * 1024 * 1024)
            rem = size_mb * 1024 * 1024
            while rem > 0:
                w = min(len(chunk), rem)
                f_out.write(chunk[:w])
                rem -= w
        cand_toks = []
        for _ek in ("GH_TOKEN", "GITHUB_TOKEN", "F4CK_GH_TOKEN"):
            _tv = (os.getenv(_ek) or "").strip()
            if _tv and _tv not in cand_toks:
                cand_toks.append(_tv)
        for sc in stream_counts:
            if notify_chat_id and notify_mid and bot_token:
                done_lines = [
                    f"<code>{k}-stream: {v['speed_mbs']:.1f} MB/s ({v['elapsed_s']:.2f}s)</code>"
                    for k, v in results.items()
                    if v.get("rc") == 0 and v.get("speed_mbs", 0) > 0
                ]
                prog_html = (
                    f"<b>10-GBPS GITHUB BOOST BENCHMARK ({size_mb} MB)</b>\n"
                    f"• <b>Đang đo:</b> <code>{sc}-stream</code> -> Fastly 100-Gbps CDN..."
                )
                if done_lines:
                    prog_html += "\n• <b>Đã xong:</b> " + " | ".join(done_lines)
                _tg_edit(bot_token, notify_chat_id, int(notify_mid), prog_html)
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
                    "--keep-siblings",
                ],
                capture_output=True,
                text=True,
                timeout=240,
            )
            total_dt = max(0.05, time.time() - t0)
            commit_dt = total_dt
            pure_dt = total_dt
            got_mf = False
            for ln in (p.stdout or "").splitlines():
                if ln.startswith("GH_MANIFEST "):
                    try:
                        mf = json.loads(ln[len("GH_MANIFEST "):])
                        if isinstance(mf, dict) and mf.get("parts"):
                            got_mf = True
                        w_ms = int(mf.get("wire_elapsed_ms") or 0)
                        pw_ms = int(mf.get("pure_wire_ms") or 0)
                        if w_ms > 0:
                            commit_dt = max(0.05, w_ms / 1000.0)
                            pure_dt = commit_dt
                        if pw_ms > 0:
                            pure_dt = max(0.02, pw_ms / 1000.0)
                        for pt in mf.get("parts") or []:
                            aid = pt.get("asset_id")
                            if aid and cand_toks:
                                for c_tok in cand_toks:
                                    dr = _http_sess.delete(
                                        f"https://api.github.com/repos/{GH_REPO}/releases/assets/{aid}",
                                        headers={"Authorization": f"Bearer {c_tok}", "Accept": "application/vnd.github+json"},
                                        timeout=10,
                                    )
                                    if dr.status_code in (200, 204, 404):
                                        break
                    except Exception:
                        pass
            if p.returncode == 0 and got_mf:
                pure_mbps = size_mb / pure_dt
                commit_mbps = size_mb / commit_dt
                total_mbps = size_mb / total_dt
            else:
                pure_mbps = 0.0
                commit_mbps = 0.0
                total_mbps = 0.0
            print(
                f"[BENCH] size={size_mb}MB streams={sc} pure_wire={pure_dt:.2f}s ({pure_mbps:.1f} MB/s) "
                f"e2e_commit={commit_dt:.2f}s ({commit_mbps:.1f} MB/s) total={total_dt:.2f}s ({total_mbps:.1f} MB/s) rc={p.returncode}"
            )
            results[str(sc)] = {
                "elapsed_s": round(pure_dt, 3),
                "pure_wire_s": round(pure_dt, 3),
                "commit_elapsed_s": round(commit_dt, 3),
                "total_elapsed_s": round(total_dt, 3),
                "speed_mbs": round(pure_mbps, 2),
                "commit_speed_mbs": round(commit_mbps, 2),
                "total_speed_mbs": round(total_mbps, 2),
                "size_mb": size_mb,
                "rc": p.returncode,
            }
    finally:
        test_file.unlink(missing_ok=True)
        _set_worker_state(False, "", "idle")
        done_payload: Dict[str, Any] = {
            "filename": bench_name,
            "bench_results": results,
            "bench_size_mb": size_mb,
            "done": True,
        }
        if notify_chat_id:
            done_payload["bench_notify"] = {
                "chat_id": int(notify_chat_id),
                "status_mid": int(notify_mid) if notify_mid else None,
            }
        _notify_vps_job_done(done_payload)


def _choose_stage_dir(file_size: int) -> Path:
    if os.path.isdir("/dev/shm") and shutil.disk_usage("/dev/shm").free > file_size + 4 * 1024**3:
        d = Path("/dev/shm/f4ck_bg_stage")
    elif os.path.isdir("/mnt") and shutil.disk_usage("/mnt").free > file_size + 4 * 1024**3:
        d = Path("/mnt/f4ck_bg_stage")
    else:
        d = Path("/tmp/f4ck_bg_stage")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _extract_worker_json_dict(raw_stdout: str) -> Dict[str, Any]:
    txt = str(raw_stdout or "").strip()
    if not txt:
        return {}
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    s_idx = txt.find("{")
    e_idx = txt.rfind("}")
    if s_idx >= 0 and e_idx > s_idx:
        try:
            obj = json.loads(txt[s_idx : e_idx + 1])
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    return {}


def _coerce_worker_count(raw_val: Any) -> int:
    if isinstance(raw_val, bool):
        return int(raw_val)
    if isinstance(raw_val, int):
        return max(0, raw_val)
    if isinstance(raw_val, list):
        return len([x for x in raw_val if x])
    try:
        return max(0, int(raw_val or 0))
    except Exception:
        return 0


def _parse_worker_unfairplay_json(stdout_txt: str, target_mode: str) -> Dict[str, Any]:
    obj = _extract_worker_json_dict(stdout_txt)
    if target_mode == "macos":
        recs = obj.get("records") if isinstance(obj.get("records"), list) else []
        return {
            "target_mode": "macos",
            "sinf_records_converted": len(recs),
            "macos_sinf_converted": len(recs) > 0,
            "plaintext_verified": False,
            "account_data_stripped": False,
            "encrypted_images": len(recs),
            "decrypted_pages": 0,
            "removed_license_files": 0,
            "removed_metadata_fields": 0,
        }
    return {
        "target_mode": "ios",
        "plaintext_verified": bool(obj.get("PlaintextVerified")),
        "encrypted_images": int(obj.get("EncryptedImages") or 0),
        "decrypted_pages": int(obj.get("DecryptedPages") or 0),
        "account_data_stripped": bool(obj.get("AccountDataStripped")),
        "removed_license_files": _coerce_worker_count(obj.get("RemovedLicenseFiles")),
        "removed_metadata_fields": _coerce_worker_count(obj.get("RemovedMetadataFields")),
    }


def _ensure_strict_model_dir_perms(model_dir: Path) -> None:
    try:
        if model_dir and model_dir.is_dir():
            if (model_dir.stat().st_mode & 0o077) != 0:
                os.chmod(model_dir, 0o700)
            for mf in model_dir.iterdir():
                if mf.is_file() and (mf.stat().st_mode & 0o077) != 0:
                    os.chmod(mf, 0o600)
    except Exception:
        pass


def _run_worker_unfairplay_cli(
    enc_ipa: Path,
    out_ipa: Path,
    tmp_dir: Path,
    target_mode: str = "ios",
    strip_account_data: bool = True,
) -> Tuple[int, str, Dict[str, Any]]:
    _ensure_strict_model_dir_perms(MODEL_DIR)
    dec_env = os.environ.copy()
    dec_env["F4CK_FAST_DECRYPT"] = "1"
    report_path = tmp_dir / "decrypt_report.json"
    try:
        if target_mode == "macos":
            cmd = [
                UNFAIRPLAY_BIN,
                "convert-ipa-macos",
                "--model",
                str(MODEL_DIR),
                "--input",
                str(enc_ipa),
                "--output",
                str(out_ipa),
                "--force",
            ]
        else:
            cmd = [
                UNFAIRPLAY_BIN,
                "decrypt",
                "--model",
                str(MODEL_DIR),
                "--input",
                str(enc_ipa),
                "--output",
                str(out_ipa),
                "--report",
                str(report_path),
                "--force",
            ]
            if strip_account_data:
                cmd.extend(["--strip-account-data", "--preserve-sealed-resources"])
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1200,
            env=dec_env,
        )
        if (
            proc.returncode != 0
            and target_mode == "ios"
            and strip_account_data
            and "--preserve-sealed-resources" in ((proc.stderr or "") + (proc.stdout or ""))
        ):
            proc = subprocess.run(
                [
                    UNFAIRPLAY_BIN,
                    "decrypt",
                    "--model",
                    str(MODEL_DIR),
                    "--input",
                    str(enc_ipa),
                    "--output",
                    str(out_ipa),
                    "--report",
                    str(report_path),
                    "--force",
                    "--strip-account-data",
                ],
                capture_output=True,
                text=True,
                timeout=1200,
                env=dec_env,
            )
        if (
            proc.returncode != 0
            and target_mode == "ios"
            and "unknown flag" in ((proc.stderr or "") + (proc.stdout or "")).lower()
        ):
            proc = subprocess.run(
                [
                    UNFAIRPLAY_BIN,
                    "decrypt",
                    "--model",
                    str(MODEL_DIR),
                    "--input",
                    str(enc_ipa),
                    "--output",
                    str(out_ipa),
                    "--force",
                ],
                capture_output=True,
                text=True,
                timeout=1200,
                env=dec_env,
            )
        dec_err = strip_ansi(((proc.stdout or "") + "\n" + (proc.stderr or "")).strip())[-400:]
        telem = _parse_worker_unfairplay_json(proc.stdout or "", target_mode) if proc.returncode == 0 else {
            "target_mode": target_mode,
        }
        return proc.returncode, dec_err, telem
    finally:
        try:
            report_path.unlink(missing_ok=True)
        except Exception:
            pass


def process_one_job(bot_token: str, job: Dict[str, Any]) -> None:
    if job.get("bench_only"):
        _run_bench_job(bot_token, job)
        return

    chat_id = int(job.get("chat_id") or 0)
    status_mid = int(job["status_mid"]) if job.get("status_mid") else None
    reply_to_mid = int(job["reply_to_mid"]) if job.get("reply_to_mid") else None
    web_job_id = str(job.get("web_job_id") or "")
    bundle_id = str(job.get("bundle_id") or "")
    item_id = str(job.get("item_id") or "")
    title = str(job.get("title") or bundle_id or item_id or "Game")
    version = str(job.get("version") or "1.0")
    country = str(job.get("country") or "VN").upper()
    acc_cc = str(job.get("acc_cc") or country).upper()
    est_size = int(job.get("est_size") or 0)
    dl_url = str(job.get("dl_url") or "")
    icon_url = str(job.get("icon_url") or "")
    artist = str(job.get("artist") or "")
    gh_streams = int(job.get("gh_streams") or 0)
    raw_mode = str(job.get("target_mode") or "ios").strip().lower()
    target_mode = "macos" if raw_mode in ("macos", "mac", "apple_silicon", "macos_arm64") else "ios"
    strip_account_data = bool(job.get("strip_account_data", True)) if target_mode == "ios" else False

    safe_title = sanitize_filename(title, bundle_id or item_id or "Game")
    safe_ver = sanitize_filename(version, "1.0")
    suffix = "macOS" if target_mode == "macos" else "Decrypted"
    out_filename = os.path.basename(str(job.get("out_filename") or "").strip()) or f"{safe_title}_v{safe_ver}_{suffix}.ipa"
    store_link = str(job.get("store_url") or "") or (
        f"https://apps.apple.com/app/id{item_id}" if item_id.isdigit() else "https://apps.apple.com/"
    )
    mode_tag = " [macOS FairPlay]" if target_mode == "macos" else ""
    caption_base = (
        f"<code>{html.escape(bundle_id or item_id)}</code>{mode_tag}\n"
        f"Version: <code>{html.escape(version)}</code>\n"
        f"<a href=\"{html.escape(store_link)}\">App Store Link</a>"
    )
    if target_mode == "macos":
        _notify_vps_job_done({
            "filename": out_filename,
            "web_job_id": web_job_id,
            "fallback_needed": True,
            "error": "macOS FairPlay conversion runs exclusively on private VPS",
        })
        return
    notified_vps = False
    _set_worker_state(True, out_filename, "downloading", pct=1.0, job_id=web_job_id, push_now=True)

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
        dl_spd = 0.0
        err_txt = ""
        if cdn_url_pre and sinfs_pre:
            downloaded_ok, dl_spd = download_cdn_via_go_10gbps(
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
                "web_job_id": web_job_id,
                "fallback_needed": True,
                "error": err_txt or "CDN download failed on worker",
            })
            notified_vps = True
            return

        _set_worker_state(True, out_filename, "decrypting", pct=99.0, speed_mbs=dl_spd, push_now=True)
        _tg_edit(
            bot_token,
            chat_id,
            status_mid,
            caption_base + ("\nConverting for macOS..." if target_mode == "macos" else "\nDecrypting..."),
        )
        dec_rc, dec_err, dec_telem = _run_worker_unfairplay_cli(
            enc_ipa=enc_ipa,
            out_ipa=out_ipa,
            tmp_dir=tmp_dir,
            target_mode=target_mode,
            strip_account_data=strip_account_data,
        )
        enc_ipa.unlink(missing_ok=True)
        Path(f"{enc_ipa}.sinfs.json").unlink(missing_ok=True)
        if dec_rc != 0 or not out_ipa.is_file():
            _notify_vps_job_done({
                "filename": out_filename,
                "web_job_id": web_job_id,
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
        _set_worker_state(True, out_filename, "uploading", pct=1.0, total_bytes=dec_size, push_now=True)
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
            _set_worker_state(
                True,
                out_filename,
                "delivering",
                pct=99.0,
                speed_mbs=float(gh_mf.get("upload_speed_mbs") or 0.0),
                done_bytes=dec_size,
                total_bytes=dec_size,
                push_now=False,
            )
            _notify_vps_job_done({
                "filename": out_filename,
                "web_job_id": web_job_id,
                "title": title,
                "version": version,
                "bundle_id": bundle_id,
                "item_id": item_id,
                "country": country,
                "icon_url": icon_url,
                "artist": artist,
                "store_url": store_link,
                "total_size": dec_size,
                "gh_repo": gh_mf.get("repo") or GH_REPO,
                "gh_tag": gh_mf.get("tag") or "v1.0.0",
                "gh_release_id": gh_mf.get("release_id"),
                "gh_parts": gh_mf["parts"],
                "upload_speed_mbs": gh_mf.get("upload_speed_mbs") or 0.0,
                "e2e_commit_speed_mbs": gh_mf.get("e2e_commit_speed_mbs") or 0.0,
                "pure_wire_s": gh_mf.get("pure_wire_s") or 0.0,
                "e2e_commit_s": gh_mf.get("e2e_commit_s") or 0.0,
                "total_elapsed_s": gh_mf.get("total_elapsed_s") or 0.0,
                "dl_speed_mbs": dl_spd,
                **dec_telem,
            })
            notified_vps = True
            _set_worker_state(False, "", "idle", job_id="", push_now=False)

            # Fast-path for <= 19 MB: Telegram pulls directly from our 100-Gbps HTTPS URL!
            if chat_id and dec_size <= 19 * 1024 * 1024 and dl_url:
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

            temp_card_mid = None
            if chat_id:
                temp_card_mid = _tg_send(
                    bot_token,
                    chat_id,
                    caption_base,
                    reply_to_mid=reply_to_mid,
                    reply_markup=kb,
                )
                _tg_delete(bot_token, chat_id, status_mid)

            # Stage 5a (Single-Asset HTTP 302 Fastly Promotion) & Stage 5b (Native Telegram .IPA Upgrade)
            need_promote = (
                dec_size <= MAX_GH_SINGLE_ASSET_SIZE
                and len(gh_mf["parts"]) > 1
                and not job.get("skip_bg_promote")
            )
            need_bg_tg = bool(chat_id) and dec_size <= MAX_BOT_PART_SIZE and not job.get("skip_bg_tg")
            bg_refs = int(need_promote) + int(need_bg_tg)
            if bg_refs > 0:
                stage_dir = _choose_stage_dir(dec_size)
                staged_ipa = stage_dir / f"{int(time.time())}_{secrets.token_hex(3)}_{out_filename}"
                try:
                    shutil.move(str(out_ipa), str(staged_ipa))
                    staged_ref = _StagedFileRef(staged_ipa, refs=bg_refs)
                    if need_promote:
                        try:
                            _bg_cdn_executor.submit(
                                _bg_promote_single_gh_asset,
                                staged_ref,
                                out_filename,
                                dec_size,
                                list(gh_mf["parts"]),
                            )
                        except Exception:
                            staged_ref.release()
                    if need_bg_tg:
                        try:
                            _bg_tg_executor.submit(
                                _bg_telegram_native_upgrade,
                                bot_token,
                                chat_id,
                                staged_ref,
                                out_filename,
                                caption_base,
                                reply_to_mid,
                                kb,
                                temp_card_mid,
                            )
                        except Exception:
                            staged_ref.release()
                except Exception:
                    staged_ipa.unlink(missing_ok=True)
            return

        # Fallback if GitHub Releases CDN was unavailable
        ok_fb = False
        if chat_id:
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
            _notify_vps_job_done({
                "filename": out_filename,
                "web_job_id": web_job_id,
                "fallback_needed": True,
                "error": "Upload failed on both GH Releases and TeleCloud fallback",
            })
            notified_vps = True
    finally:
        _set_worker_state(False, "", "idle", push_now=True)
        if not notified_vps:
            _notify_vps_job_done({
                "filename": out_filename,
                "web_job_id": web_job_id,
                "fallback_needed": True,
                "error": "Worker terminated unexpectedly during job processing",
            })
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main() -> None:
    bot_token = os.environ["F4CK_BOT_TOKEN"].strip()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    idle_since = time.time()
    try:
        while (time.time() - idle_since < WARM_IDLE_SECONDS) and (
            time.time() - _worker_boot_ts < MAX_WORKER_LIFETIME_SECONDS
        ):
            try:
                r = _http_sess.get(VPS_POP_URL, timeout=6)
                if r.status_code == 200:
                    job_data = r.json()
                    if job_data and (job_data.get("chat_id") is not None or job_data.get("bench_only")):
                        process_one_job(bot_token, job_data)
                        idle_since = time.time()
                        continue
            except Exception:
                pass
            time.sleep(0.15)
    finally:
        _set_worker_state(True, "", "draining_bg")
        _bg_cdn_executor.shutdown(wait=True)
        _bg_tg_executor.shutdown(wait=True)
        _set_worker_state(False, "", "stopped")
        _worker_stop_evt.set()


if __name__ == "__main__":
    main()

