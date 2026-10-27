#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
F4CK CHEAT — GitHub Actions 10-Gbps Hot-Standby Hyper-Decrypt Daemon
- Zero Cold-Start (0.2s pickup from VPS queue via HTTP Keep-Alive polling)
- Ultra-Fast ipatool (skips redundant Lookup, prioritizes F4CK_COUNTRY_HINT, hits updateProduct directly + aria2c 16 streams)
- Live real-time download progress (% & MB/s) during Step [1/3]
- Zero-Copy RAM FairPlay Decrypt via unfairplay in /dev/shm (~1-2s)
- Concurrent GitHub Release CDN upload + 16-Stream MTProto upload
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

IPATOOL_BIN = "/usr/local/bin/ipatool"
UNFAIRPLAY_BIN = "/usr/local/bin/unfairplay"
KEYCHAIN_PASSPHRASE = "f4ck_vps_keychain"
MODEL_DIR = Path("/root/.config/unfairplay/models")
GH_REPO = os.getenv("GITHUB_REPOSITORY", "tranqvinh03112006-byte/test")
RELEASE_TAG = "v1.0.0"
VPS_POP_URL = "https://160-191-242-70.sslip.io/dl_dec/gh_warm_pop/f4ck_unfairplay_dl_secret_2026/__gh_next_job__.json"

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


def ensure_release_exists() -> None:
    try:
        r = subprocess.run(
            ["gh", "release", "view", RELEASE_TAG, "--repo", GH_REPO],
            capture_output=True,
            timeout=15,
        )
        if r.returncode != 0:
            subprocess.run(
                ["gh", "release", "create", RELEASE_TAG, "--title", "F4CK Decrypted IPA Vault", "--notes", "Auto-Decrypted IPAs", "--repo", GH_REPO],
                capture_output=True,
                timeout=20,
            )
    except Exception:
        pass


def upload_to_github_release(ipa_path: Path) -> Optional[str]:
    proc = subprocess.run(
        ["gh", "release", "upload", RELEASE_TAG, str(ipa_path), "--clobber", "--repo", GH_REPO],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode == 0:
        q_name = urllib.parse.quote(ipa_path.name)
        return f"https://github.com/{GH_REPO}/releases/download/{RELEASE_TAG}/{q_name}"
    return None


def upload_to_gofile(ipa_path: Path) -> Optional[str]:
    try:
        srv_resp = requests.get("https://api.gofile.io/servers", timeout=10).json()
        servers = srv_resp.get("data", {}).get("servers", [])
        srv = servers[0]["name"] if servers else "store1"
        with open(ipa_path, "rb") as fh:
            up_resp = requests.post(
                f"https://{srv}.gofile.io/contents/uploadfile",
                files={"file": (ipa_path.name, fh)},
                timeout=600,
            ).json()
        return up_resp.get("data", {}).get("downloadPage")
    except Exception:
        return None


def upload_via_telethon_10gbps(
    bot_token: str,
    chat_id: int,
    ipa_path: Path,
    base_caption: str,
    status_mid: Optional[int],
    shared_cdn: Dict[str, Any],
) -> bool:
    from telethon import TelegramClient, helpers
    from telethon.network import MTProtoSender
    from telethon.tl.functions.upload import SaveBigFilePartRequest
    from telethon.tl.types import DocumentAttributeFilename, InputFileBig

    async def _run() -> bool:
        session_file = f"/tmp/gh_tele_{hashlib.md5(bot_token.encode()).hexdigest()[:8]}"
        client = TelegramClient(
            session_file,
            TG_API_ID,
            TG_API_HASH,
            receive_updates=False,
            sequential_updates=False,
            flood_sleep_threshold=180,
        )
        await client.start(bot_token=bot_token)
        raw_senders: list[Any] = []
        try:
            total_sz = ipa_path.stat().st_size
            part_size = 512 * 1024
            part_count = (total_sz + part_size - 1) // part_size
            conn_count = max(1, min(16, part_count))
            file_id = helpers.generate_random_long()

            dc = await client._get_dc(client.session.dc_id)

            async def _make_sender() -> Any:
                s = MTProtoSender(client.session.auth_key, loggers=client._log)
                await s.connect(
                    client._connection(
                        dc.ip_address,
                        dc.port,
                        dc.id,
                        loggers=client._log,
                    )
                )
                return s

            raw_senders = list(await asyncio.gather(*(_make_sender() for _ in range(conn_count))))
            q: asyncio.Queue[Optional[Tuple[int, bytes]]] = asyncio.Queue(maxsize=64)
            sent_bytes = 0
            start_ts = time.time()
            last_edit = [start_ts]
            total_workers = len(raw_senders) * 2

            async def _producer() -> None:
                with open(ipa_path, "rb") as fh:
                    for idx in range(part_count):
                        chunk = fh.read(part_size)
                        if not chunk:
                            break
                        await q.put((idx, chunk))
                for _ in range(total_workers):
                    await q.put(None)

            async def _worker(sender_obj: Any) -> None:
                nonlocal sent_bytes
                while True:
                    item = await q.get()
                    if item is None:
                        break
                    idx, chunk = item
                    req = SaveBigFilePartRequest(file_id, idx, part_count, chunk)
                    await client._call(sender_obj, req)
                    sent_bytes += len(chunk)
                    now = time.time()
                    if status_mid and (now - last_edit[0] >= 2.8):
                        last_edit[0] = now
                        elapsed = max(0.1, now - start_ts)
                        speed_mb = (sent_bytes / (1024 * 1024)) / elapsed
                        pct = min(99.9, (sent_bytes * 100.0 / total_sz) if total_sz > 0 else 0.0)
                        cur_cap = shared_cdn.get("caption", base_caption)
                        cur_kb = shared_cdn.get("kb")
                        msg_txt = (
                            cur_cap
                            + f"\n\n<b>[3/3] Đang bắn File IPA từ GitHub 10-Gbps vào Telegram:</b> "
                            + f"<b>{pct:.1f}%</b> (<code>{format_bytes(sent_bytes)} / {format_bytes(total_sz)}</code> — <code>{speed_mb:.1f} MB/s</code>)"
                        )
                        threading.Thread(
                            target=_tg_edit,
                            args=(bot_token, chat_id, status_mid, msg_txt, cur_kb),
                            daemon=True,
                        ).start()

            workers = [asyncio.create_task(_producer())]
            for s in raw_senders:
                workers.append(asyncio.create_task(_worker(s)))
                workers.append(asyncio.create_task(_worker(s)))

            await asyncio.gather(*workers)

            final_cap = shared_cdn.get("caption", base_caption)
            input_file = InputFileBig(file_id, part_count, ipa_path.name)
            await client.send_file(
                int(chat_id),
                file=input_file,
                caption=final_cap,
                parse_mode="html",
                force_document=True,
                attributes=[DocumentAttributeFilename(file_name=ipa_path.name)],
            )
            return True
        finally:
            for s in raw_senders:
                try:
                    await asyncio.wait_for(s.disconnect(), timeout=3)
                except Exception:
                    pass
            await client.disconnect()

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_run())
    finally:
        loop.close()


def process_one_job(bot_token: str, job: Dict[str, Any]) -> None:
    chat_id = int(str(job["chat_id"]).strip())
    status_mid_str = str(job.get("status_mid") or "").strip()
    status_mid = int(status_mid_str) if status_mid_str.isdigit() else None
    bundle_id = str(job.get("bundle_id") or "").strip()
    item_id = str(job.get("item_id") or "").strip()
    title = str(job.get("title") or "").strip() or bundle_id or item_id or "Game"
    version = str(job.get("version") or "").strip() or "1.0"
    country = (str(job.get("country") or "").strip() or "VN").upper()
    est_size = int(job.get("size_bytes") or 0)

    work_root = Path("/dev/shm") if Path("/dev/shm").is_dir() and shutil.disk_usage("/dev/shm").free > 4 * 1024**3 else Path("/tmp")
    tmp_dir = Path(tempfile.mkdtemp(prefix="f4ck_gh10g_", dir=str(work_root)))

    try:
        t0 = time.time()
        _tg_edit(
            bot_token,
            chat_id,
            status_mid,
            f"<b>ĐANG KÉO IPA TRÊN CỔNG 10-GBPS (AZURE HOT-NODE)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Tên Game:</b> <b>{html.escape(title)}</b>\n"
            f"• <b>Bundle ID:</b> <code>{html.escape(bundle_id or item_id)}</code>\n"
            f"• <b>Phiên bản:</b> <code>{html.escape(version)}</code> ({country})\n"
            f"• <b>Trạng thái:</b> <b>[1/3] Đang lấy link CDN &amp; tải 16 luồng 10-Gbps...</b>",
        )

        enc_ipa = tmp_dir / "encrypted.ipa"
        enc_tmp = Path(f"{enc_ipa}.tmp")
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
        if bundle_id:
            dl_cmd.extend(["-b", bundle_id])

        env = os.environ.copy()
        env["F4CK_COUNTRY_HINT"] = country

        proc = subprocess.Popen(dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        last_dl_edit = time.time()
        dl_start_bytes_ts = None

        while proc.poll() is None:
            time.sleep(0.4)
            cur_sz = 0
            if enc_tmp.is_file():
                cur_sz = enc_tmp.stat().st_size
            elif enc_ipa.is_file():
                cur_sz = enc_ipa.stat().st_size

            if cur_sz > 0 and dl_start_bytes_ts is None:
                dl_start_bytes_ts = time.time()

            now = time.time()
            if cur_sz > 0 and status_mid and (now - last_dl_edit >= 1.8):
                last_dl_edit = now
                el = max(0.1, now - (dl_start_bytes_ts or t0))
                spd = (cur_sz / (1024 * 1024)) / el
                pct_str = f"<b>{min(99.9, cur_sz * 100.0 / est_size):.1f}%</b> — " if est_size > 0 else ""
                total_str = f" / {format_bytes(est_size)}" if est_size > 0 else ""
                threading.Thread(
                    target=_tg_edit,
                    args=(
                        bot_token,
                        chat_id,
                        status_mid,
                        f"<b>ĐANG KÉO IPA TRÊN CỔNG 10-GBPS (AZURE HOT-NODE)</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"• <b>Tên Game:</b> <b>{html.escape(title)}</b>\n"
                        f"• <b>Bundle ID:</b> <code>{html.escape(bundle_id or item_id)}</code>\n"
                        f"• <b>Phiên bản:</b> <code>{html.escape(version)}</code> ({country})\n"
                        f"• <b>Trạng thái:</b> <b>[1/3] Đang kéo từ Apple CDN:</b> {pct_str}<code>{format_bytes(cur_sz)}{total_str} ({spd:.1f} MB/s)</code>",
                    ),
                    daemon=True,
                ).start()

        stdout_s, stderr_s = proc.communicate(timeout=10)
        if proc.returncode != 0 or not enc_ipa.is_file():
            err_txt = strip_ansi(((stdout_s or "") + "\n" + (stderr_s or "")).strip())[-500:]
            _tg_edit(
                bot_token,
                chat_id,
                status_mid,
                f"<b>LỖI TẢI IPA TỪ APPLE (IPATOOL 10-GBPS):</b>\n"
                f"• <b>Game:</b> <b>{html.escape(title)}</b>\n"
                f"• <b>Chi tiết:</b>\n<code>{html.escape(err_txt or 'Không tải được file IPA')}</code>",
            )
            return

        dl_elapsed = max(0.1, time.time() - (dl_start_bytes_ts or t0))
        enc_size = enc_ipa.stat().st_size
        dl_speed = (enc_size / (1024 * 1024)) / dl_elapsed

        t_dec = time.time()
        _tg_edit(
            bot_token,
            chat_id,
            status_mid,
            f"<b>ĐANG BÓC KHÓA FAIRPLAY DRM TRÊN RAM (10-GBPS)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Tên Game:</b> <b>{html.escape(title)}</b>\n"
            f"• <b>Tốc độ tải Apple CDN:</b> <code>{format_bytes(enc_size)} trong {dl_elapsed:.1f}s ({dl_speed:.1f} MB/s)</code>\n"
            f"• <b>Trạng thái:</b> <b>[2/3] Đang giải mã Mach-O bằng UnfairPlay Zero-Copy...</b>",
        )

        safe_title = sanitize_filename(title, bundle_id or item_id or "Game")
        safe_ver = sanitize_filename(version, "1.0")
        out_filename = f"{safe_title}_v{safe_ver}_Decrypted.ipa"
        out_ipa = tmp_dir / out_filename

        dec_proc = subprocess.run(
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
            timeout=300,
        )
        if dec_proc.returncode != 0 or not out_ipa.is_file():
            err_txt = strip_ansi(((dec_proc.stdout or "") + "\n" + (dec_proc.stderr or "")).strip())[-500:]
            _tg_edit(
                bot_token,
                chat_id,
                status_mid,
                f"<b>LỖI GIẢI MÃ UNFAIRPLAY:</b>\n"
                f"• <b>Game:</b> <b>{html.escape(title)}</b>\n"
                f"• <b>Chi tiết:</b>\n<code>{html.escape(err_txt or 'Giải mã thất bại')}</code>",
            )
            return

        dec_elapsed = max(0.1, time.time() - t_dec)
        dec_size = out_ipa.stat().st_size

        try:
            enc_ipa.unlink()
        except Exception:
            pass

        base_caption = (
            f"<b>ĐÃ GIẢI MÃ FAIRPLAY HOÀN TẤT (CRYPTID = 0)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Tên Game:</b> <b>{html.escape(title)}</b>\n"
            f"• <b>Bundle ID:</b> <code>{html.escape(bundle_id or item_id)}</code>\n"
            f"• <b>Phiên bản:</b> <code>{html.escape(version)}</code> ({country})\n"
            f"• <b>Dung lượng:</b> <code>{format_bytes(dec_size)}</code>\n"
            f"• <b>Tốc độ xử lý (10Gbps):</b> <code>Tải {dl_elapsed:.1f}s ({dl_speed:.0f} MB/s) | Giải mã {dec_elapsed:.1f}s</code>\n"
            f"• <b>FairPlay DRM:</b> <code>Đã gỡ 100% (cryptid = 0)</code>"
        )

        # Predict the exact GitHub Release CDN URL immediately!
        q_name = urllib.parse.quote(out_ipa.name)
        predicted_gh_url = f"https://github.com/{GH_REPO}/releases/download/{RELEASE_TAG}/{q_name}"

        shared_cdn: Dict[str, Any] = {
            "caption": base_caption,
            "kb": None,
            "url": None,
        }

        def _bg_upload_cdn() -> None:
            if dec_size <= 1950 * 1024 * 1024:
                url = upload_to_github_release(out_ipa)
            else:
                url = upload_to_gofile(out_ipa)
            if url:
                shared_cdn["url"] = url
                shared_cdn["kb"] = {
                    "inline_keyboard": [
                        [{"text": f"Tải Trực Tiếp IPA ({format_bytes(dec_size)} - GitHub CDN Max Speed)", "url": url}]
                    ]
                }
                shared_cdn["caption"] = (
                    base_caption
                    + f"\n• <b>Link tải tốc độ cao (GitHub CDN):</b> <a href=\"{html.escape(url)}\">Bấm vào đây để tải IPA ngay</a>"
                )
                _tg_edit(
                    bot_token,
                    chat_id,
                    status_mid,
                    shared_cdn["caption"]
                    + "\n\n<b>[3/3] Nút Tải Trực Tiếp (GitHub CDN) đã sẵn sàng bên dưới! File IPA cũng đang được bắn thẳng vào khung chat...</b>",
                    reply_markup=shared_cdn["kb"],
                )

        # For files <= 19 MB, upload to GitHub Release (takes ~0.5s) and let Telegram pull via URL
        if dec_size <= 19 * 1024 * 1024:
            _bg_upload_cdn()
            if shared_cdn["url"]:
                try:
                    resp = _http_sess.post(
                        f"https://api.telegram.org/bot{bot_token}/sendDocument",
                        json={
                            "chat_id": chat_id,
                            "document": shared_cdn["url"],
                            "caption": shared_cdn["caption"],
                            "parse_mode": "HTML",
                            "reply_markup": shared_cdn["kb"],
                        },
                        timeout=35,
                    ).json()
                    if resp.get("ok"):
                        _tg_delete(bot_token, chat_id, status_mid)
                        return
                except Exception:
                    pass

        # Start CDN upload in parallel with 16-stream Telethon upload!
        cdn_thread = threading.Thread(target=_bg_upload_cdn, daemon=True)
        cdn_thread.start()

        if dec_size <= 1990 * 1024 * 1024:
            ok = upload_via_telethon_10gbps(
                bot_token=bot_token,
                chat_id=chat_id,
                ipa_path=out_ipa,
                base_caption=base_caption,
                status_mid=status_mid,
                shared_cdn=shared_cdn,
            )
            if ok:
                _tg_delete(bot_token, chat_id, status_mid)
                return

        cdn_thread.join(timeout=120)
        _tg_edit(bot_token, chat_id, status_mid, shared_cdn["caption"], reply_markup=shared_cdn["kb"])
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main() -> None:
    bot_token = os.environ["F4CK_BOT_TOKEN"].strip()
    ensure_release_exists()

    # 1. Process initial workflow_dispatch job if provided
    init_chat = os.getenv("INPUT_CHAT_ID", "").strip()
    if init_chat and (os.getenv("INPUT_BUNDLE_ID", "").strip() or os.getenv("INPUT_ITEM_ID", "").strip()):
        process_one_job(
            bot_token,
            {
                "chat_id": init_chat,
                "status_mid": os.getenv("INPUT_STATUS_MID", "").strip(),
                "bundle_id": os.getenv("INPUT_BUNDLE_ID", "").strip(),
                "item_id": os.getenv("INPUT_ITEM_ID", "").strip(),
                "title": os.getenv("INPUT_TITLE", "").strip(),
                "version": os.getenv("INPUT_VERSION", "").strip(),
                "country": os.getenv("INPUT_COUNTRY", "").strip(),
            },
        )

    # 2. Enter 24/7 Hot-Standby Daemon Loop (0.0s cold start for all subsequent requests!)
    print("Entering 24/7 Hot-Standby 10-Gbps Daemon Loop (polling VPS every 0.4s)...", flush=True)
    daemon_start = time.time()
    while time.time() - daemon_start < 5 * 3600:
        try:
            r = _http_sess.get(VPS_POP_URL, timeout=6)
            if r.status_code == 200:
                job_data = r.json()
                if job_data and job_data.get("chat_id"):
                    print(f"Hot-Standby picked up job: {job_data.get('title')} ({job_data.get('bundle_id')})", flush=True)
                    process_one_job(bot_token, job_data)
                    continue
        except Exception:
            pass
        time.sleep(0.4)


if __name__ == "__main__":
    main()
