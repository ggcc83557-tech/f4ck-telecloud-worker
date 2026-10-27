#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
F4CK CHEAT — GitHub Actions 10-Gbps Hyper-Decrypt Worker
Runs inside Azure 10-Gbps Runner:
  1. Downloads encrypted IPA from Apple CDN via aria2c (16 streams, ~500 MB/s) into /dev/shm RAM
  2. Decrypts FairPlay via Zero-Copy unfairplay in RAM (~1-3s)
  3. Publishes decrypted IPA to GitHub Release CDN (v1.0.0) in ~1-3s and immediately sends Direct Download Button
  4. Streams decrypted IPA into Telegram Bot chat via 16-connection MTProto + cryptg at Azure 10-Gbps speed
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

TG_API_ID = 2040
TG_API_HASH = "b18441a1ff607e10a989891a5462e627"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


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
        requests.post(f"https://api.telegram.org/bot{bot_token}/editMessageText", json=payload, timeout=10)
    except Exception:
        pass


def _tg_delete(bot_token: str, chat_id: Any, message_id: Optional[int]) -> None:
    if not message_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/deleteMessage",
            json={"chat_id": chat_id, "message_id": int(message_id)},
            timeout=8,
        )
    except Exception:
        pass


def upload_to_github_release(ipa_path: Path) -> Optional[str]:
    """
    Uploads decrypted IPA (< 2 GB) to GitHub Release v1.0.0 over internal Azure 10-Gbps network.
    """
    try:
        subprocess.run(
            ["gh", "release", "view", RELEASE_TAG, "--repo", GH_REPO],
            check=True,
            capture_output=True,
            timeout=15,
        )
    except Exception:
        subprocess.run(
            ["gh", "release", "create", RELEASE_TAG, "--title", "F4CK Decrypted IPA Vault", "--notes", "Auto-Decrypted IPAs", "--repo", GH_REPO],
            capture_output=True,
            timeout=20,
        )

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
    """
    Fallback for massive games (> 2 GB) that exceed GitHub Release's 2 GB per-asset limit.
    """
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
    caption: str,
    status_mid: Optional[int],
    reply_markup: Optional[dict],
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
                    if status_mid and (now - last_edit[0] >= 3.0):
                        last_edit[0] = now
                        elapsed = max(0.1, now - start_ts)
                        speed_mb = (sent_bytes / (1024 * 1024)) / elapsed
                        pct = min(99.9, (sent_bytes * 100.0 / total_sz) if total_sz > 0 else 0.0)
                        msg_txt = (
                            caption
                            + f"\n\n<b>[3/3] Đang bắn File IPA từ GitHub 10-Gbps vào Telegram:</b> "
                            + f"<b>{pct:.1f}%</b> (<code>{format_bytes(sent_bytes)} / {format_bytes(total_sz)}</code> — <code>{speed_mb:.1f} MB/s</code>)"
                        )
                        threading.Thread(
                            target=_tg_edit,
                            args=(bot_token, chat_id, status_mid, msg_txt, reply_markup),
                            daemon=True,
                        ).start()

            workers = [asyncio.create_task(_producer())]
            for s in raw_senders:
                workers.append(asyncio.create_task(_worker(s)))
                workers.append(asyncio.create_task(_worker(s)))

            await asyncio.gather(*workers)

            input_file = InputFileBig(file_id, part_count, ipa_path.name)
            await client.send_file(
                int(chat_id),
                file=input_file,
                caption=caption,
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


def main() -> None:
    bot_token = os.environ["F4CK_BOT_TOKEN"].strip()
    chat_id = int(os.environ["INPUT_CHAT_ID"].strip())
    status_mid_str = os.getenv("INPUT_STATUS_MID", "").strip()
    status_mid = int(status_mid_str) if status_mid_str.isdigit() else None
    bundle_id = os.getenv("INPUT_BUNDLE_ID", "").strip()
    item_id = os.getenv("INPUT_ITEM_ID", "").strip()
    title = os.getenv("INPUT_TITLE", "").strip() or bundle_id or item_id or "Game"
    version = os.getenv("INPUT_VERSION", "").strip() or "1.0"
    country = (os.getenv("INPUT_COUNTRY", "").strip() or "VN").upper()

    work_root = Path("/dev/shm") if Path("/dev/shm").is_dir() and shutil.disk_usage("/dev/shm").free > 4 * 1024**3 else Path("/tmp")
    tmp_dir = Path(tempfile.mkdtemp(prefix="f4ck_gh10g_", dir=str(work_root)))

    try:
        t0 = time.time()
        _tg_edit(
            bot_token,
            chat_id,
            status_mid,
            f"<b>ĐANG TẢI IPA TRÊN CỤM GITHUB 10-GBPS (AZURE)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Tên Game:</b> <b>{html.escape(title)}</b>\n"
            f"• <b>Bundle ID:</b> <code>{html.escape(bundle_id or item_id)}</code>\n"
            f"• <b>Phiên bản:</b> <code>{html.escape(version)}</code> ({country})\n"
            f"• <b>Trạng thái:</b> <b>[1/3] Đang kéo IPA từ Apple CDN bằng 16 luồng 10-Gbps...</b>",
        )

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
        if bundle_id:
            dl_cmd.extend(["-b", bundle_id])
        else:
            dl_cmd.extend(["-i", item_id])

        dl_proc = subprocess.run(dl_cmd, capture_output=True, text=True, timeout=600)
        if dl_proc.returncode != 0 or not enc_ipa.is_file():
            err_txt = strip_ansi(((dl_proc.stdout or "") + "\n" + (dl_proc.stderr or "")).strip())[-500:]
            _tg_edit(
                bot_token,
                chat_id,
                status_mid,
                f"<b>LỖI TẢI IPA TỪ APPLE (IPATOOL 10-GBPS):</b>\n"
                f"• <b>Game:</b> <b>{html.escape(title)}</b>\n"
                f"• <b>Chi tiết:</b>\n<code>{html.escape(err_txt or 'Không tải được file IPA')}</code>",
            )
            return

        dl_elapsed = max(0.1, time.time() - t0)
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

        # Free RAM immediately by removing encrypted.ipa
        try:
            enc_ipa.unlink()
        except Exception:
            pass

        # Upload to GitHub Release CDN (if <= 1.95 GB) or GoFile CDN (if > 1.95 GB)
        if dec_size <= 1950 * 1024 * 1024:
            dl_url = upload_to_github_release(out_ipa)
        else:
            dl_url = upload_to_gofile(out_ipa)

        kb = None
        link_line = ""
        if dl_url:
            kb = {
                "inline_keyboard": [
                    [{"text": f"Tải Trực Tiếp IPA ({format_bytes(dec_size)} - GitHub CDN Max Speed)", "url": dl_url}]
                ]
            }
            link_line = f"\n• <b>Link tải tốc độ cao (GitHub CDN):</b> <a href=\"{html.escape(dl_url)}\">Bấm vào đây để tải IPA ngay</a>"

        caption_html = (
            f"<b>ĐÃ GIẢI MÃ FAIRPLAY HOÀN TẤT (CRYPTID = 0)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Tên Game:</b> <b>{html.escape(title)}</b>\n"
            f"• <b>Bundle ID:</b> <code>{html.escape(bundle_id or item_id)}</code>\n"
            f"• <b>Phiên bản:</b> <code>{html.escape(version)}</code> ({country})\n"
            f"• <b>Dung lượng:</b> <code>{format_bytes(dec_size)}</code>\n"
            f"• <b>Tốc độ xử lý (GitHub 10Gbps):</b> <code>Tải {dl_elapsed:.1f}s ({dl_speed:.0f} MB/s) | Giải mã {dec_elapsed:.1f}s</code>\n"
            f"• <b>FairPlay DRM:</b> <code>Đã gỡ 100% (cryptid = 0)</code>"
            f"{link_line}"
        )

        # IMMEDIATELY show the GitHub CDN Direct Download Button!
        _tg_edit(
            bot_token,
            chat_id,
            status_mid,
            caption_html
            + "\n\n<b>[3/3] Bạn có thể bấm nút Tải Trực Tiếp (GitHub CDN) bên dưới để tải ngay lập tức, hoặc chờ vài giây File IPA đang được bắn thẳng vào khung chat...</b>",
            reply_markup=kb,
        )

        # Fast-path 1: <= 19 MB -> Telegram URL pull directly from GitHub CDN
        if dl_url and dec_size <= 19 * 1024 * 1024:
            try:
                resp = requests.post(
                    f"https://api.telegram.org/bot{bot_token}/sendDocument",
                    json={
                        "chat_id": chat_id,
                        "document": dl_url,
                        "caption": caption_html,
                        "parse_mode": "HTML",
                        "reply_markup": kb,
                    },
                    timeout=35,
                ).json()
                if resp.get("ok"):
                    _tg_delete(bot_token, chat_id, status_mid)
                    return
            except Exception:
                pass

        # Fast-path 2: <= 1,990 MB -> 16-connection Telethon MTProto + cryptg over Azure 10-Gbps
        if dec_size <= 1990 * 1024 * 1024:
            ok = upload_via_telethon_10gbps(
                bot_token=bot_token,
                chat_id=chat_id,
                ipa_path=out_ipa,
                caption=caption_html,
                status_mid=status_mid,
                reply_markup=kb,
            )
            if ok:
                _tg_delete(bot_token, chat_id, status_mid)
                return

        # If > 2GB (Telegram limit) or upload skipped, keep the clean final message with the CDN button
        _tg_edit(bot_token, chat_id, status_mid, caption_html, reply_markup=kb)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
