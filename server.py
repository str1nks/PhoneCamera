#!/usr/bin/env python3
"""
Phone Cam Bridge — телефон как веб-камера для ПК.

Поднимает HTTPS-сайт на порту 4829 (локальная сеть). Телефон открывает его,
даёт доступ к камере и шлёт видео по WebSocket (TCP, без потерь пакетов):
JPEG-кадры (каждый кадр самодостаточен, битых «хвостов» не бывает).
Сервер принимает поток и отдаёт его в виртуальную камеру (pyvirtualcam: OBS / v4l2loopback).
"""
import argparse
import _thread
import asyncio
import atexit
import collections
import io
import ipaddress
import json
import logging
import os
import re
import shutil
import socket
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import av
import numpy as np
from aiohttp import WSMsgType, web

if getattr(sys, "frozen", False):          # запущено как .exe
    RES = Path(sys._MEIPASS)               # сюда PyInstaller распаковывает index.html
    BASE = Path(sys.executable).parent     # рядом с .exe — тут будут жить certs/
else:
    RES = BASE = Path(__file__).resolve().parent

STATIC = RES / "static"
CERT_DIR = BASE / "certs"
DEFAULT_PORT = 4829

log = logging.getLogger("bridge")


# ───────────────────────────── сеть и сертификат ─────────────────────────────

def local_ips():
    """IP-адреса ПК в локальной сети (основной — первым)."""
    ips = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ips.append(s.getsockname()[0])
        s.close()
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.append(info[4][0])
    except OSError:
        pass
    out = []
    for ip in ips:
        if ip != "127.0.0.1" and not ip.startswith("169.254.") and ip not in out:
            out.append(ip)

    def rank(ip):
        # домашняя сеть обычно 192.168.x.x; адреса вида x.x.x.1 часто принадлежат
        # виртуальным адаптерам (Docker, WSL, Hyper-V, VPN), их ставим в конец
        r = 0 if ip.startswith("192.168.") else 1 if ip.startswith("10.") else 2
        return (r + (3 if ip.endswith(".1") else 0), out.index(ip))

    return sorted(out, key=rank)


def ensure_cert(ips):
    """Самоподписанный сертификат с SAN под текущие IP (нужен HTTPS для getUserMedia)."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    CERT_DIR.mkdir(exist_ok=True)
    cert_p, key_p, meta_p = CERT_DIR / "cert.pem", CERT_DIR / "key.pem", CERT_DIR / "ips.json"
    wanted = sorted(ips)
    if cert_p.exists() and key_p.exists() and meta_p.exists():
        try:
            if json.loads(meta_p.read_text()) == wanted:
                return cert_p, key_p
        except ValueError:
            pass

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Phone Cam Bridge")])
    san = [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
    try:
        san.append(x509.DNSName(socket.gethostname()))
    except ValueError:
        pass
    san += [x509.IPAddress(ipaddress.ip_address(ip)) for ip in ips]

    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=825))
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    key_p.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    cert_p.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    meta_p.write_text(json.dumps(wanted))
    log.info("Создан самоподписанный сертификат для: %s", ", ".join(ips) or "localhost")
    return cert_p, key_p


# ───────────────────────────── виртуальная камера ────────────────────────────

class VirtualCamSink:
    """Принимает av.VideoFrame, вписывает в кадр заданного размера и шлёт в виртуальную камеру."""

    def __init__(self, width, height, fps, backend=None, device=None):
        self.w, self.h, self.fps = width, height, fps
        self.backend, self.device = backend, device
        self.cam = None
        self._lock = threading.Lock()
        self.mirror = False
        self.rotate = 0  # 0 / 90 / 180 / 270 по часовой
        self.canvas = np.zeros((height, width, 3), np.uint8)

    @property
    def label(self):
        return f"{self.w}x{self.h}@{self.fps}"

    def reconfigure(self, width, height, fps):
        """Меняет размер и FPS выхода на лету. Возвращает True, если что-то изменилось.
        Виртуальную камеру нельзя перенастроить, поэтому закрываем её — она откроется заново
        с новыми параметрами при следующем кадре."""
        with self._lock:
            if (width, height, fps) == (self.w, self.h, self.fps):
                return False
            if self.cam is not None:
                try:
                    self.cam.close()
                except Exception:  # noqa: BLE001
                    pass
                self.cam = None
            self.w, self.h, self.fps = width, height, fps
            self.canvas = np.zeros((height, width, 3), np.uint8)
        log.info("Выход виртуальной камеры изменён: %s", self.label)
        return True

    def _open(self):
        import pyvirtualcam

        kwargs = dict(width=self.w, height=self.h, fps=self.fps, fmt=pyvirtualcam.PixelFormat.RGB)
        if self.backend:
            kwargs["backend"] = self.backend
        if self.device:
            kwargs["device"] = self.device
        self.cam = pyvirtualcam.Camera(**kwargs)
        log.info("Виртуальная камера: %s (%s)", self.cam.device, self.label)

    def push(self, frame):
        with self._lock:
            self._push(frame)

    def _push(self, frame):
        if self.cam is None:
            self._open()

        sw, sh = frame.width, frame.height
        swapped = self.rotate in (90, 270)
        rw, rh = (sh, sw) if swapped else (sw, sh)  # размер после поворота
        scale = min(self.w / rw, self.h / rh)
        nrw = max(2, int(rw * scale) // 2 * 2)
        nrh = max(2, int(rh * scale) // 2 * 2)
        nw, nh = (nrh, nrw) if swapped else (nrw, nrh)  # размер до поворота

        img = frame.reformat(width=nw, height=nh, format="rgb24").to_ndarray()
        if self.rotate:
            img = np.rot90(img, k=(-self.rotate // 90) % 4)
        if self.mirror:
            img = img[:, ::-1]

        ih, iw = img.shape[:2]
        self.canvas.fill(0)
        y, x = (self.h - ih) // 2, (self.w - iw) // 2
        self.canvas[y : y + ih, x : x + iw] = img
        self.cam.send(self.canvas)

    def close(self):
        with self._lock:
            if self.cam is not None:
                try:
                    self.cam.close()
                except Exception:  # noqa: BLE001
                    pass
                self.cam = None


# ───────────────────────────── декодирование JPEG ────────────────────────────

def make_jpeg_decoder():
    """Возвращает (функция bytes-like -> RGB ndarray, имя библиотеки). OpenCV быстрее Pillow."""
    try:
        import cv2

        cv2.setNumThreads(2)

        def decode(data):
            img = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
            if img is None:
                raise ValueError("не удалось декодировать JPEG")
            return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        return decode, "OpenCV"
    except ImportError:
        pass
    try:
        from PIL import Image
    except ImportError:
        raise SystemExit("Нужна библиотека для JPEG: pip install opencv-python-headless  (или pillow)")

    def decode(data):
        with Image.open(io.BytesIO(data)) as im:
            return np.ascontiguousarray(np.asarray(im.convert("RGB")))

    return decode, "Pillow"


# ───────────────────────────── мост WebSocket (TCP) ──────────────────────────
#
# Бинарные сообщения от телефона: первый байт — тип, дальше данные
#   0x01 — JPEG-кадр
# Текстовые (JSON): ping, transform, state. В обратную сторону: info, pong, set.
#   state — телефон сообщает ПК, какие настройки и возможности камеры у него есть сейчас
#   set   — ПК просит телефон изменить настройку (так работает меню «управлять камерой»)

class Session:
    def __init__(self, ws):
        self.ws = ws
        self.v_pending = None          # последний принятый, но ещё не обработанный кадр
        self.v_event = asyncio.Event()
        self.remote = None             # адрес телефона
        self.streaming = False         # True, как только пришёл первый кадр видео
        self.state = None              # настройки камеры, которые прислал телефон
        self.state_ver = 0


class Bridge:
    def __init__(self, cam, decoder):
        self.cam = cam
        self.decode = decoder
        self.sessions = set()
        self._stats_task = None
        # один поток: кадры обрабатываются строго по порядку, и закрытие камеры не обгоняет push
        self._video_exec = ThreadPoolExecutor(max_workers=1, thread_name_prefix="video")
        self.st = dict(v_in=0, v_out=0, v_drop=0, proc=0.0, bytes=0)
        self.loop = None

    async def startup(self, app):
        self.loop = asyncio.get_running_loop()

    # ── доступ из терминального меню (оно живёт в отдельном потоке) ──

    def current(self):
        sessions = list(self.sessions)
        return sessions[-1] if sessions else None

    def streaming(self):
        s = self.current()
        return bool(s and s.streaming)

    def send_to_phone(self, payload):
        s = self.current()
        if s is None or self.loop is None:
            return False
        asyncio.run_coroutine_threadsafe(s.ws.send_str(json.dumps(payload)), self.loop)
        return True

    @staticmethod
    def clean_state(data):
        """Проверяет и приводит к безопасному виду настройки, присланные телефоном."""

        def items(raw, ident):
            out = []
            for it in raw if isinstance(raw, list) else []:
                if not isinstance(it, dict) or it.get("type") not in ("toggle", "select", "range"):
                    continue
                if not isinstance(it.get(ident), str):
                    continue
                c = {
                    "type": it["type"], ident: it[ident],
                    "label": str(it.get("label", it[ident]))[:60],
                    "group": "out" if it.get("group") == "out" else "cam",
                }
                if c["type"] == "toggle":
                    c["value"] = bool(it.get("value"))
                elif c["type"] == "select":
                    opts = [
                        [str(o[0]), str(o[1])[:60]]
                        for o in (it.get("options") or [])
                        if isinstance(o, (list, tuple)) and len(o) == 2
                    ]
                    if not opts:
                        continue
                    c["options"], c["value"] = opts, str(it.get("value"))
                else:
                    try:
                        lo, hi, val = float(it["min"]), float(it["max"]), float(it["value"])
                        st = float(it.get("step") or 0)
                    except (KeyError, TypeError, ValueError):
                        continue
                    if not hi > lo:
                        continue
                    c.update(min=lo, max=hi, value=val, step=st if st > 0 else (hi - lo) / 100)
                out.append(c)
            return out

        if not isinstance(data, dict):
            return {"fields": [], "caps": []}
        return {"fields": items(data.get("fields"), "id"), "caps": items(data.get("caps"), "key")}

    def info(self):
        return {
            "type": "info", "video": self.cam.label,
            "out": {"w": self.cam.w, "h": self.cam.h, "fps": self.cam.fps},
        }

    # ── видео ──

    def _process_video(self, data):
        img = self.decode(data)
        self.cam.push(av.VideoFrame.from_ndarray(img, format="rgb24"))

    async def _video_worker(self, sess):
        """Берёт только самый свежий кадр: если ПК не успевает, старые кадры выбрасываются,
        а не копятся в очереди (иначе задержка росла бы без конца)."""
        loop = asyncio.get_running_loop()
        last = 0.0
        while True:
            await sess.v_event.wait()
            sess.v_event.clear()
            data, sess.v_pending = sess.v_pending, None
            if data is None:
                continue
            now = loop.time()
            if now - last < 0.8 / self.cam.fps:  # не гоним в камеру быстрее её FPS
                continue
            last = now
            t0 = time.perf_counter()
            try:
                await loop.run_in_executor(self._video_exec, self._process_video, data)
                self.st["v_out"] += 1
            except Exception as e:  # noqa: BLE001
                log.error("Ошибка обработки кадра / виртуальной камеры: %s", e)
                await asyncio.sleep(1)
            self.st["proc"] += time.perf_counter() - t0

    # ── управление ──

    def _on_control(self, data):
        """Применяет настройки с телефона. Возвращает True, если изменился выход камеры."""
        self.cam.mirror = bool(data.get("mirror"))
        try:
            rot = int(data.get("rotate", 0)) % 360
        except (TypeError, ValueError):
            rot = 0
        self.cam.rotate = rot if rot in (0, 90, 180, 270) else 0

        out = data.get("out")
        if isinstance(out, dict):
            try:
                w = int(out["w"]) // 2 * 2  # размеры должны быть чётными
                h = int(out["h"]) // 2 * 2
                fps = int(out["fps"])
            except (KeyError, TypeError, ValueError):
                return False
            if 160 <= w <= 7680 and 120 <= h <= 4320 and 1 <= fps <= 120:
                return self.cam.reconfigure(w, h, fps)
        return False

    async def _stats_loop(self):
        prev = time.perf_counter()
        while self.sessions:
            await asyncio.sleep(5)
            now = time.perf_counter()
            dt, prev = now - prev, now
            s = self.st
            log.info(
                "видео: пришло %.0f к/с, в камеру %.0f к/с, пропущено устаревших %d, обработка %.1f мс/кадр"
                " | вход %.1f Мбит/с",
                s["v_in"] / dt, s["v_out"] / dt, s["v_drop"], s["proc"] / max(s["v_out"], 1) * 1000,
                s["bytes"] * 8 / dt / 1e6,
            )
            self.st = dict(v_in=0, v_out=0, v_drop=0, proc=0.0, bytes=0)
        self._stats_task = None

    def _close_if_idle(self):
        if not self.sessions:
            self.cam.close()  # чтобы в приложениях не висел «замёрзший» кадр

    # ── WebSocket ──

    async def ws_handler(self, request):
        ws = web.WebSocketResponse(max_msg_size=32 * 1024 * 1024, heartbeat=10, compress=False)
        await ws.prepare(request)
        loop = asyncio.get_running_loop()

        sess = Session(ws)
        sess.remote = request.remote
        old = list(self.sessions)  # одна активная сессия
        self.sessions.add(sess)
        for o in old:
            self.sessions.discard(o)
            await o.ws.close()
        if self._stats_task is None:
            self._stats_task = asyncio.ensure_future(self._stats_loop())
        log.info("Телефон подключён: %s", request.remote)

        worker = asyncio.ensure_future(self._video_worker(sess))
        try:
            await ws.send_str(json.dumps(self.info()))
            async for msg in ws:
                if msg.type == WSMsgType.BINARY:
                    d = msg.data
                    if len(d) < 2:
                        continue
                    self.st["bytes"] += len(d)
                    if d[0] == 1:
                        sess.streaming = True
                        self.st["v_in"] += 1
                        if sess.v_pending is not None:
                            self.st["v_drop"] += 1
                        sess.v_pending = memoryview(d)[1:]
                        sess.v_event.set()
                elif msg.type == WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except ValueError:
                        continue
                    kind = data.get("type") if isinstance(data, dict) else None
                    if kind == "ping":
                        await ws.send_str(json.dumps({"type": "pong", "t": data.get("t")}))
                    elif kind == "transform":
                        if self._on_control(data):
                            await ws.send_str(json.dumps(self.info()))
                    elif kind == "state":
                        sess.state = self.clean_state(data)
                        sess.state_ver += 1
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            worker.cancel()
            sess.streaming = False
            self.sessions.discard(sess)
            log.info("Телефон отключён")
            if not self.sessions:
                await loop.run_in_executor(self._video_exec, self._close_if_idle)
        return ws

    async def close_sessions(self, app):
        for s in list(self.sessions):
            await s.ws.close()

    async def shutdown(self, app):
        self.sessions.clear()
        self._video_exec.shutdown(wait=True, cancel_futures=True)
        self.cam.close()


async def index(request):
    return web.FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-store"})


# ───────────────────────────── QR-код ────────────────────────────────────────

def print_qr(url):
    """Рисует QR-код со ссылкой в консоли (нужен пакет qrcode: pip install qrcode)."""
    try:
        import qrcode
    except ImportError:
        print("  (Чтобы показать QR-код: pip install qrcode)\n")
        return
    qr = qrcode.QRCode(border=2, error_correction=qrcode.constants.ERROR_CORRECT_M)
    qr.add_data(url)
    qr.make(fit=True)
    m = qr.get_matrix()  # True = тёмный модуль; border уже включён
    if len(m) % 2:
        m.append([False] * len(m[0]))

    # QR читается как тёмные модули на светлом фоне. В тёмной консоли «светлым» рисуем
    # закрашенные символы, поэтому цвета инвертируем (свет = блок, тьма = пробел).
    enc = (getattr(sys.stdout, "encoding", None) or "ascii").lower()
    try:
        "█▀▄".encode(enc)
        full, top, bottom, empty = "█", "▀", "▄", " "
        rows = []
        for y in range(0, len(m), 2):
            line = ""
            for a, b in zip(m[y], m[y + 1]):
                line += empty if (a and b) else bottom if a else top if b else full
            rows.append(line)
    except (UnicodeEncodeError, LookupError):  # консоль без Unicode — рисуем «##»
        rows = ["".join("  " if v else "##" for v in row) for row in m]
    print("\n".join("      " + r for r in rows))
    print()


# ───────────────────────────── терминальное меню ─────────────────────────────
#
# Пока телефона нет, в консоли обычные логи. Когда телефон подключился и пошло видео,
# логи прячутся в буфер и появляется меню: 1 — управлять камерой, 2 — логи, 3 — адрес.

CLEAR = "\x1b[2J\x1b[H"


class TermLog(logging.Handler):
    """Хранит последние строки логов; печатает их сразу, только когда live=True."""

    def __init__(self):
        super().__init__()
        self.buf = collections.deque(maxlen=1000)
        self.live = True
        self.errors = 0  # ошибок с момента, когда логи были скрыты

    def emit(self, record):
        try:
            line = self.format(record)
        except Exception:  # noqa: BLE001
            return
        self.buf.append(line)  # emit вызывается под self.lock
        if record.levelno >= logging.ERROR:
            self.errors += 1
        if self.live:
            print(line, flush=True)


class Keys:
    """Чтение клавиш без Enter. get(timeout) -> 'up'/'down'/'left'/'right'/'enter'/'esc'/символ или None."""

    _WIN = {"H": "up", "P": "down", "K": "left", "M": "right", "I": "pgup", "Q": "pgdn"}
    _SEQ = re.compile(r"\x1b(?:\[[0-9;]*[A-Za-z~]|O[A-Za-z])")
    _POSIX = {
        "\x1b[A": "up", "\x1bOA": "up", "\x1b[B": "down", "\x1bOB": "down",
        "\x1b[C": "right", "\x1bOC": "right", "\x1b[D": "left", "\x1bOD": "left",
        "\x1b[5~": "pgup", "\x1b[6~": "pgdn",
    }

    def __init__(self):
        self.win = os.name == "nt"
        self.ok = bool(sys.stdin and sys.stdout and sys.stdin.isatty() and sys.stdout.isatty())
        self._old = None
        self._buf = ""

    def start(self):
        if not self.ok or self.win:
            return
        import termios
        import tty

        fd = sys.stdin.fileno()
        self._old = termios.tcgetattr(fd)
        tty.setcbreak(fd)  # Ctrl+C продолжает работать
        atexit.register(self.stop)

    def stop(self):
        if self._old is not None:
            import termios

            try:
                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self._old)
            except Exception:  # noqa: BLE001
                pass
            self._old = None

    def get(self, timeout=0.1):
        return self._get_win(timeout) if self.win else self._get_posix(timeout)

    def _get_win(self, timeout):
        import msvcrt

        end = time.monotonic() + timeout
        while True:
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch in ("\x00", "\xe0"):
                    return self._WIN.get(msvcrt.getwch(), "?")
                if ch == "\x03":  # Ctrl+C приходит как обычный символ
                    _thread.interrupt_main()
                    return "?"
                return {"\r": "enter", "\x1b": "esc"}.get(ch, ch)
            if time.monotonic() >= end:
                return None
            time.sleep(0.02)

    def _get_posix(self, timeout):
        import select

        if not self._buf:
            r, _, _ = select.select([sys.stdin], [], [], timeout)
            if not r:
                return None
            self._buf = os.read(sys.stdin.fileno(), 64).decode(errors="ignore")
            if not self._buf:
                return None
        b = self._buf
        if b[0] == "\x1b":
            m = self._SEQ.match(b)
            if m:
                self._buf = b[m.end():]
                return self._POSIX.get(m.group(), "?")
            self._buf = b[1:]
            return "esc"
        self._buf = b[1:]
        return "enter" if b[0] in "\r\n" else b[0]


class Console:
    SECTIONS = {
        ("field", "cam"): "Камера и видео",
        ("cap", "cam"): "Объектив, экспозиция, фокус",
        ("field", "out"): "Вывод на ПК",
    }

    def __init__(self, bridge, tlog, show_info):
        self.bridge, self.tlog, self.show_info = bridge, tlog, show_info
        self.keys = Keys()
        self._seen = -1e9  # когда в последний раз шёл видеопоток

    def start(self):
        if self.keys.ok:
            threading.Thread(target=self._run, name="console", daemon=True).start()

    def stop(self):
        self.keys.stop()

    def _alive(self):
        """Идёт ли видеопоток. Короткие обрывы связи (телефон сам переподключается) не считаются."""
        now = time.monotonic()
        if self.bridge.streaming():
            self._seen = now
            return True
        return now - self._seen < 3.0

    def _run(self):
        self.keys.start()
        while True:
            try:
                if self._alive():
                    self._menu()
                elif self.keys.get(0.2) in ("i", "ш"):  # ш — та же клавиша в русской раскладке
                    self.show_info(True)
            except Exception:  # noqa: BLE001
                self.tlog.live = True
                log.exception("Ошибка терминального меню")
                time.sleep(1)

    # ── главное меню ──

    def _menu(self):
        t = self.tlog
        t.live = False
        t.errors = 0
        self._draw_menu()
        last = t.errors
        while self._alive():
            k = self.keys.get(0.2)
            if k in ("1", "2", "3"):
                {"1": self._control, "2": self._logs, "3": self._info}[k]()
                t.errors = 0 if k == "2" else t.errors
                self._draw_menu()
                last = t.errors
            elif t.errors != last:
                self._draw_menu()
                last = t.errors
        t.live = True
        print(CLEAR + "  Телефон отключён. Жду подключения…\n", flush=True)
        self.show_info(False)

    def _draw_menu(self):
        s = self.bridge.current()
        cam = self.bridge.cam
        err = f" (ошибок: {self.tlog.errors})" if self.tlog.errors else ""
        print(
            CLEAR
            + "  Phone Cam Bridge\n"
            + "  ────────────────────────────────────────\n"
            + f"  Телефон подключён: {s.remote if s else '…'} · видеопоток идёт\n"
            + f"  Виртуальная камера: {cam.label}\n\n"
            + "    1 — управлять камерой\n"
            + f"    2 — логи{err}\n"
            + "    3 — адрес для подключения / QR\n\n"
            + "  Нажмите цифру. Выход из программы — Ctrl+C.",
            flush=True,
        )

    def _info(self):
        print(CLEAR, end="")
        self.show_info(True)
        print("  Любая клавиша — назад в меню", flush=True)
        while self._alive():
            if self.keys.get(0.2) is not None:
                break

    def _logs(self):
        t = self.tlog
        print(CLEAR + "  Логи (Enter / Esc — назад в меню)\n", flush=True)
        with t.lock:  # чтобы между выводом истории и «живым» режимом не потерялись строки
            for line in list(t.buf)[-200:]:
                print(line)
            sys.stdout.flush()
            t.live = True
        try:
            while self._alive():
                if self.keys.get(0.2) in ("enter", "esc", "q", "й"):
                    break
        finally:
            t.live = False

    # ── управление камерой ──

    @staticmethod
    def _ikey(src, it):
        return (src, it.get("id", it.get("key")))

    def _items(self, s):
        st = s.state if s else None
        if not st:
            return []
        cam = [("field", f) for f in st["fields"] if f["group"] != "out"]
        caps = [("cap", c) for c in st["caps"]]
        out = [("field", f) for f in st["fields"] if f["group"] == "out"]
        return cam + caps + out

    @staticmethod
    def _fmt(v, step):
        return f"{v:.0f}" if step >= 1 else f"{v:.2f}" if step >= 0.01 else f"{v:.3f}"

    def _value(self, it):
        t, v = it["type"], it["value"]
        if t == "toggle":
            return "● вкл" if v else "○ выкл"
        if t == "select":
            label = next((o[1] for o in it["options"] if o[0] == v), v)
            return f"◂ {label or '—'} ▸"
        lo, hi, st = it["min"], it["max"], it["step"]
        n = max(0, min(20, round(20 * (v - lo) / (hi - lo))))
        return f"{'█' * n}{'░' * (20 - n)} {self._fmt(v, st)}  ({self._fmt(lo, st)}–{self._fmt(hi, st)})"

    def _draw_control(self, s, items, sel):
        cols, rows = shutil.get_terminal_size((100, 30))
        width = max(40, cols - 1)
        lines, sec, sel_row = [], None, 0
        for i, (src, it) in enumerate(items):
            name = self.SECTIONS[(src, "out" if it["group"] == "out" else "cam")]
            if name != sec:
                sec = name
                lines.append(("\x1b[1m" + f"  {name}"[:width] + "\x1b[0m"))
            row = f" {'▸' if i == sel else ' '} {it['label'][:26]:<26} {self._value(it)}"[:width]
            if i == sel:
                sel_row = len(lines)
                row = "\x1b[7m" + row + "\x1b[0m"
            lines.append(row)
        if not items:
            lines.append("  Жду данные от телефона…")

        body_h = max(5, rows - 5)
        top = max(0, min(sel_row - body_h // 2, len(lines) - body_h))
        view = lines[top: top + body_h]
        head = [
            f"  Управление камерой телефона · {s.remote if s else '…'}",
            "  " + "─" * min(width - 2, 70),
        ]
        foot = [
            "",
            "  ↑↓ выбор · ←→ изменить · PgUp/PgDn ±10 шагов · Enter — переключить · Esc — назад"[:width],
        ]
        out = "\x1b[H" + "".join(x + "\x1b[K\n" for x in head + view + foot) + "\x1b[J"
        sys.stdout.write(out)
        sys.stdout.flush()

    def _apply(self, src, it, key):
        t = it["type"]
        back = key in ("left", "pgdn")
        if t == "toggle":
            new = not it["value"]
        elif t == "select":
            vals = [o[0] for o in it["options"]]
            i = vals.index(it["value"]) if it["value"] in vals else 0
            new = vals[(i + (-1 if back else 1)) % len(vals)]
        else:
            if key == "enter":
                return
            lo, hi, st = it["min"], it["max"], it["step"]
            d = (-1 if back else 1) * (10 if key in ("pgup", "pgdn") else 1)
            new = min(hi, max(lo, it["value"] + d * st))
            new = min(hi, max(lo, round(lo + round((new - lo) / st) * st, 6)))
            if new == it["value"]:
                return
        it["value"] = new  # сразу показываем новое значение; телефон потом пришлёт актуальное
        if src == "field":
            payload = {"type": "set", "kind": "field", "id": it["id"], "value": new}
        else:
            payload = {"type": "set", "kind": "cap", "key": it["key"], "value": new}
        self.bridge.send_to_phone(payload)

    def _control(self):
        sys.stdout.write(CLEAR + "\x1b[?25l")  # прячем курсор
        sel, sel_key, seen_ver, dirty = 0, None, -1, True
        try:
            while self._alive():
                s = self.bridge.current()
                ver = s.state_ver if s else -1
                if ver != seen_ver:
                    seen_ver, dirty = ver, True
                items = self._items(s)
                for i, (src, it) in enumerate(items):
                    if self._ikey(src, it) == sel_key:
                        sel = i
                        break
                sel = max(0, min(sel, len(items) - 1))
                if dirty:
                    self._draw_control(s, items, sel)
                    dirty = False
                k = self.keys.get(0.1)
                if k is None:
                    continue
                if k in ("esc", "q", "й", "0"):
                    return
                if not items:
                    continue
                if k == "up":
                    sel = (sel - 1) % len(items)
                elif k == "down":
                    sel = (sel + 1) % len(items)
                elif k in ("left", "right", "pgup", "pgdn", "enter", " "):
                    self._apply(*items[sel], "right" if k == " " else k)
                sel_key = self._ikey(*items[sel])
                dirty = True
        finally:
            sys.stdout.write("\x1b[?25h")
            sys.stdout.flush()


# ───────────────────────────── запуск ────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Телефон как виртуальная камера для ПК")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--width", type=int, default=1920, help="ширина виртуальной камеры")
    p.add_argument("--height", type=int, default=1080, help="высота виртуальной камеры")
    p.add_argument("--fps", type=int, default=30, help="FPS виртуальной камеры")
    p.add_argument("--backend", default=None, help="бэкенд pyvirtualcam (obs, unitycapture, v4l2loopback…)")
    p.add_argument("--cam-device", default=None, help="устройство виртуальной камеры (например /dev/video10)")
    p.add_argument("--no-qr", action="store_true", help="не показывать QR-код со ссылкой")
    p.add_argument("--no-menu", action="store_true", help="не показывать терминальное меню (только логи)")
    p.add_argument("--hidden", action="store_true", help="стартовать скрытым в трее")
    return p.parse_args()


def main():
    args = parse_args()
    try:
        sys.stdout.reconfigure(errors="replace")  # чтобы редкие символы не роняли вывод
    except (AttributeError, ValueError):
        pass
    if os.name == "nt":
        os.system("")  # включает ANSI-последовательности в консоли Windows

    tlog = TermLog()
    tlog.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    logging.basicConfig(level=logging.INFO, handlers=[tlog])

    decoder, decoder_name = make_jpeg_decoder()
    cam = VirtualCamSink(args.width, args.height, args.fps, args.backend, args.cam_device)
    bridge = Bridge(cam, decoder)

    # ── HTTPS ──
    ips = local_ips()
    cert, key = ensure_cert(ips)
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(cert, key)

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", bridge.ws_handler)
    app.on_startup.append(bridge.startup)
    app.on_shutdown.append(bridge.close_sessions)
    app.on_cleanup.append(bridge.shutdown)

    url = f"https://{ips[0] if ips else 'localhost'}:{args.port}"

    def show_info(full=True):
        """Информация по подключению: адрес, запасные адреса, QR-код."""
        print("\n  Откройте на телефоне (тот же Wi-Fi):\n")
        print(f"      {url}\n")
        if len(ips) > 1:
            print("  Если не открывается, попробуйте другой адрес ПК:")
            for ip in ips[1:]:
                print(f"      https://{ip}:{args.port}")
            print()
        if full:
            if not args.no_qr:
                print("  Или отсканируйте QR-код камерой телефона:")
                print_qr(url)
            print("  Браузер предупредит о сертификате — это нормально: «Дополнительно» → «Всё равно перейти».\n")
            print(f"  Транспорт: WebSocket поверх TCP, видео JPEG (декодер: {decoder_name})")
            print(f"  Видео -> виртуальная камера {cam.label}\n")
        sys.stdout.flush()

    console = None
    if not args.no_menu:
        console = Console(bridge, tlog, show_info)
    show_info(True)
    if console and console.keys.ok:
        print("  Когда телефон подключится и пойдёт видео, здесь появится меню:")
        print("  1 — управлять камерой, 2 — логи. Клавиша i — показать адрес ещё раз.\n")
        console.start()

    if os.name == "nt":
        from tray import start_tray
        start_tray(hidden=args.hidden)

    try:
        web.run_app(app, host=args.host, port=args.port, ssl_context=ssl_ctx, print=None, access_log=None)
    finally:
        if console:
            console.stop()


if __name__ == "__main__":
    main()
