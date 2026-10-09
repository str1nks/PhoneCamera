import ctypes, os, threading, _thread
import pystray
from pathlib import Path
from PIL import Image, ImageDraw
import sys

if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).resolve().parent

def _icon_image():
    icon_path = BASE_DIR / "icon.ico"
    if icon_path.exists():
        return Image.open(icon_path)
    
    # Резервный вариант, если файл icon.ico не найден
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.ellipse((4, 4, 60, 60), fill=(30, 140, 255, 255))
    d.ellipse((22, 22, 42, 42), fill=(255, 255, 255, 255))
    return img

k32, u32 = ctypes.windll.kernel32, ctypes.windll.user32

def _icon_image():
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.ellipse((4, 4, 60, 60), fill=(30, 140, 255, 255))
    d.ellipse((22, 22, 42, 42), fill=(255, 255, 255, 255))
    return img


def start_tray(hidden=False):
    con = k32.GetConsoleWindow()
    hwnd = u32.GetAncestor(con, 3) or con      # 3 = корневое окно (нужно для Windows Terminal)
    u32.DeleteMenu(u32.GetSystemMenu(con, False), 0xF060, 0)  # убираем крестик: он убил бы сервер
    state = {"visible": not hidden}

    def toggle(icon=None, item=None):
        state["visible"] = not state["visible"]
        u32.ShowWindow(hwnd, 5 if state["visible"] else 0)  # 5 = показать, 0 = скрыть
        if state["visible"]:
            u32.SetForegroundWindow(hwnd)

    def quit_app(icon, item):
        icon.stop()
        _thread.interrupt_main()                          # штатное завершение сервера
        threading.Timer(3, lambda: os._exit(0)).start()   # страховка, если не завершится

    menu = pystray.Menu(
        pystray.MenuItem("Показать / скрыть окно", toggle, default=True),  # default = клик по иконке
        pystray.MenuItem("Выход", quit_app),
    )
    icon = pystray.Icon("phonecam", _icon_image(), "Phone Cam Bridge", menu)
    if hidden:
        u32.ShowWindow(hwnd, 0)
    threading.Thread(target=icon.run, daemon=True).start()