# utils/console.py
from __future__ import annotations

from colorama import Fore, Style, just_fix_windows_console

# Safe to call multiple times; fixes Windows terminal ANSI handling.
just_fix_windows_console()

RESET = Style.RESET_ALL

PALETTE = {
    "grey": Fore.LIGHTBLACK_EX,
    "red": Fore.LIGHTRED_EX,
    "green": Fore.LIGHTGREEN_EX,
    "yellow": Fore.LIGHTYELLOW_EX,
    "blue": Fore.LIGHTBLUE_EX,
    "magenta": Fore.LIGHTMAGENTA_EX,
    "cyan": Fore.LIGHTCYAN_EX,
    "white": Fore.WHITE,
}

def c(text: str, color: str | None = None, *, bold: bool = False) -> str:
    if not color:
        return text
    code = PALETTE.get(color.lower(), "")
    if not code:
        return text
    b = Style.BRIGHT if bold else ""
    return f"{b}{code}{text}{RESET}"

def cprint(text: str, color: str | None = None, *, bold: bool = False, **print_kwargs) -> None:
    print(c(text, color, bold=bold), **print_kwargs)
