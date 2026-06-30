"""
Lance tous les consumers (01 -> 04) en parallele dans des sous-processus.
Le detecteur d'anomalies utilise toujours la version V4.
Ctrl+C arrete proprement tous les processus.
Les demarrages, arrets et erreurs sont logges dans consumers/consumers.log.

Chaque consumer a une ligne de statut volume fixe qui se met a jour en place
dans la console (panneau live), pendant que les alertes/erreurs/messages de
demarrage continuent de defiler normalement au-dessus.

Usage:
    python run_consumers.py
    python run_consumers.py --sensitivity high
"""

import subprocess
import sys
import signal
import argparse
import logging
import os
import re
import threading
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CONSUMERS_DIR = os.path.join(PROJECT_DIR, "consumers")
LOG_FILE = os.path.join(CONSUMERS_DIR, "consumers.log")

# Detecte automatiquement le venv s'il existe
VENV_PYTHON = os.path.join(PROJECT_DIR, ".venv", "Scripts", "python.exe")
if not os.path.isfile(VENV_PYTHON):
    VENV_PYTHON = os.path.join(PROJECT_DIR, ".venv", "bin", "python")
if not os.path.isfile(VENV_PYTHON):
    VENV_PYTHON = sys.executable

BASE_CONSUMERS = [
    ("01_coinbase_cleaner",      "01_coinbase_cleaner.py"),
    ("01bis_binance_cleaner",    "01bis_binance_cleaner.py"),
    ("02_coinbase_mongo",        "02_coinbase_mongo_consumer.py"),
    # ("03_btc_analytics",         "03_btc_analytics.py"),
]

DETECTOR = ("04_anomalie_v4", "04_detection_anomalie_v4.py")

COLORS = {
    "01_coinbase_cleaner":    "cyan",
    "01bis_binance_cleaner":  "magenta",
    "02_coinbase_mongo":      "yellow",
    "03_btc_analytics":       "green",
    "04_anomalie_v4":         "bright_red",
}

# Patterns pour detecter les erreurs dans la sortie des consumers
ERROR_PATTERNS = re.compile(
    r"(Traceback \(most recent|"
    r"\[ERROR\]|\[CRITICAL\]|"
    r"ModuleNotFoundError|ConnectionRefusedError|"
    r"UnicodeEncodeError|BulkWriteError|"
    r"Impossible de|"
    r"^\s+raise )"
)

# Pattern pour detecter les rapports de volume periodiques (toutes les 10s par consumer)
VOLUME_PATTERN = re.compile(r"messages traites")

# Logger fichier
file_logger = logging.getLogger("consumers_log")
file_logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
file_logger.addHandler(fh)


def main():
    parser = argparse.ArgumentParser(description="Lance tous les consumers Kafka")
    parser.add_argument("--sensitivity", choices=["low", "medium", "high"], default="medium")
    args = parser.parse_args()

    detector_name, detector_file = DETECTOR
    consumers = list(BASE_CONSUMERS) + [(detector_name, detector_file)]

    console = Console()

    file_logger.info("=" * 60)
    file_logger.info(f"DEMARRAGE - detecteur: v4, sensibilite: {args.sensitivity}")
    file_logger.info(f"Consumers: {[name for name, _ in consumers]}")
    file_logger.info("=" * 60)

    console.print(f"[bold]{'=' * 60}[/bold]")
    console.print(f"[bold]  Lancement de {len(consumers)} consumers (detecteur: v4)[/bold]")
    console.print(f"[bold]  Sensibilite: {args.sensitivity}[/bold]")
    console.print(f"[bold]  Log: {LOG_FILE}[/bold]")
    console.print(f"[bold]{'=' * 60}[/bold]\n")

    processes = {}
    volume_status = {name: "en attente..." for name, _ in consumers}
    status_lock = threading.Lock()

    def build_table():
        table = Table(show_header=True, header_style="bold")
        table.add_column("Consumer")
        table.add_column("Volume")
        for name, _ in consumers:
            style = COLORS.get(name, "white")
            table.add_row(f"[{style}]{name}[/{style}]", volume_status.get(name, "..."))
        return table

    for name, filename in consumers:
        filepath = os.path.join(CONSUMERS_DIR, filename)
        cmd = [VENV_PYTHON, filepath]

        if name.startswith("04_anomalie"):
            cmd += ["--sensitivity", args.sensitivity]

        style = COLORS.get(name, "white")
        console.print(f"[{style}][START] {name} -> {filename}[/{style}]")
        file_logger.info(f"[START] {name} -> {filename}")

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        processes[name] = proc

    live = Live(build_table(), console=console, refresh_per_second=4)
    live.start()

    def shutdown(signum=None, frame=None):
        live.stop()
        console.print("\n[bold]\\[SHUTDOWN] Arret de tous les consumers...[/bold]")
        file_logger.info("[SHUTDOWN] Arret demande par l'utilisateur")
        for name, proc in processes.items():
            if proc.poll() is None:
                proc.terminate()
                console.print(f"  -> {name} termine")
                file_logger.info(f"[STOP] {name} termine")
        for proc in processes.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        file_logger.info("[DONE] Tous les consumers sont arretes")
        console.print("[bold]\\[DONE] Tous les consumers sont arretes.[/bold]")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    def stream_output(name, proc):
        style = COLORS.get(name, "white")
        error_buffer = []
        try:
            for line in proc.stdout:
                if VOLUME_PATTERN.search(line):
                    stripped = line.strip()
                    volume_text = re.sub(rf"^\[{re.escape(name)}\]\s*", "", stripped)
                    with status_lock:
                        volume_status[name] = volume_text
                        live.update(build_table())
                    file_logger.info(f"[{name}] {stripped}")
                    continue

                console.print(Text(f"[{name}] ", style=style), Text(line.rstrip("\n")), sep="", soft_wrap=True)

                if ERROR_PATTERNS.search(line):
                    error_buffer.append(line.rstrip())
                    if len(error_buffer) > 10:
                        error_buffer.pop(0)
                elif error_buffer:
                    error_buffer.append(line.rstrip())
                    if len(error_buffer) > 10:
                        error_buffer.pop(0)
                    if not line.startswith(" ") and not line.startswith("\t"):
                        for err_line in error_buffer:
                            file_logger.error(f"[{name}] {err_line}")
                        error_buffer.clear()
        except Exception:
            pass

        exit_code = proc.wait()
        # Code 143 (SIGTERM) et -15 sont des arrets normaux via shutdown
        if exit_code in (0, 143, -15):
            file_logger.info(f"[EXIT] {name} termine normalement (code {exit_code})")
        else:
            file_logger.error(f"[CRASH] {name} termine avec code {exit_code}")
            if error_buffer:
                for err_line in error_buffer:
                    file_logger.error(f"[{name}] {err_line}")

    threads = []
    for name, proc in processes.items():
        t = threading.Thread(target=stream_output, args=(name, proc), daemon=True)
        t.start()
        threads.append(t)

    try:
        for proc in processes.values():
            proc.wait()
    except KeyboardInterrupt:
        shutdown()
    finally:
        live.stop()


if __name__ == "__main__":
    main()
