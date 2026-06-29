"""
Lance tous les consumers (01 -> 04) en parallele dans des sous-processus.
Ctrl+C arrete proprement tous les processus.
Les demarrages, arrets et erreurs sont logges dans consumers/consumers.log.

Usage:
    python run_consumers.py              # lance avec detecteur V2
    python run_consumers.py --v3         # lance avec detecteur V3
    python run_consumers.py --sensitivity high --v3
"""

import subprocess
import sys
import signal
import argparse
import logging
import os
import re
from datetime import datetime

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

DETECTOR_VERSIONS = {
    "v2": ("04_anomalie_v2",  "04_detection_anomalie_v2.py"),
    "v3": ("04_anomalie_v3",  "04_detection_anomalie_v3.py"),
}

COLORS = {
    "01_coinbase_cleaner":    "\033[36m",   # cyan
    "01bis_binance_cleaner":  "\033[35m",   # magenta
    "02_coinbase_mongo":      "\033[33m",   # jaune
    "03_btc_analytics":       "\033[32m",   # vert
    "04_anomalie_v2":         "\033[91m",
    "04_anomalie_v3":         "\033[91m",
}
RESET = "\033[0m"

# Patterns pour detecter les erreurs dans la sortie des consumers
ERROR_PATTERNS = re.compile(
    r"(Traceback \(most recent|"
    r"\[ERROR\]|\[CRITICAL\]|"
    r"ModuleNotFoundError|ConnectionRefusedError|"
    r"UnicodeEncodeError|BulkWriteError|"
    r"Impossible de|"
    r"^\s+raise )"
)

# Logger fichier
file_logger = logging.getLogger("consumers_log")
file_logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
file_logger.addHandler(fh)


def main():
    parser = argparse.ArgumentParser(description="Lance tous les consumers Kafka")
    parser.add_argument("--v3", action="store_true", help="Utiliser le detecteur V3 (defaut: V2)")
    parser.add_argument("--sensitivity", choices=["low", "medium", "high"], default="medium")
    args = parser.parse_args()

    if args.v3:
        version = "v3"
    else:
        version = "v2"

    detector_name, detector_file = DETECTOR_VERSIONS[version]
    consumers = list(BASE_CONSUMERS) + [(detector_name, detector_file)]

    file_logger.info("=" * 60)
    file_logger.info(f"DEMARRAGE - detecteur: {version}, sensibilite: {args.sensitivity}")
    file_logger.info(f"Consumers: {[name for name, _ in consumers]}")
    file_logger.info("=" * 60)

    print(f"\033[1m{'=' * 60}")
    print(f"  Lancement de {len(consumers)} consumers (detecteur: {version})")
    print(f"  Sensibilite: {args.sensitivity}")
    print(f"  Log: {LOG_FILE}")
    print(f"{'=' * 60}{RESET}\n")

    processes = {}

    for name, filename in consumers:
        filepath = os.path.join(CONSUMERS_DIR, filename)
        cmd = [VENV_PYTHON, filepath]

        if name.startswith("04_anomalie"):
            cmd += ["--sensitivity", args.sensitivity]

        color = COLORS.get(name, "")
        print(f"{color}[START] {name} -> {filename}{RESET}")
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

    def shutdown(signum=None, frame=None):
        print(f"\n\033[1m[SHUTDOWN] Arret de tous les consumers...{RESET}")
        file_logger.info("[SHUTDOWN] Arret demande par l'utilisateur")
        for name, proc in processes.items():
            if proc.poll() is None:
                proc.terminate()
                print(f"  -> {name} termine")
                file_logger.info(f"[STOP] {name} termine")
        for proc in processes.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        file_logger.info("[DONE] Tous les consumers sont arretes")
        print(f"\033[1m[DONE] Tous les consumers sont arretes.{RESET}")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    import threading

    def stream_output(name, proc):
        color = COLORS.get(name, "")
        error_buffer = []
        try:
            for line in proc.stdout:
                print(f"{color}[{name}]{RESET} {line}", end="")

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


if __name__ == "__main__":
    main()
