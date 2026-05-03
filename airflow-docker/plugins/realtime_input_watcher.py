import json
import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


logging.basicConfig(
    level=os.getenv('REALTIME_WATCHER_LOG_LEVEL', 'INFO'),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)

logger = logging.getLogger(__name__)

WATCH_FOLDER = Path(os.getenv('REALTIME_WATCH_FOLDER', '/opt/airflow/data_input_realtime'))
DAG_ID = os.getenv('REALTIME_WATCHER_DAG_ID', 'realtime_ecommerce_processing')
POLL_INTERVAL_SECONDS = int(os.getenv('REALTIME_WATCH_INTERVAL_SECONDS', '5'))
MIN_FILE_AGE_SECONDS = int(os.getenv('REALTIME_WATCH_MIN_FILE_AGE_SECONDS', '2'))


def file_signature(path: Path) -> tuple[int, int]:
    stat_result = path.stat()
    return stat_result.st_size, stat_result.st_mtime_ns


def trigger_dag(input_file: Path) -> None:
    run_id = f"auto_watch__{input_file.stem}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    conf = json.dumps({'input_file': input_file.name})

    logger.info('Triggering DAG %s for %s', DAG_ID, input_file.name)
    subprocess.run(
        [
            'airflow',
            'dags',
            'trigger',
            DAG_ID,
            '--run-id',
            run_id,
            '--conf',
            conf,
        ],
        check=True,
        text=True,
    )


def main() -> None:
    logger.info('Realtime watcher started for %s', WATCH_FOLDER)

    WATCH_FOLDER.mkdir(parents=True, exist_ok=True)
    seen_signatures: dict[str, tuple[int, int]] = {}
    triggered_signatures: dict[str, tuple[int, int]] = {}

    for existing_file in WATCH_FOLDER.glob('*.csv'):
        if existing_file.is_file():
            seen_signatures[existing_file.name] = file_signature(existing_file)

    while True:
        try:
            current_files = {path.name: path for path in WATCH_FOLDER.glob('*.csv') if path.is_file()}

            for file_name, file_path in current_files.items():
                signature = file_signature(file_path)
                seen_signature = seen_signatures.get(file_name)
                age_seconds = time.time() - file_path.stat().st_mtime

                if seen_signature == signature and triggered_signatures.get(file_name) == signature:
                    continue

                if age_seconds < MIN_FILE_AGE_SECONDS:
                    continue

                if triggered_signatures.get(file_name) == signature:
                    continue

                if seen_signature != signature:
                    logger.info('Detected new or changed file: %s', file_name)

                trigger_dag(file_path)
                seen_signatures[file_name] = signature
                triggered_signatures[file_name] = signature

            removed_files = set(seen_signatures) - set(current_files)
            for removed_file in removed_files:
                seen_signatures.pop(removed_file, None)
                triggered_signatures.pop(removed_file, None)

        except subprocess.CalledProcessError as exc:
            logger.exception('Failed to trigger DAG: %s', exc)
        except Exception:
            logger.exception('Unexpected watcher error')

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()