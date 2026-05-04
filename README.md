# README - Data Engineering Project

This guide explains how to start and use the project for evaluation.

## 1. What This Project Contains

- Part 1: Batch pipeline (`yellow_taxi_batch_processing`)
- Part 2: Real-time pipeline (`realtime_ecommerce_processing`)
- Stack: Python (pandas), Apache Airflow, Azure Blob Storage, Docker

## 2. Prerequisites

- Docker Desktop running
- PowerShell terminal
- Port `8080` available
- Project opened at:
  - `airflow-docker/`

Optional for Azure write verification:

- Valid `AZURE_STORAGE_CONNECTION_STRING` in `.env`

## 3. Start The Project

Open a terminal in `airflow-docker/` and run:

```powershell
docker compose up -d
```

Wait until services are healthy (about 1-3 minutes on first run).

Open Airflow UI:

- URL: `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

Note: If credentials were changed in `.env`, use those values.

## 4. Real-Time Part (Part 2) - How To Test

### 4.1 Input folder monitored automatically

The watcher service monitors:

- `airflow-docker/data_input_realtime/`

Accepted type currently configured:

- `.csv`

### 4.2 Trigger a run

Option A: Generate a test file with the provided script (from project root):

```powershell
& "airflow-docker/.venv/Scripts/python.exe" "airflow-docker/generate_ecommerce_data.py"
```

Option B: Manually copy a `.csv` file into:

- `airflow-docker/data_input_realtime/`

The watcher triggers `realtime_ecommerce_processing` automatically.

### 4.3 Check the DAG in Airflow

In Airflow UI:

1. Open DAG `realtime_ecommerce_processing`
2. Open the newest run (run id starts with `auto_watch__`)
3. Verify tasks in order:
   - `wait_for_files`
   - `detect_files`
   - `read_data`
   - `validate_data`
   - `process_data`
   - `backup_validate`
   - `write_data`
   - `move_files`

Expected behavior:

- Bad file: run fails for that file only, file moves to `failed/`
- Good file: run succeeds, file moves to `succeeded/`
- Other files continue independently in their own runs

## 5. Where To Check Results

### 5.1 Input file routing

- Failed inputs:
  - `airflow-docker/data_input_realtime/failed/`
- Successful inputs:
  - `airflow-docker/data_input_realtime/succeeded/`

### 5.2 Local processed outputs

- `airflow-docker/output/realtime/`
- Naming pattern:
  - `output<original_input_filename>.csv`

### 5.3 Airflow logs

- `airflow-docker/logs/`

### 5.4 Azure outputs (if configured)

- Container: `processed-data`
- Uploaded from `write_data` task

## 6. Batch Part (Part 1) - Optional Quick Test

If needed, trigger `yellow_taxi_batch_processing` from Airflow UI manually.

Input location used by batch pipeline:

- `airflow-docker/data/`

## 7. Stop The Project

From `airflow-docker/`:

```powershell
docker compose down
```

If you also want to remove volumes (clean reset):

```powershell
docker compose down -v
```

## 8. Troubleshooting

### Airflow UI not opening

- Confirm Docker Desktop is running
- Confirm containers are up:

```powershell
docker compose ps
```

### DAG not auto-triggering after placing file

- Confirm watcher service is running (`realtime-file-watcher`)
- Confirm file extension is `.csv`
- Confirm file is placed in root of `data_input_realtime/` (not inside `failed/` or `succeeded/`)

### Azure write skipped

- Check if `AZURE_STORAGE_CONNECTION_STRING` is defined in `.env`
- Pipeline still writes local output even when Azure is not configured
