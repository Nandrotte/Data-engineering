# Git LFS Setup

Use Git LFS for the large data files in this project.

## What is tracked

- `airflow-docker/data_input_realtime/*.csv`
- `airflow-docker/output/realtime/*.csv`
- `airflow-docker/output/csv/*.csv`
- `airflow-docker/output/parquet/*.parquet`
- `airflow-docker/data/*.parquet`

## One-time setup

Run these commands from the project root:

```powershell
git lfs install
git add .gitattributes
git add airflow-docker/data_input_realtime/*.csv
git add airflow-docker/output/realtime/*.csv
git add airflow-docker/output/csv/*.csv
git add airflow-docker/output/parquet/*.parquet
git add airflow-docker/data/*.parquet
git commit -m "Track large data files with Git LFS"
git push
```

## If files were already committed without LFS

If large files are already in Git history, rewrite them into LFS:

```powershell
git lfs migrate import --include="airflow-docker/data_input_realtime/*.csv,airflow-docker/output/realtime/*.csv,airflow-docker/output/csv/*.csv,airflow-docker/output/parquet/*.parquet,airflow-docker/data/*.parquet"
git push --force-with-lease
```

## Check

To verify tracking:

```powershell
git lfs ls-files
```

Notes:

- CSV files can be large even though they are text.
- Parquet files should always go through LFS here.
- Other generated files like logs should stay out of Git.
