# DEA Backend Local Setup

Use a virtual environment inside this project directory. Do not reuse an
environment from another project such as `pipeline-intelligence-engine\.venv`.

## Windows PowerShell

```powershell
cd "C:\Users\Ankush.pille\medallion-data-pipeline\Archive 25"
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
uvicorn main:app --reload --port 8001
```

If PowerShell blocks activation, run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

If `python -m venv .venv` fails during `ensurepip` with a Temp directory
permission error, create the environment with Python 3.12 and point Temp at a
project-local folder for the bootstrap:

```powershell
cd "C:\Users\Ankush.pille\medallion-data-pipeline\Archive 25"
py -3.12 -m venv .venv --without-pip
New-Item -ItemType Directory -Force -Path .tmp
$env:TEMP=(Resolve-Path .tmp).Path
$env:TMP=$env:TEMP
py -3.12 -m pip --python .\.venv install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Verify The Correct Environment

Run these from `Archive 25` after activation:

```powershell
where python
where pip
python -c "import sys; print(sys.executable)"
pip show fastapi
```

Expected paths should start with:

```text
C:\Users\Ankush.pille\medallion-data-pipeline\Archive 25\.venv\
```

The backend should be available at:

```text
http://127.0.0.1:8001
```

## Notes

- Cloud secrets belong only in `.env` or request-time credentials for scans.
- Do not commit `.env`, AWS keys, Azure secrets, Databricks tokens, or OpenAI keys.
- MCP is listed in `requirements.txt` for API bridge flows, but direct local S3,
  ADLS, and LOCAL connector startup is guarded so the backend can still import
  cleanly if MCP is not present.
