# Data Engineer Agent — React UI

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Start development server (make sure your FastAPI is running on port 8004)
npm start

# 3. Open http://localhost:3000
```

## Project Structure

```
src/
  components/
    Navbar.jsx        — Top navigation with server status indicator
    UI.jsx            — Reusable components (Card, Btn, Badge, Table, etc.)
  hooks/
    useApi.js         — API call helper + base URL management
    useToast.js       — Toast notification context
  pages/
    Dashboard.jsx     — Home with stats, quick actions, client list
    Ingest.jsx        — Run orchestration (ADLS or API source)
    Pipeline.jsx      — Run pipeline per dataset with metrics
    Clients.jsx       — Browse clients and their datasets
    ApiSources.jsx    — Register and manage REST API sources
    DataQuality.jsx   — DQ rules + AI suggestions
    Browse.jsx        — Browse ADLS folders or API endpoints
  App.jsx             — Router setup
  index.js            — Entry point
  index.css           — Global styles + CSS variables
```

## Configuration

The API base URL defaults to `http://127.0.0.1:8004` and is stored in `localStorage`.
Change it from the top-right input in the navbar.

The `"proxy": "http://127.0.0.1:8004"` in `package.json` handles CORS in development.

## Requirements

- Node.js 16+
- FastAPI backend running (`uvicorn main:app --reload --port 8004`)
- PostgreSQL database running

## Pages

| Page | Route | Description |
|------|-------|-------------|
| Home | `/` | Live stats, quick actions, pipeline layer overview |
| Ingest | `/ingest` | Run orchestration (ADLS or API), live progress stages |
| Pipeline | `/pipeline` | Select client/dataset, sync, run pipeline, view metrics |
| Clients | `/clients` | Browse clients, view datasets, navigate to DQ/Pipeline |
| API Sources | `/apis` | Register REST APIs, quick-fill examples, manage configs |
| Data Quality | `/dq` | Load DQ rules, run AI suggestions, view column rules |
| Browse | `/browse` | List files or browse folders from ADLS or API |
