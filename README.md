# Olympus Connect

A data harmonization platform for healthcare that unifies multi-source clinical data into a single database. It combines intelligent column mapping, anomaly detection, and AI-powered analysis to streamline the ETL process.

Built as a submission for **Start Hack 2026**.

## Modules

The platform is organized around three core modules, named after Greek gods:

### Hermes — Data Import & Mapping

Upload CSV, Excel, or PDF files and let the engine automatically detect formats, map columns to the target schema, and flag anomalies. Review mappings, resolve issues (accept auto-fix, manually edit, ignore, or email stakeholders), preview the generated SQL, and import into the database.

### Apollo — Dashboard & Natural Language Queries

Overview dashboard with KPIs (total records, data sources, mapping coverage, open anomalies), source composition treemap, and anomaly breakdown charts. Includes an AI chat interface that translates natural language questions into SQL queries and renders the results as tables or charts.

### Athena — Quality & Risk Analysis

Analyzes imported data for quality issues and generates LLM-powered insights. Shows error/warning trends across import jobs, auto-fix rates, predicted risks with likelihood/impact scores, and table population status.

### Additional Features

- **Explorer** — Browse database tables, inspect schemas, preview rows, and run ad-hoc SQL queries with CSV export.
- **Mapping Rules** — Persistent column mapping overrides from manual corrections, searchable and editable.
- **Settings** — Database connection, LLM provider configuration (OpenRouter or local Ollama), email signature, language toggle (DE/EN), and danger zone operations.
- **Internationalization** — Full German and English UI with 600+ translation keys.

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| Frontend | React 18, TypeScript, Vite, Tailwind CSS, shadcn/ui (Radix), React Router, React Query, Recharts |
| Backend | Node.js, Express, TypeScript (tsx) |
| Database | Microsoft SQL Server (Tedious) |
| AI/LLM | OpenRouter API or local Ollama |
| File Parsing | xlsx, pdf-parse, Multer |
| Email | Nodemailer |
| Testing | Vitest, Playwright |

## Project Structure

```
engine/             Express backend — mapping engine, validators, DB loader, API
  src/
    mapping/        Column mapping, renaming, code translation, pivoting
    validators/     Data quality checks
    db/             Job tracking and data import
    server.ts       API server entry point
    cli.ts          CLI for offline analysis/import

frontend/           React SPA
  src/
    pages/          Apollo, Athena, Hermes, Explorer, Settings, MappingOverrides
    components/     Shared UI components (shadcn/ui)
    hooks/          Custom hooks (chat, i18n, mobile detection)
    lib/            API client, utilities, schema labels
```

## Getting Started

### Prerequisites

- Node.js 18+
- Microsoft SQL Server instance
- (Optional) OpenRouter API key or local Ollama for AI features

### Installation

```bash
# Install backend dependencies
cd engine
npm install

# Install frontend dependencies
cd ../frontend
npm install
```

### Running

```bash
# Start the backend (port 3001)
cd engine
npm run serve

# Start the frontend dev server (port 8080, proxies API to engine)
cd frontend
npm run dev
```

### Configuration

In the Settings page, configure:

1. **Database** — SQL Server host, port, username, and password. Use "Test Connection" to verify.
2. **LLM** — OpenRouter API key and model (defaults to `qwen/qwen3-32b`), or leave blank to use local Ollama.
3. **Language** — Switch between German and English.

## Data Flow

1. **Upload** — Files are sent to the engine via the Hermes interface
2. **Analyze** — The engine detects formats, maps columns to the target schema with confidence scores, and validates data
3. **Review** — Users inspect mappings and resolve flagged anomalies
4. **Import** — Clean data is inserted into SQL Server
5. **Monitor** — Athena surfaces quality trends and AI-generated risk predictions
6. **Query** — Apollo enables natural language exploration of the unified dataset
