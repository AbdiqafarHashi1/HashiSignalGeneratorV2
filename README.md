# HashiSignalGeneratorV2 - Phase 1 Foundation

Production-grade modular algorithmic trading system with async FastAPI backend, PostgreSQL, Redis, and Next.js dashboard.

## Stack
- Backend: FastAPI, SQLAlchemy 2.0 async, PostgreSQL, Redis, Uvicorn, Pydantic v2
- Frontend: Next.js 14 App Router, TypeScript, Tailwind, Axios
- Infra: Docker + Docker Compose

## Services
- `api`: FastAPI service on `:8000` with Swagger at `/docs`
- `dashboard`: Next.js service on `:3000`
- `postgres`: PostgreSQL 16
- `redis`: Redis 7

## Run
```bash
cp .env.example .env
docker compose up --build
```

## API Endpoints
- `POST /engine/start`
- `POST /engine/stop`
- `GET /engine/status`
- `POST /replay/start`
- `POST /replay/stop`
- `GET /risk/status`
- `GET /positions`
- `GET /trades?limit=&offset=`
- `GET /executions?limit=&offset=`
- `POST /signals/test`
- `POST /telegram/test`
- `GET /overview`

## Testing
```bash
cd backend
pytest
```
