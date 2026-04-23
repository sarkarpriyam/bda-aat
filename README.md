# Netflix Streaming Activity Log Analyzer

Real-time video streaming platform with user behavior analytics tracking.

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Frontend │───▶│  FastAPI   │───▶│   Kafka    │───▶│   PySpark  │
│  (Next.js) │    │  Backend   │    │  Cluster   │    │ Streaming  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                     │         │
                                           ┌─────────▼─────────┐
                                           │   Hadoop HDFS     │
                                           │   (Parquet)      │
                                           └──────────────────┘
```

## Quick Start

### Option 1: Docker Compose (All Services)

```bash
docker-compose up -d
```

- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- Kafka UI: http://localhost:8080
- HDFS UI: http://localhost:9870

### Option 2: Manual Setup

```bash
# Frontend
cd frontend && npm install && npm run dev

# Backend
cd backend && pip install -r requirements.txt && python main.py

# Spark (requires Kafka running)
cd spark && pip install -r requirements.txt && python streaming_analytics.py --mock
```

## Project Structure

```
frontend/          - Next.js Netflix-style UI with Mux Player
backend/           - FastAPI + Kafka producer
spark/            - PySpark streaming analytics
docker-compose.yml - Full stack deployment
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/activity` | Submit user activity (play/like/rate) |
| `GET /api/trending` | Get trending genres & scores |
| `GET /api/health` | Health check |

## Event Schema

```json
{
  "user_id": "user_123",
  "action": "play|like|rate",
  "movie_id": "A",
  "genre": "Sci-Fi",
  "timestamp": "2024-01-15T10:30:00Z",
  "rating_value": 4
}
```

## Engagement Score Formula

```
Score = (Views × 1) + (Likes × 2) + (Average Rating × 5)
```

## HDFS Storage Structure

```
/user/netflix/analytics/
└── dt=2024-01-15/
    ├── action=play/
    ├── action=like/
    └── action=rate/
```

## Tech Stack

- **Frontend**: Next.js 14, Mux Player, Lucide Icons
- **Backend**: FastAPI, aiokafka
- **Messaging**: Apache Kafka
- **Analytics**: PySpark Structured Streaming
- **Storage**: Hadoop HDFS (Parquet)