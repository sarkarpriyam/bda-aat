import os
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "web_activity"
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH", "/user/netflix/analytics")

trending_genres_cache: dict = {}
movie_scores_cache: dict = {}
recent_events: list = []
startup_event = asyncio.Event()
shutdown_event = asyncio.Event()

producer: Optional[AIOKafkaProducer] = None
consumer: Optional[AIOKafkaConsumer] = None


class ActivityEvent(BaseModel):
    user_id: str
    action: str
    movie_id: str
    genre: str
    timestamp: str
    rating_value: Optional[int] = None


class TrendingResponse(BaseModel):
    trending_genres: list
    movie_scores: list


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, consumer
    await connect_kafka()
    await start_analytics_processor()
    yield
    await disconnect_kafka()


async def connect_kafka():
    global producer, consumer
    max_retries = 10
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()
            print(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
            break
        except Exception as e:
            print(f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                print("Warning: Running without Kafka producer")

    try:
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="analytics-consumer",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
        )
        await consumer.start()
        print("Kafka consumer started")
    except Exception as e:
        print(f"Kafka consumer connection failed: {e}")
        consumer = None


async def disconnect_kafka():
    global producer, consumer
    if producer:
        await producer.stop()
    if consumer:
        await consumer.stop()


async def start_analytics_processor():
    asyncio.create_task(process_analytics())


async def process_analytics():
    window_minutes = 5
    update_interval = 5  # Update more frequently for better UX

    while True:
        try:
            cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
            recent_window = []
            for e in recent_events:
                try:
                    ts = e.get("timestamp", "")
                    if ts:
                        # Handle both formats
                        if ts.endswith("Z"):
                            ts = ts.replace("Z", "+00:00")
                        event_time = datetime.fromisoformat(ts)
                        if event_time > cutoff_time:
                            recent_window.append(e)
                except (ValueError, TypeError):
                    continue

            genre_counts: dict = {}
            movie_stats: dict = {}

            for event in recent_window:
                genre = event.get("genre")
                movie_id = event.get("movie_id")
                action = event.get("action")

                if genre:
                    genre_counts[genre] = genre_counts.get(genre, 0) + 1

                if movie_id:
                    if movie_id not in movie_stats:
                        movie_stats[movie_id] = {
                            "views": 0,
                            "likes": 0,
                            "total_rating": 0,
                            "rating_count": 0,
                            "genre": genre,
                        }

                    if action == "play":
                        movie_stats[movie_id]["views"] += 1
                    elif action == "like":
                        movie_stats[movie_id]["likes"] += 1
                    elif action == "rate" and event.get("rating_value"):
                        movie_stats[movie_id]["total_rating"] += event["rating_value"]
                        movie_stats[movie_id]["rating_count"] += 1

            sorted_genres = sorted(
                genre_counts.items(), key=lambda x: x[1], reverse=True
            )[:5]
            trending_genres_cache["trending_genres"] = [
                {"genre": g, "score": s} for g, s in sorted_genres
            ]

            movie_scores = []
            for movie_id, stats in movie_stats.items():
                avg_rating = (
                    stats["total_rating"] / stats["rating_count"]
                    if stats["rating_count"] > 0
                    else 0
                )
                score = stats["views"] * 1 + stats["likes"] * 2 + avg_rating * 5
                movie_scores.append(
                    {
                        "movie_id": movie_id,
                        "genre": stats["genre"],
                        "score": score,
                        "views": stats["views"],
                        "likes": stats["likes"],
                        "avg_rating": avg_rating,
                    }
                )

            movie_scores.sort(key=lambda x: x["score"], reverse=True)
            movie_scores_cache["movie_scores"] = movie_scores[:10]

            print(f"Analytics updated: {len(recent_window)} events in window")

        except Exception as e:
            print(f"Analytics processing error: {e}")

        await asyncio.sleep(update_interval)


app = FastAPI(
    title="Netflix Activity Analytics API",
    description="API for tracking user activity and streaming analytics",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/activity", status_code=201)
async def receive_activity(event: ActivityEvent):
    event_dict = event.model_dump()
    event_dict["timestamp"] = datetime.now().isoformat()

    if len(recent_events) > 10000:
        recent_events.clear()

    recent_events.append(event_dict)

    if producer:
        try:
            await producer.send_and_wait(KAFKA_TOPIC, event_dict)
        except Exception as e:
            print(f"Kafka send error: {e}")

    output_file = f"hdfs_{KAFKA_TOPIC}_{datetime.now().strftime('%Y%m%d')}.json"
    print(
        f"[HDFS SINK] Would sink to: {HDFS_OUTPUT_PATH}/dt={datetime.now().strftime('%Y-%m-%d')}/{event.action}"
    )
    print(
        f"[STORAGE] Event cached: {event_dict['action']} by {event_dict['user_id']} on {event_dict['movie_id']}"
    )

    return {"status": "received", "event": event_dict}


@app.get("/api/trending", response_model=TrendingResponse)
async def get_trending():
    return TrendingResponse(
        trending_genres=trending_genres_cache.get("trending_genres", []),
        movie_scores=movie_scores_cache.get("movie_scores", []),
    )


@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "kafka_connected": producer is not None,
        "events_processed": len(recent_events),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
