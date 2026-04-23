# PROJECT REPORT: REAL-TIME STREAMING WEB ACTIVITY LOG ANALYZER

## Abstract
The **Streaming Web Activity Log Analyzer** is a high-performance, scalable data engineering project designed to address the challenges of real-time user engagement monitoring in modern content platforms. In an era where millions of interactions occur per second, the ability to derive actionable insights instantaneously is critical for content recommendation and platform optimization. 

This project implements a robust data pipeline that integrates a modern web frontend (**Next.js**), a high-throughput API layer (**FastAPI**), a distributed message broker (**Apache Kafka**), a streaming analytics engine (**Apache Spark Structured Streaming**), and a distributed filesystem (**Hadoop HDFS**). By employing a "Lambda-style" architecture, the system ensures that ephemeral "trending" data is processed with minimal latency while maintaining a persistent, immutable record of all user activity for long-term historical analysis. The result is a comprehensive system capable of transforming raw, high-velocity logs into dynamic engagement scores and trending genre metrics, delivered via a real-time dashboard.

---

## 1. Introduction

### 1.1 About the Domain: Event Stream Processing
The domain of this project is **Event Stream Processing (ESP)**, a subset of Big Data engineering focusing on the intake, processing, and analysis of continuous streams of data. Unlike traditional batch processing, where data is collected and processed in large chunks, ESP treats data as a never-ending sequence of events.

In the context of a video streaming platform (similar to Netflix or YouTube), a "log" is not merely a record of system errors, but a behavioral trace. Every "Play," "Like," or "Rate" action constitutes a discrete event. The core challenges in this domain include:
- **Velocity:** Managing the sheer speed at which events are generated.
- **Volume:** Handling the vast amounts of data that accumulate over time.
- **Variability:** Dealing with spikes in traffic during popular content releases.
- **Latency:** The requirement to provide "Trending Now" insights within seconds of the activity occurring.

### 1.2 Scope of Objectives
The primary objective of this project is to build an end-to-end pipeline that simulates a production-grade analytics environment. The scope includes the following technical milestones:

1. **Event Ingestion:** Developing a seamless bridge between the client-side browser and the backend server to capture user behavioral data without impacting UI performance.
2. **Distributed Buffering:** Implementing Apache Kafka to decouple the ingestion layer from the processing layer. This ensures that if the analytics engine slows down, the events are safely queued rather than lost.
3. **Real-time Analytical Windows:** Utilizing Spark Structured Streaming to implement "Tumbling Windows." This allows the system to answer questions like *"What is the most popular genre in the last 5 minutes?"*
4. **Engagement Modeling:** Developing a weighted scoring algorithm to quantify "Engagement." By assigning different weights to views, likes, and ratings, the system creates a more accurate representation of content quality than simple view counts.
5. **Persistent Data Lake:** Designing a storage strategy using Hadoop HDFS and the Parquet columnar format. This allows the system to scale to petabytes of data while remaining efficient for analytical queries.
6. **Closed-Loop Feedback:** Implementing a frontend that polls the processed results, creating a feedback loop where user actions immediately influence the dashboard display.

---

## 2. Detailed Design Architecture

### 2.1 Proposed System Architecture
The proposed system follows a linear, unidirectional data flow designed for high availability and fault tolerance.

**The Data Pipeline Flow:**
`User Interface` $\rightarrow$ `API Gateway` $\rightarrow$ `Message Broker` $\rightarrow$ `Stream Processor` $\rightarrow$ `Storage/Visualization`

- **The Producer Layer (Next.js):** Acts as the event source. It captures user interactions and transmits them as JSON payloads.
- **The Ingestion Layer (FastAPI):** A lightweight Python-based server that validates the incoming request and acts as a Kafka Producer.
- **The Transport Layer (Apache Kafka):** A distributed commit log that stores events in the `web_activity` topic. This layer provides the "back-pressure" mechanism necessary for system stability.
- **The Processing Layer (PySpark):** The "brain" of the system. It consumes Kafka offsets, applies a predefined schema, and performs windowed aggregations.
- **The Sink Layer (HDFS & API):** The processed data is split into two paths:
    - *Long-term:* Raw logs are saved to HDFS in Parquet format, partitioned by date and action.
    - *Short-term:* Aggregated metrics are made available via the API for the dashboard.

### 2.2 Design Architecture (Technical Blueprint)
The project utilizes a **Lambda Architecture** derivative. This architecture acknowledges that there is always a trade-off between latency and accuracy.

- **The Speed Layer (Streaming):** Spark Structured Streaming processes the data in micro-batches. This layer is optimized for low latency, providing "approximate" real-time results for the live leaderboard.
- **The Batch Layer (Storage):** By sinking raw data into HDFS, the system creates a "Single Source of Truth." This data can be processed later using heavy-duty batch jobs to generate daily or monthly reports with 100% accuracy.
- **Schema Registry:** To prevent "poison pill" messages (malformed data) from crashing the Spark job, a strict `StructType` schema is enforced during the read phase. This ensures that every event has a `user_id`, `movie_id`, `action`, and `timestamp`.

### 2.3 Methodology
The implementation methodology focuses on **Scalability** and **Precision**:

1. **Decoupling Strategy:** By using Kafka, the frontend doesn't have to wait for the Spark job to finish before receiving a "success" response. The API simply acknowledges that the event was received by Kafka.
2. **Tumbling Window Logic:** The system implements 5-minute non-overlapping windows. This prevents the "Trending" list from being skewed by old data, ensuring that only current trends are highlighted.
3. **Weighted Scoring Formula:** To avoid "view-botting" or inflation, a composite Engagement Score is calculated: 
   $$\text{Score} = (\text{Views} \times 1) + (\text{Likes} \times 2) + (\text{Avg Rating} \times 5)$$
   This weights a high rating significantly more than a simple click, providing a more honest metric of quality.
4. **Columnar Storage Optimization:** Data is stored in **Parquet**. Unlike CSV, Parquet is columnar, meaning if an analyst only wants to query "actions," the system doesn't need to read the "user_ids" or "timestamps," drastically reducing I/O overhead.

---

## 3. Implementation

### 3.1 Codebase Overview
The implementation is divided into three primary modules: the Client (Frontend), the Server (Backend), and the Analytics Engine (Spark).

#### 3.1.1 Frontend Implementation (Next.js & TypeScript)
The frontend is built for responsiveness and real-time updates. 
- **State Management:** The application uses React hooks to maintain the state of the leaderboard.
- **Event Triggering:** The `sendActivityEvent` function handles the asynchronous communication. It uses the `fetch` API to POST events to the backend, ensuring that the user experience remains fluid.
- **Live Polling:** The `fetchTrending` mechanism uses a polling interval to refresh the "Trending Genres" and "Leaderboard" components, simulating a real-time websocket-like experience.

#### 3.1.2 Backend Implementation (FastAPI)
The backend is designed for maximum throughput and minimum overhead.
- **Async Processing:** FastAPI's `async def` endpoints allow the server to handle thousands of concurrent connections without blocking.
- **Kafka Integration:** The server uses the `kafka-python` library to produce messages to the `web_activity` topic. It ensures that the JSON data is correctly serialized before being sent to the broker.

#### 3.1.3 Analytics Implementation (PySpark)
The Spark engine is the most complex part of the implementation, handling the transformation of raw bytes into meaningful metrics.
- **Streaming Source:** The engine connects to Kafka using the `readStream` method, subscribing to the activity topic.
- **Schema Application:** It casts the binary Kafka value into a structured format using `from_json` and a predefined `StructType` schema.
- **Analytical Functions:** 
    - `calculate_trending_genres()`: Uses `window(col("timestamp"), "5 minutes")` to group events.
    - `calculate_engagement_score()`: Employs `groupBy("movie_id")` and `agg()` to calculate the weighted sum.
- **HDFS Sink:** The `writeStream` method is configured to save data to HDFS. The use of `.partitionBy("action")` ensures that the data is organized on disk by the type of activity, which optimizes future query performance.

### 3.2 End-to-End Logic Flow Summary
1. **Capture:** User clicks "Like" $\rightarrow$ Next.js captures event.
2. **Ingestion:** JSON sent to `/event` endpoint $\rightarrow$ FastAPI pushes to Kafka.
3. **Transport:** Kafka stores event in `web_activity` topic $\rightarrow$ Spark reads offset.
4. **Analysis:** Spark applies window $\rightarrow$ Calculates engagement score $\rightarrow$ Updates trending list.
5. **Storage:** Raw event is written to HDFS as a Parquet file.
6. **Visualization:** Next.js polls API $\rightarrow$ Dashboard updates with new trending genre.
