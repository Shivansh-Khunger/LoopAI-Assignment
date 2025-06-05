# Data Ingestion API System

A robust API system for ingesting and processing data with external API calls, built according to the Product Requirements Document specifications.

## Features

- **Ingestion API**: POST endpoint for receiving data with JSON payload containing mappings and priority
- **Status API**: GET endpoint to check ingestion status and progress
- **Background Processing**: Asynchronous processing with batch handling
- **External API Simulation**: Simulated external API calls with proper error handling
- **Real-time Status Tracking**: Monitor progress with detailed status updates
- **Priority Handling**: Support for HIGH, MEDIUM, LOW priority levels

## API Endpoints

### 1. POST /ingest

Start a new data ingestion process.

**Request Body:**

```json
{
  "mappings": [
    {
      "id": "map_001",
      "source": "source_system_1",
      "target": "target_system_1",
      "transformation": { "field1": "value1" }
    }
  ],
  "priority": "HIGH"
}
```

**Response:**

```json
{
  "request_id": "uuid-string",
  "status": "pending",
  "message": "Ingestion request created successfully..."
}
```

### 2. GET /status/{request_id}

Get the current status of an ingestion request.

**Response:**

```json
{
  "request_id": "uuid-string",
  "status": "in_progress",
  "progress": 75,
  "total_mappings": 100,
  "processed_mappings": 75,
  "failed_mappings": 0,
  "details": null
}
```

### 3. GET /status/{request_id}/results

Get detailed results of processed mappings.

**Response:**

```json
{
    "request_id": "uuid-string",
    "status": "completed",
    "total_mappings": 3,
    "processed_mappings": 3,
    "failed_mappings": 0,
    "results": [...]
}
```

### 4. GET /health

Health check endpoint.

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2025-06-05T...",
  "active_requests": 5
}
```

## Installation and Setup

1. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Server**

   ```bash
   python main.py
   ```

   Or using uvicorn directly:

   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

3. **Access the API**
   - API Base URL: `http://localhost:8000`
   - Interactive Docs: `http://localhost:8000/docs`
   - ReDoc: `http://localhost:8000/redoc`

## Testing

Run the test script to see the complete workflow in action:

```bash
python test_api.py
```

The test script will:

1. Submit a sample ingestion request
2. Monitor the status until completion
3. Retrieve detailed results
4. Display the complete workflow

## Example Usage with cURL

### 1. Start Ingestion

```bash
curl -X POST "http://localhost:8000/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": [
      {
        "id": "map_001",
        "source": "source_1",
        "target": "target_1"
      }
    ],
    "priority": "HIGH"
  }'
```

### 2. Check Status

```bash
curl "http://localhost:8000/status/{request_id}"
```

### 3. Get Results

```bash
curl "http://localhost:8000/status/{request_id}/results"
```

## Technical Implementation

- **Framework**: FastAPI for high-performance async API
- **Background Tasks**: AsyncIO for concurrent processing
- **Data Validation**: Pydantic models for request/response validation
- **Error Handling**: Comprehensive error handling with proper HTTP status codes
- **Logging**: Structured logging for monitoring and debugging
- **Storage**: In-memory storage (can be replaced with database)

## Status Values

- `pending`: Request received, waiting to start processing
- `in_progress`: Currently processing mappings
- `completed`: All mappings processed successfully
- `failed`: Processing failed due to an error

## Priority Levels

- `HIGH`: High priority processing
- `MEDIUM`: Medium priority processing
- `LOW`: Low priority processing

## Configuration

The system processes mappings in batches of 5 (configurable) with simulated external API calls. Each external API call has a 90% success rate for demonstration purposes.

## Production Considerations

For production deployment, consider:

1. **Database**: Replace in-memory storage with PostgreSQL/MongoDB
2. **Queue System**: Use Redis/RabbitMQ for proper job queuing
3. **Authentication**: Add API key or OAuth authentication
4. **Rate Limiting**: Implement rate limiting for API endpoints
5. **Monitoring**: Add metrics and monitoring (Prometheus/Grafana)
6. **Docker**: Containerize the application
7. **Load Balancing**: Use multiple instances behind a load balancer

## Error Handling

The system provides comprehensive error handling:

- Input validation errors (400 Bad Request)
- Resource not found errors (404 Not Found)
- Internal processing errors (500 Internal Server Error)
- External API failures are tracked and reported in results
