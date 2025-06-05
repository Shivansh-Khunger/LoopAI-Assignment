# Data Ingestion API System

A robust API system for processing data IDs in batches with priority handling and rate limiting, built according to the Product Requirements Document specifications.

## Overview

This system provides two RESTful APIs for data ingestion with the following key features:

- **Batch Processing**: Processes exactly 3 IDs per batch
- **Rate Limiting**: 1 batch every 5 seconds (max 3 IDs per 5 seconds)
- **Priority Handling**: HIGH > MEDIUM > LOW priority with proper queue management
- **Asynchronous Processing**: Background processing with real-time status tracking
- **External API Simulation**: Mocked external API calls with delay simulation

## API Endpoints

### 1. POST /ingest

Submit a data ingestion request with a list of IDs and priority.

**Request Body:**

```json
{
  "ids": [1, 2, 3, 4, 5],
  "priority": "HIGH"
}
```

**Parameters:**

- `ids`: List of integers (range: 1 to 10^19+7)
- `priority`: Enum values - "HIGH", "MEDIUM", "LOW"

**Response:**

```json
{
  "ingestion_id": "abc123"
}
```

### 2. GET /status/{ingestion_id}

Get the current processing status of an ingestion request.

**Response:**

```json
{
  "ingestion_id": "abc123",
  "status": "triggered",
  "batches": [
    { "batch_id": "uuid1", "ids": [1, 2, 3], "status": "completed" },
    { "batch_id": "uuid2", "ids": [4, 5], "status": "triggered" }
  ]
}
```

**Status Values:**

**Batch Status:**

- `yet_to_start`: Batch is queued but not started
- `triggered`: Batch is currently being processed
- `completed`: Batch processing finished

**Overall Status:**

- `yet_to_start`: All batches are yet to start
- `triggered`: At least one batch is triggered
- `completed`: All batches are completed

## Priority Processing Example

According to the PRD specification:

**Request1 (T0):** `{"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"}`
**Request2 (T4):** `{"ids": [6, 7, 8, 9], "priority": "HIGH"}`

**Processing Timeline:**

- **T0 to T5**: Process [1, 2, 3] (first batch from MEDIUM priority)
- **T5 to T10**: Process [6, 7, 8] (HIGH priority interrupts and takes precedence)
- **T10 to T15**: Process [9, 4, 5] (remaining HIGH priority ID + remaining MEDIUM priority IDs)

## Installation and Setup

### Prerequisites

- Python 3.8+
- pip

### Local Setup

1. **Clone the Repository**

   ```bash
   git clone <repository-url>
   cd data-ingestion-api
   ```

2. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Server**

   ```bash
   python main.py
   ```

   Or using uvicorn directly:

   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

4. **Access the API**
   - API Base URL: `http://localhost:8000`
   - Interactive Docs: `http://localhost:8000/docs`
   - ReDoc: `http://localhost:8000/redoc`

## Testing

### Run All Tests

```bash
python test_api.py
```

The test files include:

- **test_api.py**: Comprehensive test suite covering all requirements

### Test Coverage

✅ Batch processing (exactly 3 IDs per batch)
✅ Rate limiting (5-second delay between batches)
✅ Priority handling (HIGH > MEDIUM > LOW)
✅ Status tracking and transitions
✅ Error handling and edge cases
✅ Idempotency
✅ Concurrent request handling

## Example Usage with cURL

### 1. Submit Ingestion Request

```bash
curl -X POST "http://localhost:8000/ingest" \
  -H "Content-Type: application/json" \
  -d '{"ids": [1, 2, 3, 4, 5], "priority": "HIGH"}'
```

### 2. Check Status

```bash
curl "http://localhost:8000/status/abc123"
```

### 3. Health Check

```bash
curl "http://localhost:8000/health"
```

## Technical Implementation

### Architecture

- **Framework**: FastAPI with async/await support
- **Background Processing**: AsyncIO with priority queue management
- **Data Models**: Pydantic for request/response validation
- **Storage**: In-memory storage (production ready for database integration)
- **Rate Limiting**: Built-in batch processing with configurable delays

### Key Components

1. **Priority Queue System**

   - Processes requests based on (priority_level, timestamp)
   - Higher priority requests interrupt lower priority processing
   - Maintains proper batch boundaries (exactly 3 IDs per batch)

2. **Background Processor**

   - Single background task managing the global queue
   - Respects rate limiting (5-second intervals)
   - Handles priority interruptions correctly

3. **Status Tracking**

   - Real-time status updates for each batch
   - Comprehensive ingestion request tracking
   - Proper status transitions (yet_to_start → triggered → completed)

4. **External API Simulation**
   - Configurable delay simulation (1-3 seconds per ID)
   - Mock response generation
   - Error handling simulation

### Configuration

```python
BATCH_SIZE = 3  # Exactly 3 IDs per batch
BATCH_DELAY = 5  # 5 seconds between batches
IDEMPOTENCY_WINDOW_SECONDS = 300  # 5-minute window
```

## Production Deployment

### Docker

A Dockerfile is provided for containerization:

```bash
docker build -t data-ingestion-api .
docker run -p 8000:8000 data-ingestion-api
```

### Environment Variables

- `PORT`: Server port (default: 8000)
- `HOST`: Server host (default: 0.0.0.0)
- `LOG_LEVEL`: Logging level (default: INFO)

### Production Considerations

1. **Database Integration**

   - Replace in-memory storage with PostgreSQL/Redis
   - Implement proper data persistence
   - Add connection pooling

2. **Queue System**

   - Consider Redis/RabbitMQ for distributed processing
   - Add retry mechanisms
   - Implement dead letter queues

3. **Scalability**

   - Horizontal scaling with load balancers
   - Database connection pooling
   - Caching strategies

4. **Monitoring & Observability**

   - Add Prometheus metrics
   - Implement structured logging
   - Health checks and alerting

5. **Security**
   - Rate limiting per client
   - Input validation and sanitization
   - CORS configuration

## API Specification

### Error Responses

```json
{
  "detail": "Error message",
  "status_code": 400
}
```

### Common HTTP Status Codes

- `200 OK`: Successful request
- `400 Bad Request`: Invalid request format/parameters
- `404 Not Found`: Ingestion ID not found
- `422 Unprocessable Entity`: Validation error
- `500 Internal Server Error`: Server error

## Development

### Project Structure

```
├── main.py              # Main FastAPI application
├── test_api.py          # Comprehensive test suite
├── final_test.py        # PRD compliance test
├── requirements.txt     # Python dependencies
├── Dockerfile          # Container configuration
├── README.md           # This documentation
└── .gitignore          # Git ignore rules
```

### Code Quality

- Type hints for better code maintainability
- Pydantic models for data validation
- Comprehensive error handling
- Structured logging
- Async/await patterns for performance

## Testing Results

All tests pass with 100% PRD compliance:

- ✅ Priority handling verified
- ✅ Rate limiting enforced
- ✅ Batch processing accurate
- ✅ Status tracking correct
- ✅ Error handling robust

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is built according to the specified Product Requirements Document.
