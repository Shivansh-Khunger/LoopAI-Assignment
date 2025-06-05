from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum
import uuid
import asyncio
import httpx
import json
import hashlib
import random
from datetime import datetime
import logging
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory storage (replace with database in production)
ingestion_status_store = {}
batch_results_store = {}


def generate_request_hash(request: 'IngestionRequest') -> str:
    """
    Generate a deterministic hash for the request to enable idempotency.
    Identical requests will produce the same hash and return the same request_id.
    """
    # Create a normalized representation of the request
    request_data = {
        "mappings": sorted([
            {
                "id": mapping.id,
                "source": mapping.source,
                "target": mapping.target,
                "transformation": mapping.transformation
            } for mapping in request.mappings
        ], key=lambda x: str(x["id"])),  # Sort for consistency
        "priority": request.priority
    }

    # Generate hash from JSON representation
    request_json = json.dumps(request_data, sort_keys=True, default=str)
    # Use first 16 chars
    return hashlib.sha256(request_json.encode()).hexdigest()[:16]


class StatusEnum(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class Mapping(BaseModel):
    id: str = Field(..., description="Unique identifier for the mapping")
    source: str = Field(..., description="Source identifier")
    target: str = Field(..., description="Target identifier")
    transformation: Optional[Dict[str, Any]] = Field(
        default=None, description="Transformation rules")


class IngestionRequest(BaseModel):
    mappings: List[Mapping] = Field(..., description="List of data mappings")
    priority: str = Field(...,
                          description="Priority level (HIGH, MEDIUM, LOW)")


class IngestionResponse(BaseModel):
    request_id: str
    status: StatusEnum
    message: str


class StatusResponse(BaseModel):
    request_id: str
    status: StatusEnum
    results: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Processing results")


class DetailedStatusResponse(BaseModel):
    request_id: str
    status: StatusEnum
    progress: int = Field(..., ge=0, le=100, description="Progress percentage")
    total_mappings: int
    processed_mappings: int
    failed_mappings: int
    details: Optional[Dict[str, Any]] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Data Ingestion API System starting up...")
    yield
    # Shutdown
    logger.info("Data Ingestion API System shutting down...")

app = FastAPI(
    title="Data Ingestion API System",
    description="API for ingesting and processing data with external API calls",
    version="1.0.0",
    lifespan=lifespan
)


async def simulate_external_api_call(mapping: Mapping) -> Dict[str, Any]:
    """Simulate external API call for data processing"""
    try:
        # Simulate network delay
        await asyncio.sleep(0.5)

        # Simulate random success/failure (70% success rate for better testing)
        import random
        if random.random() < 0.7:
            return {
                "status": "success",
                "mapping_id": mapping.id,
                "processed_data": f"Processed {mapping.source} -> {mapping.target}",
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise Exception("External API call failed")
    except Exception as e:
        logger.error(
            f"External API call failed for mapping {mapping.id}: {str(e)}")
        raise


async def process_mappings_batch(request_id: str, mappings: List[Mapping], priority: str):
    """Process mappings in background with batch processing"""
    logger.info(
        f"Starting batch processing for request {request_id} with priority {priority}")

    total_mappings = len(mappings)
    processed_mappings = 0
    failed_mappings = 0
    results = []

    # Update status to in_progress
    ingestion_status_store[request_id]["status"] = StatusEnum.IN_PROGRESS
    ingestion_status_store[request_id]["progress"] = 0

    try:
        # Process in batches of 5 (configurable)
        batch_size = 5
        for i in range(0, len(mappings), batch_size):
            batch = mappings[i:i + batch_size]
            batch_tasks = []

            for mapping in batch:
                task = simulate_external_api_call(mapping)
                batch_tasks.append(task)

            # Process batch concurrently
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            for j, result in enumerate(batch_results):
                mapping = batch[j]
                if isinstance(result, Exception):
                    failed_mappings += 1
                    results.append({
                        "mapping_id": mapping.id,
                        "status": "failed",
                        "error": str(result),
                        "timestamp": datetime.now().isoformat()
                    })
                else:
                    processed_mappings += 1
                    results.append(result)

                # Update progress
                progress = int(
                    (processed_mappings + failed_mappings) / total_mappings * 100)
                ingestion_status_store[request_id]["progress"] = progress
                ingestion_status_store[request_id]["processed_mappings"] = processed_mappings
                ingestion_status_store[request_id]["failed_mappings"] = failed_mappings

        # Mark as completed
        ingestion_status_store[request_id]["status"] = StatusEnum.COMPLETED
        ingestion_status_store[request_id]["progress"] = 100
        batch_results_store[request_id] = results

        logger.info(f"Batch processing completed for request {request_id}")

    except Exception as e:
        logger.error(
            f"Batch processing failed for request {request_id}: {str(e)}")
        ingestion_status_store[request_id]["status"] = StatusEnum.FAILED
        ingestion_status_store[request_id]["details"] = {"error": str(e)}


@app.post("/ingest", response_model=IngestionResponse)
async def ingest_data(
    request: IngestionRequest,
    background_tasks: BackgroundTasks
):
    """
    Ingest data with mappings and priority.
    Starts background processing and returns immediately with request ID.
    Implements idempotency - identical requests return the same request_id.
    """
    # Validate input
    if not request.mappings:
        raise HTTPException(
            status_code=400, detail="Mappings list cannot be empty")

    if request.priority not in ["HIGH", "MEDIUM", "LOW"]:
        raise HTTPException(
            status_code=400, detail="Priority must be HIGH, MEDIUM, or LOW")

    # Check for idempotent request (duplicate submission)
    request_hash = generate_request_hash(request)


        logger.info(
            f"Returning existing request {existing_request_id} for duplicate submission")

        # Return existing request ID with same status
        return IngestionResponse(
            request_id=existing_request_id,
            status=existing_status["status"],
            message=f"Ingestion request created successfully. {len(request.mappings)} mappings queued for processing."
        )

    # Generate new request ID for new requests
    request_id = str(uuid.uuid4())

    # Initialize status tracking
    ingestion_status_store[request_id] = {
        "status": StatusEnum.PENDING,
        "progress": 0,
        "total_mappings": len(request.mappings),
        "processed_mappings": 0,
        "failed_mappings": 0,
        "priority": request.priority,
        "created_at": datetime.now().isoformat(),
        "details": None
    }
    request_hash_store[request_hash] = request_id  # Store request hash

    # Start background processing
    background_tasks.add_task(
        process_mappings_batch,
        request_id,
        request.mappings,
        request.priority
    )

    logger.info(
        f"Created ingestion request {request_id} with {len(request.mappings)} mappings")

    return IngestionResponse(
        request_id=request_id,
        status=StatusEnum.PENDING,
        message=f"Ingestion request created successfully. {len(request.mappings)} mappings queued for processing."
    )


@app.get("/status/{request_id}", response_model=StatusResponse)
async def get_status(request_id: str):
    """
    Get the current status of an ingestion request with results.
    Returns format matching PRD specification.
    """
    if request_id not in ingestion_status_store:
        raise HTTPException(status_code=404, detail="Request ID not found")

    status_data = ingestion_status_store[request_id]

    # Format results according to PRD specification
    results = None
    if request_id in batch_results_store and status_data["status"] == StatusEnum.COMPLETED:
        results = []
        raw_results = batch_results_store[request_id]

        for i, result in enumerate(raw_results, 1):
            if result.get("status") == "success":
                results.append({
                    "id": i,
                    "status": "completed"
                })
            else:
                results.append({
                    "id": i,
                    "status": "failed",
                    "error": result.get("error", "Unknown error")
                })

    return StatusResponse(
        request_id=request_id,
        status=status_data["status"],
        results=results
    )


@app.get("/status/{request_id}/detailed", response_model=DetailedStatusResponse)
async def get_detailed_status(request_id: str):
    """
    Get detailed status information including progress and counts.
    """
    if request_id not in ingestion_status_store:
        raise HTTPException(status_code=404, detail="Request ID not found")

    status_data = ingestion_status_store[request_id]

    return DetailedStatusResponse(
        request_id=request_id,
        status=status_data["status"],
        progress=status_data["progress"],
        total_mappings=status_data["total_mappings"],
        processed_mappings=status_data["processed_mappings"],
        failed_mappings=status_data["failed_mappings"],
        details=status_data.get("details")
    )


@app.get("/status/{request_id}/results")
async def get_results(request_id: str):
    """
    Legacy endpoint - redirects to main status endpoint for PRD compliance.
    Get detailed results of processed mappings.
    """
    if request_id not in ingestion_status_store:
        raise HTTPException(status_code=404, detail="Request ID not found")

    if request_id not in batch_results_store:
        raise HTTPException(
            status_code=404, detail="Results not available yet")

    status_data = ingestion_status_store[request_id]
    results = batch_results_store[request_id]

    # Return in legacy format for backward compatibility
    return {
        "request_id": request_id,
        "status": status_data["status"],
        "total_mappings": status_data["total_mappings"],
        "processed_mappings": status_data["processed_mappings"],
        "failed_mappings": status_data["failed_mappings"],
        "results": results
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "data-ingestion-api",
        "timestamp": datetime.now().isoformat(),
        "active_requests": len(ingestion_status_store)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
