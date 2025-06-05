from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from enum import Enum
import uuid
import asyncio
import json
import time
from datetime import datetime, timedelta
import logging
import hashlib
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory storage (replace with database in production)
ingestion_status_store = {}
batch_status_store = {}
# Stores a hash of the request body to the ingestion_id for idempotency
request_hash_store = {}
priority_queue = []  # Global priority queue for processing

# Batch configuration
BATCH_SIZE = 3  # Process exactly 3 IDs at a time as per requirements
BATCH_DELAY = 5  # 5 seconds delay between batches as per requirements
IDEMPOTENCY_WINDOW_SECONDS = 300  # 5 minutes for idempotency as per requirements

# Flag to control the background processor loop
processing_active = False


class BatchStatusEnum(str, Enum):
    YET_TO_START = "yet_to_start"
    TRIGGERED = "triggered"
    COMPLETED = "completed"
    FAILED = "failed"


class IngestionRequest(BaseModel):
    ids: List[int] = Field(..., description="List of IDs to process")
    priority: str = Field(...,
                          description="Priority level (HIGH, MEDIUM, LOW)")


class IngestionResponse(BaseModel):
    ingestion_id: str


class BatchInfo(BaseModel):
    batch_id: str
    ids: List[int]
    status: BatchStatusEnum
    results: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class StatusResponse(BaseModel):
    ingestion_id: str
    status: str
    batches: List[BatchInfo]
    created_at: str
    last_updated_at: Optional[str] = None


# Priority mapping for sorting
PRIORITY_MAP = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Data Ingestion API System starting up...")
    start_background_processor()
    yield
    # Shutdown
    logger.info("Data Ingestion API System shutting down...")
    stop_background_processor()

app = FastAPI(
    title="Data Ingestion API System",
    description="API for ingesting and processing data with external API calls",
    version="1.0.0",
    lifespan=lifespan
)


async def simulate_external_api_call(id: int) -> Dict[str, Any]:
    """Simulate external API call for a single ID"""
    try:
        # Simulate processing time for the external API
        await asyncio.sleep(0.5)

        # Return fixed response format as per requirements
        return {
            "id": id,
            "data": "processed"
        }
    except Exception as e:
        logger.error(f"External API call failed for ID {id}: {str(e)}")
        raise


async def process_batch(ingestion_id: str, batch_id: str, ids: List[int], priority: str):
    """Process a single batch of IDs"""
    try:
        logger.info(
            f"Processing batch {batch_id} for ingestion {ingestion_id} (priority: {priority}) with IDs: {ids}")

        # Mark batch as triggered
        batch_status_store[ingestion_id][batch_id]["status"] = BatchStatusEnum.TRIGGERED
        batch_status_store[ingestion_id][batch_id]["results"] = {}
        update_ingestion_status(ingestion_id)

        # Process each ID in the batch
        tasks = [simulate_external_api_call(id) for id in ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Store results and mark the batch as completed
        for i, result in enumerate(results):
            id = ids[i]
            if isinstance(result, Exception):
                batch_status_store[ingestion_id][batch_id]["results"][str(id)] = {
                    "status": "failed",
                    "error": str(result)
                }
            else:
                if isinstance(result, dict):
                    batch_status_store[ingestion_id][batch_id]["results"][str(id)] = {
                        "status": "completed",
                        "data": result.get("data", "processed")
                    }
                else:
                    batch_status_store[ingestion_id][batch_id]["results"][str(id)] = {
                        "status": "completed",
                        "data": "processed"
                    }
                    logger.warning(
                        f"Unexpected result type for ID {id}: {type(result)}")

        batch_status_store[ingestion_id][batch_id]["status"] = BatchStatusEnum.COMPLETED
        logger.info(f"Completed batch {batch_id} for ingestion {ingestion_id}")

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        batch_status_store[ingestion_id][batch_id]["status"] = BatchStatusEnum.FAILED
        batch_status_store[ingestion_id][batch_id]["error"] = str(e)
    finally:
        update_ingestion_status(ingestion_id)


def update_ingestion_status(ingestion_id: str):
    """Update the overall status of an ingestion based on its batches"""
    if ingestion_id not in ingestion_status_store:
        return

    batches_for_ingestion = batch_status_store.get(ingestion_id, {})
    all_batch_statuses = [batch_data["status"]
                          for batch_data in batches_for_ingestion.values()]

    new_overall_status = ingestion_status_store[ingestion_id]["status"]

    if not all_batch_statuses:
        new_overall_status = BatchStatusEnum.YET_TO_START
    elif all(status == BatchStatusEnum.COMPLETED for status in all_batch_statuses):
        new_overall_status = BatchStatusEnum.COMPLETED
    elif any(status == BatchStatusEnum.TRIGGERED or status == BatchStatusEnum.COMPLETED for status in all_batch_statuses):
        new_overall_status = BatchStatusEnum.TRIGGERED
    elif any(status == BatchStatusEnum.FAILED for status in all_batch_statuses):
        new_overall_status = BatchStatusEnum.FAILED
    else:
        new_overall_status = BatchStatusEnum.YET_TO_START

    if new_overall_status != ingestion_status_store[ingestion_id]["status"]:
        ingestion_status_store[ingestion_id]["status"] = new_overall_status
        ingestion_status_store[ingestion_id]["last_updated_at"] = datetime.now(
        ).isoformat()
        logger.info(
            f"Ingestion {ingestion_id} overall status updated to: {new_overall_status}")


async def process_queue():
    """Background task to process items in the priority queue."""
    global processing_active
    while processing_active:
        if not priority_queue:
            await asyncio.sleep(1)
            continue

        # Sort the queue by priority (higher value means higher priority) and then by creation time
        priority_queue.sort(key=lambda x: (x[0], x[1]), reverse=True)

        current_priority_value, timestamp, ingestion_id, batches_to_process = priority_queue.pop(
            0)
        logger.info(
            f"Picked ingestion {ingestion_id} from queue with priority {current_priority_value}")

        # Check if this ingestion is already completed or failed
        if ingestion_status_store.get(ingestion_id, {}).get("status") in [BatchStatusEnum.COMPLETED, BatchStatusEnum.FAILED]:
            logger.info(
                f"Skipping ingestion {ingestion_id} as it's already {ingestion_status_store[ingestion_id]['status']}")
            continue

        # Get the priority string for logging
        priority_str = next(
            (k for k, v in PRIORITY_MAP.items() if v == current_priority_value), "UNKNOWN")

        for batch_id, batch_data in batches_to_process.items():
            if batch_data["status"] == BatchStatusEnum.YET_TO_START:
                await process_batch(ingestion_id, batch_id, batch_data["ids"], priority_str)
                # Apply delay between batches
                if processing_active:
                    await asyncio.sleep(BATCH_DELAY)
            else:
                logger.info(
                    f"Batch {batch_id} for ingestion {ingestion_id} already processed or in progress: {batch_data['status']}")

        # After processing all batches for an ingestion, ensure final status update
        update_ingestion_status(ingestion_id)


def start_background_processor():
    """Starts the background processing task."""
    global processing_active
    if not processing_active:
        processing_active = True
        asyncio.create_task(process_queue())
        logger.info("Background processor started.")


def stop_background_processor():
    """Stops the background processing task."""
    global processing_active
    processing_active = False
    logger.info("Background processor stopping.")


def generate_request_hash(request_data: Dict[str, Any]) -> str:
    """Generate a hash for the request body for idempotency."""
    sorted_items = sorted(request_data.items())
    dumped_data = json.dumps(
        sorted_items, separators=(',', ':'), sort_keys=True)
    return hashlib.sha256(dumped_data.encode('utf-8')).hexdigest()


@app.post("/ingest", response_model=IngestionResponse, status_code=200)
async def ingest_data(request: IngestionRequest, background_tasks: BackgroundTasks):
    """Ingest data with a list of IDs and a priority."""
    # Validate input
    if not request.ids:
        raise HTTPException(status_code=400, detail="IDs list cannot be empty")

    if request.priority not in ["HIGH", "MEDIUM", "LOW"]:
        raise HTTPException(
            status_code=400, detail="Priority must be HIGH, MEDIUM, or LOW")

    request_dict = request.dict()
    request_hash = generate_request_hash(request_dict)
    current_time = datetime.now()

    # Idempotency check
    if request_hash in request_hash_store:
        stored_ingestion_id, stored_timestamp = request_hash_store[request_hash]
        if (current_time - datetime.fromisoformat(stored_timestamp)) < timedelta(seconds=IDEMPOTENCY_WINDOW_SECONDS):
            logger.info(
                f"Duplicate request detected for hash {request_hash}, returning existing ingestion ID {stored_ingestion_id}")
            return IngestionResponse(ingestion_id=stored_ingestion_id)
        else:
            logger.info(
                f"Request hash {request_hash} found, but outside idempotency window. Creating new ingestion.")

    ingestion_id = str(uuid.uuid4())
    created_at = datetime.now().isoformat()

    # Divide IDs into batches
    batches_data = {}
    for i in range(0, len(request.ids), BATCH_SIZE):
        batch_id = str(uuid.uuid4())
        batch_ids = request.ids[i:i + BATCH_SIZE]
        batches_data[batch_id] = {
            "ids": batch_ids,
            "status": BatchStatusEnum.YET_TO_START,
            "results": {}
        }

    ingestion_status_store[ingestion_id] = {
        "status": BatchStatusEnum.YET_TO_START,
        "created_at": created_at,
        "last_updated_at": created_at,
        "priority": request.priority
    }
    batch_status_store[ingestion_id] = batches_data

    # Add to priority queue
    priority_value = PRIORITY_MAP.get(request.priority.upper(), 1)
    priority_queue.append(
        (priority_value, time.time(), ingestion_id, batches_data))
    priority_queue.sort(key=lambda x: (x[0], x[1]), reverse=True)

    # Store request hash for idempotency
    request_hash_store[request_hash] = (ingestion_id, current_time.isoformat())

    logger.info(
        f"Ingestion request {ingestion_id} received with {len(request.ids)} IDs and priority {request.priority}. Divided into {len(batches_data)} batches.")
    return IngestionResponse(ingestion_id=ingestion_id)


@app.get("/status/{ingestion_id}", response_model=StatusResponse)
async def get_ingestion_status(ingestion_id: str):
    """Retrieve the current status of an ingestion request."""
    if ingestion_id not in ingestion_status_store:
        raise HTTPException(status_code=404, detail="Ingestion ID not found")

    # Get the overall status and creation time
    overall_status_data = ingestion_status_store[ingestion_id]
    status = overall_status_data["status"]
    created_at = overall_status_data["created_at"]
    last_updated_at = overall_status_data.get("last_updated_at")

    # Construct the batches information
    batches = []
    if ingestion_id in batch_status_store:
        for batch_id, batch_data in batch_status_store[ingestion_id].items():
            batches.append(BatchInfo(
                batch_id=batch_id,
                ids=batch_data["ids"],
                status=batch_data["status"],
                results=batch_data.get("results"),
                error=batch_data.get("error")
            ))

    return StatusResponse(
        ingestion_id=ingestion_id,
        status=status,
        batches=batches,
        created_at=created_at,
        last_updated_at=last_updated_at
    )


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
