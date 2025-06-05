from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from enum import Enum
import uuid
import asyncio
import json
import time
from datetime import datetime
import logging
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory storage (replace with database in production)
ingestion_status_store = {}
batch_status_store = {}
request_hash_store = {}
priority_queue = []  # Global priority queue for processing

# Batch configuration
BATCH_SIZE = 3  # Process exactly 3 IDs at a time as per requirements
BATCH_DELAY = 5  # 5 seconds delay between batches as per requirements

class BatchStatusEnum(str, Enum):
    YET_TO_START = "yet_to_start"
    TRIGGERED = "triggered"
    COMPLETED = "completed"

class IngestionRequest(BaseModel):
    ids: List[int] = Field(..., description="List of IDs to process")
    priority: str = Field(..., description="Priority level (HIGH, MEDIUM, LOW)")

class IngestionResponse(BaseModel):
    ingestion_id: str

class BatchInfo(BaseModel):
    batch_id: str
    ids: List[int]
    status: BatchStatusEnum

class StatusResponse(BaseModel):
    ingestion_id: str
    status: str
    batches: List[BatchInfo]

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
        logger.info(f"Processing batch {batch_id} (priority: {priority}) with IDs: {ids}")
        
        # Mark batch as triggered
        batch_status_store[ingestion_id][batch_id]["status"] = BatchStatusEnum.TRIGGERED
        
        # Process each ID in the batch
        tasks = [simulate_external_api_call(id) for id in ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
         # Store results and mark the batch as completed
        for i, result in enumerate(results):
            id = ids[i]
            if isinstance(result, Exception):
                batch_status_store[ingestion_id][batch_id]["results"][id] = {"status": "failed", "error": str(result)}
            else:
                # Verify that result is a dictionary and access it safely
                if isinstance(result, dict):
                    batch_status_store[ingestion_id][batch_id]["results"][id] = {
                        "status": "completed", 
                        "data": result.get("data", "processed")
                    }
                else:
                    # Handle the case where result is not a dict (unexpected but handle it safely)
                    batch_status_store[ingestion_id][batch_id]["results"][id] = {
                        "status": "completed", 
                        "data": "processed"  # Default value when result structure is unexpected
                    }
                    logger.warning(f"Unexpected result type for ID {id}: {type(result)}")
        
        batch_status_store[ingestion_id][batch_id]["status"] = BatchStatusEnum.COMPLETED
        
        # Update the overall ingestion status
        update_ingestion_status(ingestion_id)
        
        logger.info(f"Completed batch {batch_id} for ingestion {ingestion_id}")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        batch_status_store[ingestion_id][batch_id]["status"] = BatchStatusEnum.TRIGGERED
        batch_status_store[ingestion_id][batch_id]["error"] = str(e)
        update_ingestion_status(ingestion_id)

def update_ingestion_status(ingestion_id: str):
    """Update the overall status of an ingestion based on its batches"""
    batches = batch_status_store[ingestion_id]
    
    # Get all batch statuses
    statuses = [batch["status"] for batch in batches.values()]
    
    # Apply the required logic:
    # - If all batches are "yet_to_start", overall status is "yet_to_start"
    # - If at least one batch is "triggered", overall status is "triggered"
    # - If all batches are "completed", overall status is "completed"
    if all(status == BatchStatusEnum.COMPLETED for status in statuses):
        ingestion_status_store[ingestion_id]["status"] = "completed"
    elif any(status == BatchStatusEnum.TRIGGERED for status in statuses):
        ingestion_status_store[ingestion_id]["status"] = "triggered"
    else:
        ingestion_status_store[ingestion_id]["status"] = "yet_to_start"

async def background_processor():
    """Background task to process the global priority queue"""
    while True:
        try:
            if priority_queue:
                # Sort the queue by priority (HIGH > MEDIUM > LOW) and then by created_time
                priority_queue.sort(key=lambda x: (
                    0 if x["priority"] == "HIGH" else (1 if x["priority"] == "MEDIUM" else 2),
                    x["created_at"]
                ))
                
                # Get the highest priority task
                task = priority_queue.pop(0)
                ingestion_id = task["ingestion_id"]
                batch_id = task["batch_id"]
                ids = task["ids"]
                priority = task["priority"]
                
                # Process the batch
                await process_batch(ingestion_id, batch_id, ids, priority)
                
                # Wait for the required delay between batches (5 seconds)
                await asyncio.sleep(BATCH_DELAY)
            else:
                # If queue is empty, wait a short time before checking again
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in background processor: {str(e)}")
            await asyncio.sleep(1)  # Wait before retry on error

def start_background_processor():
    """Start the background processor as a task"""
    asyncio.create_task(background_processor())

@app.on_event("startup")
async def startup_event():
    """Start the background processor when the app starts"""
    start_background_processor()

@app.post("/ingest", response_model=IngestionResponse)
async def ingest_data(request: IngestionRequest):
    """
    Ingest data IDs with priority.
    Queues batches for processing and returns immediately with ingestion ID.
    Implements the required batching and priority handling.
    """
    # Validate input
    if not request.ids:
        raise HTTPException(
            status_code=400, detail="IDs list cannot be empty")

    if request.priority not in ["HIGH", "MEDIUM", "LOW"]:
        raise HTTPException(
            status_code=400, detail="Priority must be HIGH, MEDIUM, or LOW")
    
    # Generate request ID for new requests
    ingestion_id = f"ing-{str(uuid.uuid4())[:8]}"
    
    # Initialize status tracking
    ingestion_status_store[ingestion_id] = {
        "status": "yet_to_start",
        "created_at": datetime.now().isoformat(),
    }
    
    # Split into batches of 3 IDs
    batch_status_store[ingestion_id] = {}
    
    for i in range(0, len(request.ids), BATCH_SIZE):
        batch_ids = request.ids[i:i + BATCH_SIZE]
        batch_id = f"batch-{str(uuid.uuid4())[:8]}"
        
        # Create batch record
        batch_status_store[ingestion_id][batch_id] = {
            "ids": batch_ids,
            "status": BatchStatusEnum.YET_TO_START,
            "results": {},
            "created_at": datetime.now().isoformat()
        }
        
        # Queue the batch for processing with priority
        priority_queue.append({
            "ingestion_id": ingestion_id,
            "batch_id": batch_id,
            "ids": batch_ids,
            "priority": request.priority,
            "created_at": datetime.now().timestamp()
        })
    
    logger.info(f"Created ingestion request {ingestion_id} with {len(request.ids)} IDs")
    
    # Return the ingestion ID in the exact format required
    return IngestionResponse(ingestion_id=ingestion_id)

@app.get("/status/{ingestion_id}", response_model=StatusResponse)
async def get_status(ingestion_id: str):
    """
    Get the current status of an ingestion request.
    Returns format matching requirements with batches and their statuses.
    """
    if ingestion_id not in ingestion_status_store:
        raise HTTPException(status_code=404, detail="Ingestion ID not found")
    
    if ingestion_id not in batch_status_store:
        raise HTTPException(status_code=404, detail="Batch status not found")
    
    # Get the overall status
    status = ingestion_status_store[ingestion_id]["status"]
    
    # Construct the batches information
    batches = []
    for batch_id, batch_data in batch_status_store[ingestion_id].items():
        batches.append(BatchInfo(
            batch_id=batch_id,
            ids=batch_data["ids"],
            status=batch_data["status"]
        ))
    
    # Return status response in the exact format required
    return StatusResponse(
        ingestion_id=ingestion_id,
        status=status,
        batches=batches
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