#!/usr/bin/env python3
"""
Test script to verify idempotency implementation in the Data Ingestion API.
"""
import asyncio
import httpx
import json
import time
from typing import Dict, Any

# Test server URL
BASE_URL = "http://localhost:8000"


async def test_idempotency():
    """Test idempotency behavior of the /ingest endpoint"""
    async with httpx.AsyncClient() as client:
        print("🧪 Testing Idempotency Implementation")
        print("=" * 50)

        # Test payload - identical requests should return same request_id
        test_request = {
            "mappings": [
                {
                    "id": "mapping_1",
                    "source": "source_A",
                    "target": "target_B",
                    "transformation": {"rule": "uppercase"}
                },
                {
                    "id": "mapping_2",
                    "source": "source_C",
                    "target": "target_D",
                    "transformation": None
                }
            ],
            "priority": "HIGH"
        }

        print("📝 Test Request:")
        print(json.dumps(test_request, indent=2))
        print()

        # Test 1: First submission
        print("🔄 Test 1: First submission")
        try:
            response1 = await client.post(f"{BASE_URL}/ingest", json=test_request)
            if response1.status_code == 200:
                data1 = response1.json()
                request_id1 = data1["request_id"]
                status1 = data1["status"]
                print(
                    f"✅ Success: request_id = {request_id1}, status = {status1}")
            else:
                print(f"❌ Failed: {response1.status_code} - {response1.text}")
                return
        except Exception as e:
            print(f"❌ Error: {e}")
            return

        # Test 2: Identical submission (should return same request_id)
        print("\n🔄 Test 2: Identical submission (idempotency test)")
        try:
            response2 = await client.post(f"{BASE_URL}/ingest", json=test_request)
            if response2.status_code == 200:
                data2 = response2.json()
                request_id2 = data2["request_id"]
                status2 = data2["status"]
                print(
                    f"✅ Success: request_id = {request_id2}, status = {status2}")

                # Verify idempotency
                if request_id1 == request_id2:
                    print("✅ IDEMPOTENCY TEST PASSED: Same request_id returned")
                else:
                    print(
                        f"❌ IDEMPOTENCY TEST FAILED: Different request_ids ({request_id1} vs {request_id2})")
                    return
            else:
                print(f"❌ Failed: {response2.status_code} - {response2.text}")
                return
        except Exception as e:
            print(f"❌ Error: {e}")
            return

        # Test 3: Modified request (should return different request_id)
        print("\n🔄 Test 3: Modified request (should be different)")
        modified_request = test_request.copy()
        modified_request["priority"] = "MEDIUM"  # Changed priority

        try:
            response3 = await client.post(f"{BASE_URL}/ingest", json=modified_request)
            if response3.status_code == 200:
                data3 = response3.json()
                request_id3 = data3["request_id"]
                status3 = data3["status"]
                print(
                    f"✅ Success: request_id = {request_id3}, status = {status3}")

                # Verify different request gets different ID
                if request_id3 != request_id1:
                    print(
                        "✅ DIFFERENTIATION TEST PASSED: Different request_id for modified request")
                else:
                    print(
                        f"❌ DIFFERENTIATION TEST FAILED: Same request_id for different requests")
                    return
            else:
                print(f"❌ Failed: {response3.status_code} - {response3.text}")
                return
        except Exception as e:
            print(f"❌ Error: {e}")
            return

        # Test 4: Third identical submission (should still return original request_id)
        print("\n🔄 Test 4: Third identical submission")
        try:
            response4 = await client.post(f"{BASE_URL}/ingest", json=test_request)
            if response4.status_code == 200:
                data4 = response4.json()
                request_id4 = data4["request_id"]
                status4 = data4["status"]
                print(
                    f"✅ Success: request_id = {request_id4}, status = {status4}")

                # Verify still idempotent
                if request_id4 == request_id1:
                    print(
                        "✅ PERSISTENT IDEMPOTENCY TEST PASSED: Same request_id returned again")
                else:
                    print(
                        f"❌ PERSISTENT IDEMPOTENCY TEST FAILED: Different request_id ({request_id1} vs {request_id4})")
                    return
            else:
                print(f"❌ Failed: {response4.status_code} - {response4.text}")
                return
        except Exception as e:
            print(f"❌ Error: {e}")
            return

        # Test 5: Check status of original request
        print(f"\n🔄 Test 5: Check status of original request ({request_id1})")
        try:
            status_response = await client.get(f"{BASE_URL}/status/{request_id1}")
            if status_response.status_code == 200:
                status_data = status_response.json()
                print(f"✅ Status check successful:")
                print(f"   - Request ID: {status_data['request_id']}")
                print(f"   - Status: {status_data['status']}")
                print(
                    f"   - Results: {len(status_data.get('results', []) or [])} items")
            else:
                print(f"❌ Status check failed: {status_response.status_code}")
        except Exception as e:
            print(f"❌ Error checking status: {e}")

        print("\n" + "=" * 50)
        print("🎉 ALL IDEMPOTENCY TESTS COMPLETED SUCCESSFULLY!")
        print("✅ Identical requests return the same request_id")
        print("✅ Different requests return different request_ids")
        print("✅ Idempotency persists across multiple submissions")


async def test_hash_consistency():
    """Test that hash generation is consistent"""
    print("\n🧪 Testing Hash Consistency")
    print("=" * 30)

    # Import the hash function from main
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))

    try:
        from main import generate_request_hash, IngestionRequest, Mapping

        # Create test request
        mappings = [
            Mapping(id="1", source="A", target="B",
                    transformation={"rule": "test"}),
            Mapping(id="2", source="C", target="D", transformation=None)
        ]
        request = IngestionRequest(mappings=mappings, priority="HIGH")

        # Generate hash multiple times
        hash1 = generate_request_hash(request)
        hash2 = generate_request_hash(request)
        hash3 = generate_request_hash(request)

        print(f"Hash 1: {hash1}")
        print(f"Hash 2: {hash2}")
        print(f"Hash 3: {hash3}")

        if hash1 == hash2 == hash3:
            print("✅ HASH CONSISTENCY TEST PASSED: Same input produces same hash")
        else:
            print("❌ HASH CONSISTENCY TEST FAILED: Same input produces different hashes")

        # Test with different order (should produce same hash due to sorting)
        mappings_reordered = [
            Mapping(id="2", source="C", target="D", transformation=None),
            Mapping(id="1", source="A", target="B",
                    transformation={"rule": "test"})
        ]
        request_reordered = IngestionRequest(
            mappings=mappings_reordered, priority="HIGH")
        hash_reordered = generate_request_hash(request_reordered)

        print(f"Hash (reordered): {hash_reordered}")

        if hash1 == hash_reordered:
            print("✅ ORDER INDEPENDENCE TEST PASSED: Different order produces same hash")
        else:
            print(
                "❌ ORDER INDEPENDENCE TEST FAILED: Different order produces different hash")

    except ImportError as e:
        print(f"❌ Could not import from main.py: {e}")


async def main():
    """Main test function"""
    print("🚀 Starting Idempotency Tests")
    print("Please ensure the API server is running on http://localhost:8000")
    print()

    # Test if server is running
    try:
        async with httpx.AsyncClient() as client:
            health_response = await client.get(f"{BASE_URL}/health")
            if health_response.status_code == 200:
                print("✅ Server is running")
            else:
                print(
                    f"❌ Server responded with status {health_response.status_code}")
                return
    except Exception as e:
        print(f"❌ Cannot connect to server: {e}")
        print("Please start the server with: python main.py")
        return

    # Run tests
    await test_hash_consistency()
    await test_idempotency()

if __name__ == "__main__":
    asyncio.run(main())
