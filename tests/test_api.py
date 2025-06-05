#!/usr/bin/env python3
"""
Updated test script for PRD-compliant Data Ingestion API System
"""

import asyncio
import httpx
import json

BASE_URL = "http://localhost:8000"


async def test_prd_compliance():
    """Test the API compliance with PRD specifications"""

    sample_request = {
        "mappings": [
            {"id": "map_001", "source": "source_1", "target": "target_1"},
            {"id": "map_002", "source": "source_2", "target": "target_2"},
            {"id": "map_003", "source": "source_3", "target": "target_3"}
        ],
        "priority": "HIGH"
    }

    async with httpx.AsyncClient() as client:
        try:
            print("🧪 Testing PRD Compliance...")

            # 1. Test ingestion
            print("\n1. Testing POST /ingest")
            response = await client.post(f"{BASE_URL}/ingest", json=sample_request)
            if response.status_code == 200:
                result = response.json()
                request_id = result["request_id"]
                print(f"✅ Ingestion Response: {json.dumps(result, indent=2)}")
            else:
                print(f"❌ Ingestion failed: {response.status_code}")
                return

            # Wait for processing
            await asyncio.sleep(3)

            # 2. Test status endpoint (PRD format)
            print(f"\n2. Testing GET /status/{request_id}")
            status_response = await client.get(f"{BASE_URL}/status/{request_id}")
            if status_response.status_code == 200:
                status_data = status_response.json()
                print(f"✅ Status Response (PRD Format):")
                print(json.dumps(status_data, indent=2))

                # Verify PRD compliance
                required_fields = ["request_id", "status", "results"]
                missing_fields = [
                    field for field in required_fields if field not in status_data]
                if missing_fields:
                    print(f"❌ Missing required fields: {missing_fields}")
                else:
                    print("✅ All required PRD fields present!")

                    if status_data.get("results"):
                        print(f"✅ Results format verified:")
                        for result in status_data["results"]:
                            if "id" in result and "status" in result:
                                print(
                                    f"   - ID {result['id']}: {result['status']}")
                            else:
                                print(f"   ❌ Invalid result format: {result}")

            # 3. Test detailed status
            print(f"\n3. Testing GET /status/{request_id}/detailed")
            detailed_response = await client.get(f"{BASE_URL}/status/{request_id}/detailed")
            if detailed_response.status_code == 200:
                detailed_data = detailed_response.json()
                print(f"✅ Detailed Status:")
                print(f"   Progress: {detailed_data['progress']}%")
                print(
                    f"   Processed: {detailed_data['processed_mappings']}/{detailed_data['total_mappings']}")
                print(f"   Failed: {detailed_data['failed_mappings']}")

            # 4. Test health
            print(f"\n4. Testing GET /health")
            health_response = await client.get(f"{BASE_URL}/health")
            if health_response.status_code == 200:
                health_data = health_response.json()
                print(f"✅ Health: {health_data['status']}")
                print(f"   Service: {health_data['service']}")
                print(f"   Active requests: {health_data['active_requests']}")

        except Exception as e:
            print(f"❌ Test error: {e}")

if __name__ == "__main__":
    print("🧪 PRD Compliance Test - Data Ingestion API")
    print("=" * 50)

    try:
        asyncio.run(test_prd_compliance())
        print("\n✅ PRD Compliance Test Completed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
