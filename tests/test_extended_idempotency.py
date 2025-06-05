#!/usr/bin/env python3
"""
Additional test to verify idempotency during different processing stages
"""
import asyncio
import httpx
import json
import time

BASE_URL = "http://localhost:8000"


async def test_idempotency_across_lifecycle():
    """Test idempotency behavior across the complete request lifecycle"""
    async with httpx.AsyncClient() as client:
        print("🔄 Testing Idempotency Across Request Lifecycle")
        print("=" * 60)

        # Simple test request with fewer mappings for faster completion
        test_request = {
            "mappings": [
                {
                    "id": "test_1",
                    "source": "input",
                    "target": "output",
                    "transformation": {"type": "simple"}
                }
            ],
            "priority": "HIGH"
        }

        print("📝 Submitting initial request...")
        response1 = await client.post(f"{BASE_URL}/ingest", json=test_request)
        data1 = response1.json()
        request_id = data1["request_id"]
        print(f"✅ Initial request_id: {request_id}")
        print(f"   Status: {data1['status']}")

        # Test idempotency while request is PENDING
        print("\n🔄 Testing idempotency while PENDING...")
        response2 = await client.post(f"{BASE_URL}/ingest", json=test_request)
        data2 = response2.json()
        print(f"✅ Second submission request_id: {data2['request_id']}")
        print(f"   Status: {data2['status']}")
        print(
            f"   Idempotent: {'✅ YES' if data2['request_id'] == request_id else '❌ NO'}")

        # Wait a bit and test while IN_PROGRESS
        print("\n⏱️  Waiting for processing to start...")
        await asyncio.sleep(2)

        # Check current status
        status_resp = await client.get(f"{BASE_URL}/status/{request_id}")
        current_status = status_resp.json()
        print(f"   Current status: {current_status['status']}")

        print("\n🔄 Testing idempotency while processing...")
        response3 = await client.post(f"{BASE_URL}/ingest", json=test_request)
        data3 = response3.json()
        print(f"✅ Third submission request_id: {data3['request_id']}")
        print(f"   Status: {data3['status']}")
        print(
            f"   Idempotent: {'✅ YES' if data3['request_id'] == request_id else '❌ NO'}")

        # Wait for completion
        print("\n⏱️  Waiting for processing to complete...")
        max_wait = 30  # 30 seconds max
        wait_time = 0

        while wait_time < max_wait:
            await asyncio.sleep(2)
            wait_time += 2

            status_resp = await client.get(f"{BASE_URL}/status/{request_id}")
            if status_resp.status_code == 200:
                status_data = status_resp.json()
                print(f"   Status after {wait_time}s: {status_data['status']}")

                if status_data['status'] in ['completed', 'failed']:
                    break
            else:
                print(f"   Error checking status: {status_resp.status_code}")
                break

        # Test idempotency after completion
        print("\n🔄 Testing idempotency after completion...")
        response4 = await client.post(f"{BASE_URL}/ingest", json=test_request)
        data4 = response4.json()
        print(f"✅ Fourth submission request_id: {data4['request_id']}")
        print(f"   Status: {data4['status']}")
        print(
            f"   Idempotent: {'✅ YES' if data4['request_id'] == request_id else '❌ NO'}")

        # Final status check
        final_status_resp = await client.get(f"{BASE_URL}/status/{request_id}")
        if final_status_resp.status_code == 200:
            final_status = final_status_resp.json()
            print(f"\n📊 Final Status Summary:")
            print(f"   Request ID: {final_status['request_id']}")
            print(f"   Status: {final_status['status']}")
            if final_status.get('results'):
                print(
                    f"   Results: {len(final_status['results'])} items processed")

        print("\n" + "=" * 60)
        print("✅ LIFECYCLE IDEMPOTENCY TEST COMPLETED")


async def test_concurrent_idempotency():
    """Test idempotency with concurrent identical requests"""
    print("\n🔄 Testing Concurrent Idempotency")
    print("=" * 40)

    test_request = {
        "mappings": [
            {
                "id": "concurrent_test",
                "source": "parallel_input",
                "target": "parallel_output",
                "transformation": None
            }
        ],
        "priority": "MEDIUM"
    }

    async with httpx.AsyncClient() as client:
        print("🚀 Submitting 5 identical requests concurrently...")

        # Submit 5 identical requests simultaneously
        tasks = []
        for i in range(5):
            task = client.post(f"{BASE_URL}/ingest", json=test_request)
            tasks.append(task)

        responses = await asyncio.gather(*tasks)

        # Check all responses
        request_ids = []
        for i, response in enumerate(responses):
            if response.status_code == 200:
                data = response.json()
                request_ids.append(data["request_id"])
                print(
                    f"   Response {i+1}: {data['request_id']} (status: {data['status']})")
            else:
                print(f"   Response {i+1}: ERROR {response.status_code}")

        # Verify all request_ids are the same
        unique_ids = set(request_ids)
        if len(unique_ids) == 1:
            print(f"✅ CONCURRENT IDEMPOTENCY PASSED: All requests returned same ID")
            print(f"   Single request_id: {list(unique_ids)[0]}")
        else:
            print(
                f"❌ CONCURRENT IDEMPOTENCY FAILED: Got {len(unique_ids)} different IDs")
            print(f"   IDs: {unique_ids}")


async def main():
    print("🧪 Extended Idempotency Testing")
    print("Verifying idempotency across request lifecycle and concurrency")
    print()

    # Check server
    try:
        async with httpx.AsyncClient() as client:
            health = await client.get(f"{BASE_URL}/health")
            if health.status_code == 200:
                print("✅ Server is ready")
            else:
                print(f"❌ Server error: {health.status_code}")
                return
    except Exception as e:
        print(f"❌ Cannot connect to server: {e}")
        return

    await test_idempotency_across_lifecycle()
    await test_concurrent_idempotency()

    print("\n🎉 ALL EXTENDED TESTS COMPLETED!")

if __name__ == "__main__":
    asyncio.run(main())
