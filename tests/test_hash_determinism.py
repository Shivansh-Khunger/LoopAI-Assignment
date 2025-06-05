#!/usr/bin/env python3
"""
Test hash generation with different mapping orders
"""
from main import generate_request_hash, IngestionRequest, Mapping
import sys
import os
sys.path.append(os.path.dirname(__file__))


def test_hash_determinism():
    print("🧪 Testing Hash Determinism and Order Independence")
    print("=" * 60)

    # Test Case 1: Different order of mappings
    print("\n📝 Test Case 1: Different Order of Mappings")

    # Original order
    mappings_order1 = [
        Mapping(id="A", source="src1", target="tgt1",
                transformation={"rule": "test"}),
        Mapping(id="B", source="src2", target="tgt2", transformation=None),
        Mapping(id="C", source="src3", target="tgt3",
                transformation={"complex": {"nested": "value"}})
    ]
    request1 = IngestionRequest(mappings=mappings_order1, priority="HIGH")
    hash1 = generate_request_hash(request1)

    # Different order
    mappings_order2 = [
        Mapping(id="C", source="src3", target="tgt3",
                transformation={"complex": {"nested": "value"}}),
        Mapping(id="A", source="src1", target="tgt1",
                transformation={"rule": "test"}),
        Mapping(id="B", source="src2", target="tgt2", transformation=None)
    ]
    request2 = IngestionRequest(mappings=mappings_order2, priority="HIGH")
    hash2 = generate_request_hash(request2)

    # Another different order
    mappings_order3 = [
        Mapping(id="B", source="src2", target="tgt2", transformation=None),
        Mapping(id="C", source="src3", target="tgt3",
                transformation={"complex": {"nested": "value"}}),
        Mapping(id="A", source="src1", target="tgt1",
                transformation={"rule": "test"})
    ]
    request3 = IngestionRequest(mappings=mappings_order3, priority="HIGH")
    hash3 = generate_request_hash(request3)

    print(f"Hash (A,B,C order): {hash1}")
    print(f"Hash (C,A,B order): {hash2}")
    print(f"Hash (B,C,A order): {hash3}")

    if hash1 == hash2 == hash3:
        print("✅ ORDER INDEPENDENCE PASSED: All orders produce same hash")
    else:
        print("❌ ORDER INDEPENDENCE FAILED: Different orders produce different hashes")

    # Test Case 2: Different priority
    print("\n📝 Test Case 2: Different Priority")
    request_high = IngestionRequest(mappings=mappings_order1, priority="HIGH")
    request_medium = IngestionRequest(
        mappings=mappings_order1, priority="MEDIUM")
    request_low = IngestionRequest(mappings=mappings_order1, priority="LOW")

    hash_high = generate_request_hash(request_high)
    hash_medium = generate_request_hash(request_medium)
    hash_low = generate_request_hash(request_low)

    print(f"Hash (HIGH priority):   {hash_high}")
    print(f"Hash (MEDIUM priority): {hash_medium}")
    print(f"Hash (LOW priority):    {hash_low}")

    if len(set([hash_high, hash_medium, hash_low])) == 3:
        print(
            "✅ PRIORITY SENSITIVITY PASSED: Different priorities produce different hashes")
    else:
        print("❌ PRIORITY SENSITIVITY FAILED: Different priorities should produce different hashes")

    # Test Case 3: Different transformation content
    print("\n📝 Test Case 3: Different Transformation Content")
    mapping_trans1 = [
        Mapping(id="test", source="src", target="tgt",
                transformation={"rule": "uppercase"})
    ]
    mapping_trans2 = [
        Mapping(id="test", source="src", target="tgt",
                transformation={"rule": "lowercase"})
    ]
    mapping_trans3 = [
        Mapping(id="test", source="src", target="tgt", transformation=None)
    ]

    req_trans1 = IngestionRequest(mappings=mapping_trans1, priority="HIGH")
    req_trans2 = IngestionRequest(mappings=mapping_trans2, priority="HIGH")
    req_trans3 = IngestionRequest(mappings=mapping_trans3, priority="HIGH")

    hash_trans1 = generate_request_hash(req_trans1)
    hash_trans2 = generate_request_hash(req_trans2)
    hash_trans3 = generate_request_hash(req_trans3)

    print(f"Hash (uppercase rule):  {hash_trans1}")
    print(f"Hash (lowercase rule):  {hash_trans2}")
    print(f"Hash (null transform):  {hash_trans3}")

    if len(set([hash_trans1, hash_trans2, hash_trans3])) == 3:
        print("✅ TRANSFORMATION SENSITIVITY PASSED: Different transformations produce different hashes")
    else:
        print("❌ TRANSFORMATION SENSITIVITY FAILED: Different transformations should produce different hashes")

    # Test Case 4: Empty vs Non-empty mappings (edge case)
    print("\n📝 Test Case 4: Different Mapping Counts")

    single_mapping = [
        Mapping(id="single", source="src", target="tgt", transformation=None)
    ]

    double_mapping = [
        Mapping(id="single", source="src", target="tgt", transformation=None),
        Mapping(id="double", source="src2", target="tgt2", transformation=None)
    ]

    req_single = IngestionRequest(mappings=single_mapping, priority="HIGH")
    req_double = IngestionRequest(mappings=double_mapping, priority="HIGH")

    hash_single = generate_request_hash(req_single)
    hash_double = generate_request_hash(req_double)

    print(f"Hash (1 mapping):  {hash_single}")
    print(f"Hash (2 mappings): {hash_double}")

    if hash_single != hash_double:
        print("✅ MAPPING COUNT SENSITIVITY PASSED: Different mapping counts produce different hashes")
    else:
        print("❌ MAPPING COUNT SENSITIVITY FAILED: Different mapping counts should produce different hashes")

    print("\n" + "=" * 60)
    print("🎉 HASH DETERMINISM TESTS COMPLETED")


if __name__ == "__main__":
    test_hash_determinism()
