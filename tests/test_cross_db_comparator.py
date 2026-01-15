"""
Test Cross-Database Comparator
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from cross_db_comparator import CrossDatabaseComparator

def test_cross_db_comparison():
    """Test the cross-database comparison functionality"""
    print("\n" + "="*60)
    print(" "*15 + "CROSS-DB COMPARATOR TEST")
    print("="*60)

    comparator = CrossDatabaseComparator()

    # Test with a simple query
    test_query = "Find all movies from 2015"

    print(f"\n🔍 Testing query: '{test_query}'")
    print("This may take a moment as it connects to all databases...")

    try:
        results = comparator.compare_query(test_query)

        # Print summary
        summary = results.get('summary', {})
        print("\n📊 Test Results Summary:")
        print(f"  Databases tested: {summary.get('total_databases', 0)}")
        print(f"  Successful translations: {summary.get('successful_translations', 0)}")
        print(f"  Successful executions: {summary.get('successful_executions', 0)}")

        if summary.get('successful_executions', 0) > 0:
            print("✅ Cross-database comparison is working!")
        else:
            print("⚠️  Some databases may not be running or configured properly")

        # Show detailed results for one database as example
        comparisons = results.get('comparisons', {})
        if comparisons:
            first_db = list(comparisons.keys())[0]
            db_result = comparisons[first_db]
            print(f"\n📝 Example result from {first_db.upper()}:")
            print(f"  Translation success: {db_result['translation']['success']}")
            print(f"  Execution success: {db_result['execution']['success']}")
            print(f"  Result count: {db_result['execution']['result_count']}")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

def test_specific_databases():
    """Test comparison with only specific databases"""
    print("\n" + "="*60)
    print(" "*10 + "TESTING SPECIFIC DATABASE SUBSET")
    print("="*60)

    comparator = CrossDatabaseComparator()

    # Test with only MongoDB and Redis
    test_query = "Show me action movies"
    databases = ['mongodb', 'redis']

    print(f"\n🔍 Testing query: '{test_query}'")
    print(f"Databases: {', '.join(databases).upper()}")

    try:
        results = comparator.compare_query(test_query, databases)

        summary = results.get('summary', {})
        print("\n📊 Results:")
        print(f"  Successful translations: {summary.get('successful_translations', 0)}/{len(databases)}")
        print(f"  Successful executions: {summary.get('successful_executions', 0)}/{len(databases)}")

    except Exception as e:
        print(f"❌ Test failed: {e}")

def run_all_tests():
    """Run all cross-database comparator tests"""
    print("\n🚀 CROSS-DATABASE COMPARATOR TEST SUITE")
    print("="*60)

    try:
        test_cross_db_comparison()
        test_specific_databases()

        print("\n" + "="*60)
        print("✅ CROSS-DB COMPARATOR TESTS COMPLETED")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_all_tests()