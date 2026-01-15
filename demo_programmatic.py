#!/usr/bin/env python3
"""
NoSQL-LLM Demo Script
Demonstrates programmatic usage of the NoSQL-LLM system
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from cross_db_comparator import CrossDatabaseComparator
from nosql_llm_cli import NoSQLLLMInterface

def demo_programmatic_usage():
    """Demonstrate programmatic usage of the system"""
    print("🚀 NoSQL-LLM Programmatic Demo")
    print("="*50)

    # Initialize comparator
    comparator = CrossDatabaseComparator()

    # Example 1: Cross-database comparison
    print("\n📊 Example 1: Cross-Database Comparison")
    query = "Find all movies from 2015"

    print(f"Query: '{query}'")
    print("Comparing across all databases...")

    results = comparator.compare_query(query)

    # Show summary
    summary = results.get('summary', {})
    print("Results:")
    print(f"  - Databases tested: {summary.get('total_databases', 0)}")
    print(f"  - Successful translations: {summary.get('successful_translations', 0)}")
    print(f"  - Successful executions: {summary.get('successful_executions', 0)}")

    # Example 2: Single database query
    print("\n📝 Example 2: Single Database Query")
    from llm.query_translator import QueryTranslator
    from llm.query_executor import QueryExecutor

    translator = QueryTranslator()
    executor = QueryExecutor()

    # Get MongoDB schema
    schema = comparator._get_schema_context('mongodb')

    # Translate query
    translated = translator.translate_to_mongodb(query, schema)
    print(f"Translated MongoDB query: {translated.get('query', 'N/A')}")

    # Execute query
    if 'error' not in translated:
        result = executor.execute_mongodb(translated)
        print(f"Execution result: {result.get('count', 0)} documents found")

    print("\n✅ Demo completed successfully!")
    print("\n💡 For interactive usage, run: python nosql_llm_cli.py")

if __name__ == "__main__":
    demo_programmatic_usage()