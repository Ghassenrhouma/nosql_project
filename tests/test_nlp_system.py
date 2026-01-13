"""
Test Natural Language Query System (End-to-End)
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from llm.query_translator import QueryTranslator
from llm.query_executor import QueryExecutor
from schema.mongodb_schema_explorer import MongoDBSchemaExplorer
from schema.neo4j_schema_explorer import Neo4jSchemaExplorer
from schema.redis_schema_explorer import RedisSchemaExplorer
from schema.rdf_schema_explorer import RDFSchemaExplorer
from connectors.mongodb_connector import MongoDBConnector
from connectors.neo4j_connector import Neo4jConnector
from connectors.redis_connector import RedisConnector
from connectors.rdf_connector import RDFConnector

def print_section(title):
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_mongodb_nlq():
    """Test MongoDB natural language queries"""
    print_section("TEST 1: MongoDB Natural Language Queries")
    
    # Get schema
    print("\n📊 Loading MongoDB schema...")
    mongo_conn = MongoDBConnector()
    mongo_conn.connect()
    explorer = MongoDBSchemaExplorer(mongo_conn)
    schema = explorer.generate_llm_context('movies')
    
    # Initialize translator and executor
    translator = QueryTranslator()
    executor = QueryExecutor()
    
    # Test queries
    test_queries = [
        "Find all movies from 2015",
        "Show me action movies",
        "Find movies with rating above 8",
    ]
    
    for nlq in test_queries:
        print(f"\n🔍 Natural Query: {nlq}")
        
        # Translate
        translated = translator.translate_to_mongodb(nlq, schema)
        print(f"📝 MongoDB Query: {translated.get('query', {})}")
        print(f"💡 Explanation: {translated.get('explanation', 'N/A')}")
        
        # Execute
        if 'error' not in translated:
            result = executor.execute_mongodb(translated)
            if result['success']:
                print(f"✓ Found {result['count']} results")
                # Show first result
                if result['results']:
                    first = result['results'][0]
                    print(f"  Sample: {first.get('title', 'N/A')}")
            else:
                print(f"✗ Error: {result['error']}")
    
    mongo_conn.disconnect()

def test_neo4j_nlq():
    """Test Neo4j natural language queries"""
    print_section("TEST 2: Neo4j Natural Language Queries")
    
    # Get schema
    print("\n📊 Loading Neo4j schema...")
    neo4j_conn = Neo4jConnector()
    neo4j_conn.connect()
    explorer = Neo4jSchemaExplorer(neo4j_conn)
    schema = explorer.generate_llm_context()
    
    translator = QueryTranslator()
    executor = QueryExecutor()
    
    test_queries = [
        "Find all movies",
        "Who directed The Dark Knight?",
        "Show me actors in action movies"
    ]
    
    for nlq in test_queries:
        print(f"\n🔍 Natural Query: {nlq}")
        
        translated = translator.translate_to_neo4j(nlq, schema)
        print(f"📝 Cypher: {translated.get('cypher', 'N/A')}")
        print(f"💡 Explanation: {translated.get('explanation', 'N/A')}")
        
        if 'error' not in translated:
            result = executor.execute_neo4j(translated)
            if result['success']:
                print(f"✓ Found {result['count']} results")
                if result['results']:
                    print(f"  Sample: {result['results'][0]}")
            else:
                print(f"✗ Error: {result['error']}")
    
    neo4j_conn.disconnect()

def test_redis_nlq():
    """Test Redis natural language queries"""
    print_section("TEST 3: Redis Natural Language Queries")
    
    print("\n📊 Loading Redis schema...")
    redis_conn = RedisConnector()
    redis_conn.connect()
    explorer = RedisSchemaExplorer(redis_conn)
    schema = explorer.generate_llm_context()
    
    translator = QueryTranslator()
    executor = QueryExecutor()
    
    test_queries = [
        "Get top 5 rated movies",
        "Find all genres",
        "Show me action movie IDs"
    ]
    
    for nlq in test_queries:
        print(f"\n🔍 Natural Query: {nlq}")
        
        translated = translator.translate_to_redis(nlq, schema)
        print(f"📝 Redis Commands: {translated.get('commands', [])}")
        print(f"💡 Explanation: {translated.get('explanation', 'N/A')}")
        
        if 'error' not in translated:
            result = executor.execute_redis(translated)
            if result['success']:
                print(f"✓ Executed {result['count']} command(s)")
                if result['results']:
                    print(f"  Result: {result['results'][0]}")
            else:
                print(f"✗ Error: {result['error']}")
    
    redis_conn.disconnect()

def test_rdf_nlq():
    """Test RDF natural language queries"""
    print_section("TEST 4: RDF/SPARQL Natural Language Queries")
    
    print("\n📊 Loading RDF schema...")
    rdf_conn = RDFConnector()
    rdf_conn.connect()
    explorer = RDFSchemaExplorer(rdf_conn)
    schema = explorer.generate_llm_context()
    
    translator = QueryTranslator()
    executor = QueryExecutor()
    
    test_queries = [
        "Find all movies",
        "Show me drama movies",
        "Who directed movies in 1924?"
    ]
    
    for nlq in test_queries:
        print(f"\n🔍 Natural Query: {nlq}")
        
        translated = translator.translate_to_sparql(nlq, schema)
        print(f"📝 SPARQL: {translated.get('sparql', 'N/A')[:100]}...")
        print(f"💡 Explanation: {translated.get('explanation', 'N/A')}")
        
        if 'error' not in translated:
            result = executor.execute_sparql(translated)
            if result['success']:
                print(f"✓ Found {result['count']} results")
                if result['results']:
                    print(f"  Sample: {result['results'][0]}")
            else:
                print(f"✗ Error: {result['error']}")
    
    rdf_conn.disconnect()

def run_all_tests():
    """Run all NLQ tests"""
    print("\n" + "="*70)
    print(" "*15 + "NATURAL LANGUAGE QUERY SYSTEM TEST")
    print("="*70)
    
    try:
        test_mongodb_nlq()
        test_neo4j_nlq()
        test_redis_nlq()
        test_rdf_nlq()
        
        print("\n" + "="*70)
        print("✅ ALL NLQ TESTS COMPLETED")
        print("="*70)
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_all_tests()