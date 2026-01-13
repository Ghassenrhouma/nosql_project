"""
Test MongoDB Schema Explorer
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.mongodb_connector import MongoDBConnector
from schema.mongodb_schema_explorer import MongoDBSchemaExplorer

def print_section(title):
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_field_analysis():
    """Test field type analysis"""
    print_section("TEST 1: Field Type Analysis")
    
    connector = MongoDBConnector()
    connector.connect()
    
    explorer = MongoDBSchemaExplorer(connector)
    
    # Analyze movies collection
    print("\n🔍 Analyzing 'movies' collection fields...")
    fields = explorer.analyze_field_types('movies', sample_size=100)
    
    print(f"\nFound {len(fields)} fields:\n")
    for field_name, field_info in list(fields.items())[:10]:
        print(f"  {field_name}:")
        print(f"    Type: {field_info['type']}")
        print(f"    Presence: {field_info['presence']}")
        if field_info.get('array'):
            print(f"    Array: Yes")
        if field_info.get('nested'):
            print(f"    Nested: Yes")
        if field_info.get('sample_values'):
            print(f"    Samples: {field_info['sample_values'][:2]}")
    
    connector.disconnect()

def test_relationship_inference():
    """Test relationship inference"""
    print_section("TEST 2: Relationship Inference")
    
    connector = MongoDBConnector()
    connector.connect()
    
    explorer = MongoDBSchemaExplorer(connector)
    
    print("\n🔗 Inferring relationships...")
    relationships = explorer.infer_relationships('movies')
    
    if relationships:
        print(f"\nFound {len(relationships)} potential relationships:")
        for rel in relationships:
            print(f"  {rel['from_collection']}.{rel['from_field']} → {rel['to_collection']}")
    else:
        print("\nNo relationships found (this is normal for movies collection)")
    
    connector.disconnect()

def test_collection_schema():
    """Test complete collection schema"""
    print_section("TEST 3: Collection Schema")
    
    connector = MongoDBConnector()
    connector.connect()
    
    explorer = MongoDBSchemaExplorer(connector)
    
    print("\n📋 Getting complete schema for 'movies'...")
    schema = explorer.get_collection_schema('movies', sample_size=100)
    
    print(f"\nCollection: {schema['collection']}")
    print(f"Documents: {schema['document_count']:,}")
    print(f"Fields: {len(schema['fields'])}")
    print(f"Relationships: {len(schema['relationships'])}")
    print(f"Indexes: {len(schema['indexes'])}")
    
    connector.disconnect()

def test_schema_summary():
    """Test schema summary generation"""
    print_section("TEST 4: Schema Summary")
    
    connector = MongoDBConnector()
    connector.connect()
    
    explorer = MongoDBSchemaExplorer(connector)
    
    print("\n📄 Generating human-readable summary...")
    schema = explorer.get_collection_schema('movies', sample_size=100)
    summary = explorer.generate_schema_summary(schema)
    
    print("\n" + summary)
    
    connector.disconnect()

def test_llm_context():
    """Test LLM context generation"""
    print_section("TEST 5: LLM Context Generation")
    
    connector = MongoDBConnector()
    connector.connect()
    
    explorer = MongoDBSchemaExplorer(connector)
    
    print("\n🤖 Generating LLM-optimized context...")
    llm_context = explorer.generate_llm_context('movies')
    
    print("\n" + llm_context)
    
    connector.disconnect()

def test_database_schema():
    """Test full database schema"""
    print_section("TEST 6: Full Database Schema")
    
    connector = MongoDBConnector()
    connector.connect()
    
    explorer = MongoDBSchemaExplorer(connector)
    
    print("\n🗄️ Analyzing entire database...")
    print("(This may take a minute...)")
    
    db_schema = explorer.get_database_schema(sample_size=100)
    
    print(f"\nDatabase: {db_schema['database']}")
    print(f"Collections: {db_schema['total_collections']}")
    
    for coll_name, coll_schema in db_schema['collections'].items():
        print(f"\n  {coll_name}:")
        print(f"    Documents: {coll_schema['document_count']:,}")
        print(f"    Fields: {len(coll_schema['fields'])}")
    
    connector.disconnect()

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print(" "*15 + "MONGODB SCHEMA EXPLORER TESTS")
    print("="*70)
    
    try:
        test_field_analysis()
        test_relationship_inference()
        test_collection_schema()
        test_schema_summary()
        test_llm_context()
        test_database_schema()
        
        print("\n" + "="*70)
        print("✅ ALL TESTS COMPLETED")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_all_tests()