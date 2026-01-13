"""
Test Redis Schema Explorer
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.redis_connector import RedisConnector
from schema.redis_schema_explorer import RedisSchemaExplorer

def print_section(title):
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_key_patterns():
    """Test key pattern detection"""
    print_section("TEST 1: Key Pattern Detection")
    
    connector = RedisConnector()
    connector.connect()
    
    explorer = RedisSchemaExplorer(connector)
    
    print("\n🔑 Detecting key patterns...")
    patterns = explorer.get_key_patterns(sample_size=500)
    
    print(f"\nFound {len(patterns)} patterns:\n")
    for pattern, keys in list(patterns.items())[:10]:
        print(f"  {pattern}: {len(keys)} keys")
        print(f"    Examples: {keys[:3]}")
    
    connector.disconnect()

def test_pattern_analysis():
    """Test individual pattern analysis"""
    print_section("TEST 2: Pattern Analysis")
    
    connector = RedisConnector()
    connector.connect()
    
    explorer = RedisSchemaExplorer(connector)
    
    # Get first pattern
    patterns = explorer.get_key_patterns(sample_size=500)
    if patterns:
        pattern, keys = list(patterns.items())[0]
        
        print(f"\n🔍 Analyzing pattern: {pattern}")
        analysis = explorer.analyze_key_pattern(pattern, keys, sample_size=10)
        
        print(f"\nPattern: {analysis['pattern']}")
        print(f"Total keys: {analysis['total_keys']}")
        print(f"Primary type: {analysis['primary_type']}")
        print(f"Type distribution: {analysis['types']}")
        
        # Structure details
        structure = analysis.get('structure', {})
        for data_type, info in structure.items():
            print(f"\n{data_type.upper()} structure:")
            if info.get('field_names'):
                print(f"  Fields: {', '.join(info['field_names'][:10])}")
            if info.get('sample_values'):
                print(f"  Sample: {info['sample_values'][0]}")
    
    connector.disconnect()

def test_database_schema():
    """Test complete database schema"""
    print_section("TEST 3: Complete Database Schema")
    
    connector = RedisConnector()
    connector.connect()
    
    explorer = RedisSchemaExplorer(connector)
    
    print("\n📊 Analyzing entire database...")
    print("(This may take a minute...)")
    
    schema = explorer.get_database_schema(sample_size=500)
    
    print(f"\nDatabase: {schema['database']}")
    print(f"Total keys: {schema['total_keys']:,}")
    print(f"Patterns: {len(schema['patterns'])}")
    
    # Show metadata
    metadata = schema.get('metadata', {})
    if metadata:
        print("\nMetadata:")
        for key, value in metadata.items():
            print(f"  {key}: {value}")
    
    connector.disconnect()

def test_schema_summary():
    """Test schema summary generation"""
    print_section("TEST 4: Schema Summary")
    
    connector = RedisConnector()
    connector.connect()
    
    explorer = RedisSchemaExplorer(connector)
    
    print("\n📄 Generating schema summary...")
    schema = explorer.get_database_schema(sample_size=500)
    summary = explorer.generate_schema_summary(schema)
    
    print("\n" + summary)
    
    connector.disconnect()

def test_llm_context():
    """Test LLM context generation"""
    print_section("TEST 5: LLM Context Generation")
    
    connector = RedisConnector()
    connector.connect()
    
    explorer = RedisSchemaExplorer(connector)
    
    print("\n🤖 Generating LLM-optimized context...")
    llm_context = explorer.generate_llm_context()
    
    print("\n" + llm_context)
    
    connector.disconnect()

def test_relationships():
    """Test relationship inference"""
    print_section("TEST 6: Relationship Inference")
    
    connector = RedisConnector()
    connector.connect()
    
    explorer = RedisSchemaExplorer(connector)
    
    print("\n🔗 Inferring relationships...")
    relationships = explorer.infer_relationships()
    
    if relationships:
        print(f"\nFound {len(relationships)} potential relationships:")
        for rel in relationships[:10]:
            print(f"  {rel['from_pattern']} -> {rel['to_pattern']}")
    else:
        print("\nNo relationships detected")
    
    connector.disconnect()

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print(" "*15 + "REDIS SCHEMA EXPLORER TESTS")
    print("="*70)
    
    try:
        test_key_patterns()
        test_pattern_analysis()
        test_database_schema()
        test_schema_summary()
        test_llm_context()
        test_relationships()
        
        print("\n" + "="*70)
        print("✅ ALL TESTS COMPLETED")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_all_tests()