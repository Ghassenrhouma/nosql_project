"""
Test Neo4j Schema Explorer
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.neo4j_connector import Neo4jConnector
from schema.neo4j_schema_explorer import Neo4jSchemaExplorer

def print_section(title):
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_labels_and_types():
    """Test getting labels and relationship types"""
    print_section("TEST 1: Labels and Relationship Types")
    
    connector = Neo4jConnector()
    connector.connect()
    
    explorer = Neo4jSchemaExplorer(connector)
    
    # Get labels
    labels = explorer.get_node_labels()
    print(f"\n🏷️  Node Labels ({len(labels)}):")
    for label in labels:
        count = connector.count_nodes(label)
        print(f"  - {label}: {count:,} nodes")
    
    # Get relationship types
    rel_types = explorer.get_relationship_types()
    print(f"\n🔗 Relationship Types ({len(rel_types)}):")
    for rel_type in rel_types:
        count = connector.count_relationships(rel_type)
        print(f"  - {rel_type}: {count:,} relationships")
    
    connector.disconnect()

def test_node_properties():
    """Test node property analysis"""
    print_section("TEST 2: Node Property Analysis")
    
    connector = Neo4jConnector()
    connector.connect()
    
    explorer = Neo4jSchemaExplorer(connector)
    
    # Analyze Movie node properties
    print("\n🎬 Analyzing Movie node properties...")
    properties = explorer.analyze_node_properties('Movie', sample_size=50)
    
    print(f"\nFound {len(properties)} properties:\n")
    for prop_name, prop_info in list(properties.items())[:10]:
        print(f"  {prop_name}:")
        print(f"    Type: {prop_info['type']}")
        print(f"    Presence: {prop_info['presence']}")
        if prop_info.get('sample_values'):
            print(f"    Samples: {prop_info['sample_values'][:2]}")
    
    connector.disconnect()

def test_relationship_patterns():
    """Test relationship pattern detection"""
    print_section("TEST 3: Relationship Patterns")
    
    connector = Neo4jConnector()
    connector.connect()
    
    explorer = Neo4jSchemaExplorer(connector)
    
    print("\n🔗 Detecting relationship patterns...")
    patterns = explorer.get_relationship_patterns()
    
    print(f"\nFound {len(patterns)} patterns:\n")
    for pattern in patterns[:10]:
        print(f"  ({pattern['source_label']})-[:{pattern['relationship_type']}]->({pattern['target_label']}) [{pattern['count']} instances]")
    
    connector.disconnect()

def test_node_schema():
    """Test complete node schema"""
    print_section("TEST 4: Complete Node Schema")
    
    connector = Neo4jConnector()
    connector.connect()
    
    explorer = Neo4jSchemaExplorer(connector)
    
    print("\n📋 Getting complete schema for Movie nodes...")
    schema = explorer.get_node_schema('Movie', sample_size=50)
    
    print(f"\nLabel: {schema['label']}")
    print(f"Node count: {schema['node_count']:,}")
    print(f"Properties: {len(schema['properties'])}")
    print(f"Outgoing relationships: {len(schema['outgoing_relationships'])}")
    print(f"Incoming relationships: {len(schema['incoming_relationships'])}")
    
    if schema['outgoing_relationships']:
        print("\nOutgoing relationships:")
        for rel in schema['outgoing_relationships'][:3]:
            print(f"  - {rel['type']} -> {rel['target_labels']}")
    
    connector.disconnect()

def test_relationship_schema():
    """Test relationship schema"""
    print_section("TEST 5: Relationship Schema")
    
    connector = Neo4jConnector()
    connector.connect()
    
    explorer = Neo4jSchemaExplorer(connector)
    
    # Get first relationship type
    rel_types = explorer.get_relationship_types()
    if rel_types:
        rel_type = rel_types[0]
        
        print(f"\n🔗 Analyzing {rel_type} relationships...")
        schema = explorer.get_relationship_schema(rel_type, sample_size=50)
        
        print(f"\nRelationship type: {schema['relationship_type']}")
        print(f"Count: {schema['relationship_count']:,}")
        print(f"Properties: {len(schema['properties'])}")
        
        if schema['patterns']:
            print("\nPatterns:")
            for pattern in schema['patterns'][:3]:
                print(f"  ({pattern['source_labels']}) -> ({pattern['target_labels']})")
    
    connector.disconnect()

def test_schema_summary():
    """Test schema summary generation"""
    print_section("TEST 6: Schema Summary")
    
    connector = Neo4jConnector()
    connector.connect()
    
    explorer = Neo4jSchemaExplorer(connector)
    
    print("\n📄 Generating schema summary...")
    schema = explorer.get_graph_schema(sample_size=50)
    summary = explorer.generate_schema_summary(schema)
    
    print("\n" + summary)
    
    connector.disconnect()

def test_llm_context():
    """Test LLM context generation"""
    print_section("TEST 7: LLM Context Generation")
    
    connector = Neo4jConnector()
    connector.connect()
    
    explorer = Neo4jSchemaExplorer(connector)
    
    print("\n🤖 Generating LLM-optimized context...")
    llm_context = explorer.generate_llm_context()
    
    print("\n" + llm_context[:1000] + "...")  # First 1000 chars
    
    connector.disconnect()

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print(" "*15 + "NEO4J SCHEMA EXPLORER TESTS")
    print("="*70)
    
    try:
        test_labels_and_types()
        test_node_properties()
        test_relationship_patterns()
        test_node_schema()
        test_relationship_schema()
        test_schema_summary()
        test_llm_context()
        
        print("\n" + "="*70)
        print("✅ ALL TESTS COMPLETED")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_all_tests()