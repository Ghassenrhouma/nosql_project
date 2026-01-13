"""
Neo4j Connector Test Script
Tests connection and operations on the movie graph
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.neo4j_connector import Neo4jConnector

def print_section(title):
    """Print formatted section header"""
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_connection():
    """Test 1: Connection"""
    print_section("TEST 1: Connection")
    
    connector = Neo4jConnector()
    
    if connector.connect():
        print("✓ Connection successful")
        
        info = connector.get_connection_info()
        print(f"  URI: {info['uri']}")
        print(f"  Database: {info['database']}")
        print(f"  Connected: {info['connected']}")
        
        return connector
    else:
        print("✗ Connection failed")
        return None

def test_database_info(connector):
    """Test 2: Database information"""
    print_section("TEST 2: Database Information")
    
    # Get labels
    labels = connector.get_labels()
    print(f"\n📊 Node Labels ({len(labels)}):")
    for label in labels:
        count = connector.count_nodes(label)
        print(f"  - {label}: {count:,} nodes")
    
    # Get relationship types
    rel_types = connector.get_relationship_types()
    print(f"\n🔗 Relationship Types ({len(rel_types)}):")
    for rel_type in rel_types:
        count = connector.count_relationships(rel_type)
        print(f"  - {rel_type}: {count:,} relationships")
    
    # Total counts
    total_nodes = connector.count_nodes()
    total_rels = connector.count_relationships()
    print(f"\n📈 Total Statistics:")
    print(f"  - Total Nodes: {total_nodes:,}")
    print(f"  - Total Relationships: {total_rels:,}")

def test_find_nodes(connector):
    """Test 3: Find nodes"""
    print_section("TEST 3: Find Nodes")
    
    # Find movies
    print("\n🎬 Sample Movies:")
    movies = connector.find_nodes('Movie', limit=5)
    for movie in movies:
        print(f"  - {movie.get('title')} ({movie.get('year')})")
        print(f"    Rating: {movie.get('imdb_rating', 'N/A')}")
    
    # Find people
    print("\n👤 Sample People:")
    people = connector.find_nodes('Person', limit=5)
    for person in people:
        print(f"  - {person.get('name')}")

def test_find_relationships(connector):
    """Test 4: Find relationships"""
    print_section("TEST 4: Find Relationships")
    
    # Find actors and their movies
    print("\n🎭 Actors and Movies:")
    query = """
    MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
    RETURN p.name as actor, m.title as movie
    LIMIT 10
    """
    results = connector.execute_query(query)
    for record in results:
        print(f"  - {record['actor']} acted in '{record['movie']}'")
    
    # Find directors and their movies
    print("\n🎬 Directors and Movies:")
    query = """
    MATCH (p:Person)-[r:DIRECTED]->(m:Movie)
    RETURN p.name as director, m.title as movie
    LIMIT 10
    """
    results = connector.execute_query(query)
    for record in results:
        print(f"  - {record['director']} directed '{record['movie']}'")

def test_complex_queries(connector):
    """Test 5: Complex graph queries"""
    print_section("TEST 5: Complex Queries")
    
    # Find movies by year
    print("\n📅 Movies from 2010:")
    query = """
    MATCH (m:Movie)
    WHERE m.year = 2010
    RETURN m.title, m.imdb_rating
    ORDER BY m.imdb_rating DESC
    LIMIT 5
    """
    results = connector.execute_query(query)
    for record in results:
        print(f"  - {record['m.title']}: {record['m.imdb_rating']}")
    
    # Find actors who worked with specific director
    print("\n🎬 Actors who worked with Christopher Nolan:")
    query = """
    MATCH (d:Person)-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(a:Person)
    WHERE d.name = 'Christopher Nolan'
    RETURN DISTINCT a.name as actor, count(m) as movies
    ORDER BY movies DESC
    LIMIT 5
    """
    results = connector.execute_query(query)
    for record in results:
        print(f"  - {record['actor']}: {record['movies']} movie(s)")
    
    # Find co-actors
    print("\n👥 Actors who appeared together:")
    query = """
    MATCH (a1:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(a2:Person)
    WHERE a1.name < a2.name
    RETURN a1.name as actor1, a2.name as actor2, m.title as movie
    LIMIT 5
    """
    results = connector.execute_query(query)
    for record in results:
        print(f"  - {record['actor1']} and {record['actor2']} in '{record['movie']}'")

def test_aggregations(connector):
    """Test 6: Aggregation queries"""
    print_section("TEST 6: Aggregations")
    
    # Most prolific actors
    print("\n🌟 Most Prolific Actors:")
    query = """
    MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
    RETURN p.name as actor, count(m) as movie_count
    ORDER BY movie_count DESC
    LIMIT 5
    """
    results = connector.execute_query(query)
    for record in results:
        print(f"  - {record['actor']}: {record['movie_count']} movies")
    
    # Most prolific directors
    print("\n🎬 Most Prolific Directors:")
    query = """
    MATCH (p:Person)-[:DIRECTED]->(m:Movie)
    RETURN p.name as director, count(m) as movie_count
    ORDER BY movie_count DESC
    LIMIT 5
    """
    results = connector.execute_query(query)
    for record in results:
        print(f"  - {record['director']}: {record['movie_count']} movies")

def test_crud_operations(connector):
    """Test 7: CRUD operations"""
    print_section("TEST 7: CRUD Operations")
    
    # CREATE
    print("\n➕ Creating test nodes:")
    connector.create_node('Person', {'name': 'Test Actor', 'born': 1990})
    connector.create_node('Movie', {'title': 'Test Movie', 'year': 2024})
    print("  ✓ Created Person and Movie nodes")
    
    # CREATE relationship
    print("\n🔗 Creating relationship:")
    connector.create_relationship(
        'Person', {'name': 'Test Actor'},
        'Movie', {'title': 'Test Movie'},
        'ACTED_IN',
        {'role': 'Lead'}
    )
    print("  ✓ Created ACTED_IN relationship")
    
    # READ
    print("\n📖 Reading test nodes:")
    test_person = connector.find_nodes('Person', {'name': 'Test Actor'})
    if test_person:
        print(f"  ✓ Found: {test_person[0].get('name')}")
    
    # UPDATE
    print("\n✏️ Updating node:")
    connector.update_node(
        'Person',
        {'name': 'Test Actor'},
        {'born': 1991, 'updated': True}
    )
    print("  ✓ Updated Person node")
    
    # DELETE
    print("\n🗑️ Deleting test nodes:")
    connector.delete_node('Person', {'name': 'Test Actor'})
    connector.delete_node('Movie', {'title': 'Test Movie'})
    print("  ✓ Deleted test nodes")

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print(" "*20 + "NEO4J TEST SUITE")
    print("="*70)
    
    connector = test_connection()
    
    if not connector:
        print("\n✗ Connection failed. Cannot continue tests.")
        print("\nMake sure:")
        print("1. Neo4j Docker container is running")
        print("2. Port 7687 is accessible")
        print("3. Credentials are correct (neo4j/password123)")
        return
    
    try:
        test_database_info(connector)
        test_find_nodes(connector)
        test_find_relationships(connector)
        test_complex_queries(connector)
        test_aggregations(connector)
        test_crud_operations(connector)
        
        print("\n" + "="*70)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY")
        print("="*70)
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        connector.disconnect()
        print("\n✓ Disconnected from Neo4j\n")

if __name__ == "__main__":
    run_all_tests()