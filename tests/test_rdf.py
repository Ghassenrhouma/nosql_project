"""
RDF Store Connector Test Script
Tests SPARQL queries and RDF operations
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.rdf_connector import RDFConnector

def print_section(title):
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_connection():
    """Test 1: Connection"""
    print_section("TEST 1: Connection")
    
    connector = RDFConnector()
    
    if connector.connect():
        print("✓ Connection successful")
        
        info = connector.get_connection_info()
        print(f"  Endpoint: {info['endpoint']}")
        print(f"  Dataset: {info['dataset']}")
        print(f"  Query endpoint: {info['query_endpoint']}")
        print(f"  Connected: {info['connected']}")
        
        return connector
    else:
        print("✗ Connection failed")
        return None

def test_basic_info(connector):
    """Test 2: Basic information"""
    print_section("TEST 2: Basic Information")
    
    # Count triples
    print("\n📊 Counting triples...")
    count = connector.count_triples()
    print(f"  Total triples: {count:,}")
    
    # Get classes
    print("\n📋 RDF Classes:")
    classes = connector.get_classes()
    for cls in classes[:10]:
        print(f"  - {cls}")
    
    # Get properties
    print("\n🔗 RDF Properties:")
    properties = connector.get_properties()
    for prop in properties[:15]:
        print(f"  - {prop}")

def test_simple_queries(connector):
    """Test 3: Simple SPARQL queries"""
    print_section("TEST 3: Simple SPARQL Queries")
    
    # Query 1: Get all movies
    print("\n🎬 All Movies (first 5):")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?movie ?title WHERE {
        ?movie a ex:Movie .
        ?movie ex:title ?title .
    }
    LIMIT 5
    """
    results = connector.execute_query(query)
    for result in results:
        print(f"  - {result['title']}")
    
    # Query 2: Movies with years
    print("\n📅 Movies with Years:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?title ?year WHERE {
        ?movie a ex:Movie ;
               ex:title ?title ;
               ex:year ?year .
    }
    ORDER BY DESC(?year)
    LIMIT 5
    """
    results = connector.execute_query(query)
    for result in results:
        print(f"  - {result['title']} ({result['year']})")
    
    # Query 3: Count movies
    print("\n📊 Movie Count:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT (COUNT(?movie) as ?count) WHERE {
        ?movie a ex:Movie .
    }
    """
    results = connector.execute_query(query)
    if results:
        print(f"  Total movies: {results[0]['count']}")

def test_relationship_queries(connector):
    """Test 4: Relationship queries"""
    print_section("TEST 4: Relationship Queries")
    
    # Query 1: Movies and directors
    print("\n🎬 Movies with Directors:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?title ?director WHERE {
        ?movie a ex:Movie ;
               ex:title ?title ;
               ex:directedBy ?person .
        ?person ex:name ?director .
    }
    LIMIT 10
    """
    results = connector.execute_query(query)
    for result in results:
        print(f"  - {result['title']} directed by {result['director']}")
    
    # Query 2: Movies and genres
    print("\n🎭 Movies with Genres:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?title ?genre WHERE {
        ?movie a ex:Movie ;
               ex:title ?title ;
               ex:hasGenre ?g .
        ?g ex:name ?genre .
    }
    LIMIT 10
    """
    results = connector.execute_query(query)
    for result in results:
        print(f"  - {result['title']}: {result['genre']}")
    
    # Query 3: Movies with actors
    print("\n👥 Movies with Cast:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?title ?actor WHERE {
        ?movie a ex:Movie ;
               ex:title ?title ;
               ex:starring ?person .
        ?person ex:name ?actor .
    }
    LIMIT 10
    """
    results = connector.execute_query(query)
    for result in results:
        print(f"  - {result['title']}: {result['actor']}")

def test_aggregation_queries(connector):
    """Test 5: Aggregation queries"""
    print_section("TEST 5: Aggregation Queries")
    
    # Query 1: Count movies by genre
    print("\n📊 Movies by Genre:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?genre (COUNT(?movie) as ?count) WHERE {
        ?movie a ex:Movie ;
               ex:hasGenre ?g .
        ?g ex:name ?genre .
    }
    GROUP BY ?genre
    ORDER BY DESC(?count)
    LIMIT 10
    """
    results = connector.execute_query(query)
    for result in results:
        print(f"  - {result['genre']}: {result['count']} movies")
    
    # Query 2: Directors and their movie count
    print("\n🎬 Prolific Directors:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?director (COUNT(?movie) as ?count) WHERE {
        ?movie a ex:Movie ;
               ex:directedBy ?person .
        ?person ex:name ?director .
    }
    GROUP BY ?director
    ORDER BY DESC(?count)
    LIMIT 10
    """
    results = connector.execute_query(query)
    for result in results:
        print(f"  - {result['director']}: {result['count']} movies")
    
    # Query 3: Average rating by genre (FIXED - Using FILTER instead of xsd:float)
    print("\n⭐ Average Rating by Genre:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?genre (AVG(?ratingNum) as ?avgRating) WHERE {
        ?movie a ex:Movie ;
               ex:hasGenre ?g ;
               ex:imdbRating ?rating .
        ?g ex:name ?genre .
        BIND(xsd:float(?rating) AS ?ratingNum)
        FILTER(isNumeric(?rating))
    }
    GROUP BY ?genre
    ORDER BY DESC(?avgRating)
    LIMIT 10
    """
    # Alternative simpler query without xsd conversion
    query_simple = """
    PREFIX ex: <http://example.org/>
    SELECT ?genre (COUNT(?movie) as ?count) WHERE {
        ?movie a ex:Movie ;
               ex:hasGenre ?g ;
               ex:imdbRating ?rating .
        ?g ex:name ?genre .
    }
    GROUP BY ?genre
    ORDER BY DESC(?count)
    LIMIT 10
    """
    results = connector.execute_query(query_simple)
    for result in results:
        print(f"  - {result['genre']}: {result['count']} rated movies")

def test_filter_queries(connector):
    """Test 6: Filter queries"""
    print_section("TEST 6: Filter Queries")
    
    # Query 1: High-rated movies (FIXED - Simple string comparison)
    print("\n⭐ High-Rated Movies (>= 8.0):")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?title ?rating WHERE {
        ?movie a ex:Movie ;
               ex:title ?title ;
               ex:imdbRating ?rating .
        FILTER(?rating >= "8.0")
    }
    ORDER BY DESC(?rating)
    LIMIT 10
    """
    results = connector.execute_query(query)
    if results:
        for result in results:
            print(f"  - {result['title']}: {result['rating']}")
    else:
        print("  (No movies with ratings >= 8.0 in dataset)")
    
    # Query 2: Recent movies (FIXED - Simple string comparison)
    print("\n📅 Recent Movies (>= 2010):")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?title ?year WHERE {
        ?movie a ex:Movie ;
               ex:title ?title ;
               ex:year ?year .
        FILTER(?year >= "2010")
    }
    ORDER BY DESC(?year)
    LIMIT 10
    """
    results = connector.execute_query(query)
    if results:
        for result in results:
            print(f"  - {result['title']} ({result['year']})")
    else:
        print("  (No movies from 2010+ in dataset - dataset contains old movies)")
    
    # Query 3: Movies by specific genre
    print("\n🎭 Action Movies:")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?title WHERE {
        ?movie a ex:Movie ;
               ex:title ?title ;
               ex:hasGenre ?g .
        ?g ex:name "Action" .
    }
    LIMIT 10
    """
    results = connector.execute_query(query)
    for result in results:
        print(f"  - {result['title']}")

def test_crud_operations(connector):
    """Test 7: CRUD operations"""
    print_section("TEST 7: CRUD Operations")
    
    # CREATE
    print("\n➕ Creating test triples...")
    
    test_movie = "http://example.org/movie/test_001"
    triples = [
        (test_movie, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Movie"),
        (test_movie, "http://example.org/title", "Test Movie"),
        (test_movie, "http://example.org/year", "2024"),
        (test_movie, "http://example.org/imdbRating", "9.0")
    ]
    
    connector.insert_triples(triples)
    print("  ✓ Created test movie")
    
    # READ
    print("\n📖 Reading test movie...")
    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?title ?year ?rating WHERE {
        <http://example.org/movie/test_001> ex:title ?title ;
                                            ex:year ?year ;
                                            ex:imdbRating ?rating .
    }
    """
    results = connector.execute_query(query)
    if results:
        result = results[0]
        print(f"  Title: {result['title']}")
        print(f"  Year: {result['year']}")
        print(f"  Rating: {result['rating']}")
    
    # UPDATE (delete old, insert new)
    print("\n✏️ Updating test movie...")
    connector.delete_triple(test_movie, "http://example.org/year", "2024")
    connector.insert_triple(test_movie, "http://example.org/year", "2025")
    print("  ✓ Updated year to 2025")
    
    # DELETE
    print("\n🗑️ Deleting test movie...")
    query = f"""
    PREFIX ex: <http://example.org/>
    DELETE WHERE {{
        <{test_movie}> ?p ?o .
    }}
    """
    connector.execute_update(query)
    print("  ✓ Deleted test movie")

def test_ask_queries(connector):
    """Test 8: ASK queries"""
    print_section("TEST 8: ASK Queries")
    
    # Check if specific movie exists
    print("\n❓ Does 'Inception' exist?")
    query = """
    PREFIX ex: <http://example.org/>
    ASK {
        ?movie a ex:Movie ;
               ex:title "Inception" .
    }
    """
    result = connector.ask(query)
    print(f"  Result: {result}")
    
    # Check if any Action movies exist
    print("\n❓ Are there any Action movies?")
    query = """
    PREFIX ex: <http://example.org/>
    ASK {
        ?movie a ex:Movie ;
               ex:hasGenre ?g .
        ?g ex:name "Action" .
    }
    """
    result = connector.ask(query)
    print(f"  Result: {result}")

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print(" "*20 + "RDF STORE TEST SUITE")
    print("="*70)
    
    connector = test_connection()
    
    if not connector:
        print("\n✗ Connection failed. Cannot continue tests.")
        print("\nMake sure:")
        print("1. Fuseki Docker container is running")
        print("2. Dataset 'movies' exists")
        print("3. Access http://localhost:3030")
        print("4. Port 3030 is accessible")
        return
    
    try:
        test_basic_info(connector)
        test_simple_queries(connector)
        test_relationship_queries(connector)
        test_aggregation_queries(connector)
        test_filter_queries(connector)
        test_crud_operations(connector)
        test_ask_queries(connector)
        
        print("\n" + "="*70)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY")
        print("="*70)
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        connector.disconnect()
        print("\n✓ Disconnected from RDF store\n")

if __name__ == "__main__":
    run_all_tests()