"""
MongoDB Connector Test Script for Movies-Only Setup
Tests connection and queries on just the movies collection
"""

import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.mongodb_connector import MongoDBConnector

def print_section(title):
    """Print a formatted section header"""
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_connection():
    """Test 1: Connection to MongoDB"""
    print_section("TEST 1: Connection")
    
    connector = MongoDBConnector()
    
    if connector.connect():
        print("✓ Connection successful")
        print(f"  Database: {connector.database_name}")
        
        if connector.test_connection():
            print("✓ Connection is alive")
        
        info = connector.get_connection_info()
        print(f"  Host: {info['host']}:{info['port']}")
        
        return connector
    else:
        print("✗ Connection failed")
        return None

def test_database_structure(connector):
    """Test 2: Explore database structure"""
    print_section("TEST 2: Database Structure")
    
    # List all databases
    databases = connector.get_databases()
    print(f"\n📚 Databases ({len(databases)}):")
    for db in databases:
        print(f"  - {db}")
    
    # List collections in sample_mflix
    collections = connector.get_collections('sample_mflix')
    print(f"\n📁 Collections in 'sample_mflix' ({len(collections)}):")
    for coll in collections:
        count = connector.count_documents(coll)
        print(f"  - {coll}: {count:,} documents")

def test_movies_stats(connector):
    """Test 3: Get movies collection statistics"""
    print_section("TEST 3: Movies Collection Statistics")
    
    stats = connector.get_collection_stats('movies')
    
    if stats and stats.get('collection'):
        print(f"\n📊 {stats['collection']}:")
        print(f"  Documents: {stats['document_count']:,}")
        print(f"  Size: {stats['size_bytes']:,} bytes ({stats['size_bytes']/1024/1024:.2f} MB)")
        print(f"  Avg doc size: {stats['avg_doc_size_bytes']:,} bytes")
        print(f"  Indexes: {stats['indexes']}")
        
        if stats['fields']:
            print(f"  Fields ({len(stats['fields'])}): {', '.join(stats['fields'][:15])}")
            if len(stats['fields']) > 15:
                print(f"         ... and {len(stats['fields']) - 15} more")
    else:
        print("✗ Could not get collection stats")

def test_basic_queries(connector):
    """Test 4: Basic find operations"""
    print_section("TEST 4: Basic Queries")
    
    # Query 1: Find one movie
    print("\n🎬 Sample Movie:")
    movie = connector.find_one('movies')
    if movie:
        print(f"  Title: {movie.get('title', 'N/A')}")
        print(f"  Year: {movie.get('year', 'N/A')}")
        print(f"  Genres: {', '.join(movie.get('genres', []))}")
        directors = movie.get('directors', [])
        if directors:
            print(f"  Directors: {', '.join(directors)}")
        imdb = movie.get('imdb', {})
        if imdb:
            print(f"  IMDb Rating: {imdb.get('rating', 'N/A')}")
    
    # Query 2: Count total movies
    total = connector.count_documents('movies')
    print(f"\n📊 Total Movies: {total:,}")
    
    # Query 3: Find movies from 2015
    print("\n🎬 Movies from 2015:")
    movies_2015 = connector.find_many('movies', {'year': 2015}, limit=5)
    print(f"  Found {len(movies_2015)} (showing first 5):")
    for movie in movies_2015:
        print(f"    - {movie.get('title')}")
    
    # Query 4: Count movies by year
    year_count = connector.count_documents('movies', {'year': 2015})
    print(f"  Total 2015 movies: {year_count}")

def test_filtered_queries(connector):
    """Test 5: Complex filtered queries"""
    print_section("TEST 5: Filtered Queries")
    
    # Query 1: Action movies
    print("\n🎬 Action Movies (sample):")
    action_movies = connector.find_many('movies', {'genres': 'Action'}, limit=5)
    action_count = connector.count_documents('movies', {'genres': 'Action'})
    print(f"  Total action movies: {action_count:,}")
    print(f"  Showing first {len(action_movies)}:")
    for movie in action_movies:
        print(f"    - {movie.get('title')} ({movie.get('year')})")
    
    # Query 2: Highly rated movies
    print("\n⭐ Highly Rated Movies (>= 8.5):")
    high_rated = connector.find_many(
        'movies',
        {'imdb.rating': {'$gte': 8.5}},
        limit=5,
        sort=[('imdb.rating', -1)]
    )
    print(f"  Found {len(high_rated)} (showing top 5):")
    for movie in high_rated:
        rating = movie.get('imdb', {}).get('rating', 'N/A')
        print(f"    - {movie.get('title')} ({movie.get('year')}): {rating}")
    
    # Query 3: Movies by specific director (if exists)
    print("\n🎬 Movies by Christopher Nolan:")
    nolan_movies = connector.find_many(
        'movies',
        {'directors': 'Christopher Nolan'},
        limit=10
    )
    if nolan_movies:
        print(f"  Found {len(nolan_movies)} movies:")
        for movie in nolan_movies:
            print(f"    - {movie.get('title')} ({movie.get('year')})")
    else:
        print("  No movies found (try another director)")

def test_aggregation(connector):
    """Test 6: Aggregation queries"""
    print_section("TEST 6: Aggregation Queries")
    
    # Aggregation 1: Count movies by genre
    print("\n📊 Top 10 Genres by Movie Count:")
    genre_pipeline = [
        {'$unwind': '$genres'},
        {'$group': {'_id': '$genres', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    genres = connector.aggregate('movies', genre_pipeline)
    for i, genre in enumerate(genres, 1):
        print(f"  {i}. {genre['_id']}: {genre['count']:,} movies")
    
    # Aggregation 2: Movies by decade
    print("\n📊 Movies by Decade:")
    decade_pipeline = [
        {'$match': {'year': {'$gte': 1900, '$lte': 2020}}},
        {'$addFields': {'decade': {'$subtract': ['$year', {'$mod': ['$year', 10]}]}}},
        {'$group': {'_id': '$decade', 'count': {'$sum': 1}}},
        {'$sort': {'_id': 1}}
    ]
    decades = connector.aggregate('movies', decade_pipeline)
    for decade in decades:
        print(f"  {decade['_id']}s: {decade['count']:,} movies")
    
    # Aggregation 3: Average rating by genre
    print("\n📊 Average IMDb Rating by Genre (Top 5, min 100 movies):")
    rating_pipeline = [
        {'$match': {'imdb.rating': {'$exists': True}}},
        {'$unwind': '$genres'},
        {'$group': {
            '_id': '$genres',
            'avg_rating': {'$avg': '$imdb.rating'},
            'count': {'$sum': 1}
        }},
        {'$match': {'count': {'$gte': 100}}},
        {'$sort': {'avg_rating': -1}},
        {'$limit': 5}
    ]
    ratings = connector.aggregate('movies', rating_pipeline)
    for rating in ratings:
        print(f"  {rating['_id']}: {rating['avg_rating']:.2f} ({rating['count']} movies)")

def test_distinct_values(connector):
    """Test 7: Get distinct values"""
    print_section("TEST 7: Distinct Values")
    
    # Get distinct genres
    print("\n🎭 All Genres:")
    genres = connector.get_distinct_values('movies', 'genres')
    genres_sorted = sorted([g for g in genres if g])
    print(f"  Found {len(genres_sorted)} unique genres:")
    for i in range(0, len(genres_sorted), 5):
        print(f"  {', '.join(genres_sorted[i:i+5])}")
    
    # Get years with movies
    print("\n📅 Year Range:")
    years = connector.get_distinct_values('movies', 'year')
    years = sorted([y for y in years if y and isinstance(y, int)])
    if years:
        print(f"  From {min(years)} to {max(years)}")
        print(f"  Total: {len(years)} different years")
    
    # Get sample directors
    print("\n🎬 Sample Directors:")
    all_directors = connector.get_distinct_values('movies', 'directors')
    all_directors = sorted([d for d in all_directors if d])
    print(f"  Total unique directors: {len(all_directors):,}")
    print(f"  First 20: {', '.join(all_directors[:20])}")

def test_crud_operations(connector):
    """Test 8: CRUD operations on test collection"""
    print_section("TEST 8: CRUD Operations (Test Collection)")
    
    test_collection = 'test_movies'
    
    # Clean up from previous tests
    connector.delete_many(test_collection, {})
    
    # INSERT
    print("\n➕ Testing INSERT:")
    doc1 = {
        'title': 'Test Movie',
        'year': 2024,
        'genres': ['Drama', 'Action'],
        'imdb': {'rating': 8.5}
    }
    doc_id = connector.insert_one(test_collection, doc1)
    print(f"  ✓ Inserted document with ID: {doc_id}")
    
    # INSERT MANY
    docs = [
        {'title': 'Movie 2', 'year': 2024, 'genres': ['Comedy']},
        {'title': 'Movie 3', 'year': 2024, 'genres': ['Action']},
        {'title': 'Movie 4', 'year': 2023, 'genres': ['Drama']}
    ]
    count = connector.insert_many(test_collection, docs)
    print(f"  ✓ Inserted {count} more documents")
    
    # READ
    print("\n📖 Testing READ:")
    all_docs = connector.find_many(test_collection, {})
    print(f"  ✓ Found {len(all_docs)} documents")
    for doc in all_docs:
        print(f"    - {doc.get('title')}")
    
    # UPDATE
    print("\n✏️ Testing UPDATE:")
    modified = connector.update_one(
        test_collection,
        {'title': 'Test Movie'},
        {'$set': {'imdb': {'rating': 9.0}, 'updated': True}}
    )
    print(f"  ✓ Updated {modified} document")
    
    # Verify update
    updated_doc = connector.find_one(test_collection, {'title': 'Test Movie'})
    if updated_doc:
        print(f"  New rating: {updated_doc.get('imdb', {}).get('rating')}")
    
    # DELETE
    print("\n🗑️ Testing DELETE:")
    deleted = connector.delete_many(test_collection, {})
    print(f"  ✓ Deleted {deleted} documents (cleanup)")
    
    remaining = connector.count_documents(test_collection)
    print(f"  ✓ Remaining documents: {remaining}")

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print(" "*15 + "MONGODB MOVIES-ONLY TEST SUITE")
    print("="*70)
    
    # Test connection
    connector = test_connection()
    
    if not connector:
        print("\n✗ Connection failed. Cannot continue tests.")
        print("\nMake sure:")
        print("1. MongoDB Docker container is running")
        print("2. Movies data has been loaded")
        print("3. Port 27017 is accessible")
        return
    
    try:
        # Run all tests
        test_database_structure(connector)
        test_movies_stats(connector)
        test_basic_queries(connector)
        test_filtered_queries(connector)
        test_aggregation(connector)
        test_distinct_values(connector)
        test_crud_operations(connector)
        
        print("\n" + "="*70)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY")
        print("="*70)
        print("\n💡 Note: You're currently using only the movies collection.")
        print("   To get the full sample dataset, run: python data/reimport_mflix.py")
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        connector.disconnect()
        print("\n✓ Disconnected from MongoDB\n")

if __name__ == "__main__":
    run_all_tests()