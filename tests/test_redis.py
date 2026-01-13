"""
Redis Connector Test Script
Tests connection and operations
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.redis_connector import RedisConnector

def print_section(title):
    """Print formatted section header"""
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_connection():
    """Test 1: Connection"""
    print_section("TEST 1: Connection")
    
    connector = RedisConnector()
    
    if connector.connect():
        print("✓ Connection successful")
        
        info = connector.get_connection_info()
        print(f"  Host: {info['host']}:{info['port']}")
        print(f"  Database: {info['db']}")
        print(f"  Redis Version: {info.get('redis_version', 'unknown')}")
        print(f"  Connected: {info['connected']}")
        
        return connector
    else:
        print("✗ Connection failed")
        return None

def test_database_info(connector):
    """Test 2: Database information"""
    print_section("TEST 2: Database Information")
    
    stats = connector.get_stats()
    
    print(f"\n📊 Redis Statistics:")
    print(f"  Redis Version: {stats.get('redis_version')}")
    print(f"  Total Keys: {stats.get('total_keys'):,}")
    print(f"  Memory Used: {stats.get('used_memory')}")
    print(f"  Connected Clients: {stats.get('connected_clients')}")
    print(f"  Commands Processed: {stats.get('total_commands_processed'):,}")
    print(f"  Hit Rate: {stats.get('hit_rate')}")

def test_string_operations(connector):
    """Test 3: String operations"""
    print_section("TEST 3: String Operations")
    
    # SET
    print("\n➕ Setting keys:")
    connector.set('test:string', 'Hello Redis!')
    connector.set('test:number', '42')
    connector.set('test:temp', 'Expires soon', ex=60)
    print("  ✓ Set 3 keys")
    
    # GET
    print("\n📖 Getting keys:")
    val1 = connector.get('test:string')
    val2 = connector.get('test:number')
    print(f"  test:string = {val1}")
    print(f"  test:number = {val2}")
    
    # EXISTS
    print("\n🔍 Checking existence:")
    exists = connector.exists('test:string', 'test:nonexistent')
    print(f"  {exists} key(s) exist")
    
    # TTL
    print("\n⏱️ Checking TTL:")
    ttl = connector.ttl('test:temp')
    print(f"  test:temp expires in {ttl} seconds")
    
    # DELETE
    print("\n🗑️ Deleting keys:")
    deleted = connector.delete('test:string', 'test:number', 'test:temp')
    print(f"  ✓ Deleted {deleted} key(s)")

def test_hash_operations(connector):
    """Test 4: Hash operations"""
    print_section("TEST 4: Hash Operations")
    
    # HSET
    print("\n➕ Creating hash:")
    connector.hset('user:1', mapping={
        'name': 'John Doe',
        'email': 'john@example.com',
        'age': '30',
        'city': 'Paris'
    })
    print("  ✓ Created user:1 hash")
    
    # HGET
    print("\n📖 Getting hash field:")
    name = connector.hget('user:1', 'name')
    print(f"  name = {name}")
    
    # HGETALL
    print("\n📖 Getting entire hash:")
    user = connector.hgetall('user:1')
    for field, value in user.items():
        print(f"  {field}: {value}")
    
    # HEXISTS
    print("\n🔍 Checking field existence:")
    exists = connector.hexists('user:1', 'email')
    print(f"  email field exists: {exists}")
    
    # HDEL
    print("\n🗑️ Deleting hash:")
    connector.delete('user:1')
    print("  ✓ Deleted user:1")

def test_list_operations(connector):
    """Test 5: List operations"""
    print_section("TEST 5: List Operations")
    
    # RPUSH
    print("\n➕ Creating list:")
    connector.rpush('movies:recent', 'Inception', 'Interstellar', 'The Prestige')
    print("  ✓ Added 3 movies")
    
    # LRANGE
    print("\n📖 Getting list:")
    movies = connector.lrange('movies:recent', 0, -1)
    print(f"  Movies: {', '.join(movies)}")
    
    # LLEN
    print("\n📏 List length:")
    length = connector.llen('movies:recent')
    print(f"  Length: {length}")
    
    # LPUSH
    print("\n➕ Adding to front:")
    connector.lpush('movies:recent', 'Tenet')
    movies = connector.lrange('movies:recent', 0, -1)
    print(f"  Movies: {', '.join(movies)}")
    
    # RPOP
    print("\n📤 Removing from end:")
    removed = connector.rpop('movies:recent')
    print(f"  Removed: {removed}")
    
    # Cleanup
    connector.delete('movies:recent')

def test_set_operations(connector):
    """Test 6: Set operations"""
    print_section("TEST 6: Set Operations")
    
    # SADD
    print("\n➕ Creating set:")
    connector.sadd('genres', 'Action', 'Drama', 'Comedy', 'Thriller')
    print("  ✓ Added 4 genres")
    
    # SMEMBERS
    print("\n📖 Getting set members:")
    genres = connector.smembers('genres')
    print(f"  Genres: {', '.join(sorted(genres))}")
    
    # SISMEMBER
    print("\n🔍 Checking membership:")
    is_member = connector.sismember('genres', 'Action')
    print(f"  'Action' in set: {is_member}")
    
    # SCARD
    print("\n📏 Set size:")
    size = connector.scard('genres')
    print(f"  Size: {size}")
    
    # SREM
    print("\n➖ Removing member:")
    connector.srem('genres', 'Comedy')
    genres = connector.smembers('genres')
    print(f"  Remaining: {', '.join(sorted(genres))}")
    
    # Cleanup
    connector.delete('genres')

def test_sorted_set_operations(connector):
    """Test 7: Sorted set operations"""
    print_section("TEST 7: Sorted Set Operations")
    
    # ZADD
    print("\n➕ Creating sorted set (movie ratings):")
    connector.zadd('top_movies', {
        'The Shawshank Redemption': 9.3,
        'The Godfather': 9.2,
        'The Dark Knight': 9.0,
        'Inception': 8.8,
        'Interstellar': 8.6
    })
    print("  ✓ Added 5 movies with ratings")
    
    # ZREVRANGE (highest to lowest)
    print("\n🏆 Top 3 movies:")
    top_3 = connector.zrevrange('top_movies', 0, 2, withscores=True)
    for i, (movie, score) in enumerate(top_3, 1):
        print(f"  {i}. {movie}: {score}")
    
    # ZSCORE
    print("\n⭐ Getting specific score:")
    score = connector.zscore('top_movies', 'Inception')
    print(f"  Inception rating: {score}")
    
    # ZCARD
    print("\n📏 Sorted set size:")
    size = connector.zcard('top_movies')
    print(f"  Total movies: {size}")
    
    # Cleanup
    connector.delete('top_movies')

def test_json_operations(connector):
    """Test 8: JSON operations"""
    print_section("TEST 8: JSON Operations")
    
    # Store JSON
    print("\n➕ Storing JSON object:")
    movie = {
        'title': 'Inception',
        'year': 2010,
        'director': 'Christopher Nolan',
        'cast': ['Leonardo DiCaprio', 'Tom Hardy'],
        'rating': 8.8
    }
    connector.set_json('movie:json:1', movie)
    print("  ✓ Stored movie as JSON")
    
    # Retrieve JSON
    print("\n📖 Retrieving JSON object:")
    retrieved = connector.get_json('movie:json:1')
    if retrieved:
        print(f"  Title: {retrieved['title']}")
        print(f"  Year: {retrieved['year']}")
        print(f"  Cast: {', '.join(retrieved['cast'])}")
    
    # Cleanup
    connector.delete('movie:json:1')

def test_movie_data(connector):
    """Test 9: Query loaded movie data"""
    print_section("TEST 9: Query Movie Data")
    
    # Check if data is loaded
    total_movies = connector.get('stats:total_movies')
    
    if not total_movies:
        print("\n⚠️  No movie data found!")
        print("  Run: python data/load_redis_movies.py")
        return
    
    print(f"\n📊 Total movies in Redis: {total_movies}")
    
    # Get a sample movie
    print("\n🎬 Sample Movie:")
    keys = connector.keys('movie:*')
    if keys:
        # Get first movie hash
        movie_keys = [k for k in keys if ':' in k and k.count(':') == 1]
        if movie_keys:
            movie_data = connector.hgetall(movie_keys[0])
            print(f"  Key: {movie_keys[0]}")
            for field, value in list(movie_data.items())[:5]:
                print(f"  {field}: {value}")
    
    # Top rated movies
    print("\n⭐ Top 5 Rated Movies:")
    top_rated = connector.zrevrange('movies:by_rating', 0, 4, withscores=True)
    for movie_id, rating in top_rated:
        movie_key = f"movie:{movie_id}"
        movie = connector.hgetall(movie_key)
        title = movie.get('title', 'Unknown')
        print(f"  - {title}: {rating}")
    
    # Genre statistics
    print("\n🎭 Genres:")
    genre_keys = connector.keys('genre:*:movies')
    genre_counts = []
    for key in genre_keys[:5]:  # Show first 5
        genre = key.split(':')[1]
        count = connector.scard(key)
        genre_counts.append((genre, count))
    
    for genre, count in sorted(genre_counts, key=lambda x: x[1], reverse=True):
        print(f"  - {genre}: {count} movies")

def test_utility_operations(connector):
    """Test 10: Utility operations"""
    print_section("TEST 10: Utility Operations")
    
    # Get all keys
    print("\n🔑 Key patterns:")
    all_keys = connector.keys('*')
    print(f"  Total keys: {len(all_keys)}")
    
    # Group by pattern
    patterns = {}
    for key in all_keys[:100]:  # Sample first 100
        pattern = key.split(':')[0] if ':' in key else 'other'
        patterns[pattern] = patterns.get(pattern, 0) + 1
    
    print("\n  Key types:")
    for pattern, count in sorted(patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"    {pattern}: {count}")
    
    # Database size
    print(f"\n📏 Database size: {connector.dbsize()} keys")

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print(" "*22 + "REDIS TEST SUITE")
    print("="*70)
    
    connector = test_connection()
    
    if not connector:
        print("\n✗ Connection failed. Cannot continue tests.")
        print("\nMake sure:")
        print("1. Redis Docker container is running")
        print("2. Port 6379 is accessible")
        return
    
    try:
        test_database_info(connector)
        test_string_operations(connector)
        test_hash_operations(connector)
        test_list_operations(connector)
        test_set_operations(connector)
        test_sorted_set_operations(connector)
        test_json_operations(connector)
        test_movie_data(connector)
        test_utility_operations(connector)
        
        print("\n" + "="*70)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY")
        print("="*70)
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        connector.disconnect()
        print("\n✓ Disconnected from Redis\n")

if __name__ == "__main__":
    run_all_tests()