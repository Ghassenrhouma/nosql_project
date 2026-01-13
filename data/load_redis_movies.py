"""
Load sample movie data into Redis
Transforms MongoDB movie data into Redis key-value structures
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.redis_connector import RedisConnector
from connectors.mongodb_connector import MongoDBConnector

def main():
    print("\n" + "="*60)
    print(" "*15 + "REDIS MOVIE DATA LOADER")
    print("="*60)
    
    # Connect to MongoDB
    print("\n📊 Connecting to MongoDB...")
    mongo_conn = MongoDBConnector()
    if not mongo_conn.connect():
        print("✗ Failed to connect to MongoDB")
        return
    
    # Connect to Redis
    print("📊 Connecting to Redis...")
    redis_conn = RedisConnector()
    if not redis_conn.connect():
        print("✗ Failed to connect to Redis")
        mongo_conn.disconnect()
        return
    
    try:
        # Clear existing data
        print("\n⚠️  Clearing existing Redis data...")
        redis_conn.flushdb()
        
        # Get movies from MongoDB
        print("\n📥 Fetching movies from MongoDB...")
        movies = mongo_conn.find_many('movies', {}, limit=100)
        print(f"✓ Fetched {len(movies)} movies")
        
        if not movies:
            print("✗ No movies found in MongoDB!")
            return
        
        print("\n🔄 Transforming and loading data into Redis...")
        
        movie_count = 0
        genre_sets = {}
        
        for i, movie in enumerate(movies, 1):
            movie_id = movie.get('_id', str(i))
            title = movie.get('title', 'Unknown')
            year = movie.get('year')
            
            # 1. Store movie as hash
            movie_key = f"movie:{movie_id}"
            movie_data = {
                'title': title,
                'year': str(year) if year else 'unknown',
                'plot': movie.get('plot', '')[:200],
                'runtime': str(movie.get('runtime', 'unknown')),
                'rated': movie.get('rated', 'N/A'),
            }
            
            # Add IMDb data
            imdb = movie.get('imdb', {})
            if imdb.get('rating'):
                movie_data['imdb_rating'] = str(imdb['rating'])
            if imdb.get('votes'):
                movie_data['imdb_votes'] = str(imdb['votes'])
            
            redis_conn.hset(movie_key, mapping=movie_data)
            movie_count += 1
            
            # 2. Store genres as sets
            genres = movie.get('genres', [])
            if genres:
                # Store genres for this movie
                genre_key = f"movie:{movie_id}:genres"
                redis_conn.sadd(genre_key, *genres)
                
                # Add movie to each genre's set
                for genre in genres:
                    genre_movies_key = f"genre:{genre}:movies"
                    redis_conn.sadd(genre_movies_key, movie_id)
                    if genre not in genre_sets:
                        genre_sets[genre] = 0
                    genre_sets[genre] += 1
            
            # 3. Add to sorted set by rating (if available)
            if imdb.get('rating'):
                redis_conn.zadd('movies:by_rating', {movie_id: float(imdb['rating'])})
            
            # 4. Add to sorted set by year (if available)
            if year:
                redis_conn.zadd('movies:by_year', {movie_id: float(year)})
            
            # 5. Store cast as list (if available)
            cast = movie.get('cast', [])
            if cast:
                cast_key = f"movie:{movie_id}:cast"
                redis_conn.rpush(cast_key, *cast[:10])  # Limit to 10 actors
            
            # 6. Store directors as list (if available)
            directors = movie.get('directors', [])
            if directors:
                directors_key = f"movie:{movie_id}:directors"
                redis_conn.rpush(directors_key, *directors)
            
            # Progress
            if i % 20 == 0:
                print(f"  Progress: {i}/{len(movies)} movies processed")
        
        # Store metadata
        redis_conn.set('stats:total_movies', str(movie_count))
        redis_conn.set('stats:total_genres', str(len(genre_sets)))
        
        # Print summary
        print("\n" + "="*60)
        print("📈 Load Summary:")
        print("="*60)
        print(f"  Movies loaded: {movie_count}")
        print(f"  Genres found: {len(genre_sets)}")
        print(f"  Total Redis keys: {redis_conn.dbsize()}")
        
        print("\n📊 Top Genres:")
        sorted_genres = sorted(genre_sets.items(), key=lambda x: x[1], reverse=True)
        for genre, count in sorted_genres[:10]:
            print(f"  - {genre}: {count} movies")
        
        print("\n✅ Data loaded successfully!")
        
        print("\n💡 Example Redis commands to try:")
        print("   # Get a movie")
        print(f"   HGETALL movie:{movies[0].get('_id')}")
        print("   ")
        print("   # Get top rated movies")
        print("   ZREVRANGE movies:by_rating 0 9 WITHSCORES")
        print("   ")
        print("   # Get movies in a genre")
        print("   SMEMBERS genre:Action:movies")
        print("   ")
        print("   # Get movie cast")
        print(f"   LRANGE movie:{movies[0].get('_id')}:cast 0 -1")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        mongo_conn.disconnect()
        redis_conn.disconnect()

if __name__ == "__main__":
    main()