"""
Load sample movie data into HBase
Transforms MongoDB movie data into HBase column-family structure
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.hbase_connector import HBaseConnector
from connectors.mongodb_connector import MongoDBConnector

def main():
    print("\n" + "="*60)
    print(" "*15 + "HBASE MOVIE DATA LOADER")
    print("="*60)
    
    # Connect to MongoDB
    print("\n📊 Connecting to MongoDB...")
    mongo_conn = MongoDBConnector()
    if not mongo_conn.connect():
        print("✗ Failed to connect to MongoDB")
        return
    
    # Connect to HBase
    print("📊 Connecting to HBase...")
    hbase_conn = HBaseConnector()
    if not hbase_conn.connect():
        print("✗ Failed to connect to HBase")
        print("\n💡 Make sure HBase is running and Thrift server is enabled:")
        print("   docker exec -it hbase_nosql_project hbase thrift start")
        mongo_conn.disconnect()
        return
    
    try:
        table_name = 'movies'
        
        # Check if table exists and delete it
        if hbase_conn.table_exists(table_name):
            print(f"\n⚠️  Table '{table_name}' exists. Deleting...")
            hbase_conn.delete_table(table_name, disable=True)
            print("  ✓ Deleted")
        
        # Create table with column families
        print(f"\n📋 Creating table '{table_name}' with column families...")
        column_families = {
            'info': {},          # Basic movie information
            'ratings': {},       # Rating information
            'people': {},        # Cast and directors
            'metadata': {}       # Additional metadata
        }
        
        if not hbase_conn.create_table(table_name, column_families):
            print("✗ Failed to create table")
            return
        
        print("  ✓ Table created with column families:")
        for cf in column_families.keys():
            print(f"    - {cf}")
        
        # Get movies from MongoDB
        print("\n📥 Fetching movies from MongoDB...")
        movies = mongo_conn.find_many('movies', {}, limit=100)
        print(f"✓ Fetched {len(movies)} movies")
        
        if not movies:
            print("✗ No movies found in MongoDB!")
            return
        
        print("\n🔄 Loading data into HBase...")
        print("  Column family structure:")
        print("    info: title, year, plot, runtime, rated")
        print("    ratings: imdb_rating, imdb_votes")
        print("    people: directors, cast")
        print("    metadata: genres, languages")
        
        movie_count = 0
        
        for i, movie in enumerate(movies, 1):
            # Generate row key from title and year
            title = movie.get('title', 'Unknown')
            year = movie.get('year', 0)
            row_key = f"movie_{i:05d}"  # movie_00001, movie_00002, etc.
            
            # Prepare data for HBase
            data = {}
            
            # Info column family
            data['info:title'] = title
            if year:
                data['info:year'] = str(year)
            if movie.get('plot'):
                data['info:plot'] = movie['plot'][:500]  # Truncate
            if movie.get('runtime'):
                data['info:runtime'] = str(movie['runtime'])
            if movie.get('rated'):
                data['info:rated'] = movie['rated']
            
            # Ratings column family
            imdb = movie.get('imdb', {})
            if imdb.get('rating'):
                data['ratings:imdb_rating'] = str(imdb['rating'])
            if imdb.get('votes'):
                data['ratings:imdb_votes'] = str(imdb['votes'])
            
            # People column family
            directors = movie.get('directors', [])
            if directors:
                data['people:directors'] = ', '.join(directors[:5])
            
            cast = movie.get('cast', [])
            if cast:
                data['people:cast'] = ', '.join(cast[:10])
            
            # Metadata column family
            genres = movie.get('genres', [])
            if genres:
                data['metadata:genres'] = ', '.join(genres)
            
            languages = movie.get('languages', [])
            if languages:
                data['metadata:languages'] = ', '.join(languages)
            
            # Insert into HBase
            hbase_conn.put(table_name, row_key, data)
            movie_count += 1
            
            # Progress indicator
            if i % 20 == 0:
                print(f"  Progress: {i}/{len(movies)} movies loaded")
        
        # Print summary
        print("\n" + "="*60)
        print("📈 Load Summary:")
        print("="*60)
        print(f"  Movies loaded: {movie_count}")
        print(f"  Table: {table_name}")
        print(f"  Column families: {len(column_families)}")
        
        # Verify data
        print("\n🔍 Verifying data...")
        row_count = hbase_conn.count_rows(table_name)
        print(f"  ✓ Table contains {row_count} rows")
        
        # Show sample row
        print("\n📄 Sample row (movie_00001):")
        sample_row = hbase_conn.get(table_name, 'movie_00001')
        if sample_row:
            for col, val in list(sample_row.items())[:10]:
                print(f"  {col}: {val[:50] if len(val) > 50 else val}")
        
        print("\n✅ Data loaded successfully!")
        
        print("\n💡 Example HBase operations:")
        print("  # Start HBase shell:")
        print("  docker exec -it hbase_nosql_project hbase shell")
        print("\n  # List tables:")
        print("  list")
        print("\n  # Scan table:")
        print("  scan 'movies', {LIMIT => 5}")
        print("\n  # Get specific row:")
        print("  get 'movies', 'movie_00001'")
        print("\n  # Get specific column family:")
        print("  get 'movies', 'movie_00001', {COLUMN => 'info'}")
        print("\n  # Count rows:")
        print("  count 'movies'")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        mongo_conn.disconnect()
        hbase_conn.disconnect()

if __name__ == "__main__":
    main()