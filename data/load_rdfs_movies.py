"""
Load sample movie data into RDF store (Fuseki)
Transforms MongoDB movie data into RDF triples
"""

import sys
import os
import re

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.rdf_connector import RDFConnector
from connectors.mongodb_connector import MongoDBConnector

def clean_string(s: str) -> str:
    """
    Clean string for safe use in RDF (removes control characters)
    
    Args:
        s: String to clean
    
    Returns:
        Cleaned string
    """
    if not s:
        return ""
    
    # Remove control characters
    s = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', s)
    
    # Remove multiple spaces
    s = re.sub(r'\s+', ' ', s)
    
    return s.strip()

def safe_uri_part(s: str) -> str:
    """
    Create safe URI part from string
    
    Args:
        s: String to convert
    
    Returns:
        Safe URI part
    """
    if not s:
        return "unknown"
    
    # Replace non-alphanumeric with underscore
    safe = re.sub(r'[^\w\s-]', '', s)
    safe = re.sub(r'[\s-]+', '_', safe)
    
    return safe[:100]  # Limit length

def main():
    print("\n" + "="*60)
    print(" "*15 + "RDF MOVIE DATA LOADER")
    print("="*60)
    
    # Connect to MongoDB
    print("\n📊 Connecting to MongoDB...")
    mongo_conn = MongoDBConnector()
    if not mongo_conn.connect():
        print("✗ Failed to connect to MongoDB")
        return
    
    # Connect to RDF store
    print("📊 Connecting to RDF store (Fuseki)...")
    rdf_conn = RDFConnector()
    if not rdf_conn.connect():
        print("✗ Failed to connect to RDF store")
        print("\n💡 Make sure:")
        print("1. Fuseki container is running")
        print("2. Dataset 'movies' exists")
        print("3. Access http://localhost:3030 to create dataset")
        mongo_conn.disconnect()
        return
    
    try:
        # Clear existing data
        print("\n⚠️  Clearing existing RDF data...")
        rdf_conn.clear_graph()
        print("  ✓ Cleared")
        
        # Get movies from MongoDB
        print("\n📥 Fetching movies from MongoDB...")
        movies = mongo_conn.find_many('movies', {}, limit=50)
        print(f"✓ Fetched {len(movies)} movies")
        
        if not movies:
            print("✗ No movies found in MongoDB!")
            return
        
        print("\n🔄 Converting to RDF triples...")
        print("  Creating:")
        print("    - Movie instances")
        print("    - Person instances (actors, directors)")
        print("    - Genre instances")
        print("    - Relationships between entities")
        
        total_triples = 0
        movie_count = 0
        people = set()
        genres = set()
        
        # Batch triples for efficiency
        batch_triples = []
        
        for i, movie in enumerate(movies, 1):
            movie_id = f"movie_{i:05d}"
            movie_uri = f"http://example.org/movie/{movie_id}"
            
            # Clean strings
            title = clean_string(movie.get('title', 'Unknown'))
            year = movie.get('year')
            
            # Movie type
            batch_triples.append((
                movie_uri,
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://example.org/Movie"
            ))
            
            # Movie properties
            batch_triples.append((movie_uri, "http://example.org/title", title))
            
            if year:
                batch_triples.append((movie_uri, "http://example.org/year", str(year)))
            
            if movie.get('plot'):
                plot = clean_string(movie['plot'][:200])  # Truncate
                if plot:
                    batch_triples.append((movie_uri, "http://example.org/plot", plot))
            
            if movie.get('runtime'):
                batch_triples.append((movie_uri, "http://example.org/runtime", str(movie['runtime'])))
            
            if movie.get('rated'):
                rated = clean_string(movie['rated'])
                if rated:
                    batch_triples.append((movie_uri, "http://example.org/rated", rated))
            
            # IMDb rating
            imdb = movie.get('imdb', {})
            if imdb.get('rating'):
                batch_triples.append((movie_uri, "http://example.org/imdbRating", str(imdb['rating'])))
            
            # Genres
            for genre in movie.get('genres', [])[:5]:
                if genre:
                    genre_clean = clean_string(genre)
                    if not genre_clean:
                        continue
                    
                    genre_safe = safe_uri_part(genre_clean)
                    genre_uri = f"http://example.org/genre/{genre_safe}"
                    
                    # Create genre if not exists
                    if genre not in genres:
                        batch_triples.append((
                            genre_uri,
                            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                            "http://example.org/Genre"
                        ))
                        batch_triples.append((genre_uri, "http://example.org/name", genre_clean))
                        genres.add(genre)
                    
                    # Link movie to genre
                    batch_triples.append((movie_uri, "http://example.org/hasGenre", genre_uri))
            
            # Directors
            for director in movie.get('directors', [])[:3]:
                if director:
                    director_clean = clean_string(director)
                    if not director_clean:
                        continue
                    
                    director_safe = safe_uri_part(director_clean)
                    person_uri = f"http://example.org/person/{director_safe}"
                    
                    # Create person if not exists
                    if director not in people:
                        batch_triples.append((
                            person_uri,
                            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                            "http://example.org/Person"
                        ))
                        batch_triples.append((person_uri, "http://example.org/name", director_clean))
                        batch_triples.append((person_uri, "http://example.org/role", "Director"))
                        people.add(director)
                    
                    # Link movie to director
                    batch_triples.append((movie_uri, "http://example.org/directedBy", person_uri))
            
            # Cast (actors)
            for actor in movie.get('cast', [])[:5]:
                if actor:
                    actor_clean = clean_string(actor)
                    if not actor_clean:
                        continue
                    
                    actor_safe = safe_uri_part(actor_clean)
                    person_uri = f"http://example.org/person/{actor_safe}"
                    
                    # Create person if not exists
                    if actor not in people:
                        batch_triples.append((
                            person_uri,
                            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                            "http://example.org/Person"
                        ))
                        batch_triples.append((person_uri, "http://example.org/name", actor_clean))
                        batch_triples.append((person_uri, "http://example.org/role", "Actor"))
                        people.add(actor)
                    
                    # Link movie to actor
                    batch_triples.append((movie_uri, "http://example.org/starring", person_uri))
            
            movie_count += 1
            
            # Insert in batches of 500 triples
            if len(batch_triples) >= 500:
                success = rdf_conn.insert_triples(batch_triples)
                if success:
                    total_triples += len(batch_triples)
                batch_triples = []
                print(f"  Progress: {i}/{len(movies)} movies, {total_triples} triples inserted")
        
        # Insert remaining triples
        if batch_triples:
            success = rdf_conn.insert_triples(batch_triples)
            if success:
                total_triples += len(batch_triples)
        
        # Print summary
        print("\n" + "="*60)
        print("📈 Load Summary:")
        print("="*60)
        print(f"  Movies loaded: {movie_count}")
        print(f"  People created: {len(people)}")
        print(f"  Genres created: {len(genres)}")
        print(f"  Total triples: {total_triples}")
        
        # Verify data
        print("\n🔍 Verifying data...")
        triple_count = rdf_conn.count_triples()
        print(f"  ✓ RDF store contains {triple_count:,} triples")
        
        # Get classes
        classes = rdf_conn.get_classes()
        print(f"\n📊 RDF Classes ({len(classes)}):")
        for cls in classes[:10]:
            print(f"  - {cls}")
        
        # Sample query
        print("\n📄 Sample movies (SPARQL query):")
        query = """
        PREFIX ex: <http://example.org/>
        SELECT ?movie ?title ?year WHERE {
            ?movie a ex:Movie .
            ?movie ex:title ?title .
            OPTIONAL { ?movie ex:year ?year }
        }
        ORDER BY ?year
        LIMIT 5
        """
        results = rdf_conn.execute_query(query)
        for result in results:
            year = result.get('year', 'N/A')
            print(f"  - {result['title']} ({year})")
        
        print("\n✅ Data loaded successfully!")
        
        print("\n💡 Example SPARQL queries:")
        print("\n  # Find all movies:")
        print("  PREFIX ex: <http://example.org/>")
        print("  SELECT ?title WHERE { ?movie a ex:Movie ; ex:title ?title } LIMIT 10")
        
        print("\n  # Find movies by genre:")
        print("  PREFIX ex: <http://example.org/>")
        print("  SELECT ?title ?genre WHERE {")
        print("    ?movie a ex:Movie ;")
        print("           ex:title ?title ;")
        print("           ex:hasGenre ?g .")
        print("    ?g ex:name ?genre .")
        print("  } LIMIT 10")
        
        print("\n  # Find movies by director:")
        print("  PREFIX ex: <http://example.org/>")
        print("  SELECT ?title ?director WHERE {")
        print("    ?movie a ex:Movie ;")
        print("           ex:title ?title ;")
        print("           ex:directedBy ?d .")
        print("    ?d ex:name ?director .")
        print("  } LIMIT 10")
        
        print("\n💡 Access Fuseki UI: http://localhost:3030")
        print("   Dataset: movies")
        print("   Query endpoint: http://localhost:3030/movies/query")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        mongo_conn.disconnect()
        rdf_conn.disconnect()

if __name__ == "__main__":
    main()