"""
Load sample movie data into Neo4j
Creates a simple movie graph from MongoDB data
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.neo4j_connector import Neo4jConnector
from connectors.mongodb_connector import MongoDBConnector

def main():
    print("\n" + "="*60)
    print(" "*15 + "NEO4J MOVIE GRAPH LOADER")
    print("="*60)
    
    # Connect to MongoDB
    print("\n📊 Connecting to MongoDB...")
    mongo_conn = MongoDBConnector()
    if not mongo_conn.connect():
        print("✗ Failed to connect to MongoDB")
        return
    
    # Connect to Neo4j
    print("📊 Connecting to Neo4j...")
    neo4j_conn = Neo4jConnector()
    if not neo4j_conn.connect():
        print("✗ Failed to connect to Neo4j")
        mongo_conn.disconnect()
        return
    
    try:
        # Clear existing data
        print("\n⚠️  Clearing existing Neo4j data...")
        neo4j_conn.clear_database()
        
        # Get movies from MongoDB
        print("\n📥 Fetching movies from MongoDB...")
        movies = mongo_conn.find_many('movies', {}, limit=50)
        print(f"✓ Fetched {len(movies)} movies")
        
        if not movies:
            print("✗ No movies found in MongoDB!")
            return
        
        # Create Movie nodes
        print("\n🎬 Creating Movie nodes in Neo4j...")
        movie_count = 0
        person_count = 0
        acted_in_count = 0
        directed_count = 0
        people = set()
        
        for i, movie in enumerate(movies, 1):
            # Create Movie node
            movie_props = {
                'title': movie.get('title', 'Unknown'),
                'year': movie.get('year'),
                'plot': movie.get('plot', '')[:200] if movie.get('plot') else '',
            }
            
            # Add optional fields
            if movie.get('genres'):
                movie_props['genres'] = movie.get('genres', [])
            
            imdb = movie.get('imdb', {})
            if imdb.get('rating'):
                movie_props['imdb_rating'] = imdb.get('rating')
            if imdb.get('votes'):
                movie_props['imdb_votes'] = imdb.get('votes')
            
            if movie.get('runtime'):
                movie_props['runtime'] = movie.get('runtime')
            
            # Remove None values
            movie_props = {k: v for k, v in movie_props.items() if v is not None}
            
            # Create the movie node
            neo4j_conn.create_node('Movie', movie_props)
            movie_count += 1
            
            # Create Person nodes and relationships for cast
            cast = movie.get('cast', [])
            if cast:
                for actor_name in cast[:5]:  # Limit to 5 actors per movie
                    if actor_name and actor_name not in people:
                        neo4j_conn.create_node('Person', {'name': actor_name})
                        people.add(actor_name)
                        person_count += 1
                    
                    # Create ACTED_IN relationship
                    if actor_name:
                        try:
                            neo4j_conn.create_relationship(
                                'Person', {'name': actor_name},
                                'Movie', {'title': movie_props['title']},
                                'ACTED_IN'
                            )
                            acted_in_count += 1
                        except:
                            pass  # Skip if relationship fails
            
            # Create Person nodes and relationships for directors
            directors = movie.get('directors', [])
            if directors:
                for director_name in directors:
                    if director_name and director_name not in people:
                        neo4j_conn.create_node('Person', {'name': director_name})
                        people.add(director_name)
                        person_count += 1
                    
                    # Create DIRECTED relationship
                    if director_name:
                        try:
                            neo4j_conn.create_relationship(
                                'Person', {'name': director_name},
                                'Movie', {'title': movie_props['title']},
                                'DIRECTED'
                            )
                            directed_count += 1
                        except:
                            pass  # Skip if relationship fails
            
            # Progress indicator
            if i % 10 == 0:
                print(f"  Progress: {i}/{len(movies)} movies processed")
        
        # Print summary
        print("\n" + "="*60)
        print("📈 Graph Creation Summary:")
        print("="*60)
        print(f"  Movies created: {movie_count}")
        print(f"  People created: {person_count}")
        print(f"  ACTED_IN relationships: {acted_in_count}")
        print(f"  DIRECTED relationships: {directed_count}")
        
        print("\n✅ Movie graph created successfully!")
        print("\n💡 Open Neo4j Browser: http://localhost:7474")
        print("   Username: neo4j")
        print("   Password: password123")
        print("\n   Try these queries:")
        print("   MATCH (m:Movie) RETURN m LIMIT 25")
        print("   MATCH (m:Movie) WHERE m.year = 2010 RETURN m.title, m.imdb_rating ORDER BY m.imdb_rating DESC")
        print("   MATCH (p:Person)-[:ACTED_IN]->(m:Movie) RETURN p, m LIMIT 25")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        mongo_conn.disconnect()
        neo4j_conn.disconnect()

if __name__ == "__main__":
    main()