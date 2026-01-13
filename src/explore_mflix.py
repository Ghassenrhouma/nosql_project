"""
Interactive exploration of sample_mflix database
Shows example queries useful for NLQ translation testing
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from connectors.mongodb_connector import MongoDBConnector

def print_header(title):
    """Print formatted header"""
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def explore_movies(connector):
    """Explore movies collection with various queries"""
    print_header("🎬 EXPLORING MOVIES")
    
    # Basic stats
    total = connector.count_documents('movies')
    print(f"\nTotal movies: {total:,}")
    
    # Query examples that would be good for NLQ
    queries = [
        {
            'description': "Movies from 2015",
            'query': {'year': 2015},
            'limit': 3
        },
        {
            'description': "Action movies with high ratings",
            'query': {'genres': 'Action', 'imdb.rating': {'$gte': 8.0}},
            'limit': 5
        },
        {
            'description': "Movies directed by Christopher Nolan",
            'query': {'directors': 'Christopher Nolan'},
            'limit': 10
        },
        {
            'description': "Recent Drama movies (2015-2020)",
            'query': {'genres': 'Drama', 'year': {'$gte': 2015, '$lte': 2020}},
            'limit': 5
        }
    ]
    
    for q in queries:
        print(f"\n📝 {q['description']}:")
        results = connector.find_many('movies', q['query'], limit=q['limit'])
        print(f"   Found: {len(results)} results")
        for movie in results:
            title = movie.get('title', 'N/A')
            year = movie.get('year', 'N/A')
            rating = movie.get('imdb', {}).get('rating', 'N/A')
            print(f"   - {title} ({year}) - Rating: {rating}")

def explore_aggregations(connector):
    """Show aggregation examples"""
    print_header("📊 AGGREGATION EXAMPLES")
    
    # Example 1: Count by genre
    print("\n📊 Movie Count by Genre:")
    pipeline = [
        {'$unwind': '$genres'},
        {'$group': {'_id': '$genres', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    results = connector.aggregate('movies', pipeline)
    for r in results:
        print(f"   {r['_id']}: {r['count']:,}")
    
    # Example 2: Average rating by decade
    print("\n📊 Average Rating by Decade:")
    pipeline = [
        {'$match': {'year': {'$gte': 1980, '$lte': 2020}, 'imdb.rating': {'$exists': True}}},
        {'$addFields': {'decade': {'$subtract': ['$year', {'$mod': ['$year', 10]}]}}},
        {'$group': {
            '_id': '$decade',
            'avg_rating': {'$avg': '$imdb.rating'},
            'count': {'$sum': 1}
        }},
        {'$sort': {'_id': 1}}
    ]
    results = connector.aggregate('movies', pipeline)
    for r in results:
        print(f"   {r['_id']}s: {r['avg_rating']:.2f} ({r['count']} movies)")

def show_nlq_examples(connector):
    """Show examples of natural language queries and their MongoDB equivalents"""
    print_header("💬 NATURAL LANGUAGE QUERY EXAMPLES")
    
    examples = [
        {
            'nlq': "Find all movies from 2015",
            'mongodb': "db.movies.find({year: 2015})",
            'query': {'year': 2015}
        },
        {
            'nlq': "Show me action movies with rating above 8",
            'mongodb': "db.movies.find({genres: 'Action', 'imdb.rating': {$gte: 8}})",
            'query': {'genres': 'Action', 'imdb.rating': {'$gte': 8}}
        },
        {
            'nlq': "Count movies by genre",
            'mongodb': "db.movies.aggregate([{$unwind: '$genres'}, {$group: {_id: '$genres', count: {$sum: 1}}}])",
            'query': None  # Aggregation
        },
        {
            'nlq': "Find movies directed by Christopher Nolan",
            'mongodb': "db.movies.find({directors: 'Christopher Nolan'})",
            'query': {'directors': 'Christopher Nolan'}
        },
        {
            'nlq': "Get comedy movies from the 90s",
            'mongodb': "db.movies.find({genres: 'Comedy', year: {$gte: 1990, $lte: 1999}})",
            'query': {'genres': 'Comedy', 'year': {'$gte': 1990, '$lte': 1999}}
        }
    ]
    
    for i, ex in enumerate(examples, 1):
        print(f"\n{i}. Natural Language: \"{ex['nlq']}\"")
        print(f"   MongoDB Query: {ex['mongodb']}")
        
        if ex['query']:
            count = connector.count_documents('movies', ex['query'])
            results = connector.find_many('movies', ex['query'], limit=2)
            print(f"   Results: {count} movies found")
            if results:
                print(f"   Sample: {results[0].get('title')}")

def main():
    """Main exploration function"""
    print("\n" + "="*70)
    print(" "*20 + "SAMPLE_MFLIX EXPLORER")
    print("="*70)
    
    connector = MongoDBConnector()
    
    if not connector.connect():
        print("✗ Failed to connect to MongoDB")
        return
    
    try:
        explore_movies(connector)
        explore_aggregations(connector)
        show_nlq_examples(connector)
        
        print("\n" + "="*70)
        print("✅ Exploration Complete!")
        print("="*70)
        print("\nThese query examples will be useful for:")
        print("  - Testing LLM query generation (Sprint 3)")
        print("  - Validating natural language translation")
        print("  - Building test cases for your project")
        
    finally:
        connector.disconnect()

if __name__ == "__main__":
    main()