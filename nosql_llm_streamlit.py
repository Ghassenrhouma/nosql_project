"""
NoSQL-LLM Streamlit Web Interface
Modern web interface for natural language querying across NoSQL databases
"""

import streamlit as st
import sys
import os
import time
import pandas as pd
from typing import Dict, List, Any

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from cross_db_comparator import CrossDatabaseComparator
from llm.query_translator import QueryTranslator
from llm.query_executor import QueryExecutor

# Page configuration
st.set_page_config(
    page_title="NoSQL-LLM Query Interface",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(45deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 2rem;
    }
    .query-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
        margin: 20px 0;
    }
    .result-card {
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .success-card {
        border-left: 5px solid #28a745;
    }
    .error-card {
        border-left: 5px solid #dc3545;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        text-align: center;
        margin: 10px 0;
    }
    .stButton>button {
        background: linear-gradient(45deg, #1f77b4, #ff7f0e);
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        font-weight: bold;
    }
    .stButton>button:hover {
        background: linear-gradient(45deg, #155a8a, #e66a0a);
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'comparator' not in st.session_state:
    st.session_state.comparator = CrossDatabaseComparator()
if 'translator' not in st.session_state:
    st.session_state.translator = QueryTranslator()
if 'executor' not in st.session_state:
    st.session_state.executor = QueryExecutor()

# Database configurations
DATABASES = {
    'mongodb': {'name': 'MongoDB', 'icon': '🍃', 'color': '#47A248'},
    'neo4j': {'name': 'Neo4j', 'icon': '🕸️', 'color': '#008CC1'},
    'redis': {'name': 'Redis', 'icon': '🔴', 'color': '#DC382D'},
    'hbase': {'name': 'HBase', 'icon': '📊', 'color': '#7B1FA2'},
    'rdf': {'name': 'RDF/SPARQL', 'icon': '🔗', 'color': '#FF6B35'}
}

def main():
    """Main Streamlit application"""

    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/database.png", width=80)
        st.title("🎬 NoSQL-LLM")
        st.markdown("---")

        # Navigation
        page = st.radio(
            "Navigation",
            ["🏠 Home", "🔍 Query Database", "🔄 Compare Databases", "📊 Schema Explorer", "⚙️ System Status"],
            label_visibility="collapsed"
        )

        st.markdown("---")

        # Database status
        st.subheader("🗄️ Database Status")
        show_database_status()

    # Main content
    if page == "🏠 Home":
        show_home_page()
    elif page == "🔍 Query Database":
        show_single_query_page()
    elif page == "🔄 Compare Databases":
        show_comparison_page()
    elif page == "📊 Schema Explorer":
        show_schema_page()
    elif page == "⚙️ System Status":
        show_system_status_page()

def show_home_page():
    """Home page with overview"""
    st.markdown('<h1 class="main-header">🎬 NoSQL-LLM Query Interface</h1>', unsafe_allow_html=True)

    st.markdown("""
    ## 🌟 Welcome to the Future of Database Querying

    Query **5 different NoSQL databases** using natural language! Our AI-powered system translates your plain English questions into database-specific queries.

    ### ✨ Key Features
    - **🗣️ Natural Language Queries** - Ask questions in plain English
    - **🔄 Cross-Database Comparison** - Compare queries across MongoDB, Neo4j, Redis, HBase, and RDF
    - **📊 Schema Exploration** - Understand database structures
    - **⚡ Real-time Results** - Instant query execution and results
    - **🎯 Smart Translation** - AI-powered query generation using Google Gemini
    """)

    # Feature cards
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3>🍃 MongoDB</h3>
            <p>Document Store<br>JSON Queries</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3>🕸️ Neo4j</h3>
            <p>Graph Database<br>Cypher Queries</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="metric-card">
            <h3>🔴 Redis</h3>
            <p>Key-Value Store<br>Advanced Structures</p>
        </div>
        """, unsafe_allow_html=True)

    # Example queries
    st.markdown("### 💡 Example Queries")
    examples = [
        "Find all movies from 2015",
        "Show me action movies with rating above 8",
        "Who directed The Dark Knight?",
        "Find comedy movies from the 90s",
        "What movies star Leonardo DiCaprio?"
    ]

    for example in examples:
        if st.button(f"🔍 {example}", key=f"example_{hash(example)}"):
            st.session_state.selected_query = example
            st.rerun()

    # Quick stats
    st.markdown("### 📈 System Statistics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Databases", "5", "Supported")
    with col2:
        st.metric("Query Types", "5", "Different")
    with col3:
        st.metric("AI Model", "Gemini 2.0", "Latest")
    with col4:
        st.metric("Languages", "5", "Query Languages")

def show_single_query_page():
    """Single database query page"""
    st.markdown('<h2 class="main-header">🔍 Single Database Query</h2>', unsafe_allow_html=True)

    # Database selection
    st.markdown("### Select Database")
    db_cols = st.columns(len(DATABASES))

    selected_db = None
    for i, (db_key, db_info) in enumerate(DATABASES.items()):
        with db_cols[i]:
            if st.button(f"{db_info['icon']} {db_info['name']}", key=f"select_{db_key}"):
                selected_db = db_key

    if selected_db:
        st.session_state.selected_database = selected_db
        st.success(f"Selected: {DATABASES[selected_db]['name']}")

    # Query input
    st.markdown("### Enter Your Query")
    query = st.text_input(
        "Natural Language Query",
        placeholder="e.g., Find all movies from 2015",
        value=st.session_state.get('selected_query', ''),
        key="single_query_input"
    )

    if st.button("🚀 Execute Query", type="primary", use_container_width=True):
        if not query.strip():
            st.error("Please enter a query")
            return

        if not hasattr(st.session_state, 'selected_database'):
            st.error("Please select a database first")
            return

        execute_single_query(query, st.session_state.selected_database)

def execute_single_query(query: str, db_key: str):
    """Execute a single database query"""
    with st.spinner(f"Processing query on {DATABASES[db_key]['name']}..."):
        progress_bar = st.progress(0)

        # Get schema
        progress_bar.progress(20, "Analyzing database schema...")
        schema_context = st.session_state.comparator._get_schema_context(db_key)

        # Translate query
        progress_bar.progress(50, "Translating to database query...")
        if db_key == 'mongodb':
            translated = st.session_state.translator.translate_to_mongodb(query, schema_context)
        elif db_key == 'neo4j':
            translated = st.session_state.translator.translate_to_neo4j(query, schema_context)
        elif db_key == 'redis':
            translated = st.session_state.translator.translate_to_redis(query, schema_context)
        elif db_key == 'hbase':
            translated = st.session_state.translator.translate_to_hbase(query, schema_context)
        elif db_key == 'rdf':
            translated = st.session_state.translator.translate_to_sparql(query, schema_context)

        # Execute query
        progress_bar.progress(80, "Executing query...")
        if 'error' not in translated:
            if db_key == 'mongodb':
                result = st.session_state.executor.execute_mongodb(translated)
            elif db_key == 'neo4j':
                result = st.session_state.executor.execute_neo4j(translated)
            elif db_key == 'redis':
                result = st.session_state.executor.execute_redis(translated)
            elif db_key == 'hbase':
                result = st.session_state.executor.execute_hbase(translated)
            elif db_key == 'rdf':
                result = st.session_state.executor.execute_sparql(translated)
        else:
            result = {'success': False, 'error': translated.get('error')}

        progress_bar.progress(100, "Complete!")
        progress_bar.empty()

    # Display results
    display_single_result(query, db_key, translated, result)

def format_result_for_display(result: Any, db_key: str = None) -> Dict[str, Any]:
    """Format a single result for user-friendly display"""
    
    # Handle HBase result format: {'row_key': '...', 'data': {...}}
    if isinstance(result, dict) and 'data' in result and 'row_key' in result:
        # HBase returns {'row_key': 'movie_00001', 'data': {'info:title': '...', ...}}
        # Extract the data part and flatten column families
        data = result.get('data', {})
        
        # Flatten HBase column family structure immediately
        flattened = {}
        for key, value in data.items():
            if ':' in key:
                # Extract the column name after the colon
                _, col_name = key.split(':', 1)
                flattened[col_name] = value
            else:
                flattened[key] = value
        result = flattened
    
    # Handle Neo4j objects - check for properties attribute first
    if hasattr(result, 'properties'):
        try:
            formatted = dict(result.properties)
            # Add metadata if available
            if hasattr(result, 'labels'):
                formatted['_labels'] = list(result.labels)
            if hasattr(result, 'type'):
                formatted['_type'] = result.type
            if hasattr(result, 'element_id'):
                formatted['_id'] = result.element_id
            elif hasattr(result, 'id'):
                formatted['_id'] = result.id
            return formatted
        except Exception as e:
            # If properties access fails, try to get string representation
            return {'neo4j_object': str(result), 'error': str(e)}
    elif isinstance(result, dict):
        # Check if this is a Neo4j result with movie node + directors/cast collections
        if 'm' in result:
            # Neo4j query returned: {m: Movie, directors: [...], cast: [...]}
            # The 'm' value could be either a Node object or already converted to dict
            if hasattr(result['m'], 'properties'):
                # It's still a Node object - convert it
                formatted = dict(result['m'].properties)
            elif isinstance(result['m'], dict):
                # Already converted to dict by the connector
                formatted = result['m'].copy()
            else:
                # Unknown format, skip
                formatted = {}
            
            # Add directors and cast arrays if present (don't overwrite, add as new keys)
            if 'directors' in result and result['directors']:
                # Filter out None values from the collect()
                directors_list = [d for d in result['directors'] if d is not None]
                if directors_list:
                    formatted['directors'] = directors_list
            if 'cast' in result and result['cast']:
                # Filter out None values from the collect()
                cast_list = [c for c in result['cast'] if c is not None]
                if cast_list:
                    formatted['cast'] = cast_list
            
            return formatted
        
        # Check if this is a Redis command result
        if 'command' in result and 'result' in result:
            # Redis command result
            command = result['command']
            cmd_result = result['result']
            if isinstance(cmd_result, list) and len(cmd_result) > 0:
                # List result (like ZRANGE, SMEMBERS, etc.)
                # Check if the list contains dictionaries (movie data already fetched by executor)
                if isinstance(cmd_result[0], dict) and not ('Value' in cmd_result[0] and 'Index' in cmd_result[0]):
                    # Movie dictionaries already fetched - need to add genres/cast/directors
                    movie_data = []
                    try:
                        from connectors.redis_connector import RedisConnector
                        redis_conn = RedisConnector()
                        redis_conn.connect()

                        for movie_dict in cmd_result[:10]:
                            # Make a copy to avoid modifying the original
                            movie_copy = movie_dict.copy()
                            # Extract the Redis key if present
                            movie_key = movie_copy.get('_key', '')
                            
                            if movie_key and 'movie:' in str(movie_key):
                                # Fetch additional data from separate keys (genres, cast, directors)
                                genres_set = redis_conn.smembers(f"{movie_key}:genres")
                                cast_list = redis_conn.lrange(f"{movie_key}:cast", 0, -1)
                                directors_list = redis_conn.lrange(f"{movie_key}:directors", 0, -1)
                                
                                # Add to the movie dictionary
                                movie_copy['genres'] = list(genres_set) if genres_set else []
                                movie_copy['cast'] = cast_list if cast_list else []
                                movie_copy['directors'] = directors_list if directors_list else []
                            
                            movie_data.append(movie_copy)

                        redis_conn.disconnect()
                        return movie_data
                    except Exception as e:
                        # If connection fails, return the dictionaries as-is
                        return cmd_result[:10]
                else:
                    # Regular list result - return as list of dictionaries for DataFrame
                    return [{
                        'Value': str(item),
                        'Index': i
                    } for i, item in enumerate(cmd_result[:10])]
            elif isinstance(cmd_result, list):
                # Empty list
                return []
            elif isinstance(cmd_result, dict):
                # Dict result (like HGETALL) - return as single row DataFrame
                # Check if this is a movie hash, and if so, fetch additional data
                if 'command' in result and 'HGETALL' in result['command'] and 'movie:' in result['command']:
                    try:
                        from connectors.redis_connector import RedisConnector
                        redis_conn = RedisConnector()
                        redis_conn.connect()
                        
                        # Extract movie ID from command
                        import re
                        match = re.search(r'movie:(\w+)', result['command'])
                        if match:
                            movie_id = match.group(1)
                            # Fetch additional data
                            genres_set = redis_conn.smembers(f"movie:{movie_id}:genres")
                            cast_list = redis_conn.lrange(f"movie:{movie_id}:cast", 0, -1)
                            directors_list = redis_conn.lrange(f"movie:{movie_id}:directors", 0, -1)
                            
                            # Add to result
                            cmd_result['genres'] = ', '.join(genres_set) if genres_set else None
                            cmd_result['cast'] = ', '.join(cast_list[:3]) if cast_list else None
                            if len(cast_list) > 3:
                                cmd_result['cast'] += f' +{len(cast_list)-3} more'
                            cmd_result['directors'] = ', '.join(directors_list) if directors_list else None
                        
                        redis_conn.disconnect()
                    except Exception:
                        pass  # If fetching fails, just use the hash data
                
                return [cmd_result]
            else:
                # Single value result (like GET) - return as single row DataFrame
                return [{
                    'Command': command,
                    'Result': str(cmd_result)
                }]
        else:
            # Already a dictionary, but check for nested objects
            formatted = {}
            for key, value in result.items():
                if hasattr(value, 'properties'):
                    # Nested Neo4j object
                    try:
                        formatted.update(dict(value.properties))
                        if hasattr(value, 'labels'):
                            formatted['_labels'] = list(value.labels)
                        if hasattr(value, 'type'):
                            formatted['_type'] = value.type
                        if hasattr(value, 'element_id'):
                            formatted['_id'] = value.element_id
                        elif hasattr(value, 'id'):
                            formatted['_id'] = value.id
                    except Exception:
                        formatted[key] = str(value)
                elif isinstance(value, (list, tuple)):
                    # Handle lists of nodes/relationships
                    formatted_list = []
                    for item in value:
                        if hasattr(item, 'properties'):
                            try:
                                formatted_list.append(dict(item.properties))
                            except Exception:
                                formatted_list.append(str(item))
                        else:
                            formatted_list.append(str(item))
                    formatted[key] = formatted_list
                else:
                    formatted[key] = value
            return formatted
    else:
        # Other types - convert to string representation
        return {'value': str(result)}

def standardize_movie_result(result: Any, db_key: str) -> Dict[str, Any]:
    """
    Standardize any database result to a common movie format with consistent columns.
    
    Returns a dictionary with these standard columns:
    - title: Movie title
    - year: Release year
    - genres: Genres as comma-separated string
    - imdb_rating: IMDb rating
    - directors: Directors as comma-separated string
    - cast: Cast members as comma-separated string (max 3)
    - plot: Movie plot/summary (truncated)
    """
    standardized = {
        'title': None,
        'year': None,
        'genres': None,
        'imdb_rating': None,
        'directors': None,
        'cast': None,
        'plot': None
    }
    
    # First, convert to a dictionary if it's not already
    if hasattr(result, 'properties'):
        # Neo4j Node object
        try:
            result = dict(result.properties)
        except:
            return standardized
    elif not isinstance(result, dict):
        return standardized
    
    # Handle HBase column family format (e.g., 'info:title', 'ratings:imdb_rating')
    # This should already be flattened by format_result_for_display, but check just in case
    if db_key == 'hbase':
        # Check if we still have column family format (not already flattened)
        has_colons = any(':' in str(key) for key in result.keys())
        if has_colons:
            # Flatten HBase column family structure
            flattened = {}
            for key, value in result.items():
                if ':' in str(key):
                    # Extract the column name after the colon
                    _, col_name = key.split(':', 1)
                    flattened[col_name] = value
                else:
                    flattened[key] = value
            result = flattened
        
        # Map HBase-specific field names to standard names
        if 'imdb_rating' in result and 'rating' not in result:
            result['rating'] = result['imdb_rating']
    
    # Extract title
    for field in ['title', 'Title', 'movie_title', 'name', 'Name']:
        if field in result and result[field] is not None and str(result[field]).strip():
            standardized['title'] = str(result[field])
            break
    
    # Extract year
    for field in ['year', 'Year', 'release_year', 'releaseYear']:
        if field in result and result[field] is not None and str(result[field]).strip():
            try:
                standardized['year'] = int(result[field])
            except (ValueError, TypeError):
                standardized['year'] = str(result[field])
            break
    
    # Extract genres - handle arrays and comma-separated strings
    for field in ['genres', 'Genres', 'genre', 'Genre']:
        if field in result and result[field] is not None:
            genres = result[field]
            if isinstance(genres, (list, tuple)) and genres:
                # Filter out None and empty strings
                genre_list = [str(g) for g in genres if g]
                if genre_list:
                    standardized['genres'] = ', '.join(genre_list)
            elif isinstance(genres, str) and genres.strip():
                standardized['genres'] = genres
            break
    
    # Extract IMDb rating - check nested and flat structures
    rating_found = False
    if 'imdb' in result and isinstance(result['imdb'], dict):
        rating = result['imdb'].get('rating')
        if rating is not None:
            try:
                standardized['imdb_rating'] = float(rating)
                rating_found = True
            except (ValueError, TypeError):
                standardized['imdb_rating'] = str(rating)
                rating_found = True
    
    if not rating_found:
        for field in ['imdb_rating', 'rating', 'Rating', 'imdbRating', 'imdb_rating']:
            if field in result and result[field] is not None and str(result[field]).strip():
                try:
                    standardized['imdb_rating'] = float(result[field])
                    rating_found = True
                except (ValueError, TypeError):
                    standardized['imdb_rating'] = str(result[field])
                    rating_found = True
                break
    
    # Extract directors - handle arrays and strings
    for field in ['directors', 'Directors', 'director', 'Director', 'directorName']:
        if field in result and result[field] is not None:
            directors = result[field]
            if isinstance(directors, (list, tuple)) and directors:
                # Filter out None and empty strings
                director_list = [str(d) for d in directors if d]
                if director_list:
                    standardized['directors'] = ', '.join(director_list)
            elif isinstance(directors, str) and directors.strip():
                standardized['directors'] = directors
            break
    
    # Extract cast - handle arrays and strings, limit to 3
    for field in ['cast', 'Cast', 'actors', 'Actors', 'actorName']:
        if field in result and result[field] is not None:
            cast = result[field]
            if isinstance(cast, (list, tuple)) and cast:
                # Filter out None values first
                valid_cast = [c for c in cast if c]
                cast_list = [str(c) for c in valid_cast[:3]]
                if len(valid_cast) > 3:
                    cast_list.append(f'+{len(valid_cast)-3} more')
                if cast_list:
                    standardized['cast'] = ', '.join(cast_list)
            elif isinstance(cast, str) and cast.strip():
                # Handle comma-separated strings
                parts = [p.strip() for p in cast.split(',') if p.strip()]
                if len(parts) > 3:
                    parts = parts[:3] + [f'+{len(parts)-3} more']
                standardized['cast'] = ', '.join(parts)
            break
    
    # Extract plot - truncate for display
    for field in ['plot', 'Plot', 'summary', 'Summary', 'description']:
        if field in result and result[field] is not None:
            plot_text = str(result[field])
            if plot_text.strip():
                if len(plot_text) > 150:
                    standardized['plot'] = plot_text[:150] + '...'
                else:
                    standardized['plot'] = plot_text
                break
    
    return standardized

def format_results_for_display(results: List[Any], max_results: int = 10, db_key: str = None) -> List[Dict[str, Any]]:
    """Format a list of results for DataFrame display with standardized columns"""
    if not results:
        return []

    # Special handling for RDF SPARQL results - aggregate by title
    if db_key == 'rdf' and results and isinstance(results[0], dict):
        # Check if this looks like SPARQL results with multiple rows per movie
        if 'title' in results[0] and any(key in results[0] for key in ['genreName', 'directorName', 'actorName']):
            # Aggregate results by title
            movies = {}
            for row in results:
                title = row.get('title')
                if not title:
                    continue
                
                if title not in movies:
                    # Initialize movie entry
                    movies[title] = {
                        'title': title,
                        'year': row.get('year'),
                        'plot': row.get('plot'),
                        'rating': row.get('rating'),
                        'genres': set(),
                        'directors': set(),
                        'cast': set()
                    }
                
                # Add genre, director, actor if present
                if row.get('genreName'):
                    movies[title]['genres'].add(row['genreName'])
                if row.get('directorName'):
                    movies[title]['directors'].add(row['directorName'])
                if row.get('actorName'):
                    movies[title]['cast'].add(row['actorName'])
            
            # Convert sets to comma-separated strings and format
            aggregated = []
            for movie in list(movies.values())[:max_results]:
                # Convert sets to lists
                movie['genres'] = ', '.join(sorted(movie['genres'])) if movie['genres'] else None
                movie['directors'] = ', '.join(sorted(movie['directors'])) if movie['directors'] else None
                cast_list = sorted(movie['cast'])
                if len(cast_list) > 3:
                    movie['cast'] = ', '.join(cast_list[:3]) + f' +{len(cast_list)-3} more'
                else:
                    movie['cast'] = ', '.join(cast_list) if cast_list else None
                
                # Standardize the aggregated movie
                standardized = standardize_movie_result(movie, db_key)
                aggregated.append(standardized)
            
            return aggregated

    # Standard processing for other databases
    formatted_results = []
    for result in results[:max_results]:
        formatted = format_result_for_display(result, db_key)
        # Handle case where format_result_for_display returns a list (like Redis movie data)
        if isinstance(formatted, list):
            # Standardize each item in the list
            for item in formatted:
                standardized = standardize_movie_result(item, db_key)
                formatted_results.append(standardized)
        else:
            # Standardize the single result
            standardized = standardize_movie_result(formatted, db_key)
            formatted_results.append(standardized)

    return formatted_results

def display_single_result(query: str, db_key: str, translated: dict, result: dict):
    """Display results from single database query"""
    db_info = DATABASES[db_key]

    # Query summary
    st.markdown("### 📝 Query Summary")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Database", f"{db_info['icon']} {db_info['name']}")
    with col2:
        st.metric("Query Status", "✅ Success" if result.get('success') else "❌ Failed")
    with col3:
        st.metric("Results Found", result.get('count', 0))

    # Translated query
    st.markdown("### 🔧 Translated Query")
    if 'error' not in translated:
        if db_key == 'mongodb':
            # For MongoDB, show the full operation structure
            operation = translated.get('operation', 'find')
            if operation in ['update_one', 'update_many', 'delete_one', 'delete_many', 'insert_one', 'insert_many']:
                # Show the full MongoDB operation
                query_text = f"""Operation: {operation}
Query: {translated.get('query', {})}
Update: {translated.get('update', {})}
Document: {translated.get('document', {})}
Documents: {translated.get('documents', [])}"""
            else:
                query_text = str(translated.get('query', {}))
        elif db_key == 'neo4j':
            # For Neo4j, show the full operation structure
            operation = translated.get('operation')
            if operation in ['update_node', 'delete_node', 'create_node', 'create_relationship']:
                # Show the full Neo4j operation
                query_text = f"""Operation: {operation}
Label: {translated.get('label', '')}
Match Properties: {translated.get('match_properties', {})}
Update Properties: {translated.get('update_properties', {})}
Properties: {translated.get('properties', {})}
From Label: {translated.get('from_label', '')}
From Properties: {translated.get('from_properties', {})}
To Label: {translated.get('to_label', '')}
To Properties: {translated.get('to_properties', {})}
Relationship Type: {translated.get('relationship_type', '')}"""
            elif operation in ['filter_by_genre', 'filter_by_year', 'filter_by_director', 'filter_by_cast', 'filter_by_multiple']:
                # Show the generated Cypher query from the execution result
                query_text = result.get('cypher', 'No query generated')
            else:
                query_text = translated.get('cypher', '')
        elif 'operation' in translated and db_key in ['redis', 'hbase', 'rdf']:
            # Show CRUD operation details for Redis, HBase, RDF
            operation = translated.get('operation')
            query_text = f"""Operation: {operation}
Title: {translated.get('title', '')}
Field: {translated.get('field', '')}
Value: {translated.get('value', '')}
Updates: {translated.get('updates', {})}
Keys: {translated.get('keys', [])}
Row Key: {translated.get('row_key', '')}
Subject: {translated.get('subject', '')}
Predicate: {translated.get('predicate', '')}
Table: {translated.get('table', '')}"""
        else:
            query_text = translated.get('query') or translated.get('cypher') or translated.get('sparql') or str(translated.get('commands', ''))
        st.code(query_text, language=get_query_language(db_key))

        if translated.get('explanation'):
            st.info(f"💡 **Explanation:** {translated['explanation']}")
    else:
        st.error(f"Translation Error: {translated['error']}")

    # Results
    st.markdown("### 📊 Results")
    if result.get('success'):
        # Check if this is a CRUD operation result
        results_data = result.get('results', [])
        
        # CRUD operations typically return a single result dict with specific keys
        is_crud = False
        first_result = {}
        
        if results_data:
            if isinstance(results_data, list) and len(results_data) > 0:
                first_result = results_data[0]
            elif isinstance(results_data, dict):
                first_result = results_data
        
        # Check for CRUD operation result patterns (MongoDB, Neo4j, Redis, HBase, RDF)
        crud_keys = ['matched_count', 'modified_count', 'deleted_count', 'inserted_id', 'inserted_ids', 
                    'properties_set', 'nodes_deleted', 'node_id', 'relationship_created',
                    'updated', 'deleted', 'keys', 'subject', 'predicate', 'created', 'movie_id']
        # Note: 'row_key' alone is not CRUD - HBase find operations also return row_key
        # Only treat as CRUD if has row_key AND (created/updated/deleted)
        is_crud = any(key in first_result for key in crud_keys) if first_result else False
        
        # Special case: HBase row_key is only CRUD if it has created/updated/deleted
        if 'row_key' in first_result and not any(k in first_result for k in ['created', 'updated', 'deleted']):
            is_crud = False
        
        if is_crud:
            # This is a CRUD operation result
            st.success("✅ Operation completed successfully!")
            
            # MongoDB CRUD results
            if 'matched_count' in first_result and 'modified_count' in first_result:
                matched = first_result['matched_count']
                modified = first_result['modified_count']
                if modified > 0:
                    st.info(f"📝 **Update Result:** {matched} document(s) matched, {modified} document(s) modified")
                elif matched > 0:
                    st.warning(f"⚠️ **Update Result:** {matched} document(s) matched but 0 modified (values may be the same)")
                else:
                    st.error(f"❌ **Update Result:** No documents matched the query")
            elif 'deleted_count' in first_result:
                deleted = first_result['deleted_count']
                if deleted > 0:
                    st.info(f"🗑️ **Delete Result:** {deleted} document(s) deleted from MongoDB")
                else:
                    st.error(f"❌ **Delete Result:** No documents matched the query - nothing deleted")
            elif 'inserted_id' in first_result:
                st.info(f"➕ **Insert Result:** Document inserted with ID: {first_result['inserted_id']}")
            elif 'inserted_ids' in first_result:
                st.info(f"➕ **Insert Result:** {len(first_result['inserted_ids'])} documents inserted")
            
            # Neo4j CRUD results
            elif 'properties_set' in first_result:
                props_set = first_result['properties_set']
                if props_set > 0:
                    st.info(f"📝 **Update Result:** {props_set} properties updated in Neo4j node")
                else:
                    st.warning(f"⚠️ **Update Result:** No properties updated (node may not exist or values unchanged)")
            elif 'nodes_deleted' in first_result:
                deleted = first_result['nodes_deleted']
                if deleted > 0:
                    st.info(f"🗑️ **Delete Result:** {deleted} node(s) deleted from Neo4j")
                else:
                    st.error(f"❌ **Delete Result:** No nodes matched - nothing deleted")
            elif 'node_id' in first_result:
                node_id = first_result['node_id']
                if node_id:
                    st.info(f"➕ **Insert Result:** Node created in Neo4j with ID: {node_id}")
                else:
                    st.warning(f"⚠️ **Insert Result:** Node may already exist or creation failed")
            
            # Redis CRUD results
            elif 'movie_id' in first_result and 'created' in first_result:
                # Redis create operation
                created = first_result.get('created', False)
                title = first_result.get('title', '')
                year = first_result.get('year', '')
                if created:
                    st.info(f"➕ **Create Result:** Movie '{title}' ({year}) created in Redis")
                else:
                    st.error(f"❌ **Create Result:** Failed to create movie '{title}'")
            elif 'keys' in first_result:
                # This is a Redis delete operation (keys is a list)
                deleted = first_result.get('deleted_count', 0)
                keys = first_result.get('keys', [])
                title = first_result.get('title', '')
                if deleted > 0:
                    title_msg = f" for movie '{title}'" if title else ""
                    st.info(f"🗑️ **Delete Result:** {deleted} Redis key(s) deleted{title_msg}")
                else:
                    title_msg = f" for '{title}'" if title else ""
                    st.error(f"❌ **Delete Result:** No keys deleted from Redis{title_msg}")
            elif 'updated' in first_result and 'field' in first_result:
                # This is a Redis update operation
                key = first_result.get('key', '')
                field = first_result.get('field', '')
                value = first_result.get('value', '')
                title = first_result.get('title', '')
                updated = first_result.get('updated', None)
                # Note: HSET returns 1 if new field, 0 if existing field updated
                # Both are success cases
                if updated is not None:
                    title_msg = f" (movie: '{title}')" if title else ""
                    st.success(f"✅ **Update Result:** Redis field '{field}' updated to '{value}'{title_msg}")
                else:
                    title_msg = f" for '{title}'" if title else ""
                    st.warning(f"⚠️ **Update Result:** Redis field '{field}' not updated{title_msg}")
            
            # HBase CRUD results
            elif 'row_key' in first_result:
                row_key = first_result.get('row_key', '')
                title = first_result.get('title', '')
                if 'created' in first_result:
                    # HBase create operation
                    success = first_result.get('created', False)
                    year = first_result.get('year', '')
                    if success:
                        st.info(f"➕ **Create Result:** Movie '{title}' ({year}) created in HBase")
                    else:
                        st.error(f"❌ **Create Result:** Failed to create movie '{title}'")
                elif 'updated' in first_result:
                    # HBase put/update operation
                    success = first_result.get('updated', False)
                    updates = first_result.get('updates', {})
                    if success:
                        title_msg = f" for movie '{title}'" if title else f" '{row_key}'"
                        st.info(f"📝 **Update Result:** HBase row{title_msg} updated successfully")
                    else:
                        title_msg = f" '{title}'" if title else f" '{row_key}'"
                        st.error(f"❌ **Update Result:** Failed to update HBase row{title_msg}")
                elif 'deleted' in first_result:
                    # HBase delete operation
                    success = first_result.get('deleted', False)
                    if success:
                        title_msg = f" for movie '{title}'" if title else f" '{row_key}'"
                        st.info(f"🗑️ **Delete Result:** HBase row{title_msg} deleted successfully")
                    else:
                        title_msg = f" '{title}'" if title else f" '{row_key}'"
                        st.error(f"❌ **Delete Result:** Failed to delete HBase row{title_msg}")
            
            # RDF CRUD results
            elif 'subject' in first_result:
                subject = first_result.get('subject', '')
                title = first_result.get('title', '')
                if 'created' in first_result:
                    # RDF create operation
                    success = first_result.get('created', False)
                    year = first_result.get('year', '')
                    if success:
                        st.info(f"➕ **Create Result:** Movie '{title}' ({year}) created in RDF")
                    else:
                        st.error(f"❌ **Create Result:** Failed to create movie '{title}'")
                elif 'deleted' in first_result:
                    # RDF delete operation
                    success = first_result.get('deleted', False)
                    if success:
                        title_msg = f" for movie '{title}'" if title else ""
                        st.info(f"🗑️ **Delete Result:** All triples{title_msg} deleted from RDF")
                    else:
                        title_msg = f" '{title}'" if title else ""
                        st.error(f"❌ **Delete Result:** Failed to delete triples for{title_msg}")
                elif 'updated' in first_result and 'field' in first_result:
                    # RDF update operation (find_and_update)
                    field = first_result.get('field', '')
                    old_value = first_result.get('old_value', '')
                    new_value = first_result.get('new_value', '')
                    success = first_result.get('updated', False)
                    if success:
                        title_msg = f" for movie '{title}'" if title else ""
                        st.info(f"📝 **Update Result:** RDF field '{field}'{title_msg} updated to '{new_value}'")
                    else:
                        st.error(f"❌ **Update Result:** Failed to update RDF field")
                elif 'updated' in first_result and 'predicate' in first_result:
                    # RDF update operation (direct update with predicate)
                    predicate = first_result.get('predicate', '')
                    old_value = first_result.get('old_value', '')
                    new_value = first_result.get('new_value', '')
                    success = first_result.get('updated', False)
                    if success:
                        st.info(f"📝 **Update Result:** RDF triple updated - '{subject}' {predicate.split('/')[-1]} changed from '{old_value}' to '{new_value}'")
                    else:
                        st.error(f"❌ **Update Result:** Failed to update RDF triple")
            
            elif 'relationship_created' in first_result:
                created = first_result['relationship_created']
                if created:
                    st.info(f"🔗 **Create Result:** Relationship created in Neo4j")
                else:
                    st.warning(f"⚠️ **Create Result:** Relationship may already exist or creation failed")
            else:
                st.json(first_result)
        else:
            # This is a query result
            total_count = result.get('count', 0)
            display_count = min(len(results_data) if results_data else 0, 10)
            if total_count > display_count:
                st.success(f"Found {total_count} results (showing first {display_count})")
            else:
                st.success(f"Found {total_count} results")

            # Display sample results
            if results_data:
                # Format results for display with standardization
                formatted_results = format_results_for_display(results_data, 10, db_key)

                if formatted_results:
                    # Create DataFrame with consistent column order
                    df = pd.DataFrame(formatted_results)
                    # Ensure column order is consistent
                    column_order = ['title', 'year', 'genres', 'imdb_rating', 'directors', 'cast', 'plot']
                    # Only include columns that exist in the data
                    df = df[[col for col in column_order if col in df.columns]]
                    
                    st.dataframe(df, use_container_width=True)

                    # Show raw data in expandable section for debugging
                    with st.expander("🔍 View Raw Data (Before Standardization)"):
                        st.write("**First 3 raw results:**")
                        st.json(results_data[:3])
                        
                        st.write("**First formatted result (after initial formatting, before standardization):**")
                        # Show what format_result_for_display returns
                        first_formatted = format_result_for_display(results_data[0] if results_data else {}, db_key)
                        st.json(first_formatted)
                        
                        st.write("**First standardized result:**")
                        if formatted_results:
                            st.json(formatted_results[0])
                        
                        st.write("**Keys in first result:**")
                        if results_data and isinstance(results_data[0], dict):
                            st.write(list(results_data[0].keys()))
                            # If Neo4j result with 'm' key
                            if 'm' in results_data[0]:
                                st.write("**Neo4j movie node properties:**")
                                if hasattr(results_data[0]['m'], 'properties'):
                                    props = dict(results_data[0]['m'].properties)
                                    st.json(props)
                                elif isinstance(results_data[0]['m'], dict):
                                    st.json(results_data[0]['m'])
                        elif hasattr(results_data[0] if results_data else None, 'properties'):
                            st.write("Neo4j Node object - properties:", list(dict(results_data[0].properties).keys()) if results_data else [])

                if len(results_data) > 10:
                    st.info(f"Showing first 10 of {len(results_data)} results")
            else:
                st.info("Query executed successfully but returned no results")

    # Translated Query Section
    else:
        st.error(f"Query execution failed: {result.get('error', 'Unknown error')}")

def show_comparison_page():
    """Cross-database comparison page"""
    st.markdown('<h2 class="main-header">🔄 Cross-Database Comparison</h2>', unsafe_allow_html=True)

    st.markdown("""
    Compare the same natural language query across multiple NoSQL databases to see how different systems handle the same question.
    """)

    # Query input
    query = st.text_input(
        "Natural Language Query",
        placeholder="e.g., Find all movies from 2015",
        key="comparison_query_input"
    )

    # Database selection
    st.markdown("### Select Databases to Compare")
    cols = st.columns(3)
    selected_dbs = []

    for i, (db_key, db_info) in enumerate(DATABASES.items()):
        with cols[i % 3]:
            if st.checkbox(f"{db_info['icon']} {db_info['name']}", value=True, key=f"compare_{db_key}"):
                selected_dbs.append(db_key)

    if st.button("🚀 Compare Across Databases", type="primary", use_container_width=True):
        if not query.strip():
            st.error("Please enter a query")
            return

        if not selected_dbs:
            st.error("Please select at least one database")
            return

        execute_comparison(query, selected_dbs)

def execute_comparison(query: str, databases: List[str]):
    """Execute cross-database comparison"""
    with st.spinner(f"Comparing across {len(databases)} databases..."):
        results = st.session_state.comparator.compare_query(query, databases)

    # Display comparison results
    display_comparison_results(query, results)

def display_comparison_results(query: str, results: dict):
    """Display cross-database comparison results"""
    summary = results.get('summary', {})

    # Summary metrics
    st.markdown("### 📊 Comparison Summary")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Databases Tested", summary.get('total_databases', 0))
    with col2:
        st.metric("Successful Translations", summary.get('successful_translations', 0))
    with col3:
        st.metric("Successful Executions", summary.get('successful_executions', 0))
    with col4:
        st.metric("Total Time", ".2f")

    # Detailed results for each database
    st.markdown("### 🗄️ Database Results")

    comparisons = results.get('comparisons', {})

    for db_key, data in comparisons.items():
        db_info = DATABASES.get(db_key, {'name': db_key, 'icon': '❓', 'color': '#666'})

        with st.expander(f"{db_info['icon']} {db_info['name']}", expanded=True):
            col1, col2, col3 = st.columns(3)

            # Translation status
            trans = data['translation']
            with col1:
                if trans['success']:
                    st.success("✅ Translation")
                else:
                    st.error("❌ Translation")
                    st.write(f"Error: {trans.get('error', 'Unknown')}")

            # Execution status
            exec_data = data['execution']
            with col2:
                if exec_data['success']:
                    # Show total count, but clarify if showing samples
                    total_count = exec_data['result_count']
                    sample_count = len(exec_data.get('sample_results', []))
                    if total_count > sample_count and sample_count > 0:
                        st.success(f"✅ Execution ({total_count} total)")
                    else:
                        st.success(f"✅ Execution ({total_count} results)")
                else:
                    st.error("❌ Execution")
                    st.write(f"Error: {exec_data.get('error', 'Unknown')}")

            # Timing
            with col3:
                st.info(".2f")

            # Show translated query
            if trans['success']:
                # Get the operation type
                operation = trans.get('operation', '')
                
                # For Neo4j filter operations, get the generated Cypher from execution result
                if db_key == 'neo4j' and operation in ['filter_by_genre', 'filter_by_year', 'filter_by_director', 'filter_by_cast', 'filter_by_multiple']:
                    query_text = exec_data.get('cypher', 'No query generated')
                # For Neo4j create operations, show operation details
                elif db_key == 'neo4j' and operation == 'create_node':
                    query_text = f"Operation: {operation}\n"
                    if 'label' in trans:
                        query_text += f"Label: {trans['label']}\n"
                    if 'properties' in trans:
                        query_text += f"Properties: {trans['properties']}\n"
                # For Neo4j update/delete operations, show operation details
                elif db_key == 'neo4j' and operation in ['update_node', 'delete_node']:
                    query_text = f"Operation: {operation}\n"
                    if 'label' in trans:
                        query_text += f"Label: {trans['label']}\n"
                    if 'match_properties' in trans:
                        query_text += f"Match: {trans['match_properties']}\n"
                    if 'update_properties' in trans:
                        query_text += f"Update: {trans['update_properties']}\n"
                    if 'properties' in trans:
                        query_text += f"Properties: {trans['properties']}\n"
                # For RDF filter operations, get the generated SPARQL from execution result
                elif db_key == 'rdf' and operation in ['filter_by_genre', 'filter_by_year', 'filter_by_director', 'filter_by_cast', 'filter_by_multiple']:
                    query_text = exec_data.get('sparql', 'No query generated')
                # For RDF create operations, show operation details
                elif db_key == 'rdf' and operation == 'create':
                    query_text = f"Operation: {operation}\n"
                    if 'title' in trans:
                        query_text += f"Title: {trans['title']}\n"
                    if 'year' in trans:
                        query_text += f"Year: {trans['year']}\n"
                    if 'genres' in trans:
                        query_text += f"Genres: {trans['genres']}\n"
                # For RDF update/delete operations, show operation details
                elif db_key == 'rdf' and operation in ['find_and_update', 'find_and_delete', 'delete', 'update']:
                    query_text = f"Operation: {operation}\n"
                    if 'title' in trans:
                        query_text += f"Title: {trans['title']}\n"
                    if 'field' in trans:
                        query_text += f"Field: {trans['field']}\n"
                    if 'value' in trans:
                        query_text += f"Value: {trans['value']}\n"
                    if 'subject' in trans:
                        query_text += f"Subject: {trans['subject']}\n"
                # For MongoDB CRUD operations, show operation details
                elif db_key == 'mongodb' and operation in ['insert_one', 'insert_many', 'update_one', 'update_many', 'delete_one', 'delete_many']:
                    query_text = f"Operation: {operation}\n"
                    if 'document' in trans:
                        query_text += f"Document: {trans['document']}\n"
                    if 'documents' in trans:
                        query_text += f"Documents: {trans['documents']}\n"
                    if 'query' in trans:
                        query_text += f"Query: {trans['query']}\n"
                    if 'update' in trans:
                        query_text += f"Update: {trans['update']}\n"
                # For Redis, HBase filter operations or CRUD operations
                elif db_key in ['redis', 'hbase'] and operation in ['filter_by_genre', 'filter_by_year', 'filter_by_director', 'filter_by_cast', 'filter_by_multiple', 'find', 'create', 'find_and_delete', 'find_and_update']:
                    query_text = f"Operation: {operation}\n"
                    if 'filters' in trans:
                        query_text += f"Filters: {trans['filters']}\n"
                    if 'genre' in trans:
                        query_text += f"Genre: {trans['genre']}\n"
                    if 'year' in trans:
                        query_text += f"Year: {trans['year']}\n"
                    if 'director' in trans:
                        query_text += f"Director: {trans['director']}\n"
                    if 'actor' in trans:
                        query_text += f"Actor: {trans['actor']}\n"
                    if 'title' in trans:
                        query_text += f"Title: {trans['title']}\n"
                    if 'field' in trans:
                        query_text += f"Field: {trans['field']}\n"
                    if 'value' in trans:
                        query_text += f"Value: {trans['value']}\n"
                else:
                    # Default: get query from translation
                    query_text = trans.get('query') or trans.get('cypher') or trans.get('sparql') or str(trans.get('commands', ''))
                
                if query_text:
                    st.code(query_text, language=get_query_language(db_key))

                if trans.get('explanation'):
                    st.info(f"💡 {trans['explanation']}")

            # Show sample results (but not for update operations)
            is_update_operation = operation in ['update_node', 'update_one', 'update_many', 'find_and_update', 'update', 'delete_node', 'delete_one', 'delete_many', 'find_and_delete', 'delete']
            if exec_data['success'] and exec_data.get('sample_results') and not is_update_operation:
                total_count = exec_data['result_count']
                sample_count = len(exec_data['sample_results'])
                if total_count > sample_count:
                    st.markdown(f"**Sample Results (showing {sample_count} of {total_count}):**")
                else:
                    st.markdown(f"**Results ({sample_count}):**")
                formatted_samples = format_results_for_display(exec_data['sample_results'], 3, db_key)
                if formatted_samples:
                    df = pd.DataFrame(formatted_samples)
                    # Ensure consistent column order
                    column_order = ['title', 'year', 'genres', 'imdb_rating', 'directors', 'cast', 'plot']
                    df = df[[col for col in column_order if col in df.columns]]
                    st.dataframe(df, use_container_width=True)
                else:
                    # If formatting failed, show raw data
                    st.markdown("**Raw Results:**")
                    for i, sample in enumerate(exec_data['sample_results'][:3]):
                        if isinstance(sample, dict):
                            st.json(sample)
                        else:
                            st.write(f"{i+1}. {sample}")

def show_schema_page():
    """Database schema exploration page"""
    st.markdown('<h2 class="main-header">📊 Schema Explorer</h2>', unsafe_allow_html=True)

    st.markdown("""
    Explore the structure and schema of different databases to understand their data models.
    """)

    # Database selection
    selected_db = st.selectbox(
        "Select Database to Explore",
        options=list(DATABASES.keys()),
        format_func=lambda x: f"{DATABASES[x]['icon']} {DATABASES[x]['name']}"
    )

    if st.button("🔍 Explore Schema", type="primary"):
        explore_database_schema(selected_db)

def explore_database_schema(db_key: str):
    """Explore schema of selected database"""
    db_info = DATABASES[db_key]

    with st.spinner(f"Exploring {db_info['name']} schema..."):
        schema_info = st.session_state.comparator._get_detailed_schema(db_key)

    st.markdown(f"### {db_info['icon']} {db_info['name']} Schema")

    if 'error' in schema_info:
        st.error(f"Schema exploration failed: {schema_info['error']}")
        return

    # Display schema information
    if schema_info:
        # Convert to DataFrame for display
        schema_items = []
        for key, value in schema_info.items():
            if isinstance(value, dict):
                schema_items.append({
                    'Element': key,
                    'Type': 'Object/Dict',
                    'Details': f"{len(value)} properties"
                })
            elif isinstance(value, list):
                schema_items.append({
                    'Element': key,
                    'Type': 'Array/List',
                    'Details': f"{len(value)} items"
                })
            else:
                schema_items.append({
                    'Element': key,
                    'Type': str(type(value).__name__),
                    'Details': str(value)[:100]
                })

        if schema_items:
            df = pd.DataFrame(schema_items)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No schema information available")
    else:
        st.info("No schema information available")

def show_system_status_page():
    """System status and information page"""
    st.markdown('<h2 class="main-header">⚙️ System Status</h2>', unsafe_allow_html=True)

    st.markdown("### 🗄️ Database Connectivity")

    # Check each database
    status_cols = st.columns(len(DATABASES))

    for i, (db_key, db_info) in enumerate(DATABASES.items()):
        with status_cols[i]:
            with st.spinner(f"Checking {db_info['name']}..."):
                status = check_database_connection(db_key)

            if status['connected']:
                st.success(f"{db_info['icon']} {db_info['name']}")
                st.caption("✅ Connected")
            else:
                st.error(f"{db_info['icon']} {db_info['name']}")
                st.caption(f"❌ {status.get('error', 'Not connected')}")

    # System information
    st.markdown("### 💻 System Information")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        **Core Components:**
        - ✅ Google Gemini LLM
        - ✅ Query Translator
        - ✅ Query Executor
        - ✅ Schema Explorers
        - ✅ Cross-DB Comparator
        """)

    with col2:
        st.markdown("""
        **Supported Features:**
        - ✅ Natural Language Queries
        - ✅ Multi-Database Support
        - ✅ Schema Exploration
        - ✅ Query Validation
        - ✅ Performance Metrics
        """)

    # Recent activity (if any)
    st.markdown("### 📈 Recent Activity")
    st.info("System ready for queries. All components initialized successfully.")

def check_database_connection(db_key: str) -> dict:
    """Check if database is connected"""
    try:
        if db_key == 'mongodb':
            from connectors.mongodb_connector import MongoDBConnector
            conn = MongoDBConnector()
            connected = conn.connect()
            if connected:
                conn.disconnect()
            return {'connected': connected}
        elif db_key == 'neo4j':
            from connectors.neo4j_connector import Neo4jConnector
            conn = Neo4jConnector()
            connected = conn.connect()
            if connected:
                conn.disconnect()
            return {'connected': connected}
        elif db_key == 'redis':
            from connectors.redis_connector import RedisConnector
            conn = RedisConnector()
            connected = conn.connect()
            if connected:
                conn.disconnect()
            return {'connected': connected}
        elif db_key == 'hbase':
            from connectors.hbase_connector import HBaseConnector
            conn = HBaseConnector()
            connected = conn.connect()
            if connected:
                conn.disconnect()
            return {'connected': connected}
        elif db_key == 'rdf':
            from connectors.rdf_connector import RDFConnector
            conn = RDFConnector()
            connected = conn.connect()
            if connected:
                conn.disconnect()
            return {'connected': connected}
    except Exception as e:
        return {'connected': False, 'error': str(e)}

    return {'connected': False, 'error': 'Unknown error'}

def get_query_language(db_key: str) -> str:
    """Get the appropriate language for syntax highlighting"""
    language_map = {
        'mongodb': 'javascript',
        'neo4j': 'cypher',
        'redis': 'bash',
        'hbase': 'sql',
        'rdf': 'sparql'
    }
    return language_map.get(db_key, 'text')

def show_database_status():
    """Show database status in sidebar"""
    for db_key, db_info in DATABASES.items():
        status = check_database_connection(db_key)
        if status['connected']:
            st.success(f"{db_info['icon']} {db_info['name']}")
        else:
            st.error(f"{db_info['icon']} {db_info['name']}")

if __name__ == "__main__":
    main()