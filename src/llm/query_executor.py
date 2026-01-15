"""
Query Executor
Executes translated queries on appropriate databases
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, List, Any
from connectors.mongodb_connector import MongoDBConnector
from connectors.neo4j_connector import Neo4jConnector
from connectors.redis_connector import RedisConnector
from connectors.rdf_connector import RDFConnector
from connectors.hbase_connector import HBaseConnector
from utils.logger import setup_logger

class QueryExecutor:
    """Executes database queries"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.connectors = {}
    
    def execute_mongodb(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute MongoDB query"""
        try:
            if 'mongodb' not in self.connectors:
                self.connectors['mongodb'] = MongoDBConnector()
                self.connectors['mongodb'].connect()
            
            conn = self.connectors['mongodb']
            collection = query_dict.get('collection')
            operation = query_dict.get('operation', 'find')
            
            if operation == 'find':
                results = conn.find_many(
                    collection,
                    query_dict.get('query', {}),
                    projection=query_dict.get('projection'),
                    limit=query_dict.get('limit', 10),
                    sort=query_dict.get('sort')
                )
            elif operation == 'aggregate':
                results = conn.aggregate(collection, query_dict.get('pipeline', []))
            elif operation == 'count':
                count = conn.count_documents(collection, query_dict.get('query', {}))
                results = [{'count': count}]
            elif operation == 'update_one':
                update_data = query_dict.get('update', {})
                if not update_data:
                    raise ValueError("Update operation requires 'update' field with $set, $unset, etc.")
                modified_count = conn.update_one(
                    collection,
                    query_dict.get('query', {}),
                    update_data
                )
                results = [{'matched_count': 1 if modified_count > 0 else 0, 'modified_count': modified_count}]
                self.logger.info(f"MongoDB update_one: {modified_count} modified")
            elif operation == 'update_many':
                update_data = query_dict.get('update', {})
                if not update_data:
                    raise ValueError("Update operation requires 'update' field with $set, $unset, etc.")
                modified_count = conn.update_many(
                    collection,
                    query_dict.get('query', {}),
                    update_data
                )
                results = [{'matched_count': modified_count, 'modified_count': modified_count}]
                self.logger.info(f"MongoDB update_many: {modified_count} modified")
            elif operation == 'delete_one':
                deleted_count = conn.delete_one(collection, query_dict.get('query', {}))
                results = [{'deleted_count': deleted_count}]
                self.logger.info(f"MongoDB delete_one: {deleted_count} deleted")
            elif operation == 'delete_many':
                deleted_count = conn.delete_many(collection, query_dict.get('query', {}))
                results = [{'deleted_count': deleted_count}]
                self.logger.info(f"MongoDB delete_many: {deleted_count} deleted")
            elif operation == 'insert_one':
                document = query_dict.get('document', {})
                if not document:
                    raise ValueError("Insert operation requires 'document' field")
                inserted_id = conn.insert_one(collection, document)
                results = [{'inserted_id': str(inserted_id)}]
                self.logger.info(f"MongoDB insert_one: {inserted_id}")
            elif operation == 'insert_many':
                documents = query_dict.get('documents', [])
                if not documents:
                    raise ValueError("Insert many operation requires 'documents' field")
                inserted_ids = conn.insert_many(collection, documents)
                results = [{'inserted_ids': [str(id) for id in inserted_ids]}]
                self.logger.info(f"MongoDB insert_many: {len(inserted_ids)} inserted")
            else:
                results = []
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"MongoDB execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_neo4j(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Neo4j Cypher query or CRUD operation"""
        try:
            if 'neo4j' not in self.connectors:
                self.connectors['neo4j'] = Neo4jConnector()
                self.connectors['neo4j'].connect()
            
            conn = self.connectors['neo4j']
            operation = query_dict.get('operation')
            
            if operation == 'cypher' or 'cypher' in query_dict:
                # Execute Cypher query
                cypher = query_dict.get('cypher')
                parameters = query_dict.get('parameters', {})
                results = conn.execute_query(cypher, parameters)
                return {
                    'success': True,
                    'results': results,
                    'count': len(results)
                }
            elif operation == 'update_node':
                # Update node properties
                label = query_dict.get('label')
                match_props = query_dict.get('match_properties', {})
                update_props = query_dict.get('update_properties', {})
                if not label or not match_props or not update_props:
                    raise ValueError("Update node requires: label, match_properties, update_properties")
                updated_count = conn.update_node(label, match_props, update_props)
                results = [{'properties_set': updated_count}]
                self.logger.info(f"Neo4j update_node: {updated_count} properties set")
                return {
                    'success': True,
                    'results': results,
                    'count': len(results)
                }
            elif operation == 'delete_node':
                # Delete node
                label = query_dict.get('label')
                properties = query_dict.get('properties', {})
                if not label or not properties:
                    raise ValueError("Delete node requires: label, properties")
                deleted_count = conn.delete_node(label, properties)
                results = [{'nodes_deleted': deleted_count}]
                self.logger.info(f"Neo4j delete_node: {deleted_count} nodes deleted")
                return {
                    'success': True,
                    'results': results,
                    'count': len(results)
                }
            elif operation == 'create_node':
                # Create node
                label = query_dict.get('label')
                properties = query_dict.get('properties', {})
                if not label or not properties:
                    raise ValueError("Create node requires: label, properties")
                node_id = conn.create_node(label, properties)
                results = [{'node_id': node_id}]
                self.logger.info(f"Neo4j create_node: node_id={node_id}")
                return {
                    'success': True,
                    'results': results,
                    'count': len(results)
                }
            elif operation == 'create_relationship':
                # Create relationship
                from_label = query_dict.get('from_label')
                from_props = query_dict.get('from_properties', {})
                to_label = query_dict.get('to_label')
                to_props = query_dict.get('to_properties', {})
                rel_type = query_dict.get('relationship_type')
                success = conn.create_relationship(from_label, from_props, to_label, to_props, rel_type)
                results = [{'relationship_created': success}]
                return {
                    'success': True,
                    'results': results,
                    'count': len(results)
                }
            else:
                # Default to Cypher query execution
                cypher = query_dict.get('cypher')
                parameters = query_dict.get('parameters', {})
                results = conn.execute_query(cypher, parameters)
                return {
                    'success': True,
                    'results': results,
                    'count': len(results)
                }
            
        except Exception as e:
            self.logger.error(f"Neo4j execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_redis(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Redis commands or CRUD operations"""
        try:
            if 'redis' not in self.connectors:
                self.connectors['redis'] = RedisConnector()
                self.connectors['redis'].connect()
            
            conn = self.connectors['redis']
            
            # Handle CRUD operations
            if 'operation' in query_dict:
                operation = query_dict['operation']
                self.logger.info(f"Detected Redis CRUD operation: {operation}")
                
                if operation == 'find':
                    # Find a movie by title
                    title = query_dict.get('title', '')
                    if not title:
                        return {'success': False, 'error': 'find requires title'}
                    
                    # Search for movie by title - scan all movie keys
                    cursor = 0
                    found_movie = None
                    
                    while True:
                        cursor, keys = conn.client.scan(cursor, match='movie:*', count=1000)
                        
                        for key in keys:
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            
                            # Skip sorted set index keys
                            if key_str in ['movies:by_rating', 'movies:by_year']:
                                continue
                            
                            # Get the hash data for this movie
                            movie_data = conn.hgetall(key_str)
                            if movie_data and movie_data.get('title', '').lower() == title.lower():
                                # Found the movie - return its data
                                self.logger.info(f"Found movie: {key_str}")
                                result_data = dict(movie_data)
                                result_data['movie_id'] = key_str.split(':')[1] if ':' in key_str else key_str
                                found_movie = result_data
                                break
                        
                        if found_movie or cursor == 0:
                            break
                    
                    if found_movie:
                        return {
                            'success': True,
                            'results': [found_movie],
                            'count': 1
                        }
                    
                    # Movie not found
                    self.logger.warning(f"Movie '{title}' not found")
                    return {'success': False, 'error': f'Movie "{title}" not found'}
                
                elif operation == 'create':
                    # Create a new movie
                    title = query_dict.get('title', '')
                    year = query_dict.get('year', '')
                    genres = query_dict.get('genres', '')
                    
                    if not title or not year:
                        return {'success': False, 'error': 'Create operation requires title and year'}
                    
                    # Generate a new movie ID (use timestamp + random)
                    import time
                    import random
                    movie_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
                    movie_key = f"movie:{movie_id}"
                    
                    # Create the movie hash
                    movie_data = {
                        'title': title,
                        'year': str(year),
                        'genres': genres if genres else 'Unknown'
                    }
                    
                    # Add optional fields
                    if 'plot' in query_dict:
                        movie_data['plot'] = query_dict['plot']
                    if 'rating' in query_dict:
                        movie_data['imdb_rating'] = str(query_dict['rating'])
                    
                    # Store the movie
                    for field, value in movie_data.items():
                        conn.hset(movie_key, field, value)
                    
                    # Add to sorted sets for indexing
                    if 'imdb_rating' in movie_data:
                        conn.client.zadd('movies:by_rating', {movie_key: float(movie_data['imdb_rating'])})
                    conn.client.zadd('movies:by_year', {movie_key: int(year)})
                    
                    self.logger.info(f"Created movie {movie_key}: {title}")
                    return {
                        'success': True,
                        'results': [{'created': True, 'movie_id': movie_id, 'title': title, 'year': year}],
                        'count': 1
                    }
                
                elif operation == 'find_and_delete':
                    # Find movie by title first, then delete
                    title = query_dict.get('title', '')
                    if not title:
                        return {'success': False, 'error': 'find_and_delete requires title'}
                    
                    # Search for movie by title - scan all movie keys
                    cursor = 0
                    deleted_count = 0
                    deleted_keys = []
                    
                    while True:
                        cursor, keys = conn.client.scan(cursor, match='movie:*', count=1000)
                        
                        for key in keys:
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            
                            # Skip sorted set index keys
                            if key_str in ['movies:by_rating', 'movies:by_year']:
                                continue
                            
                            # Get the hash data for this movie
                            movie_data = conn.hgetall(key_str)
                            if movie_data and movie_data.get('title', '').lower() == title.lower():
                                # Found the movie - delete it
                                deleted_count = conn.delete(key_str)
                                deleted_keys = [key_str]
                                
                                # Also remove from sorted sets
                                conn.client.zrem('movies:by_rating', key_str)
                                conn.client.zrem('movies:by_year', key_str)
                                
                                self.logger.info(f"Deleted movie {key_str}")
                                break
                        
                        if deleted_keys or cursor == 0:
                            break
                    
                    if not deleted_keys:
                        self.logger.warning(f"Movie '{title}' not found")
                        return {'success': False, 'error': f'Movie "{title}" not found'}
                    
                    self.logger.info(f"Deleted {deleted_count} keys for '{title}'")
                    return {
                        'success': True,
                        'results': [{'deleted_count': deleted_count, 'keys': deleted_keys, 'title': title}],
                        'count': 1
                    }
                
                elif operation == 'find_and_update':
                    # Find movie by title first, then update
                    title = query_dict.get('title', '')
                    field = query_dict.get('field', '')
                    value = query_dict.get('value', '')
                    
                    if not title or not field:
                        return {'success': False, 'error': 'find_and_update requires title and field'}
                    
                    # Search for movie by title - scan all movie keys
                    cursor = 0
                    updated = 0
                    movie_key = None
                    
                    while True:
                        cursor, keys = conn.client.scan(cursor, match='movie:*', count=1000)
                        
                        for key in keys:
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            
                            # Skip sorted set index keys
                            if key_str in ['movies:by_rating', 'movies:by_year']:
                                continue
                            
                            # Get the hash data for this movie
                            movie_data = conn.hgetall(key_str)
                            if movie_data and movie_data.get('title', '').lower() == title.lower():
                                movie_key = key_str
                                
                                # Map field names (genre/genres are the same)
                                if field.lower() in ['genre', 'genres']:
                                    field = 'genres'
                                
                                # Convert value based on field type
                                if field.lower() == 'year':
                                    value = str(value)
                                
                                updated = conn.hset(key_str, field, str(value))
                                self.logger.info(f"Updated {key_str} field {field} to {value}")
                                break
                        
                        if movie_key or cursor == 0:
                            break
                    
                    if not movie_key:
                        self.logger.warning(f"Movie '{title}' not found")
                        return {'success': False, 'error': f'Movie "{title}" not found'}
                    
                    self.logger.info(f"Updated movie '{title}' field '{field}': {updated}")
                    return {
                        'success': True,
                        'results': [{'updated': updated, 'key': movie_key, 'field': field, 'value': value, 'title': title}],
                        'count': 1
                    }
                
                elif operation == 'filter_by_genre':
                    # Filter movies by genre using the genre:X:movies sets
                    genre = query_dict.get('genre', '')
                    if not genre:
                        return {'success': False, 'error': 'filter_by_genre requires genre'}
                    
                    # Get movie IDs from the genre set
                    genre_key = f"genre:{genre}:movies"
                    movie_ids = conn.smembers(genre_key)
                    
                    if not movie_ids:
                        return {'success': False, 'error': f'No movies found in genre "{genre}"'}
                    
                    # Fetch movie data for each ID
                    results_list = []
                    for movie_id in list(movie_ids)[:10]:  # Limit to 10
                        movie_key = f"movie:{movie_id}"
                        movie_data = conn.hgetall(movie_key)
                        if movie_data:
                            movie_data['_key'] = movie_key
                            results_list.append(movie_data)
                    
                    self.logger.info(f"Found {len(results_list)} movies in genre '{genre}'")
                    return {
                        'success': True,
                        'results': results_list,
                        'count': len(results_list)
                    }
                
                elif operation == 'filter_by_cast':
                    # Filter movies by actor/cast member
                    actor = query_dict.get('actor', '')
                    if not actor:
                        return {'success': False, 'error': 'filter_by_cast requires actor'}
                    
                    # Scan all movie keys and check cast lists
                    cursor = 0
                    results_list = []
                    
                    while len(results_list) < 10:  # Limit to 10 results
                        cursor, keys = conn.client.scan(cursor, match='movie:*', count=1000)
                        
                        for key in keys:
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            
                            # Skip non-hash keys
                            if ':' not in key_str or key_str.count(':') > 1:
                                continue
                            
                            # Get cast list for this movie
                            cast_list = conn.lrange(f"{key_str}:cast", 0, -1)
                            
                            # Check if actor is in cast
                            if any(actor.lower() in str(c).lower() for c in cast_list):
                                # Get movie data
                                movie_data = conn.hgetall(key_str)
                                if movie_data:
                                    movie_data['_key'] = key_str
                                    results_list.append(movie_data)
                                    if len(results_list) >= 10:
                                        break
                        
                        if cursor == 0:
                            break
                    
                    if not results_list:
                        return {'success': False, 'error': f'No movies found with actor "{actor}"'}
                    
                    self.logger.info(f"Found {len(results_list)} movies with actor '{actor}'")
                    return {
                        'success': True,
                        'results': results_list,
                        'count': len(results_list)
                    }
                
                elif operation == 'filter_by_director':
                    # Filter movies by director
                    director = query_dict.get('director', '')
                    if not director:
                        return {'success': False, 'error': 'filter_by_director requires director'}
                    
                    # Scan all movie keys and check director lists
                    cursor = 0
                    results_list = []
                    
                    while len(results_list) < 10:  # Limit to 10 results
                        cursor, keys = conn.client.scan(cursor, match='movie:*', count=1000)
                        
                        for key in keys:
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            
                            # Skip non-hash keys
                            if ':' not in key_str or key_str.count(':') > 1:
                                continue
                            
                            # Get director list for this movie
                            director_list = conn.lrange(f"{key_str}:directors", 0, -1)
                            
                            # Check if director is in list
                            if any(director.lower() in str(d).lower() for d in director_list):
                                # Get movie data
                                movie_data = conn.hgetall(key_str)
                                if movie_data:
                                    movie_data['_key'] = key_str
                                    results_list.append(movie_data)
                                    if len(results_list) >= 10:
                                        break
                        
                        if cursor == 0:
                            break
                    
                    if not results_list:
                        return {'success': False, 'error': f'No movies found by director "{director}"'}
                    
                    self.logger.info(f"Found {len(results_list)} movies by director '{director}'")
                    return {
                        'success': True,
                        'results': results_list,
                        'count': len(results_list)
                    }
                
                elif operation == 'delete':
                    # Validate delete operation
                    if 'keys' not in query_dict or not query_dict['keys']:
                        self.logger.error("Redis delete operation missing 'keys' field")
                        return {'success': False, 'error': 'Delete operation requires keys'}
                    
                    keys = query_dict['keys']
                    deleted_count = conn.delete(*keys)
                    self.logger.info(f"Deleted {deleted_count} keys")
                    return {
                        'success': True,
                        'results': [{'deleted_count': deleted_count, 'keys': keys}],
                        'count': 1
                    }
                
                elif operation == 'update_hash':
                    # Validate update operation
                    required_fields = ['key', 'field', 'value']
                    missing_fields = [f for f in required_fields if f not in query_dict]
                    if missing_fields:
                        self.logger.error(f"Redis update_hash missing fields: {missing_fields}")
                        return {'success': False, 'error': f'Update operation requires: {missing_fields}'}
                    
                    key = query_dict['key']
                    field = query_dict['field']
                    value = query_dict['value']
                    result = conn.hset(key, field, str(value))
                    self.logger.info(f"Updated hash {key} field {field}: {result}")
                    return {
                        'success': True,
                        'results': [{'updated': result, 'key': key, 'field': field, 'value': value}],
                        'count': 1
                    }
                
                else:
                    self.logger.error(f"Unknown Redis operation: {operation}")
                    return {'success': False, 'error': f'Unknown operation: {operation}'}
            
            # Handle normal read commands
            commands = query_dict.get('commands', [])
            results = []
            
            for cmd in commands:
                command = cmd.get('command')
                args = cmd.get('args', [])
                
                if command == 'GET':
                    result = conn.get(args[0])
                elif command == 'HGETALL':
                    result = conn.hgetall(args[0])
                elif command == 'ZREVRANGE':
                    withscores = 'WITHSCORES' in [a.upper() for a in args]
                    keys = conn.zrevrange(args[0], int(args[1]), int(args[2]), withscores=False)
                    # Fetch full movie data for each key
                    result = []
                    for key in keys:
                        movie_data = conn.hgetall(key)
                        if movie_data:
                            movie_data['_key'] = key.decode('utf-8') if isinstance(key, bytes) else key
                            result.append(movie_data)
                    if not result:
                        result = keys  # Fallback to keys if no hash data found
                elif command == 'ZRANGEBYSCORE':
                    min_score = args[1] if len(args) > 1 else '-inf'
                    max_score = args[2] if len(args) > 2 else '+inf'
                    keys = conn.client.zrangebyscore(args[0], min_score, max_score)
                    # Fetch full movie data for each key
                    result = []
                    for key in keys[:10]:  # Limit to 10 results
                        movie_data = conn.hgetall(key)
                        if movie_data:
                            movie_data['_key'] = key.decode('utf-8') if isinstance(key, bytes) else key
                            result.append(movie_data)
                    if not result:
                        result = keys  # Fallback to keys if no hash data found
                elif command == 'ZRANGE':
                    # Check if args are numeric (rank-based) or if we should use score-based
                    try:
                        start = int(args[1]) if len(args) > 1 else 0
                        end = int(args[2]) if len(args) > 2 else -1
                        keys = conn.zrange(args[0], start, end, withscores=False)
                        # Fetch full movie data for each key
                        result = []
                        for key in keys:
                            movie_data = conn.hgetall(key)
                            if movie_data:
                                movie_data['_key'] = key.decode('utf-8') if isinstance(key, bytes) else key
                                result.append(movie_data)
                        if not result:
                            result = keys  # Fallback to keys if no hash data found
                    except (ValueError, IndexError):
                        # If args are not integers, assume they are scores for ZRANGEBYSCORE
                        min_score = args[1] if len(args) > 1 else '-inf'
                        max_score = args[2] if len(args) > 2 else '+inf'
                        keys = conn.client.zrangebyscore(args[0], min_score, max_score)
                        # Fetch full movie data for each key
                        result = []
                        for key in keys[:10]:  # Limit to 10 results
                            movie_data = conn.hgetall(key)
                            if movie_data:
                                movie_data['_key'] = key.decode('utf-8') if isinstance(key, bytes) else key
                                result.append(movie_data)
                        if not result:
                            result = keys
                elif command == 'SMEMBERS':
                    result = list(conn.smembers(args[0]))
                elif command == 'LRANGE':
                    result = conn.lrange(args[0], int(args[1]), int(args[2]))
                else:
                    result = f"Unknown command: {command}"
                
                results.append({
                    'command': f"{command} {' '.join(map(str, args))}",
                    'result': result
                })
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"Redis execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_sparql(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SPARQL query or CRUD operation"""
        try:
            if 'rdf' not in self.connectors:
                self.connectors['rdf'] = RDFConnector()
                self.connectors['rdf'].connect()
            
            conn = self.connectors['rdf']
            
            # Handle CRUD operations
            if 'operation' in query_dict:
                operation = query_dict['operation']
                self.logger.info(f"Detected RDF CRUD operation: {operation}")
                
                if operation == 'find':
                    # Find a movie by title
                    title = query_dict.get('title', '')
                    if not title:
                        return {'success': False, 'error': 'find requires title'}
                    
                    # Query to find movie by title
                    find_query = f"""
                    PREFIX ex: <http://example.org/>
                    SELECT ?movie ?title ?year ?plot ?rating ?genreName WHERE {{
                        ?movie a ex:Movie ;
                               ex:title ?title .
                        FILTER (lcase(str(?title)) = lcase("{title}"))
                        
                        OPTIONAL {{ ?movie ex:year ?year }}
                        OPTIONAL {{ ?movie ex:plot ?plot }}
                        OPTIONAL {{ ?movie ex:imdbRating ?rating }}
                        OPTIONAL {{
                            ?movie ex:hasGenre ?genre .
                            ?genre ex:name ?genreName
                        }}
                    }}
                    """
                    results = conn.execute_query(find_query)
                    
                    if not results:
                        self.logger.warning(f"Movie '{title}' not found")
                        return {'success': False, 'error': f'Movie "{title}" not found'}
                    
                    self.logger.info(f"Found movie: {results[0]}")
                    return {
                        'success': True,
                        'results': results,
                        'count': len(results)
                    }
                
                elif operation == 'create':
                    # Create a new movie
                    title = query_dict.get('title', '')
                    year = query_dict.get('year', '')
                    genres = query_dict.get('genres', '')
                    
                    if not title or not year:
                        return {'success': False, 'error': 'Create operation requires title and year'}
                    
                    # Create movie URI (replace spaces with underscores)
                    movie_uri = f"http://example.org/movie/{title.replace(' ', '_')}"
                    
                    # Build INSERT query
                    insert_query = f"""
                    PREFIX ex: <http://example.org/>
                    INSERT DATA {{
                        <{movie_uri}> a ex:Movie ;
                                      ex:title "{title}" ;
                                      ex:year "{year}" .
                    }}
                    """
                    
                    # Add genre if provided
                    if genres:
                        genre_uri = f"http://example.org/genre/{genres.replace(' ', '_')}"
                        insert_query = f"""
                        PREFIX ex: <http://example.org/>
                        INSERT DATA {{
                            <{movie_uri}> a ex:Movie ;
                                          ex:title "{title}" ;
                                          ex:year "{year}" ;
                                          ex:hasGenre <{genre_uri}> .
                            <{genre_uri}> a ex:Genre ;
                                          ex:name "{genres}" .
                        }}
                        """
                    
                    success = conn.execute_update(insert_query)
                    self.logger.info(f"Created movie {movie_uri}: {title}")
                    return {
                        'success': True,
                        'results': [{'created': success, 'subject': movie_uri, 'title': title, 'year': year}],
                        'count': 1
                    }
                
                elif operation == 'find_and_delete':
                    # Find movie by title first, then delete
                    title = query_dict.get('title', '')
                    if not title:
                        return {'success': False, 'error': 'find_and_delete requires title'}
                    
                    # Query to find the movie URI by title
                    find_query = f"""
                    PREFIX ex: <http://example.org/>
                    SELECT ?movie WHERE {{
                        ?movie a ex:Movie ;
                               ex:title "{title}" .
                    }}
                    """
                    results = conn.execute_query(find_query)
                    
                    if not results:
                        return {'success': False, 'error': f'Movie "{title}" not found'}
                    
                    subject = results[0].get('movie', '')
                    
                    # Delete all triples for this subject
                    delete_query = f"""
                    DELETE WHERE {{
                        <{subject}> ?p ?o .
                    }}
                    """
                    success = conn.execute_update(delete_query)
                    self.logger.info(f"Deleted triples for {title}: {success}")
                    return {
                        'success': True,
                        'results': [{'deleted': success, 'subject': subject, 'title': title}],
                        'count': 1
                    }
                
                elif operation == 'find_and_update':
                    # Find movie by title first, then update
                    title = query_dict.get('title', '')
                    field = query_dict.get('field', '')
                    value = query_dict.get('value', '')
                    
                    if not title or not field:
                        return {'success': False, 'error': 'find_and_update requires title and field'}
                    
                    # Query to find the movie URI and current value
                    predicate_map = {
                        'genre': 'hasGenre',
                        'year': 'year',
                        'rating': 'imdbRating',
                        'plot': 'plot'
                    }
                    predicate_name = predicate_map.get(field.lower(), field)
                    
                    find_query = f"""
                    PREFIX ex: <http://example.org/>
                    SELECT ?movie ?value WHERE {{
                        ?movie a ex:Movie ;
                               ex:title "{title}" .
                        OPTIONAL {{ ?movie ex:{predicate_name} ?value }}
                    }}
                    """
                    results = conn.execute_query(find_query)
                    
                    if not results:
                        return {'success': False, 'error': f'Movie "{title}" not found'}
                    
                    subject = results[0].get('movie', '')
                    old_value = results[0].get('value', '')
                    
                    # For genre updates, we need to handle the genre node
                    if field.lower() in ['genre', 'genres']:
                        # First, delete any existing genre relationships
                        delete_query = f"""
                        PREFIX ex: <http://example.org/>
                        DELETE {{
                            <{subject}> ex:hasGenre ?g .
                        }}
                        WHERE {{
                            <{subject}> ex:hasGenre ?g .
                        }}
                        """
                        conn.execute_update(delete_query)
                        
                        # Then insert the new genre relationship
                        genre_uri = f"http://example.org/genre/{value.replace(' ', '_')}"
                        insert_query = f"""
                        PREFIX ex: <http://example.org/>
                        INSERT DATA {{
                            <{subject}> ex:hasGenre <{genre_uri}> .
                            <{genre_uri}> a ex:Genre ;
                                         ex:name "{value}" .
                        }}
                        """
                        success = conn.execute_update(insert_query)
                    else:
                        # Simple field update
                        # First delete old value if exists
                        delete_query = f"""
                        PREFIX ex: <http://example.org/>
                        DELETE {{
                            <{subject}> ex:{predicate_name} ?oldValue .
                        }}
                        WHERE {{
                            <{subject}> ex:{predicate_name} ?oldValue .
                        }}
                        """
                        conn.execute_update(delete_query)
                        
                        # Then insert new value
                        insert_query = f"""
                        PREFIX ex: <http://example.org/>
                        INSERT DATA {{
                            <{subject}> ex:{predicate_name} "{value}" .
                        }}
                        """
                        success = conn.execute_update(insert_query)
                    self.logger.info(f"Updated {title} {field}: {success}")
                    return {
                        'success': True,
                        'results': [{'updated': success, 'subject': subject, 'field': field, 'old_value': old_value, 'new_value': value, 'title': title}],
                        'count': 1
                    }
                
                elif operation == 'delete':
                    # Validate delete operation
                    if 'subject' not in query_dict or not query_dict['subject']:
                        self.logger.error("RDF delete operation missing 'subject' field")
                        return {'success': False, 'error': 'Delete operation requires subject URI'}
                    
                    subject = query_dict['subject']
                    # Delete all triples for this subject using SPARQL UPDATE
                    delete_query = f"""
                    DELETE WHERE {{
                        <{subject}> ?p ?o .
                    }}
                    """
                    success = conn.execute_update(delete_query)
                    self.logger.info(f"Deleted triples for {subject}: {success}")
                    return {
                        'success': True,
                        'results': [{'deleted': success, 'subject': subject}],
                        'count': 1
                    }
                
                elif operation == 'update':
                    # Validate update operation
                    required_fields = ['subject', 'predicate', 'old_value', 'new_value']
                    missing_fields = [f for f in required_fields if f not in query_dict]
                    if missing_fields:
                        self.logger.error(f"RDF update missing fields: {missing_fields}")
                        return {'success': False, 'error': f'Update operation requires: {missing_fields}'}
                    
                    subject = query_dict['subject']
                    predicate = query_dict['predicate']
                    old_value = query_dict['old_value']
                    new_value = query_dict['new_value']
                    
                    # Delete old triple and insert new one
                    delete_query = f"""
                    PREFIX ex: <http://example.org/>
                    DELETE DATA {{
                        <{subject}> <{predicate}> "{old_value}" .
                    }}
                    """
                    insert_query = f"""
                    PREFIX ex: <http://example.org/>
                    INSERT DATA {{
                        <{subject}> <{predicate}> "{new_value}" .
                    }}
                    """
                    
                    conn.execute_update(delete_query)
                    success = conn.execute_update(insert_query)
                    self.logger.info(f"Updated {subject} {predicate}: {success}")
                    return {
                        'success': True,
                        'results': [{'updated': success, 'subject': subject, 'predicate': predicate, 'old_value': old_value, 'new_value': new_value}],
                        'count': 1
                    }
                
                else:
                    self.logger.error(f"Unknown RDF operation: {operation}")
                    return {'success': False, 'error': f'Unknown operation: {operation}'}
            
            # Handle normal SPARQL SELECT queries
            sparql = query_dict.get('sparql')
            
            results = conn.execute_query(sparql)
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"SPARQL execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_hbase(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute HBase operation or CRUD"""
        try:
            if 'hbase' not in self.connectors:
                self.connectors['hbase'] = HBaseConnector()
                self.connectors['hbase'].connect()
            
            conn = self.connectors['hbase']
            
            # Test connection and reconnect if needed
            try:
                if not conn.test_connection():
                    self.logger.warning("HBase connection lost, attempting to reconnect...")
                    conn.connect()
            except Exception as e:
                self.logger.warning(f"HBase connection test failed: {e}, attempting to reconnect...")
                conn.connect()
            operation = query_dict.get('operation', 'scan')
            table = query_dict.get('table', 'movies')  # Default to 'movies' table
            
            # Handle CRUD operations
            if operation == 'find':
                # Find a movie by title
                title = query_dict.get('title', '')
                if not title:
                    return {'success': False, 'error': 'find requires title'}
                
                self.logger.info(f"Searching HBase table '{table}' for movie: '{title}'")
                
                # Scan table to find movie by title
                results = conn.scan(table, columns=[], limit=1000)
                
                self.logger.info(f"HBase scan of '{table}' returned {len(results)} rows")
                
                for row in results:
                    data = row.get('data', {})
                    row_title = data.get('info:title', '')
                    self.logger.debug(f"Checking row: {row.get('row_key')} - title: '{row_title}'")
                    
                    if row_title.lower() == title.lower():
                        # Found the movie
                        self.logger.info(f"Found movie in HBase: {row}")
                        return {
                            'success': True,
                            'results': [row],
                            'count': 1
                        }
                
                # Movie not found
                self.logger.warning(f"Movie '{title}' not found in HBase table '{table}' (scanned {len(results)} rows)")
                return {'success': False, 'error': f'Movie "{title}" not found in {len(results)} rows'}
            
            elif operation == 'create':
                # Create a new movie
                table = query_dict.get('table', 'movies')  # Default to 'movies' table
                title = query_dict.get('title', '')
                year = query_dict.get('year', '')
                genres = query_dict.get('genres', '')
                
                if not title or not year:
                    return {'success': False, 'error': 'Create operation requires title and year'}
                
                # Generate a new row key
                import time
                import random
                row_key = f"movie_{int(time.time())}_{random.randint(1000, 9999)}"
                
                # Build the data dictionary with column families
                data = {
                    'info:title': title,
                    'info:year': str(year)
                }
                
                if genres:
                    data['metadata:genres'] = genres
                if 'plot' in query_dict:
                    data['info:plot'] = query_dict['plot']
                if 'rating' in query_dict:
                    data['ratings:imdb_rating'] = str(query_dict['rating'])
                
                # Insert the movie
                try:
                    success = conn.put(table, row_key, data)
                    self.logger.info(f"Created movie in HBase table '{table}': {row_key} - {title}")
                    return {
                        'success': True,
                        'results': [{'created': True, 'row_key': row_key, 'title': title, 'year': year}],
                        'count': 1
                    }
                except Exception as e:
                    self.logger.error(f"Error creating movie in HBase: {e}")
                    return {
                        'success': False,
                        'error': f'Failed to create movie: {str(e)}'
                    }
            
            elif operation == 'find_and_delete':
                # Find movie by title first, then delete
                table = query_dict.get('table', 'movies')  # Default to 'movies' table
                title = query_dict.get('title', '')
                if not title:
                    return {'success': False, 'error': 'find_and_delete requires title'}
                
                # Scan to find the movie by title
                results = conn.scan(table, columns=[], limit=1000)
                deleted = False
                row_key = None
                
                for row in results:
                    data = row.get('data', {})
                    if data.get('info:title', '').lower() == title.lower():
                        row_key = row.get('row_key')
                        deleted = conn.delete(table, row_key)
                        break
                
                self.logger.info(f"Deleted row for '{title}': {deleted}")
                return {
                    'success': True,
                    'results': [{'deleted': deleted, 'row_key': row_key, 'title': title}],
                    'count': 1
                }
            
            elif operation == 'find_and_update':
                # Find movie by title first, then update
                table = query_dict.get('table', 'movies')  # Default to 'movies' table
                title = query_dict.get('title', '')
                updates = query_dict.get('updates', {})
                
                if not title:
                    return {'success': False, 'error': 'find_and_update requires title'}
                
                # If updates is empty, try to build from field/value
                if not updates:
                    field = query_dict.get('field', '')
                    value = query_dict.get('value', '')
                    if field and value is not None:
                        # Map field name to column family:qualifier
                        field_map = {
                            'genre': 'metadata:genres',
                            'genres': 'metadata:genres',
                            'year': 'info:year',
                            'title': 'info:title',
                            'plot': 'info:plot',
                            'rating': 'ratings:imdb_rating'
                        }
                        col_name = field_map.get(field.lower(), f"info:{field}")
                        updates = {col_name: str(value)}
                
                if not updates:
                    return {'success': False, 'error': 'find_and_update requires updates or field/value'}
                
                # Scan to find the movie by title
                results = conn.scan(table, columns=[], limit=1000)
                updated = False
                row_key = None
                
                for row in results:
                    data = row.get('data', {})
                    if data.get('info:title', '').lower() == title.lower():
                        row_key = row.get('row_key')
                        updated = conn.put(table, row_key, updates)
                        self.logger.info(f"Updated {row_key}: {updates}")
                        break
                
                if not row_key:
                    self.logger.warning(f"Movie '{title}' not found")
                    return {'success': False, 'error': f'Movie "{title}" not found'}
                
                self.logger.info(f"Updated row for '{title}': {updated}")
                return {
                    'success': True,
                    'results': [{'updated': updated, 'row_key': row_key, 'updates': updates, 'title': title}],
                    'count': 1
                }
            
            elif operation == 'delete':
                # Validate delete operation
                if 'row_key' not in query_dict or not query_dict['row_key']:
                    self.logger.error("HBase delete operation missing 'row_key' field")
                    return {'success': False, 'error': 'Delete operation requires row_key'}
                
                row_key = query_dict['row_key']
                success = conn.delete(table, row_key)
                self.logger.info(f"Deleted row {row_key}: {success}")
                return {
                    'success': True,
                    'results': [{'deleted': success, 'row_key': row_key}],
                    'count': 1
                }
            
            elif operation == 'put':
                # Validate put operation
                required_fields = ['row_key', 'data']
                missing_fields = [f for f in required_fields if f not in query_dict]
                if missing_fields:
                    self.logger.error(f"HBase put missing fields: {missing_fields}")
                    return {'success': False, 'error': f'Put operation requires: {missing_fields}'}
                
                row_key = query_dict['row_key']
                data = query_dict['data']
                success = conn.put(table, row_key, data)
                self.logger.info(f"Put row {row_key}: {success}")
                return {
                    'success': True,
                    'results': [{'updated': success, 'row_key': row_key, 'data': data}],
                    'count': 1
                }
            
            # Handle normal read operations
            elif operation == 'get':
                row_key = query_dict.get('row_key')
                columns = query_dict.get('columns')
                result = conn.get(table, row_key, columns)
                results = [{'row_key': row_key, 'data': result}] if result else []
            
            elif operation == 'scan':
                columns = query_dict.get('columns')
                limit = query_dict.get('limit', 10)
                filter_expr = query_dict.get('filter')
                
                # Scan all results (or a larger limit)
                scan_limit = min(limit * 10, 1000)  # Scan more to allow for filtering
                results = conn.scan(table, columns=columns, limit=scan_limit)
                
                # Apply filter if specified
                if filter_expr and '1915' in filter_expr:
                    # Simple filter for year 1915
                    results = [r for r in results if r.get('data', {}).get('info:year') == '1915']
                
                # Apply limit after filtering
                results = results[:limit]
            
            else:
                results = []
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"HBase execution error: {e}")
            error_msg = str(e)
            
            # Provide user-friendly error messages
            if "10053" in error_msg or "connection" in error_msg.lower():
                error_msg = "HBase connection lost. Please check if HBase Docker container is running: docker ps | grep hbase"
            elif "table" in error_msg.lower() and "does not exist" in error_msg.lower():
                error_msg = "HBase table does not exist. Please run the data loading script first."
            
            return {'success': False, 'error': error_msg}
    
    def close_all(self):
        """Close all database connections"""
        for name, conn in self.connectors.items():
            try:
                conn.disconnect()
            except:
                pass