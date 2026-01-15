"""
LLM Query Translator using Google Gemini API (FREE)
Translates natural language queries to database-specific queries
"""

import json
import os
from typing import Dict, Any, Optional
import google.generativeai as genai
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import setup_logger

# Load environment variables
load_dotenv()

class QueryTranslator:
    """Translates natural language to database queries using Gemini"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize query translator
        
        Args:
            api_key: Google Gemini API key (optional, reads from .env if not provided)
        """
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found. Add it to .env file or pass as parameter.")
        
        # Configure Gemini
        genai.configure(api_key=self.api_key)
        
        # Use gemini-2.0-flash-exp (the correct model name for Gemini 2.5)
        # Note: Google often uses "2.0" in the API even for 2.5 marketing name
        try:
            self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
            self.logger = setup_logger(__name__)
            self.logger.info(f"✓ Initialized Gemini with model: gemini-2.0-flash-exp")
        except:
            # Fallback to standard naming
            self.model = genai.GenerativeModel('gemini-2.5-flash')
            self.logger = setup_logger(__name__)
            self.logger.info(f"✓ Initialized Gemini with model: gemini-2.5-flash")
        
        # Configure generation settings for better JSON output
        self.generation_config = {
            'temperature': 0.1,
            'top_p': 0.8,
            'top_k': 40,
            'max_output_tokens': 2048,
        }
    
    def _extract_json(self, text: str) -> dict:
        """Extract JSON from LLM response"""
        text = text.strip()
        
        # Remove markdown code blocks
        if '```json' in text:
            text = text.split('```json')[1].split('```')[0]
        elif '```' in text:
            parts = text.split('```')
            if len(parts) >= 2:
                text = parts[1]
        
        text = text.strip()
        
        # Find JSON object
        start = text.find('{')
        end = text.rfind('}') + 1
        
        if start != -1 and end > start:
            json_str = text[start:end]
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error: {e}")
                self.logger.error(f"Text: {json_str[:200]}")
                raise
        
        return json.loads(text)
    
    
    def translate_to_mongodb(self, natural_query: str, schema_context: str) -> Dict[str, Any]:
        """
        Translate natural language to MongoDB query
        
        Args:
            natural_query: Natural language query
            schema_context: Database schema context
        
        Returns:
            Dictionary with query and explanation
        """
        prompt = f"""You are a MongoDB query expert. Translate this natural language query to a MongoDB query.

Database Schema:
{schema_context}

Natural Language Query: {natural_query}

Return ONLY valid JSON (no markdown, no extra text) with this exact structure:
{{
    "collection": "movies",
    "operation": "find",
    "query": {{}},
    "projection": {{}},
    "sort": [],
    "limit": 10,
    "explanation": "Brief explanation of the query"
}}

For aggregations, use:
{{
    "collection": "movies",
    "operation": "aggregate",
    "pipeline": [
        {{"$match": {{}}}},
        {{"$group": {{}}}},
        {{"$sort": {{}}}}
    ],
    "explanation": "Brief explanation"
}}

For updates, use:
{{
    "collection": "movies",
    "operation": "update_one",
    "query": {{"title": "Movie Title"}},
    "update": {{"$set": {{"genres": ["Action"]}}}},
    "explanation": "Brief explanation"
}}

For deletes, use:
{{
    "collection": "movies",
    "operation": "delete_one",
    "query": {{"title": "Movie Title"}},
    "explanation": "Brief explanation"
}}

MongoDB Query Rules:
- Collection is almost always "movies"
- Use operators: $eq, $gt, $gte, $lt, $lte, $in, $regex
- For "find all": query = {{}}
- For "find by field": query = {{"field": value}} or {{"field": {{"$operator": value}}}}
- For text search: {{"field": {{"$regex": "pattern", "$options": "i"}}}}
- For "movies from year": {{"year": 2015}}
- For "movies with rating above X": {{"imdb.rating": {{"$gte": X}}}}
- For "action movies": {{"genres": "Action"}}
- IMPORTANT: Leave "projection" empty {{}} to return ALL fields, or specify fields to include like {{"title": 1, "year": 1}}
- For best results, use empty projection {{}} to get all movie data

Update/Delete Rules:
- IMPORTANT: Detect keywords "update", "change", "modify", "replace", "set" → use update operations
- IMPORTANT: Detect keywords "delete", "remove" → use delete operations
- IMPORTANT: Detect keywords "add", "insert", "create" → use insert operations
- For updates: Use "update_one" (single doc) or "update_many" (multiple docs)
- Update operators: $set (replace/add field), $unset (remove field), $push (add to array), $pull (remove from array)
- For deletes: Use "delete_one" (single doc) or "delete_many" (multiple docs)
- Always specify the correct query to match the document(s)
- IMPORTANT: genres field must ALWAYS be an array like ["Action"], never just "Action"
- IMPORTANT: year field must be a number, not a string

Examples:
1. "Find all movies from 2015" → {{"collection": "movies", "operation": "find", "query": {{"year": 2015}}, "projection": {{}}, "limit": 10, "explanation": "Find movies from 2015"}}
2. "Show me action movies" → {{"collection": "movies", "operation": "find", "query": {{"genres": "Action"}}, "projection": {{}}, "limit": 10, "explanation": "Find action movies"}}
3. "Find movies with rating above 8" → {{"collection": "movies", "operation": "find", "query": {{"imdb.rating": {{"$gte": 8}}}}, "projection": {{}}, "limit": 10, "explanation": "Find highly rated movies"}}
4. "Change The Birth of a Nation genre to Action" → {{"collection": "movies", "operation": "update_one", "query": {{"title": "The Birth of a Nation"}}, "update": {{"$set": {{"genres": ["Action"]}}}}, "explanation": "Update movie genre to Action"}}
5. "Update Inception rating to 9.5" → {{"collection": "movies", "operation": "update_one", "query": {{"title": "Inception"}}, "update": {{"$set": {{"imdb.rating": 9.5}}}}, "explanation": "Update movie rating"}}
6. "Delete the movie Titanic" → {{"collection": "movies", "operation": "delete_one", "query": {{"title": "Titanic"}}, "explanation": "Delete movie Titanic"}}
7. "Remove all movies from 1990" → {{"collection": "movies", "operation": "delete_many", "query": {{"year": 1990}}, "explanation": "Delete all movies from 1990"}}

Important: Return ONLY the JSON object, no text before or after."""

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=self.generation_config
            )
            
            result = self._extract_json(response.text)
            result['database_type'] = 'mongodb'
            result['natural_query'] = natural_query
            
            self.logger.info("✓ Translated to MongoDB query")
            return result
            
        except Exception as e:
            self.logger.error(f"Error translating to MongoDB: {e}")
            return {
                'error': str(e),
                'database_type': 'mongodb',
                'natural_query': natural_query
            }
    
    def translate_to_neo4j(self, natural_query: str, schema_context: str) -> Dict[str, Any]:
        """Translate natural language to Neo4j Cypher query or CRUD operation"""
        
        prompt = f"""You are a Neo4j Cypher expert. Translate this natural language query to either a Cypher query or a CRUD operation.

Graph Schema:
{schema_context}

Natural Language Query: {natural_query}

Return ONLY valid JSON (no markdown). For queries, use:
{{
    "cypher": "MATCH (m:Movie) WHERE m.year > 2000 RETURN m.title, m.year LIMIT 10",
    "parameters": {{}},
    "explanation": "Brief explanation"
}}

For CRUD operations, use:
{{
    "operation": "update_node",
    "label": "Movie",
    "match_properties": {{"title": "Movie Title"}},
    "update_properties": {{"genres": ["Action"]}},
    "explanation": "Brief explanation"
}}

Cypher Query Rules:
- Node labels: Movie, Person
- Node pattern: (variable:Label {{property: value}})
- Relationship: (a)-[:REL_TYPE]->(b)
- WHERE for filtering: WHERE m.year > 2000
- RETURN entire nodes with related data using OPTIONAL MATCH for directors and cast
- Always add LIMIT (default 10)
- CRITICAL: ALWAYS return using this exact pattern: "RETURN m, collect(DISTINCT d.name) as directors, collect(DISTINCT a.name) as cast"

Query Pattern Template (FOLLOW THIS EXACTLY):
MATCH (m:Movie)
WHERE <optional filter conditions>
OPTIONAL MATCH (d:Person)-[:DIRECTED]->(m)
OPTIONAL MATCH (a:Person)-[:ACTED_IN]->(m)
RETURN m, collect(DISTINCT d.name) as directors, collect(DISTINCT a.name) as cast
LIMIT 10

CRUD Operations:
- IMPORTANT: Detect keywords "update", "change", "modify", "set" → use "update_node"
- IMPORTANT: Detect keywords "delete", "remove" → use "delete_node"
- IMPORTANT: Detect keywords "create", "add", "insert" → use "create_node"
- IMPORTANT: Detect single filter queries (genre OR year OR director OR actor only) → use specific filter operation
- IMPORTANT: Detect queries with multiple conditions (genre+year, director+year, etc.) → use operation: "filter_by_multiple"
- update_node: Update properties of a node (requires label, match_properties, update_properties)
- delete_node: Delete a node and its relationships (requires label, properties)
- create_node: Create a new node (requires label, properties)
- create_relationship: Create a relationship between nodes
- filter_by_genre: Filter by genre only (requires genre)
- filter_by_year: Filter by year only (requires year)
- filter_by_director: Filter by director only (requires director)
- filter_by_cast: Filter by actor only (requires actor)
- filter_by_multiple: Filter by multiple criteria (requires filters dict with any combination of: genre, director, actor, year)
- IMPORTANT: genres field must ALWAYS be an array like ["Action"], never just "Action"
- IMPORTANT: year field must be a number, not a string

Examples:
1. "Find all movies" → {{"cypher": "MATCH (m:Movie) OPTIONAL MATCH (d:Person)-[:DIRECTED]->(m) OPTIONAL MATCH (a:Person)-[:ACTED_IN]->(m) RETURN m, collect(DISTINCT d.name) as directors, collect(DISTINCT a.name) as cast LIMIT 10", "parameters": {{}}, "explanation": "Find all movies with directors and cast"}}
2. "Drama movies" → {{"operation": "filter_by_genre", "genre": "Drama", "explanation": "Filter movies by Drama genre"}}
3. "Movies from 1927" → {{"operation": "filter_by_year", "year": 1927, "explanation": "Filter movies from 1927"}}
4. "Movies by Frank Borzage" → {{"operation": "filter_by_director", "director": "Frank Borzage", "explanation": "Filter movies by director Frank Borzage"}}
5. "Drama movies from 1925" → {{"operation": "filter_by_multiple", "filters": {{"genre": "Drama", "year": 1925}}, "explanation": "Filter Drama movies from 1925"}}
6. "Movies by Frank Borzage from 1927" → {{"operation": "filter_by_multiple", "filters": {{"director": "Frank Borzage", "year": 1927}}, "explanation": "Filter movies by Frank Borzage from 1927"}}
7. "Update The Birth of a Nation genre to Action" → {{"operation": "update_node", "label": "Movie", "match_properties": {{"title": "The Birth of a Nation"}}, "update_properties": {{"genres": ["Action"]}}, "explanation": "Update movie genre to Action"}}
8. "Change Inception rating to 9.5" → {{"operation": "update_node", "label": "Movie", "match_properties": {{"title": "Inception"}}, "update_properties": {{"imdb_rating": 9.5}}, "explanation": "Update movie rating"}}
9. "Add film Influencers from 2025" → {{"operation": "create_node", "label": "Movie", "properties": {{"title": "Influencers", "year": 2025, "genres": ["Documentary"]}}, "explanation": "Create new movie node"}}
10. "Delete the movie Titanic" → {{"operation": "delete_node", "label": "Movie", "properties": {{"title": "Titanic"}}, "explanation": "Delete movie node and its relationships"}}

IMPORTANT: 
- Always use OPTIONAL MATCH to fetch directors and cast
- Always return "m, collect(DISTINCT d.name) as directors, collect(DISTINCT a.name) as cast"
- The Movie node m contains properties: title, year, plot, genres (array), imdb_rating
- Directors and cast are Person nodes connected via DIRECTED and ACTED_IN relationships

Return ONLY the JSON, no additional text."""

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=self.generation_config
            )
            
            result = self._extract_json(response.text)
            result['database_type'] = 'neo4j'
            result['natural_query'] = natural_query
            
            self.logger.info("✓ Translated to Cypher query")
            return result
            
        except Exception as e:
            self.logger.error(f"Error translating to Neo4j: {e}")
            return {
                'error': str(e),
                'database_type': 'neo4j',
                'natural_query': natural_query
            }
    
    def translate_to_redis(self, natural_query: str, schema_context: str) -> Dict[str, Any]:
        """Translate natural language to Redis commands or CRUD operations"""
        
        prompt = """You are a Redis expert. Translate this natural language query to Redis commands or CRUD operations.

Key Patterns Available:
""" + schema_context + """

Natural Language Query: """ + natural_query + """

Return ONLY valid JSON. For queries, use:
{
    "commands": [
        {"command": "ZREVRANGE", "args": ["movies:by_rating", "0", "9", "WITHSCORES"]}
    ],
    "explanation": "Brief explanation"
}

For CRUD operations, use:
{
    "operation": "delete",
    "keys": ["movie:123", "movie:123:genres"],
    "explanation": "Delete movie and related data"
}

Or:
{
    "operation": "update_hash",
    "key": "movie:123",
    "field": "title",
    "value": "New Title",
    "explanation": "Update movie title"
}

Common Redis Commands:
- GET key - get string value
- HGETALL key - get all hash fields  
- ZREVRANGE key 0 9 WITHSCORES - top 10 from sorted set (high to low)
- ZRANGEBYSCORE key min max - range by score values (for year/rating ranges)
- SMEMBERS key - all set members
- LRANGE key 0 -1 - all list elements

Special Query Operations:
- For single filter queries: {{"operation": "filter_by_genre", "genre": "Action", "explanation": "..."}}
- For multiple filter queries: {{"operation": "filter_by_multiple", "filters": {{"genre": "Drama", "year": 1927}}, "explanation": "..."}}
- Available filters: genre, year, director, actor (for cast)
- Always use filter_by_multiple when query has 2 or more conditions (genre+year, director+year, etc.)

CRUD Operations:
- IMPORTANT: Detect "show", "find", "get", "display" + movie title → use operation: "find"
- IMPORTANT: Detect "add", "insert", "create" → use operation: "create"
- IMPORTANT: Detect "delete", "remove" → use operation: "delete" OR "find_and_delete"
- IMPORTANT: Detect "update", "change", "modify" → use operation: "update_hash" OR "find_and_update"
- find: Find a movie by title (requires title)
- create: Create a new movie (requires title, year, and optionally genres, plot, rating)
- delete: Delete one or more keys (requires keys array)
- update_hash: Update hash field (requires key, field, value)
- find_and_delete: Find movie by title and delete (requires title)
- find_and_update: Find movie by title and update (requires title, field, value)

CRITICAL: When user mentions movie by TITLE (not ID number), ALWAYS use find_and_delete or find_and_update.
NEVER use delete or update_hash for title-based operations.
For ADD/INSERT/CREATE operations, always use operation: "create".
For SHOW/FIND/GET movie by title, always use operation: "find".

Examples:
1. "Top rated movies" → {{"commands": [{{"command": "ZREVRANGE", "args": ["movies:by_rating", "0", "9", "WITHSCORES"]}}], "explanation": "Get top 10 highest rated movies"}}
2. "Movies from 2015" → {{"commands": [{{"command": "ZRANGEBYSCORE", "args": ["movies:by_year", "2015", "2015"]}}], "explanation": "Find movies from 2015"}}
3. "Action movies" → {{"operation": "filter_by_genre", "genre": "Action", "explanation": "Find Action movies"}}
4. "Movies with Tom Hanks" → {{"operation": "filter_by_cast", "actor": "Tom Hanks", "explanation": "Find movies with Tom Hanks"}}
5. "Movies by Christopher Nolan" → {{"operation": "filter_by_director", "director": "Christopher Nolan", "explanation": "Find movies by Christopher Nolan"}}
6. "Drama movies from 1927" → {{"operation": "filter_by_multiple", "filters": {{"genre": "Drama", "year": 1927}}, "explanation": "Find Drama movies from 1927"}}
7. "Action films with Tom Hanks from 2010" → {{"operation": "filter_by_multiple", "filters": {{"genre": "Action", "actor": "Tom Hanks", "year": 2010}}, "explanation": "Find Action movies with Tom Hanks from 2010"}}
8. "Show me the details of Influencers" → {{"operation": "find", "title": "Influencers", "explanation": "Find movie by title"}}
9. "Get movie Inception" → {{"operation": "find", "title": "Inception", "explanation": "Find movie by title"}}
10. "Add film Influencers from 2025" → {{"operation": "create", "title": "Influencers", "year": 2025, "genres": "Documentary", "explanation": "Create new movie"}}
11. "Insert movie Test with year 2020 and genre Action" → {{"operation": "create", "title": "Test", "year": 2020, "genres": "Action", "explanation": "Create new movie"}}
12. "Delete influencers" → {{"operation": "find_and_delete", "title": "Influencers", "explanation": "Find and delete movie by title"}}
13. "Remove the movie Titanic" → {{"operation": "find_and_delete", "title": "Titanic", "explanation": "Find and delete movie by title"}}
14. "Change influencers genre to action" → {{"operation": "find_and_update", "title": "Influencers", "field": "genres", "value": "Action", "explanation": "Update movie genre"}}
15. "Update Inception year to 2020" → {{"operation": "find_and_update", "title": "Inception", "field": "year", "value": "2020", "explanation": "Update movie year"}}

Return ONLY the JSON."""

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=self.generation_config
            )
            
            result = self._extract_json(response.text)
            result['database_type'] = 'redis'
            result['natural_query'] = natural_query
            
            self.logger.info("✓ Translated to Redis commands")
            return result
            
        except Exception as e:
            self.logger.error(f"Error translating to Redis: {e}")
            return {
                'error': str(e),
                'database_type': 'redis',
                'natural_query': natural_query
            }
    
    def translate_to_sparql(self, natural_query: str, schema_context: str) -> Dict[str, Any]:
        """Translate natural language to SPARQL query or CRUD operation"""
        
        prompt = f"""You are a SPARQL expert. Translate this natural language query to SPARQL or CRUD operation.

RDF Schema:
{schema_context}

Natural Language Query: {natural_query}

Return ONLY valid JSON. For queries, use:
{{
    "sparql": "PREFIX ex: <http://example.org/>\\nSELECT ?title ?year WHERE {{ ?movie a ex:Movie ; ex:title ?title ; ex:year ?year }} LIMIT 10",
    "explanation": "Brief explanation"
}}

For CRUD operations, use:
{{
    "operation": "delete",
    "subject": "http://example.org/movie/The_Birth_of_a_Nation",
    "explanation": "Delete all triples for this movie"
}}

Or:
{{
    "operation": "update",
    "subject": "http://example.org/movie/Inception",
    "predicate": "http://example.org/imdbRating",
    "old_value": "8.8",
    "new_value": "9.5",
    "explanation": "Update movie rating"
}}

SPARQL Query Rules:
- Always include: PREFIX ex: <http://example.org/>
- Triple pattern: ?subject predicate ?object
- Multiple patterns in {{ }}
- FILTER for conditions: FILTER(?year > "2000")
- For year filters: ex:year "1915" (use string literals, NOT ^^xsd:integer)
- Always add LIMIT (default 100 to allow for multiple directors/cast per movie)
- Return ALL relevant fields using OPTIONAL patterns

CRUD Operations:
- IMPORTANT: Detect "show", "find", "get", "display" + movie title → use operation: "find"
- IMPORTANT: Detect "add", "insert", "create" → use operation: "create"
- IMPORTANT: Detect "delete", "remove" → use operation: "delete" OR "find_and_delete"
- IMPORTANT: Detect "update", "change", "modify" → use operation: "put" OR "find_and_update"
- IMPORTANT: Detect single filter queries (genre OR year OR director OR actor only) → use specific filter operation
- IMPORTANT: Detect queries with multiple conditions (genre+year, director+year, etc.) → use operation: "filter_by_multiple"
- find: Find a movie by title (requires title)
- create: Create a new movie (requires title, year, and optionally genres, plot, rating)
- delete: Delete all triples for a subject (requires subject URI)
- update: Update a triple value (requires subject, predicate, old_value, new_value)
- find_and_delete: Find movie by title and delete (requires title)
- find_and_update: Find movie by title and update field (requires title, field, value)
- filter_by_genre: Filter by genre only (requires genre)
- filter_by_year: Filter by year only (requires year)
- filter_by_director: Filter by director only (requires director)
- filter_by_cast: Filter by actor only (requires actor)
- filter_by_multiple: Filter by multiple criteria (requires filters dict with any combination of: genre, director, actor, year)

NOTE: For CRUD by movie title (not full URI), use find_and_delete or find_and_update operations.
The system will first query to find the movie URI, then perform the operation.
Movie URIs follow pattern: http://example.org/movie/Title_With_Underscores

For queries with multiple filter conditions, ALWAYS use filter_by_multiple instead of constructing complex SPARQL filters.

Query Pattern Template (IMPORTANT - Follow this exactly):
PREFIX ex: <http://example.org/>
SELECT ?title ?year ?plot ?rating ?genreName ?directorName ?actorName
WHERE {{
  ?movie a ex:Movie ;
         ex:title ?title ;
         ex:year ?year .
  OPTIONAL {{ ?movie ex:plot ?plot }}
  OPTIONAL {{ ?movie ex:imdbRating ?rating }}
  OPTIONAL {{ ?movie ex:hasGenre ?g . ?g ex:name ?genreName }}
  OPTIONAL {{ ?movie ex:directedBy ?d . ?d ex:name ?directorName }}
  OPTIONAL {{ ?movie ex:starring ?a . ?a ex:name ?actorName }}
}}
LIMIT 100

CRITICAL: When user mentions movie by TITLE (not full URI), ALWAYS use find_and_delete or find_and_update.
NEVER use delete or update for title-based operations.
For ADD/INSERT/CREATE operations, always use operation: "create".
For SHOW/FIND/GET movie by title, always use operation: "find".

Examples:
1. "Find all movies" → {{"sparql": "PREFIX ex: <http://example.org/>\\nSELECT ?title ?year ?plot ?rating ?genreName ?directorName ?actorName WHERE {{ ?movie a ex:Movie ; ex:title ?title ; ex:year ?year . OPTIONAL {{ ?movie ex:plot ?plot }} OPTIONAL {{ ?movie ex:imdbRating ?rating }} OPTIONAL {{ ?movie ex:hasGenre ?g . ?g ex:name ?genreName }} OPTIONAL {{ ?movie ex:directedBy ?d . ?d ex:name ?directorName }} OPTIONAL {{ ?movie ex:starring ?a . ?a ex:name ?actorName }} }} LIMIT 100", "explanation": "Find all movies"}}
2. "Drama movies" → {{"operation": "filter_by_genre", "genre": "Drama", "explanation": "Find all Drama movies"}}
3. "Movies from 1927" → {{"operation": "filter_by_year", "year": 1927, "explanation": "Find all movies from 1927"}}
4. "Movies by Frank Borzage" → {{"operation": "filter_by_director", "director": "Frank Borzage", "explanation": "Find all movies by Frank Borzage"}}
5. "Drama movies from 1925" → {{"operation": "filter_by_multiple", "filters": {{"genre": "Drama", "year": 1925}}, "explanation": "Find Drama movies from 1925"}}
6. "Movies by Frank Borzage from 1927" → {{"operation": "filter_by_multiple", "filters": {{"director": "Frank Borzage", "year": 1927}}, "explanation": "Find movies by Frank Borzage from 1927"}}
7. "Show me the details of Influencers" → {{"operation": "find", "title": "Influencers", "explanation": "Find movie by title"}}
8. "Get movie Inception" → {{"operation": "find", "title": "Inception", "explanation": "Find movie by title"}}
9. "Add film Influencers from 2025" → {{"operation": "create", "title": "Influencers", "year": 2025, "genres": "Documentary", "explanation": "Create new movie"}}
10. "Insert movie Test with year 2020" → {{"operation": "create", "title": "Test", "year": 2020, "explanation": "Create new movie"}}
11. "Delete influencers" → {{"operation": "find_and_delete", "title": "Influencers", "explanation": "Find and delete movie by title"}}
12. "Remove the movie Titanic" → {{"operation": "find_and_delete", "title": "Titanic", "explanation": "Find and delete movie by title"}}
13. "Change influencers genre to action" → {{"operation": "find_and_update", "title": "Influencers", "field": "genre", "value": "Action", "explanation": "Update movie genre"}}
14. "Update Inception year to 2020" → {{"operation": "find_and_update", "title": "Inception", "field": "year", "value": "2020", "explanation": "Update movie year"}}

Important: 
- Use string literals for years like "1915", never use ^^xsd:integer
- Always SELECT ?title ?year ?plot ?rating ?genreName ?directorName ?actorName (all 7 fields)
- Always include OPTIONAL patterns for plot, rating, genres, directors, and actors
- Use LIMIT 100 to get all director/cast relationships (results will be aggregated by title)

Return ONLY the JSON."""

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=self.generation_config
            )
            
            result = self._extract_json(response.text)
            result['database_type'] = 'rdf'
            result['natural_query'] = natural_query
            
            self.logger.info("✓ Translated to SPARQL query")
            return result
            
        except Exception as e:
            self.logger.error(f"Error translating to SPARQL: {e}")
            return {
                'error': str(e),
                'database_type': 'rdf',
                'natural_query': natural_query
            }
    
    def translate_to_hbase(self, natural_query: str, schema_context: str) -> Dict[str, Any]:
        """Translate natural language to HBase operations or CRUD"""
        
        prompt = f"""You are an HBase expert. Translate this natural language query to HBase scan/get operations or CRUD.

HBase Schema:
{schema_context}

Natural Language Query: {natural_query}

Return ONLY valid JSON. For queries, use:
{{
    "operation": "scan",
    "table": "movies",
    "row_key": null,
    "columns": [],
    "limit": 10,
    "explanation": "Brief explanation"
}}

IMPORTANT: Always use "columns": [] to fetch ALL columns. Never specify specific columns like ["ratings:imdb_rating"].
Even when filtering by rating or year, you must fetch all movie data (title, year, genres, directors, cast, plot, rating).
The filtering is done in the query, but the result must include all movie fields.

For CRUD operations, use:
{{
    "operation": "delete",
    "table": "movies",
    "row_key": "movie_00001",
    "explanation": "Delete movie row"
}}

Or:
{{
    "operation": "put",
    "table": "movies",
    "row_key": "movie_00001",
    "data": {{"info:title": "New Title"}},
    "explanation": "Update movie data"
}}

HBase Operations:
- scan: Get multiple rows
- get: Get single row by key
- put: Insert or update row data
- delete: Delete a row

Column families in movies table:
- info: title, year, plot, runtime, rated
- ratings: imdb_rating, imdb_votes
- people: directors, cast
- metadata: genres, languages

CRUD Operations:
- IMPORTANT: Detect "show", "find", "get", "display" + movie title → use operation: "find"
- IMPORTANT: Detect "add", "insert", "create" → use operation: "create"
- IMPORTANT: Detect "delete", "remove" → use operation: "delete" OR "find_and_delete"
- IMPORTANT: Detect "update", "change", "modify" → use operation: "put" OR "find_and_update"
- IMPORTANT: Detect queries by genre/director/cast/year → use single filter OR filter_by_multiple for multiple conditions
- find: Find a movie by title (requires title)
- create: Create a new movie (requires title, year, and optionally genres, plot, rating)
- delete: Delete a row (requires table, row_key)
- put: Insert/update data (requires table, row_key, data with column:field format)
- find_and_delete: Find movie by title and delete (requires title)
- find_and_update: Find movie by title and update (requires title, updates dict OR field/value)
- filter_by_genre: Find movies by genre (requires genre)
- filter_by_director: Find movies by director (requires director)
- filter_by_cast: Find movies by actor (requires actor)
- filter_by_year: Find movies by year (requires year)
- filter_by_multiple: Find movies by multiple criteria (requires filters dict with any combination of: genre, director, actor, year)

CRITICAL: When user mentions movie by TITLE (not row_key like movie_00001), ALWAYS use find_and_delete or find_and_update.
NEVER use delete or put for title-based operations.
For ADD/INSERT/CREATE operations, always use operation: "create".
For SHOW/FIND/GET movie by title, always use operation: "find".
For queries by GENRE/DIRECTOR/CAST/YEAR with multiple conditions, use filter_by_multiple.
For single condition queries, use the specific filter_by_* operation.
You can also use "field" and "value" instead of "updates" for find_and_update.

Examples:
1. "Find all movies" → {{"operation": "scan", "table": "movies", "columns": [], "limit": 10, "explanation": "Scan all movies"}}
2. "Get movie movie_00001" → {{"operation": "get", "table": "movies", "row_key": "movie_00001", "columns": [], "explanation": "Get specific movie"}}
3. "Drama movies" → {{"operation": "filter_by_genre", "table": "movies", "genre": "Drama", "explanation": "Find Drama movies"}}
4. "Movies by Frank Borzage" → {{"operation": "filter_by_director", "table": "movies", "director": "Frank Borzage", "explanation": "Find movies by Frank Borzage"}}
5. "Films with Charles Chaplin" → {{"operation": "filter_by_cast", "table": "movies", "actor": "Charles Chaplin", "explanation": "Find movies with Charles Chaplin"}}
6. "Movies from 1927" → {{"operation": "filter_by_year", "table": "movies", "year": 1927, "explanation": "Find movies from 1927"}}
7. "Drama movies from 1925" → {{"operation": "filter_by_multiple", "table": "movies", "filters": {{"genre": "Drama", "year": 1925}}, "explanation": "Find Drama movies from 1925"}}
8. "Movies by Frank Borzage from 1927" → {{"operation": "filter_by_multiple", "table": "movies", "filters": {{"director": "Frank Borzage", "year": 1927}}, "explanation": "Find movies by Frank Borzage from 1927"}}
9. "Show me the details of Influencers" → {{"operation": "find", "table": "movies", "title": "Influencers", "explanation": "Find movie by title"}}
10. "Get movie Inception" → {{"operation": "find", "table": "movies", "title": "Inception", "explanation": "Find movie by title"}}
11. "Add film Influencers from 2025" → {{"operation": "create", "table": "movies", "title": "Influencers", "year": 2025, "genres": "Documentary", "explanation": "Create new movie"}}
12. "Insert movie Test with year 2020" → {{"operation": "create", "table": "movies", "title": "Test", "year": 2020, "explanation": "Create new movie"}}
13. "Delete influencers" → {{"operation": "find_and_delete", "table": "movies", "title": "Influencers", "explanation": "Find and delete movie by title"}}
14. "Remove the movie Titanic" → {{"operation": "find_and_delete", "table": "movies", "title": "Titanic", "explanation": "Find and delete movie by title"}}
15. "Change influencers genre to action" → {{"operation": "find_and_update", "table": "movies", "title": "Influencers", "field": "genre", "value": "Action", "explanation": "Update movie genre"}}
16. "Update Inception year to 2020" → {{"operation": "find_and_update", "table": "movies", "title": "Inception", "field": "year", "value": "2020", "explanation": "Update movie year"}}

Return ONLY the JSON."""

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=self.generation_config
            )
            
            result = self._extract_json(response.text)
            result['database_type'] = 'hbase'
            result['natural_query'] = natural_query
            
            self.logger.info("✓ Translated to HBase operation")
            return result
            
        except Exception as e:
            self.logger.error(f"Error translating to HBase: {e}")
            return {
                'error': str(e),
                'database_type': 'hbase',
                'natural_query': natural_query
            }