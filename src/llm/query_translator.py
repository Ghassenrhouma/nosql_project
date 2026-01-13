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

MongoDB Query Rules:
- Collection is almost always "movies"
- Use operators: $eq, $gt, $gte, $lt, $lte, $in, $regex
- For "find all": query = {{}}
- For "find by field": query = {{"field": value}} or {{"field": {{"$operator": value}}}}
- For text search: {{"field": {{"$regex": "pattern", "$options": "i"}}}}
- For "movies from year": {{"year": 2015}}
- For "movies with rating above X": {{"imdb.rating": {{"$gte": X}}}}
- For "action movies": {{"genres": "Action"}}

Examples:
1. "Find all movies from 2015" → {{"collection": "movies", "operation": "find", "query": {{"year": 2015}}, "limit": 10}}
2. "Show me action movies" → {{"collection": "movies", "operation": "find", "query": {{"genres": "Action"}}, "limit": 10}}
3. "Find movies with rating above 8" → {{"collection": "movies", "operation": "find", "query": {{"imdb.rating": {{"$gte": 8}}}}, "limit": 10}}

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
        """Translate natural language to Cypher query"""
        
        prompt = f"""You are a Neo4j Cypher expert. Translate this natural language query to Cypher.

Graph Schema:
{schema_context}

Natural Language Query: {natural_query}

Return ONLY valid JSON (no markdown):
{{
    "cypher": "MATCH (m:Movie) WHERE m.year > 2000 RETURN m.title, m.year LIMIT 10",
    "parameters": {{}},
    "explanation": "Brief explanation"
}}

Cypher Query Rules:
- Node labels: Movie, Person
- Node pattern: (variable:Label {{property: value}})
- Relationship: (a)-[:REL_TYPE]->(b)
- WHERE for filtering: WHERE m.year > 2000
- RETURN variables: RETURN m.title, m.year
- Always add LIMIT (default 10)

Common patterns:
- "Find all movies": MATCH (m:Movie) RETURN m LIMIT 10
- "Movies from year": MATCH (m:Movie) WHERE m.year = 2015 RETURN m.title LIMIT 10
- "Who directed X": MATCH (p:Person)-[:DIRECTED]->(m:Movie {{title: 'X'}}) RETURN p.name
- "Actors in movie": MATCH (p:Person)-[:ACTED_IN]->(m:Movie {{title: 'X'}}) RETURN p.name

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
        """Translate natural language to Redis commands"""
        
        prompt = f"""You are a Redis expert. Translate this natural language query to Redis commands.

Key Patterns Available:
{schema_context}

Natural Language Query: {natural_query}

Return ONLY valid JSON:
{{
    "commands": [
        {{"command": "ZREVRANGE", "args": ["movies:by_rating", "0", "9", "WITHSCORES"]}}
    ],
    "explanation": "Brief explanation"
}}

Common Redis Commands:
- GET key - get string value
- HGETALL key - get all hash fields  
- ZREVRANGE key 0 9 WITHSCORES - top 10 from sorted set (high to low)
- ZRANGE key 0 9 WITHSCORES - range from sorted set (low to high)
- SMEMBERS key - all set members
- LRANGE key 0 -1 - all list elements

Use EXACT key names from the schema above.

Examples:
- "Top rated movies": {{"command": "ZREVRANGE", "args": ["movies:by_rating", "0", "9", "WITHSCORES"]}}
- "Movies from 2015": {{"command": "ZRANGE", "args": ["movies:by_year", "2015", "2015"]}}

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
        """Translate natural language to SPARQL query"""
        
        prompt = f"""You are a SPARQL expert. Translate this natural language query to SPARQL.

RDF Schema:
{schema_context}

Natural Language Query: {natural_query}

Return ONLY valid JSON:
{{
    "sparql": "PREFIX ex: <http://example.org/>\\nSELECT ?title ?year WHERE {{ ?movie a ex:Movie ; ex:title ?title ; ex:year ?year }} LIMIT 10",
    "explanation": "Brief explanation"
}}

SPARQL Query Rules:
- Always include: PREFIX ex: <http://example.org/>
- Triple pattern: ?subject predicate ?object
- Multiple patterns in {{ }}
- FILTER for conditions: FILTER(?year > "2000")
- Always add LIMIT (default 10)

Common patterns:
- All movies: ?movie a ex:Movie ; ex:title ?title
- Filter by genre: ?movie ex:hasGenre ?g . ?g ex:name "Action"
- Filter by director: ?movie ex:directedBy ?d . ?d ex:name "Name"

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
        """Translate natural language to HBase operations"""
        
        prompt = f"""You are an HBase expert. Translate this natural language query to HBase scan/get operations.

HBase Schema:
{schema_context}

Natural Language Query: {natural_query}

Return ONLY valid JSON:
{{
    "operation": "scan",
    "table": "movies",
    "row_key": null,
    "columns": ["info:title", "info:year"],
    "limit": 10,
    "explanation": "Brief explanation"
}}

HBase Operations:
- scan: Get multiple rows
- get: Get single row by key

Column families in movies table:
- info: title, year, plot, runtime, rated
- ratings: imdb_rating, imdb_votes
- people: directors, cast
- metadata: genres, languages

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