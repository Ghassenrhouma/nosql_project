"""
RDF Store Connector (Apache Jena Fuseki)
Handles SPARQL queries and RDF operations
"""

from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS
from typing import Dict, List, Any, Optional
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import setup_logger
from config.database_config import DatabaseConfig

class RDFConnector:
    """RDF Store connection and operations handler"""
    
    def __init__(self, endpoint: Optional[str] = None, dataset: Optional[str] = None,
                 username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize RDF connector
        
        Args:
            endpoint: SPARQL endpoint URL (optional)
            dataset: Dataset name (optional)
            username: Username for authentication (optional)
            password: Password for authentication (optional)
        """
        config = DatabaseConfig.get_rdf_config()
        
        self.endpoint = endpoint or config['endpoint']
        self.dataset = dataset or config['dataset']
        self.username = username or config.get('username', 'admin')
        self.password = password or config.get('password', 'admin123')
        self.query_endpoint = f"{self.endpoint}/{self.dataset}/query"
        self.update_endpoint = f"{self.endpoint}/{self.dataset}/update"
        
        self.sparql_query = None
        self.sparql_update = None
        self.logger = setup_logger(__name__)
        
        # Create namespaces
        self.EX = Namespace("http://example.org/")
        self.MOVIE = Namespace("http://example.org/movie/")
        self.PERSON = Namespace("http://example.org/person/")
    
    def connect(self) -> bool:
        """
        Establish connection to RDF store
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Connecting to RDF store at {self.endpoint}...")
            
            # Create SPARQL wrappers
            self.sparql_query = SPARQLWrapper(self.query_endpoint)
            self.sparql_update = SPARQLWrapper(self.update_endpoint)
            
            # Add authentication for UPDATE operations
            if self.username and self.password:
                self.sparql_update.setCredentials(self.username, self.password)
                self.logger.info(f"Using authentication: {self.username}")
            
            self.sparql_query.setReturnFormat(JSON)
            
            # Test connection with a simple query
            test_query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
            self.sparql_query.setQuery(test_query)
            results = self.sparql_query.query().convert()
            
            self.logger.info("✓ Connected to RDF store")
            return True
            
        except Exception as e:
            self.logger.error(f"✗ RDF connection failed: {e}")
            self.logger.error("Make sure Fuseki Docker container is running")
            return False
    
    def disconnect(self):
        """Close connection"""
        self.logger.info("RDF connection closed")
    
    def test_connection(self) -> bool:
        """Test if connection is alive"""
        try:
            if self.sparql_query:
                test_query = "ASK { ?s ?p ?o }"
                self.sparql_query.setQuery(test_query)
                self.sparql_query.query()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            'connected': self.test_connection(),
            'endpoint': self.endpoint,
            'dataset': self.dataset,
            'query_endpoint': self.query_endpoint
        }
    
    # ========== Query Operations ==========
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SPARQL SELECT query
        
        Args:
            query: SPARQL query string
        
        Returns:
            List of result bindings
        """
        try:
            self.sparql_query.setQuery(query)
            self.sparql_query.setMethod(GET)
            results = self.sparql_query.query().convert()
            
            bindings = results["results"]["bindings"]
            
            # Convert to simpler format
            simplified = []
            for binding in bindings:
                row = {}
                for var, value in binding.items():
                    row[var] = value["value"]
                simplified.append(row)
            
            self.logger.info(f"✓ Query returned {len(simplified)} results")
            return simplified
            
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return []
    
    def execute_update(self, update: str) -> bool:
        """
        Execute a SPARQL UPDATE query
        
        Args:
            update: SPARQL UPDATE string
        
        Returns:
            True if successful
        """
        try:
            self.sparql_update.setQuery(update)
            self.sparql_update.setMethod(POST)
            self.sparql_update.query()
            
            self.logger.info("✓ Update executed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error executing update: {e}")
            return False
    
    def ask(self, query: str) -> bool:
        """
        Execute a SPARQL ASK query
        
        Args:
            query: SPARQL ASK query
        
        Returns:
            Boolean result
        """
        try:
            self.sparql_query.setQuery(query)
            results = self.sparql_query.query().convert()
            return results["boolean"]
        except Exception as e:
            self.logger.error(f"Error in ASK query: {e}")
            return False
    
    # ========== Insert Operations ==========
    
    def insert_triple(self, subject: str, predicate: str, obj: str) -> bool:
        """
        Insert a single triple
        
        Args:
            subject: Subject URI
            predicate: Predicate URI
            obj: Object (URI or literal)
        
        Returns:
            True if successful
        """
        try:
            if obj.startswith('http'):
                obj_str = f"<{obj}>"
            else:
                # Escape the literal value
                obj_escaped = self._escape_literal(obj)
                obj_str = f'"{obj_escaped}"'
            
            update = f"""
            INSERT DATA {{
                <{subject}> <{predicate}> {obj_str} .
            }}
            """
            
            return self.execute_update(update)
        except Exception as e:
            self.logger.error(f"Error inserting triple: {e}")
            return False
    
    def insert_triples(self, triples: List[tuple]) -> bool:
        """
        Insert multiple triples
        
        Args:
            triples: List of (subject, predicate, object) tuples
        
        Returns:
            True if successful
        """
        try:
            triples_str = ""
            for s, p, o in triples:
                if o.startswith('http'):
                    obj_str = f"<{o}>"
                else:
                    # Escape the literal value
                    obj_escaped = self._escape_literal(o)
                    obj_str = f'"{obj_escaped}"'
                triples_str += f"<{s}> <{p}> {obj_str} .\n"
            
            update = f"""
            INSERT DATA {{
                {triples_str}
            }}
            """
            
            return self.execute_update(update)
        except Exception as e:
            self.logger.error(f"Error inserting triples: {e}")
            return False
    
    def _escape_literal(self, value: str) -> str:
        """
        Escape special characters in RDF literals
        
        Args:
            value: String value to escape
        
        Returns:
            Escaped string safe for SPARQL
        """
        if not isinstance(value, str):
            return str(value)
        
        # Escape backslashes first
        value = value.replace('\\', '\\\\')
        
        # Escape quotes
        value = value.replace('"', '\\"')
        
        # Escape newlines, returns, and tabs
        value = value.replace('\n', '\\n')
        value = value.replace('\r', '\\r')
        value = value.replace('\t', '\\t')
        
        return value
    
    # ========== Delete Operations ==========
    
    def delete_triple(self, subject: str, predicate: str, obj: str) -> bool:
        """Delete a specific triple"""
        try:
            if obj.startswith('http'):
                obj_str = f"<{obj}>"
            else:
                obj_escaped = self._escape_literal(obj)
                obj_str = f'"{obj_escaped}"'
            
            update = f"""
            DELETE DATA {{
                <{subject}> <{predicate}> {obj_str} .
            }}
            """
            
            return self.execute_update(update)
        except Exception as e:
            self.logger.error(f"Error deleting triple: {e}")
            return False
    
    def clear_graph(self, graph_uri: Optional[str] = None) -> bool:
        """
        Clear all triples (or specific graph)
        
        Args:
            graph_uri: Graph URI (if None, clears default graph)
        
        Returns:
            True if successful
        """
        try:
            if graph_uri:
                update = f"CLEAR GRAPH <{graph_uri}>"
            else:
                update = "CLEAR DEFAULT"
            
            return self.execute_update(update)
        except Exception as e:
            self.logger.error(f"Error clearing graph: {e}")
            return False
    
    # ========== Utility Operations ==========
    
    def count_triples(self) -> int:
        """Count total number of triples"""
        try:
            query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
            results = self.execute_query(query)
            if results:
                return int(results[0]['count'])
            return 0
        except Exception as e:
            self.logger.error(f"Error counting triples: {e}")
            return 0
    
    def get_classes(self) -> List[str]:
        """Get all RDF classes"""
        try:
            query = """
            SELECT DISTINCT ?class WHERE {
                ?s a ?class .
            }
            """
            results = self.execute_query(query)
            return [r['class'] for r in results]
        except Exception as e:
            self.logger.error(f"Error getting classes: {e}")
            return []
    
    def get_properties(self) -> List[str]:
        """Get all RDF properties"""
        try:
            query = """
            SELECT DISTINCT ?property WHERE {
                ?s ?property ?o .
            }
            """
            results = self.execute_query(query)
            return [r['property'] for r in results]
        except Exception as e:
            self.logger.error(f"Error getting properties: {e}")
            return []
    
    def get_subjects_of_type(self, class_uri: str, limit: int = 100) -> List[str]:
        """Get subjects of a specific type"""
        try:
            query = f"""
            SELECT ?subject WHERE {{
                ?subject a <{class_uri}> .
            }}
            LIMIT {limit}
            """
            results = self.execute_query(query)
            return [r['subject'] for r in results]
        except Exception as e:
            self.logger.error(f"Error getting subjects: {e}")
            return []
    
    # ========== Context Manager ==========
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()