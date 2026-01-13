"""
Neo4j Connector
Handles all Neo4j operations including connection, node/relationship operations
"""

from neo4j import GraphDatabase, Result
from neo4j.exceptions import ServiceUnavailable, AuthError, Neo4jError
from typing import Dict, List, Any, Optional, Union
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import setup_logger
from config.database_config import DatabaseConfig

class Neo4jConnector:
    """Neo4j connection and operations handler"""
    
    def __init__(self, uri: Optional[str] = None, username: Optional[str] = None, 
                 password: Optional[str] = None, database: Optional[str] = None):
        """
        Initialize Neo4j connector
        
        Args:
            uri: Neo4j URI (optional, uses config if not provided)
            username: Username (optional, uses config if not provided)
            password: Password (optional, uses config if not provided)
            database: Database name (optional, uses config if not provided)
        """
        config = DatabaseConfig.get_neo4j_config()
        
        self.uri = uri or config['uri']
        self.username = username or config['username']
        self.password = password or config['password']
        self.database = database or config['database']
        
        self.driver = None
        self.logger = setup_logger(__name__)
    
    def connect(self) -> bool:
        """
        Establish connection to Neo4j
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Connecting to Neo4j at {self.uri}...")
            
            # Create driver
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.username, self.password),
                max_connection_lifetime=3600
            )
            
            # Verify connectivity
            self.driver.verify_connectivity()
            
            self.logger.info(f"✓ Connected to Neo4j - Database: {self.database}")
            return True
            
        except AuthError as e:
            self.logger.error(f"✗ Authentication failed: {e}")
            return False
        except ServiceUnavailable as e:
            self.logger.error(f"✗ Service unavailable: {e}")
            self.logger.error("Make sure Neo4j Docker container is running!")
            return False
        except Exception as e:
            self.logger.error(f"✗ Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            self.logger.info("Neo4j connection closed")
    
    def test_connection(self) -> bool:
        """
        Test if connection is alive
        
        Returns:
            bool: True if connected, False otherwise
        """
        try:
            self.driver.verify_connectivity()
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about current connection
        
        Returns:
            Dictionary with connection details
        """
        return {
            'connected': self.test_connection() if self.driver else False,
            'uri': self.uri,
            'database': self.database,
            'username': self.username
        }
    
    # ========== Database Operations ==========
    
    def execute_query(self, query: str, parameters: Optional[Dict] = None) -> List[Dict]:
        """
        Execute a Cypher query and return results
        
        Args:
            query: Cypher query string
            parameters: Query parameters (default: {})
        
        Returns:
            List of result records as dictionaries
        """
        parameters = parameters or {}
        
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run(query, parameters)
                records = [dict(record) for record in result]
                self.logger.info(f"✓ Query executed, returned {len(records)} records")
                return records
        except Neo4jError as e:
            self.logger.error(f"Query execution error: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return []
    
    def execute_write(self, query: str, parameters: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Execute a write query (CREATE, UPDATE, DELETE)
        
        Args:
            query: Cypher query string
            parameters: Query parameters
        
        Returns:
            Dictionary with result summary
        """
        parameters = parameters or {}
        
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run(query, parameters)
                summary = result.consume()
                
                return {
                    'nodes_created': summary.counters.nodes_created,
                    'nodes_deleted': summary.counters.nodes_deleted,
                    'relationships_created': summary.counters.relationships_created,
                    'relationships_deleted': summary.counters.relationships_deleted,
                    'properties_set': summary.counters.properties_set,
                    'labels_added': summary.counters.labels_added,
                    'labels_removed': summary.counters.labels_removed
                }
        except Exception as e:
            self.logger.error(f"Write query error: {e}")
            return {}
    
    # ========== Node Operations ==========
    
    def create_node(self, label: str, properties: Dict) -> Optional[int]:
        """
        Create a single node
        
        Args:
            label: Node label (e.g., 'Movie', 'Person')
            properties: Node properties as dictionary
        
        Returns:
            Node ID or None if failed
        """
        try:
            query = f"""
            CREATE (n:{label} $props)
            RETURN id(n) as node_id
            """
            
            result = self.execute_query(query, {'props': properties})
            
            if result:
                node_id = result[0]['node_id']
                self.logger.info(f"✓ Created {label} node with ID: {node_id}")
                return node_id
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating node: {e}")
            return None
    
    def create_nodes_batch(self, label: str, nodes: List[Dict]) -> int:
        """
        Create multiple nodes in batch
        
        Args:
            label: Node label
            nodes: List of node property dictionaries
        
        Returns:
            Number of nodes created
        """
        try:
            query = f"""
            UNWIND $nodes as node
            CREATE (n:{label})
            SET n = node
            """
            
            result = self.execute_write(query, {'nodes': nodes})
            count = result.get('nodes_created', 0)
            self.logger.info(f"✓ Created {count} {label} nodes")
            return count
            
        except Exception as e:
            self.logger.error(f"Error creating nodes batch: {e}")
            return 0
    
    def find_nodes(self, label: str, properties: Optional[Dict] = None, 
                   limit: int = 100) -> List[Dict]:
        """
        Find nodes by label and optional properties
        
        Args:
            label: Node label
            properties: Properties to match (default: {})
            limit: Maximum number of nodes to return
        
        Returns:
            List of nodes with their properties
        """
        properties = properties or {}
        
        try:
            # Build WHERE clause
            if properties:
                where_conditions = ' AND '.join([f"n.{key} = ${key}" for key in properties.keys()])
                query = f"""
                MATCH (n:{label})
                WHERE {where_conditions}
                RETURN n
                LIMIT {limit}
                """
            else:
                query = f"""
                MATCH (n:{label})
                RETURN n
                LIMIT {limit}
                """
            
            result = self.execute_query(query, properties)
            nodes = [record['n'] for record in result]
            self.logger.info(f"✓ Found {len(nodes)} {label} nodes")
            return nodes
            
        except Exception as e:
            self.logger.error(f"Error finding nodes: {e}")
            return []
    
    def update_node(self, label: str, match_properties: Dict, 
                    update_properties: Dict) -> int:
        """
        Update node properties
        
        Args:
            label: Node label
            match_properties: Properties to match the node
            update_properties: Properties to update
        
        Returns:
            Number of properties set
        """
        try:
            where_conditions = ' AND '.join([f"n.{key} = ${key}" for key in match_properties.keys()])
            
            query = f"""
            MATCH (n:{label})
            WHERE {where_conditions}
            SET n += $update_props
            RETURN n
            """
            
            params = {**match_properties, 'update_props': update_properties}
            result = self.execute_write(query, params)
            count = result.get('properties_set', 0)
            self.logger.info(f"✓ Updated node, set {count} properties")
            return count
            
        except Exception as e:
            self.logger.error(f"Error updating node: {e}")
            return 0
    
    def delete_node(self, label: str, properties: Dict) -> int:
        """
        Delete a node (and its relationships)
        
        Args:
            label: Node label
            properties: Properties to match the node
        
        Returns:
            Number of nodes deleted
        """
        try:
            where_conditions = ' AND '.join([f"n.{key} = ${key}" for key in properties.keys()])
            
            query = f"""
            MATCH (n:{label})
            WHERE {where_conditions}
            DETACH DELETE n
            """
            
            result = self.execute_write(query, properties)
            count = result.get('nodes_deleted', 0)
            self.logger.info(f"✓ Deleted {count} node(s)")
            return count
            
        except Exception as e:
            self.logger.error(f"Error deleting node: {e}")
            return 0
    
    # ========== Relationship Operations ==========
    
    def create_relationship(self, from_label: str, from_props: Dict,
                          to_label: str, to_props: Dict,
                          rel_type: str, rel_props: Optional[Dict] = None) -> bool:
        """
        Create a relationship between two nodes
        
        Args:
            from_label: Source node label
            from_props: Source node properties to match
            to_label: Target node label
            to_props: Target node properties to match
            rel_type: Relationship type (e.g., 'ACTED_IN', 'DIRECTED')
            rel_props: Relationship properties (optional)
        
        Returns:
            True if successful, False otherwise
        """
        rel_props = rel_props or {}
        
        try:
            from_where = ' AND '.join([f"a.{key} = $from_{key}" for key in from_props.keys()])
            to_where = ' AND '.join([f"b.{key} = $to_{key}" for key in to_props.keys()])
            
            query = f"""
            MATCH (a:{from_label}), (b:{to_label})
            WHERE {from_where} AND {to_where}
            CREATE (a)-[r:{rel_type}]->(b)
            SET r = $rel_props
            RETURN r
            """
            
            params = {
                **{f'from_{k}': v for k, v in from_props.items()},
                **{f'to_{k}': v for k, v in to_props.items()},
                'rel_props': rel_props
            }
            
            result = self.execute_query(query, params)
            
            if result:
                self.logger.info(f"✓ Created {rel_type} relationship")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error creating relationship: {e}")
            return False
    
    def find_relationships(self, from_label: str, rel_type: str, 
                          to_label: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """
        Find relationships
        
        Args:
            from_label: Source node label
            rel_type: Relationship type
            to_label: Target node label (optional)
            limit: Maximum results
        
        Returns:
            List of relationship patterns
        """
        try:
            if to_label:
                query = f"""
                MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label})
                RETURN a, r, b
                LIMIT {limit}
                """
            else:
                query = f"""
                MATCH (a:{from_label})-[r:{rel_type}]->(b)
                RETURN a, r, b
                LIMIT {limit}
                """
            
            result = self.execute_query(query)
            self.logger.info(f"✓ Found {len(result)} relationships")
            return result
            
        except Exception as e:
            self.logger.error(f"Error finding relationships: {e}")
            return []
    
    # ========== Utility Operations ==========
    
    def count_nodes(self, label: Optional[str] = None) -> int:
        """
        Count nodes by label
        
        Args:
            label: Node label (if None, counts all nodes)
        
        Returns:
            Number of nodes
        """
        try:
            if label:
                query = f"MATCH (n:{label}) RETURN count(n) as count"
            else:
                query = "MATCH (n) RETURN count(n) as count"
            
            result = self.execute_query(query)
            return result[0]['count'] if result else 0
            
        except Exception as e:
            self.logger.error(f"Error counting nodes: {e}")
            return 0
    
    def count_relationships(self, rel_type: Optional[str] = None) -> int:
        """
        Count relationships by type
        
        Args:
            rel_type: Relationship type (if None, counts all)
        
        Returns:
            Number of relationships
        """
        try:
            if rel_type:
                query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
            else:
                query = "MATCH ()-[r]->() RETURN count(r) as count"
            
            result = self.execute_query(query)
            return result[0]['count'] if result else 0
            
        except Exception as e:
            self.logger.error(f"Error counting relationships: {e}")
            return 0
    
    def get_labels(self) -> List[str]:
        """
        Get all node labels in the database
        
        Returns:
            List of label names
        """
        try:
            query = "CALL db.labels()"
            result = self.execute_query(query)
            labels = [record['label'] for record in result]
            self.logger.info(f"✓ Found {len(labels)} labels")
            return labels
            
        except Exception as e:
            self.logger.error(f"Error getting labels: {e}")
            return []
    
    def get_relationship_types(self) -> List[str]:
        """
        Get all relationship types in the database
        
        Returns:
            List of relationship type names
        """
        try:
            query = "CALL db.relationshipTypes()"
            result = self.execute_query(query)
            types = [record['relationshipType'] for record in result]
            self.logger.info(f"✓ Found {len(types)} relationship types")
            return types
            
        except Exception as e:
            self.logger.error(f"Error getting relationship types: {e}")
            return []
    
    def get_schema(self) -> Dict[str, Any]:
        """
        Get database schema information
        
        Returns:
            Dictionary with schema information
        """
        try:
            labels = self.get_labels()
            rel_types = self.get_relationship_types()
            
            schema = {
                'labels': labels,
                'relationship_types': rel_types,
                'node_counts': {label: self.count_nodes(label) for label in labels},
                'relationship_counts': {rel: self.count_relationships(rel) for rel in rel_types}
            }
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting schema: {e}")
            return {}
    
    def clear_database(self) -> bool:
        """
        Delete all nodes and relationships (USE WITH CAUTION!)
        
        Returns:
            True if successful
        """
        try:
            query = "MATCH (n) DETACH DELETE n"
            self.execute_write(query)
            self.logger.info("✓ Database cleared")
            return True
        except Exception as e:
            self.logger.error(f"Error clearing database: {e}")
            return False
    
    # ========== Context Manager ==========
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()