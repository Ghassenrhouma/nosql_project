"""
MongoDB Connector
Handles all MongoDB operations including connection, CRUD, and queries
Designed for use with sample_mflix database
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import (
    ConnectionFailure, 
    ServerSelectionTimeoutError,
    OperationFailure,
    DuplicateKeyError
)
from typing import Dict, List, Any, Optional, Union
from bson import ObjectId
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import setup_logger
from config.database_config import DatabaseConfig

class MongoDBConnector:
    """MongoDB connection and operations handler"""
    
    def __init__(self, connection_string: Optional[str] = None, database_name: Optional[str] = None):
        """
        Initialize MongoDB connector
        
        Args:
            connection_string: MongoDB URI (optional, uses config if not provided)
            database_name: Database name (optional, uses config if not provided)
        """
        self.connection_string = connection_string or DatabaseConfig.get_mongodb_connection_string()
        self.database_name = database_name or DatabaseConfig.MONGODB_CONFIG['database']
        self.client = None
        self.db = None
        self.logger = setup_logger(__name__)
        
    def connect(self) -> bool:
        """
        Establish connection to MongoDB
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Connecting to MongoDB...")
            
            # Create client with timeout
            self.client = MongoClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=10000,
                socketTimeoutMS=10000
            )
            
            # Test connection with ping
            self.client.admin.command('ping')
            
            # Set default database
            self.db = self.client[self.database_name]
            
            self.logger.info(f"✓ Connected to MongoDB - Database: {self.database_name}")
            return True
            
        except ConnectionFailure as e:
            self.logger.error(f"✗ Connection failed: {e}")
            return False
        except ServerSelectionTimeoutError as e:
            self.logger.error(f"✗ Server not reachable: {e}")
            self.logger.error("Make sure MongoDB Docker container is running!")
            return False
        except Exception as e:
            self.logger.error(f"✗ Unexpected error: {e}")
            return False
    
    def disconnect(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")
    
    def test_connection(self) -> bool:
        """
        Test if connection is alive
        
        Returns:
            bool: True if connected, False otherwise
        """
        try:
            self.client.admin.command('ping')
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
            'connected': self.test_connection(),
            'database': self.database_name,
            'host': DatabaseConfig.MONGODB_CONFIG['host'],
            'port': DatabaseConfig.MONGODB_CONFIG['port']
        }
    
    # ========== Database & Collection Operations ==========
    
    def get_databases(self) -> List[str]:
        """
        List all databases
        
        Returns:
            List of database names
        """
        try:
            databases = self.client.list_database_names()
            self.logger.info(f"Found {len(databases)} databases")
            return databases
        except Exception as e:
            self.logger.error(f"Error listing databases: {e}")
            return []
    
    def get_collections(self, database_name: Optional[str] = None) -> List[str]:
        """
        List all collections in a database
        
        Args:
            database_name: Database name (uses default if not provided)
        
        Returns:
            List of collection names
        """
        try:
            db_name = database_name or self.database_name
            db = self.client[db_name]
            collections = db.list_collection_names()
            self.logger.info(f"Found {len(collections)} collections in '{db_name}'")
            return collections
        except Exception as e:
            self.logger.error(f"Error listing collections: {e}")
            return []
    
    def set_database(self, database_name: str):
        """
        Switch to a different database
        
        Args:
            database_name: Name of database to use
        """
        self.db = self.client[database_name]
        self.database_name = database_name
        self.logger.info(f"Switched to database: {database_name}")
    
    def collection_exists(self, collection_name: str) -> bool:
        """
        Check if a collection exists
        
        Args:
            collection_name: Name of collection to check
        
        Returns:
            True if collection exists, False otherwise
        """
        return collection_name in self.get_collections()
    
    # ========== CRUD Operations ==========
    
    def insert_one(self, collection_name: str, document: Dict) -> Optional[str]:
        """
        Insert a single document
        
        Args:
            collection_name: Collection to insert into
            document: Document to insert
        
        Returns:
            Inserted document ID as string, or None if failed
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            self.logger.info(f"✓ Inserted document with ID: {result.inserted_id}")
            return str(result.inserted_id)
        except DuplicateKeyError as e:
            self.logger.error(f"Duplicate key error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error inserting document: {e}")
            return None
    
    def insert_many(self, collection_name: str, documents: List[Dict]) -> int:
        """
        Insert multiple documents
        
        Args:
            collection_name: Collection to insert into
            documents: List of documents to insert
        
        Returns:
            Number of documents inserted
        """
        try:
            if not documents:
                self.logger.warning("No documents to insert")
                return 0
            
            collection = self.db[collection_name]
            result = collection.insert_many(documents, ordered=False)
            count = len(result.inserted_ids)
            self.logger.info(f"✓ Inserted {count} documents into '{collection_name}'")
            return count
        except Exception as e:
            self.logger.error(f"Error inserting documents: {e}")
            return 0
    
    def find_one(self, collection_name: str, query: Optional[Dict] = None, 
                 projection: Optional[Dict] = None) -> Optional[Dict]:
        """
        Find a single document
        
        Args:
            collection_name: Collection to search
            query: Query filter (default: {})
            projection: Fields to include/exclude
        
        Returns:
            Found document or None
        """
        try:
            collection = self.db[collection_name]
            query = query or {}
            
            document = collection.find_one(query, projection)
            
            if document:
                # Convert ObjectId to string for JSON serialization
                document = self._convert_objectid(document)
                self.logger.info(f"✓ Found document in '{collection_name}'")
            else:
                self.logger.info(f"No document found matching query in '{collection_name}'")
            
            return document
        except Exception as e:
            self.logger.error(f"Error finding document: {e}")
            return None
    
    def find_many(self, collection_name: str, query: Optional[Dict] = None, 
                  projection: Optional[Dict] = None, limit: int = 100,
                  sort: Optional[List[tuple]] = None) -> List[Dict]:
        """
        Find multiple documents
        
        Args:
            collection_name: Collection to search
            query: Query filter (default: {})
            projection: Fields to include/exclude
            limit: Maximum number of documents to return
            sort: Sort order as list of (field, direction) tuples
        
        Returns:
            List of found documents
        """
        try:
            collection = self.db[collection_name]
            query = query or {}
            
            cursor = collection.find(query, projection).limit(limit)
            
            # Apply sorting if specified
            if sort:
                cursor = cursor.sort(sort)
            
            documents = []
            for doc in cursor:
                doc = self._convert_objectid(doc)
                documents.append(doc)
            
            self.logger.info(f"✓ Found {len(documents)} documents in '{collection_name}'")
            return documents
            
        except Exception as e:
            self.logger.error(f"Error finding documents: {e}")
            return []
    
    def find_by_id(self, collection_name: str, document_id: str) -> Optional[Dict]:
        """
        Find a document by its _id
        
        Args:
            collection_name: Collection to search
            document_id: Document ID (as string)
        
        Returns:
            Found document or None
        """
        try:
            # Convert string ID to ObjectId
            obj_id = ObjectId(document_id)
            return self.find_one(collection_name, {'_id': obj_id})
        except Exception as e:
            self.logger.error(f"Error finding document by ID: {e}")
            return None
    
    def update_one(self, collection_name: str, query: Dict, 
                   update: Dict, upsert: bool = False) -> int:
        """
        Update a single document
        
        Args:
            collection_name: Collection name
            query: Query filter to find document
            update: Update operations (e.g., {'$set': {'field': 'value'}})
            upsert: If True, insert if not found
        
        Returns:
            Number of documents modified
        """
        try:
            collection = self.db[collection_name]
            result = collection.update_one(query, update, upsert=upsert)
            
            if result.modified_count > 0:
                self.logger.info(f"✓ Modified {result.modified_count} document")
            elif result.upserted_id:
                self.logger.info(f"✓ Upserted document with ID: {result.upserted_id}")
            else:
                self.logger.info("No documents modified")
            
            return result.modified_count
        except Exception as e:
            self.logger.error(f"Error updating document: {e}")
            return 0
    
    def update_many(self, collection_name: str, query: Dict, 
                    update: Dict, upsert: bool = False) -> int:
        """
        Update multiple documents
        
        Args:
            collection_name: Collection name
            query: Query filter
            update: Update operations
            upsert: If True, insert if not found
        
        Returns:
            Number of documents modified
        """
        try:
            collection = self.db[collection_name]
            result = collection.update_many(query, update, upsert=upsert)
            self.logger.info(f"✓ Modified {result.modified_count} document(s)")
            return result.modified_count
        except Exception as e:
            self.logger.error(f"Error updating documents: {e}")
            return 0
    
    def delete_one(self, collection_name: str, query: Dict) -> int:
        """
        Delete a single document
        
        Args:
            collection_name: Collection name
            query: Query filter
        
        Returns:
            Number of documents deleted
        """
        try:
            collection = self.db[collection_name]
            result = collection.delete_one(query)
            self.logger.info(f"✓ Deleted {result.deleted_count} document")
            return result.deleted_count
        except Exception as e:
            self.logger.error(f"Error deleting document: {e}")
            return 0
    
    def delete_many(self, collection_name: str, query: Dict) -> int:
        """
        Delete multiple documents
        
        Args:
            collection_name: Collection name
            query: Query filter
        
        Returns:
            Number of documents deleted
        """
        try:
            collection = self.db[collection_name]
            result = collection.delete_many(query)
            self.logger.info(f"✓ Deleted {result.deleted_count} document(s)")
            return result.deleted_count
        except Exception as e:
            self.logger.error(f"Error deleting documents: {e}")
            return 0
    
    def count_documents(self, collection_name: str, query: Optional[Dict] = None) -> int:
        """
        Count documents matching query
        
        Args:
            collection_name: Collection name
            query: Query filter (default: {})
        
        Returns:
            Number of matching documents
        """
        try:
            collection = self.db[collection_name]
            query = query or {}
            count = collection.count_documents(query)
            return count
        except Exception as e:
            self.logger.error(f"Error counting documents: {e}")
            return 0
    
    # ========== Aggregation Operations ==========
    
    def aggregate(self, collection_name: str, pipeline: List[Dict]) -> List[Dict]:
        """
        Execute an aggregation pipeline
        
        Args:
            collection_name: Collection name
            pipeline: Aggregation pipeline stages
        
        Returns:
            List of aggregation results
        """
        try:
            collection = self.db[collection_name]
            results = list(collection.aggregate(pipeline))
            
            # Convert ObjectIds in results
            results = [self._convert_objectid(doc) for doc in results]
            
            self.logger.info(f"✓ Aggregation returned {len(results)} results")
            return results
        except Exception as e:
            self.logger.error(f"Error in aggregation: {e}")
            return []
    
    # ========== Utility Operations ==========
    
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """
        Get statistics about a collection
        
        Args:
            collection_name: Collection name
        
        Returns:
            Dictionary with collection statistics
        """
        try:
            collection = self.db[collection_name]
            
            # Get document count
            count = collection.count_documents({})
            
            # Get sample document to infer schema
            sample = collection.find_one()
            fields = list(sample.keys()) if sample else []
            
            # Get collection size info
            stats_command = self.db.command('collStats', collection_name)
            
            # FIX: Convert cursor to list before getting length
            indexes = list(collection.list_indexes())
            index_count = len(indexes)
            
            stats = {
                'collection': collection_name,
                'document_count': count,
                'size_bytes': stats_command.get('size', 0),
                'avg_doc_size_bytes': stats_command.get('avgObjSize', 0),
                'fields': fields,
                'indexes': index_count,  # Fixed
                'sample_document': self._convert_objectid(sample) if sample else None
            }
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting collection stats: {e}")
            # FIX: Return proper structure even on error
            return {
                'collection': collection_name,
                'document_count': 0,
                'size_bytes': 0,
                'avg_doc_size_bytes': 0,
                'fields': [],
                'indexes': 0,
                'sample_document': None
            }
    
    def get_distinct_values(self, collection_name: str, field: str, 
                           query: Optional[Dict] = None) -> List[Any]:
        """
        Get distinct values for a field
        
        Args:
            collection_name: Collection name
            field: Field name
            query: Optional filter query
        
        Returns:
            List of distinct values
        """
        try:
            collection = self.db[collection_name]
            query = query or {}
            values = collection.distinct(field, query)
            self.logger.info(f"✓ Found {len(values)} distinct values for '{field}'")
            return values
        except Exception as e:
            self.logger.error(f"Error getting distinct values: {e}")
            return []
    
    def create_index(self, collection_name: str, field: str, 
                     unique: bool = False) -> bool:
        """
        Create an index on a field
        
        Args:
            collection_name: Collection name
            field: Field name to index
            unique: If True, create unique index
        
        Returns:
            True if successful, False otherwise
        """
        try:
            collection = self.db[collection_name]
            collection.create_index([(field, ASCENDING)], unique=unique)
            self.logger.info(f"✓ Created index on '{field}' in '{collection_name}'")
            return True
        except Exception as e:
            self.logger.error(f"Error creating index: {e}")
            return False
    
    def execute_query(self, query_string: str) -> Union[List[Dict], Dict, None]:
        """
        Execute a MongoDB query from a string (for LLM-generated queries)
        This method will be useful in later sprints for NLQ translation
        
        Args:
            query_string: MongoDB query as string
        
        Returns:
            Query results or None if error
        """
        try:
            # This is a placeholder for later implementation
            # Will be used to execute LLM-generated queries
            self.logger.warning("execute_query not yet fully implemented")
            return None
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return None
    
    # ========== Helper Methods ==========
    
    def _convert_objectid(self, document: Union[Dict, List]) -> Union[Dict, List]:
        """
        Recursively convert ObjectId to string for JSON serialization
        
        Args:
            document: Document or list of documents
        
        Returns:
            Document with ObjectIds converted to strings
        """
        if isinstance(document, dict):
            for key, value in document.items():
                if isinstance(value, ObjectId):
                    document[key] = str(value)
                elif isinstance(value, dict):
                    document[key] = self._convert_objectid(value)
                elif isinstance(value, list):
                    document[key] = [self._convert_objectid(item) if isinstance(item, dict) 
                                    else str(item) if isinstance(item, ObjectId) 
                                    else item for item in value]
        return document
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()