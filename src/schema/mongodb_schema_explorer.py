"""
MongoDB Schema Explorer
Extracts and analyzes MongoDB collection schemas
"""

import sys
import os
from typing import Dict, List, Any, Optional
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.mongodb_connector import MongoDBConnector
from utils.logger import setup_logger

class MongoDBSchemaExplorer:
    """Explores and analyzes MongoDB database schema"""
    
    def __init__(self, connector: MongoDBConnector):
        """
        Initialize schema explorer
        
        Args:
            connector: MongoDB connector instance
        """
        self.connector = connector
        self.logger = setup_logger(__name__)
    
    def analyze_field_types(self, collection_name: str, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Analyze field types in a collection by sampling documents
        
        Args:
            collection_name: Collection name
            sample_size: Number of documents to sample
        
        Returns:
            Dictionary with field type information
        """
        try:
            # Sample documents
            documents = self.connector.find_many(collection_name, {}, limit=sample_size)
            
            if not documents:
                return {}
            
            # Track field information
            field_info = defaultdict(lambda: {
                'count': 0,
                'types': defaultdict(int),
                'null_count': 0,
                'sample_values': [],
                'nested': False,
                'array': False
            })
            
            # Analyze each document
            for doc in documents:
                self._analyze_document(doc, field_info, prefix='')
            
            # Calculate statistics
            total_docs = len(documents)
            result = {}
            
            for field, info in field_info.items():
                # Determine primary type
                if info['types']:
                    primary_type = max(info['types'].items(), key=lambda x: x[1])[0]
                else:
                    primary_type = 'unknown'
                
                result[field] = {
                    'type': primary_type,
                    'all_types': dict(info['types']),
                    'presence': f"{(info['count'] / total_docs * 100):.1f}%",
                    'count': info['count'],
                    'null_count': info['null_count'],
                    'nested': info['nested'],
                    'array': info['array'],
                    'sample_values': info['sample_values'][:5]  # First 5 samples
                }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error analyzing field types: {e}")
            return {}
    
    def _analyze_document(self, doc: Dict, field_info: Dict, prefix: str = ''):
        """
        Recursively analyze a document's fields
        
        Args:
            doc: Document to analyze
            field_info: Dictionary to store field information
            prefix: Field path prefix for nested fields
        """
        for key, value in doc.items():
            if key == '_id':
                continue  # Skip _id field
            
            field_path = f"{prefix}.{key}" if prefix else key
            
            # Increment field count
            field_info[field_path]['count'] += 1
            
            # Determine type
            if value is None:
                field_info[field_path]['null_count'] += 1
                field_info[field_path]['types']['null'] += 1
            elif isinstance(value, bool):
                field_info[field_path]['types']['boolean'] += 1
                if len(field_info[field_path]['sample_values']) < 5:
                    field_info[field_path]['sample_values'].append(value)
            elif isinstance(value, int):
                field_info[field_path]['types']['integer'] += 1
                if len(field_info[field_path]['sample_values']) < 5:
                    field_info[field_path]['sample_values'].append(value)
            elif isinstance(value, float):
                field_info[field_path]['types']['float'] += 1
                if len(field_info[field_path]['sample_values']) < 5:
                    field_info[field_path]['sample_values'].append(value)
            elif isinstance(value, str):
                field_info[field_path]['types']['string'] += 1
                if len(field_info[field_path]['sample_values']) < 5:
                    field_info[field_path]['sample_values'].append(value[:50])  # Truncate
            elif isinstance(value, list):
                field_info[field_path]['array'] = True
                if value:
                    # Check array element types
                    element_type = type(value[0]).__name__
                    field_info[field_path]['types'][f'array<{element_type}>'] += 1
                    if len(field_info[field_path]['sample_values']) < 5:
                        field_info[field_path]['sample_values'].append(value[:3])  # First 3 elements
                else:
                    field_info[field_path]['types']['array<empty>'] += 1
            elif isinstance(value, dict):
                field_info[field_path]['nested'] = True
                field_info[field_path]['types']['object'] += 1
                # Recursively analyze nested object
                self._analyze_document(value, field_info, prefix=field_path)
            else:
                field_info[field_path]['types'][type(value).__name__] += 1
    
    def infer_relationships(self, collection_name: str, sample_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Infer potential relationships based on field naming patterns
        
        Args:
            collection_name: Collection name
            sample_size: Number of documents to sample
        
        Returns:
            List of potential relationships
        """
        try:
            relationships = []
            
            # Get field information
            fields = self.analyze_field_types(collection_name, sample_size)
            
            # Look for fields that might be foreign keys
            for field_name, field_info in fields.items():
                # Check for _id suffix pattern
                if field_name.endswith('_id') and field_info['type'] in ['string', 'ObjectId']:
                    potential_target = field_name[:-3] + 's'  # e.g., user_id -> users
                    
                    # Check if target collection exists
                    collections = self.connector.get_collections()
                    if potential_target in collections:
                        relationships.append({
                            'from_collection': collection_name,
                            'from_field': field_name,
                            'to_collection': potential_target,
                            'type': 'reference',
                            'confidence': 'high'
                        })
            
            return relationships
            
        except Exception as e:
            self.logger.error(f"Error inferring relationships: {e}")
            return []
    
    def get_collection_schema(self, collection_name: str, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Get complete schema information for a collection
        
        Args:
            collection_name: Collection name
            sample_size: Number of documents to sample
        
        Returns:
            Complete schema information
        """
        try:
            # Get basic stats
            count = self.connector.count_documents(collection_name)
            
            # Analyze fields
            fields = self.analyze_field_types(collection_name, sample_size)
            
            # Infer relationships
            relationships = self.infer_relationships(collection_name, sample_size)
            
            # Get sample document
            sample = self.connector.find_one(collection_name)
            
            # Get indexes
            collection = self.connector.db[collection_name]
            indexes = list(collection.list_indexes())
            index_info = []
            for idx in indexes:
                index_info.append({
                    'name': idx.get('name'),
                    'keys': list(idx.get('key', {}).keys()),
                    'unique': idx.get('unique', False)
                })
            
            schema = {
                'collection': collection_name,
                'document_count': count,
                'fields': fields,
                'relationships': relationships,
                'indexes': index_info,
                'sample_document': sample
            }
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting collection schema: {e}")
            return {}
    
    def get_database_schema(self, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Get complete schema for entire database
        
        Args:
            sample_size: Number of documents to sample per collection
        
        Returns:
            Complete database schema
        """
        try:
            collections = self.connector.get_collections()
            
            database_schema = {
                'database': self.connector.database_name,
                'total_collections': len(collections),
                'collections': {}
            }
            
            for collection in collections:
                self.logger.info(f"Analyzing collection: {collection}")
                schema = self.get_collection_schema(collection, sample_size)
                database_schema['collections'][collection] = schema
            
            return database_schema
            
        except Exception as e:
            self.logger.error(f"Error getting database schema: {e}")
            return {}
    
    def generate_schema_summary(self, schema: Dict[str, Any]) -> str:
        """
        Generate a human-readable schema summary for LLM consumption
        
        Args:
            schema: Schema dictionary
        
        Returns:
            Schema summary as string
        """
        try:
            lines = []
            
            if 'database' in schema:
                # Database-level schema
                lines.append(f"Database: {schema['database']}")
                lines.append(f"Collections: {schema['total_collections']}")
                lines.append("")
                
                for coll_name, coll_schema in schema.get('collections', {}).items():
                    lines.extend(self._format_collection_summary(coll_schema))
                    lines.append("")
            else:
                # Single collection schema
                lines.extend(self._format_collection_summary(schema))
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.error(f"Error generating schema summary: {e}")
            return ""
    
    def _format_collection_summary(self, schema: Dict[str, Any]) -> List[str]:
        """Format a single collection schema summary"""
        lines = []
        
        lines.append(f"Collection: {schema.get('collection', 'unknown')}")
        lines.append(f"  Documents: {schema.get('document_count', 0):,}")
        
        # Fields
        fields = schema.get('fields', {})
        if fields:
            lines.append(f"  Fields ({len(fields)}):")
            for field_name, field_info in list(fields.items())[:20]:  # Limit to 20 fields
                type_str = field_info['type']
                if field_info.get('array'):
                    type_str = f"[{type_str}]"
                if field_info.get('nested'):
                    type_str = f"{{{type_str}}}"
                
                lines.append(f"    - {field_name}: {type_str} (present in {field_info['presence']})")
        
        # Relationships
        relationships = schema.get('relationships', [])
        if relationships:
            lines.append(f"  Relationships ({len(relationships)}):")
            for rel in relationships:
                lines.append(f"    - {rel['from_field']} → {rel['to_collection']}")
        
        # Indexes
        indexes = schema.get('indexes', [])
        if indexes:
            lines.append(f"  Indexes ({len(indexes)}):")
            for idx in indexes:
                keys = ', '.join(idx['keys'])
                unique = ' (unique)' if idx.get('unique') else ''
                lines.append(f"    - {idx['name']}: {keys}{unique}")
        
        return lines
    
    def generate_llm_context(self, collection_name: Optional[str] = None) -> str:
        """
        Generate schema context optimized for LLM query translation
        
        Args:
            collection_name: Specific collection (if None, generates for all)
        
        Returns:
            LLM-friendly schema description
        """
        try:
            if collection_name:
                schema = self.get_collection_schema(collection_name)
                return self._format_llm_collection(schema)
            else:
                schema = self.get_database_schema()
                return self._format_llm_database(schema)
                
        except Exception as e:
            self.logger.error(f"Error generating LLM context: {e}")
            return ""
    
    def _format_llm_collection(self, schema: Dict[str, Any]) -> str:
        """Format collection schema for LLM"""
        lines = []
        
        lines.append(f"# MongoDB Collection: {schema.get('collection')}")
        lines.append("")
        lines.append(f"Total documents: {schema.get('document_count', 0):,}")
        lines.append("")
        
        # Field definitions
        lines.append("## Fields:")
        fields = schema.get('fields', {})
        for field_name, field_info in fields.items():
            type_info = field_info['type']
            if field_info.get('array'):
                type_info = f"Array of {type_info}"
            
            examples = field_info.get('sample_values', [])
            example_str = f" (e.g., {examples[0]})" if examples else ""
            
            lines.append(f"- `{field_name}`: {type_info}{example_str}")
        
        # Relationships
        relationships = schema.get('relationships', [])
        if relationships:
            lines.append("")
            lines.append("## Relationships:")
            for rel in relationships:
                lines.append(f"- `{rel['from_field']}` references collection `{rel['to_collection']}`")
        
        return "\n".join(lines)
    
    def _format_llm_database(self, schema: Dict[str, Any]) -> str:
        """Format database schema for LLM"""
        lines = []
        
        lines.append(f"# MongoDB Database: {schema.get('database')}")
        lines.append("")
        lines.append(f"Total collections: {schema.get('total_collections')}")
        lines.append("")
        
        for coll_name, coll_schema in schema.get('collections', {}).items():
            lines.append(f"## Collection: {coll_name}")
            lines.append(f"Documents: {coll_schema.get('document_count', 0):,}")
            
            # Key fields only (top 10)
            fields = coll_schema.get('fields', {})
            field_items = list(fields.items())[:10]
            if field_items:
                lines.append("Key fields:")
                for field_name, field_info in field_items:
                    lines.append(f"  - {field_name}: {field_info['type']}")
            
            lines.append("")
        
        return "\n".join(lines)