"""
Database Configuration
"""

import os
from typing import Dict, Any

class DatabaseConfig:
    """Configuration class for database connections"""
    
    # MongoDB Configuration
    MONGODB_CONFIG = {
        'host': 'localhost',
        'port': 27017,
        'database': 'sample_mflix',
        'username': '',
        'password': '',
    }
    RDF_CONFIG = {
    'endpoint': 'http://localhost:3030',
    'dataset': 'movies',
    'username': 'admin',
    'password': 'admin123'
    }
    # Neo4j Configuration
    NEO4J_CONFIG = {
        'uri': 'bolt://localhost:7687',
        'username': 'neo4j',
        'password': 'password123',
        'database': 'neo4j'
    }
    
    # Redis Configuration
    REDIS_CONFIG = {
        'host': 'localhost',
        'port': 6379,
        'password': '',
        'db': 0
    }
    
    # HBase Configuration
    HBASE_CONFIG = {
        'host': 'localhost',
        'port': 9090,  # Thrift port
    }
    
    # RDF Store Configuration (Apache Jena Fuseki)
    RDF_CONFIG = {
        'endpoint': 'http://localhost:3030',
        'dataset': 'movies'
    }
    
    @staticmethod
    def get_mongodb_connection_string() -> str:
        """Generate MongoDB connection string"""
        config = DatabaseConfig.MONGODB_CONFIG
        if config['username'] and config['password']:
            return f"mongodb://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        else:
            return f"mongodb://{config['host']}:{config['port']}/"
    
    @staticmethod
    def get_mongodb_config() -> Dict[str, Any]:
        return DatabaseConfig.MONGODB_CONFIG.copy()
    
    @staticmethod
    def get_redis_config() -> Dict[str, Any]:
        return DatabaseConfig.REDIS_CONFIG.copy()
    
    @staticmethod
    def get_neo4j_config() -> Dict[str, Any]:
        return DatabaseConfig.NEO4J_CONFIG.copy()
    
    @staticmethod
    def get_neo4j_uri() -> str:
        return DatabaseConfig.NEO4J_CONFIG['uri']
    
    @staticmethod
    def get_hbase_config() -> Dict[str, Any]:
        return DatabaseConfig.HBASE_CONFIG.copy()
    
    @staticmethod
    def get_rdf_config() -> Dict[str, Any]:
        return DatabaseConfig.RDF_CONFIG.copy()