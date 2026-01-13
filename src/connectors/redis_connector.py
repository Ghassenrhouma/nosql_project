"""
Redis Connector
Handles all Redis operations including connection and key-value operations
"""

import redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    RedisError
)
from typing import Dict, List, Any, Optional, Union
import json
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import setup_logger
from config.database_config import DatabaseConfig

class RedisConnector:
    """Redis connection and operations handler"""
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 password: Optional[str] = None, db: Optional[int] = None):
        """
        Initialize Redis connector
        
        Args:
            host: Redis host (optional, uses config if not provided)
            port: Redis port (optional, uses config if not provided)
            password: Redis password (optional, uses config if not provided)
            db: Database number (optional, uses config if not provided)
        """
        config = DatabaseConfig.get_redis_config()
        
        self.host = host or config['host']
        self.port = port or config['port']
        self.password = password or config['password']
        self.db = db or config['db']
        
        self.client = None
        self.logger = setup_logger(__name__)
    
    def connect(self) -> bool:
        """
        Establish connection to Redis
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Connecting to Redis at {self.host}:{self.port}...")
            
            # Create Redis client
            connection_params = {
                'host': self.host,
                'port': self.port,
                'db': self.db,
                'decode_responses': True,  # Automatically decode responses to strings
                'socket_timeout': 5,
                'socket_connect_timeout': 5
            }
            
            if self.password:
                connection_params['password'] = self.password
            
            self.client = redis.Redis(**connection_params)
            
            # Test connection
            self.client.ping()
            
            self.logger.info(f"✓ Connected to Redis - Database: {self.db}")
            return True
            
        except RedisConnectionError as e:
            self.logger.error(f"✗ Connection failed: {e}")
            self.logger.error("Make sure Redis Docker container is running!")
            return False
        except Exception as e:
            self.logger.error(f"✗ Unexpected error: {e}")
            return False
    
    def disconnect(self):
        """Close Redis connection"""
        if self.client:
            self.client.close()
            self.logger.info("Redis connection closed")
    
    def test_connection(self) -> bool:
        """
        Test if connection is alive
        
        Returns:
            bool: True if connected, False otherwise
        """
        try:
            return self.client.ping()
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about current connection
        
        Returns:
            Dictionary with connection details
        """
        info = {
            'connected': self.test_connection() if self.client else False,
            'host': self.host,
            'port': self.port,
            'db': self.db
        }
        
        if self.client:
            try:
                server_info = self.client.info('server')
                info['redis_version'] = server_info.get('redis_version', 'unknown')
            except:
                pass
        
        return info
    
    # ========== String Operations ==========
    
    def set(self, key: str, value: Union[str, int, float], ex: Optional[int] = None) -> bool:
        """
        Set a key-value pair
        
        Args:
            key: Key name
            value: Value to store
            ex: Expiration time in seconds (optional)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.client.set(key, value, ex=ex)
            self.logger.info(f"✓ Set key '{key}'")
            return result
        except Exception as e:
            self.logger.error(f"Error setting key '{key}': {e}")
            return False
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value by key
        
        Args:
            key: Key name
        
        Returns:
            Value or None if key doesn't exist
        """
        try:
            value = self.client.get(key)
            if value:
                self.logger.info(f"✓ Retrieved key '{key}'")
            return value
        except Exception as e:
            self.logger.error(f"Error getting key '{key}': {e}")
            return None
    
    def delete(self, *keys: str) -> int:
        """
        Delete one or more keys
        
        Args:
            *keys: Key names to delete
        
        Returns:
            Number of keys deleted
        """
        try:
            count = self.client.delete(*keys)
            self.logger.info(f"✓ Deleted {count} key(s)")
            return count
        except Exception as e:
            self.logger.error(f"Error deleting keys: {e}")
            return 0
    
    def exists(self, *keys: str) -> int:
        """
        Check if keys exist
        
        Args:
            *keys: Key names to check
        
        Returns:
            Number of keys that exist
        """
        try:
            return self.client.exists(*keys)
        except Exception as e:
            self.logger.error(f"Error checking key existence: {e}")
            return 0
    
    def expire(self, key: str, seconds: int) -> bool:
        """
        Set expiration time for a key
        
        Args:
            key: Key name
            seconds: Expiration time in seconds
        
        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.client.expire(key, seconds)
            if result:
                self.logger.info(f"✓ Set expiration for '{key}': {seconds}s")
            return result
        except Exception as e:
            self.logger.error(f"Error setting expiration: {e}")
            return False
    
    def ttl(self, key: str) -> int:
        """
        Get time to live for a key
        
        Args:
            key: Key name
        
        Returns:
            TTL in seconds, -1 if no expiry, -2 if key doesn't exist
        """
        try:
            return self.client.ttl(key)
        except Exception as e:
            self.logger.error(f"Error getting TTL: {e}")
            return -2
    
    # ========== Hash Operations ==========
    
    def hset(self, name: str, key: Optional[str] = None, value: Optional[str] = None,
             mapping: Optional[Dict] = None) -> int:
        """
        Set hash field(s)
        
        Args:
            name: Hash name
            key: Field name (if setting single field)
            value: Field value (if setting single field)
            mapping: Dictionary of field-value pairs (if setting multiple fields)
        
        Returns:
            Number of fields added
        """
        try:
            if mapping:
                count = self.client.hset(name, mapping=mapping)
            else:
                count = self.client.hset(name, key, value)
            self.logger.info(f"✓ Set {count} field(s) in hash '{name}'")
            return count
        except Exception as e:
            self.logger.error(f"Error setting hash: {e}")
            return 0
    
    def hget(self, name: str, key: str) -> Optional[str]:
        """
        Get hash field value
        
        Args:
            name: Hash name
            key: Field name
        
        Returns:
            Field value or None
        """
        try:
            return self.client.hget(name, key)
        except Exception as e:
            self.logger.error(f"Error getting hash field: {e}")
            return None
    
    def hgetall(self, name: str) -> Dict[str, str]:
        """
        Get all fields and values from hash
        
        Args:
            name: Hash name
        
        Returns:
            Dictionary of field-value pairs
        """
        try:
            data = self.client.hgetall(name)
            self.logger.info(f"✓ Retrieved hash '{name}' with {len(data)} fields")
            return data
        except Exception as e:
            self.logger.error(f"Error getting hash: {e}")
            return {}
    
    def hdel(self, name: str, *keys: str) -> int:
        """
        Delete hash fields
        
        Args:
            name: Hash name
            *keys: Field names to delete
        
        Returns:
            Number of fields deleted
        """
        try:
            count = self.client.hdel(name, *keys)
            self.logger.info(f"✓ Deleted {count} field(s) from hash '{name}'")
            return count
        except Exception as e:
            self.logger.error(f"Error deleting hash fields: {e}")
            return 0
    
    def hexists(self, name: str, key: str) -> bool:
        """
        Check if hash field exists
        
        Args:
            name: Hash name
            key: Field name
        
        Returns:
            True if field exists, False otherwise
        """
        try:
            return self.client.hexists(name, key)
        except Exception as e:
            self.logger.error(f"Error checking hash field: {e}")
            return False
    
    def hkeys(self, name: str) -> List[str]:
        """
        Get all field names from hash
        
        Args:
            name: Hash name
        
        Returns:
            List of field names
        """
        try:
            return self.client.hkeys(name)
        except Exception as e:
            self.logger.error(f"Error getting hash keys: {e}")
            return []
    
    def hvals(self, name: str) -> List[str]:
        """
        Get all values from hash
        
        Args:
            name: Hash name
        
        Returns:
            List of values
        """
        try:
            return self.client.hvals(name)
        except Exception as e:
            self.logger.error(f"Error getting hash values: {e}")
            return []
    
    # ========== List Operations ==========
    
    def lpush(self, name: str, *values: str) -> int:
        """
        Push values to the left (head) of list
        
        Args:
            name: List name
            *values: Values to push
        
        Returns:
            Length of list after operation
        """
        try:
            length = self.client.lpush(name, *values)
            self.logger.info(f"✓ Pushed {len(values)} value(s) to list '{name}'")
            return length
        except Exception as e:
            self.logger.error(f"Error pushing to list: {e}")
            return 0
    
    def rpush(self, name: str, *values: str) -> int:
        """
        Push values to the right (tail) of list
        
        Args:
            name: List name
            *values: Values to push
        
        Returns:
            Length of list after operation
        """
        try:
            length = self.client.rpush(name, *values)
            self.logger.info(f"✓ Pushed {len(values)} value(s) to list '{name}'")
            return length
        except Exception as e:
            self.logger.error(f"Error pushing to list: {e}")
            return 0
    
    def lrange(self, name: str, start: int, end: int) -> List[str]:
        """
        Get range of elements from list
        
        Args:
            name: List name
            start: Start index (0-based)
            end: End index (use -1 for all elements)
        
        Returns:
            List of elements
        """
        try:
            elements = self.client.lrange(name, start, end)
            self.logger.info(f"✓ Retrieved {len(elements)} element(s) from list '{name}'")
            return elements
        except Exception as e:
            self.logger.error(f"Error getting list range: {e}")
            return []
    
    def llen(self, name: str) -> int:
        """
        Get length of list
        
        Args:
            name: List name
        
        Returns:
            Length of list
        """
        try:
            return self.client.llen(name)
        except Exception as e:
            self.logger.error(f"Error getting list length: {e}")
            return 0
    
    def lpop(self, name: str, count: Optional[int] = None) -> Union[str, List[str], None]:
        """
        Remove and return elements from left (head) of list
        
        Args:
            name: List name
            count: Number of elements to pop (optional)
        
        Returns:
            Popped element(s) or None
        """
        try:
            return self.client.lpop(name, count)
        except Exception as e:
            self.logger.error(f"Error popping from list: {e}")
            return None
    
    def rpop(self, name: str, count: Optional[int] = None) -> Union[str, List[str], None]:
        """
        Remove and return elements from right (tail) of list
        
        Args:
            name: List name
            count: Number of elements to pop (optional)
        
        Returns:
            Popped element(s) or None
        """
        try:
            return self.client.rpop(name, count)
        except Exception as e:
            self.logger.error(f"Error popping from list: {e}")
            return None
    
    # ========== Set Operations ==========
    
    def sadd(self, name: str, *values: str) -> int:
        """
        Add members to set
        
        Args:
            name: Set name
            *values: Members to add
        
        Returns:
            Number of members added
        """
        try:
            count = self.client.sadd(name, *values)
            self.logger.info(f"✓ Added {count} member(s) to set '{name}'")
            return count
        except Exception as e:
            self.logger.error(f"Error adding to set: {e}")
            return 0
    
    def smembers(self, name: str) -> set:
        """
        Get all members of set
        
        Args:
            name: Set name
        
        Returns:
            Set of members
        """
        try:
            members = self.client.smembers(name)
            self.logger.info(f"✓ Retrieved set '{name}' with {len(members)} members")
            return members
        except Exception as e:
            self.logger.error(f"Error getting set members: {e}")
            return set()
    
    def sismember(self, name: str, value: str) -> bool:
        """
        Check if value is member of set
        
        Args:
            name: Set name
            value: Value to check
        
        Returns:
            True if member exists, False otherwise
        """
        try:
            return self.client.sismember(name, value)
        except Exception as e:
            self.logger.error(f"Error checking set membership: {e}")
            return False
    
    def srem(self, name: str, *values: str) -> int:
        """
        Remove members from set
        
        Args:
            name: Set name
            *values: Members to remove
        
        Returns:
            Number of members removed
        """
        try:
            count = self.client.srem(name, *values)
            self.logger.info(f"✓ Removed {count} member(s) from set '{name}'")
            return count
        except Exception as e:
            self.logger.error(f"Error removing from set: {e}")
            return 0
    
    def scard(self, name: str) -> int:
        """
        Get number of members in set
        
        Args:
            name: Set name
        
        Returns:
            Number of members
        """
        try:
            return self.client.scard(name)
        except Exception as e:
            self.logger.error(f"Error getting set cardinality: {e}")
            return 0
    
    # ========== Sorted Set Operations ==========
    
    def zadd(self, name: str, mapping: Dict[str, float]) -> int:
        """
        Add members with scores to sorted set
        
        Args:
            name: Sorted set name
            mapping: Dictionary of member-score pairs
        
        Returns:
            Number of members added
        """
        try:
            count = self.client.zadd(name, mapping)
            self.logger.info(f"✓ Added {count} member(s) to sorted set '{name}'")
            return count
        except Exception as e:
            self.logger.error(f"Error adding to sorted set: {e}")
            return 0
    
    def zrange(self, name: str, start: int, end: int, 
               withscores: bool = False) -> Union[List[str], List[tuple]]:
        """
        Get range of members from sorted set (by rank)
        
        Args:
            name: Sorted set name
            start: Start rank
            end: End rank
            withscores: If True, return scores with members
        
        Returns:
            List of members or (member, score) tuples
        """
        try:
            members = self.client.zrange(name, start, end, withscores=withscores)
            self.logger.info(f"✓ Retrieved {len(members)} member(s) from sorted set '{name}'")
            return members
        except Exception as e:
            self.logger.error(f"Error getting sorted set range: {e}")
            return []
    
    def zrevrange(self, name: str, start: int, end: int,
                  withscores: bool = False) -> Union[List[str], List[tuple]]:
        """
        Get range of members from sorted set in reverse order (highest to lowest)
        
        Args:
            name: Sorted set name
            start: Start rank
            end: End rank
            withscores: If True, return scores with members
        
        Returns:
            List of members or (member, score) tuples
        """
        try:
            members = self.client.zrevrange(name, start, end, withscores=withscores)
            self.logger.info(f"✓ Retrieved {len(members)} member(s) from sorted set '{name}'")
            return members
        except Exception as e:
            self.logger.error(f"Error getting sorted set reverse range: {e}")
            return []
    
    def zscore(self, name: str, value: str) -> Optional[float]:
        """
        Get score of member in sorted set
        
        Args:
            name: Sorted set name
            value: Member name
        
        Returns:
            Score or None if member doesn't exist
        """
        try:
            return self.client.zscore(name, value)
        except Exception as e:
            self.logger.error(f"Error getting sorted set score: {e}")
            return None
    
    def zcard(self, name: str) -> int:
        """
        Get number of members in sorted set
        
        Args:
            name: Sorted set name
        
        Returns:
            Number of members
        """
        try:
            return self.client.zcard(name)
        except Exception as e:
            self.logger.error(f"Error getting sorted set cardinality: {e}")
            return 0
    
    # ========== JSON Operations (requires RedisJSON module, optional) ==========
    
    def set_json(self, key: str, obj: Any) -> bool:
        """
        Store Python object as JSON string
        
        Args:
            key: Key name
            obj: Python object (dict, list, etc.)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            json_str = json.dumps(obj)
            return self.set(key, json_str)
        except Exception as e:
            self.logger.error(f"Error setting JSON: {e}")
            return False
    
    def get_json(self, key: str) -> Optional[Any]:
        """
        Get JSON string and parse to Python object
        
        Args:
            key: Key name
        
        Returns:
            Parsed Python object or None
        """
        try:
            json_str = self.get(key)
            if json_str:
                return json.loads(json_str)
            return None
        except Exception as e:
            self.logger.error(f"Error getting JSON: {e}")
            return None
    
    # ========== Utility Operations ==========
    
    def keys(self, pattern: str = '*') -> List[str]:
        """
        Get keys matching pattern
        
        Args:
            pattern: Pattern to match (default: '*' for all keys)
        
        Returns:
            List of matching keys
        """
        try:
            keys = self.client.keys(pattern)
            self.logger.info(f"✓ Found {len(keys)} key(s) matching pattern '{pattern}'")
            return keys
        except Exception as e:
            self.logger.error(f"Error getting keys: {e}")
            return []
    
    def dbsize(self) -> int:
        """
        Get total number of keys in database
        
        Returns:
            Number of keys
        """
        try:
            return self.client.dbsize()
        except Exception as e:
            self.logger.error(f"Error getting database size: {e}")
            return 0
    
    def flushdb(self) -> bool:
        """
        Delete all keys in current database (USE WITH CAUTION!)
        
        Returns:
            True if successful
        """
        try:
            self.client.flushdb()
            self.logger.info("✓ Database flushed")
            return True
        except Exception as e:
            self.logger.error(f"Error flushing database: {e}")
            return False
    
    def info(self, section: Optional[str] = None) -> Dict:
        """
        Get Redis server information
        
        Args:
            section: Info section (e.g., 'server', 'memory', 'stats')
        
        Returns:
            Dictionary with server info
        """
        try:
            return self.client.info(section)
        except Exception as e:
            self.logger.error(f"Error getting server info: {e}")
            return {}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get database statistics
        
        Returns:
            Dictionary with statistics
        """
        try:
            info = self.client.info()
            
            stats = {
                'redis_version': info.get('redis_version', 'unknown'),
                'total_keys': self.dbsize(),
                'used_memory': info.get('used_memory_human', 'unknown'),
                'connected_clients': info.get('connected_clients', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
            }
            
            # Calculate hit rate
            hits = stats['keyspace_hits']
            misses = stats['keyspace_misses']
            if hits + misses > 0:
                stats['hit_rate'] = f"{(hits / (hits + misses) * 100):.2f}%"
            else:
                stats['hit_rate'] = 'N/A'
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting stats: {e}")
            return {}
    
    # ========== Context Manager ==========
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()