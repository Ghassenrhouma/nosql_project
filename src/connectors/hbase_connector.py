"""
HBase Connector
Handles all HBase operations including connection and column-family operations
"""

import happybase
from typing import Dict, List, Any, Optional, Union
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import setup_logger
from config.database_config import DatabaseConfig

class HBaseConnector:
    """HBase connection and operations handler"""
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        """
        Initialize HBase connector
        
        Args:
            host: HBase Thrift host (optional)
            port: HBase Thrift port (optional)
        """
        config = DatabaseConfig.get_hbase_config()
        
        self.host = host or config['host']
        self.port = port or config['port']
        
        self.connection = None
        self.logger = setup_logger(__name__)
    
    def connect(self) -> bool:
        """
        Establish connection to HBase
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Connecting to HBase at {self.host}:{self.port}...")
            
            self.connection = happybase.Connection(
                host=self.host,
                port=self.port,
                timeout=60000,  # Increase to 60 seconds
                autoconnect=True,
                table_prefix=None,
                table_prefix_separator=b'_',
                compat='0.98',  # Add compatibility mode
                transport='buffered'
            )
            
            # Test connection by listing tables
            self.connection.tables()
            
            self.logger.info("✓ Connected to HBase")
            return True
            
        except Exception as e:
            self.logger.error(f"✗ HBase connection failed: {e}")
            self.logger.error("Make sure HBase Docker container is running and Thrift server is enabled")
            return False
    
    def disconnect(self):
        """Close HBase connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("HBase connection closed")
    
    def test_connection(self) -> bool:
        """Test if connection is alive"""
        try:
            if self.connection:
                self.connection.tables()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        return {
            'connected': self.test_connection(),
            'host': self.host,
            'port': self.port
        }
    
    # ========== Table Operations ==========
    
    def list_tables(self) -> List[str]:
        """List all tables"""
        try:
            tables = [table.decode('utf-8') if isinstance(table, bytes) else table 
                     for table in self.connection.tables()]
            self.logger.info(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            self.logger.error(f"Error listing tables: {e}")
            return []
    
    def create_table(self, table_name: str, column_families: Dict[str, Dict]) -> bool:
        """
        Create a new table
        
        Args:
            table_name: Table name
            column_families: Dictionary of column family configurations
                           e.g., {'cf1': {'max_versions': 3}, 'cf2': {}}
        
        Returns:
            True if successful
        """
        try:
            self.connection.create_table(table_name, column_families)
            self.logger.info(f"✓ Created table '{table_name}'")
            return True
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
            return False
    
    def delete_table(self, table_name: str, disable: bool = True) -> bool:
        """
        Delete a table
        
        Args:
            table_name: Table name
            disable: Whether to disable table first
        
        Returns:
            True if successful
        """
        try:
            if disable:
                self.connection.disable_table(table_name)
            self.connection.delete_table(table_name)
            self.logger.info(f"✓ Deleted table '{table_name}'")
            return True
        except Exception as e:
            self.logger.error(f"Error deleting table: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        try:
            tables = self.list_tables()
            return table_name in tables
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}")
            return False
    
    # ========== Row Operations ==========
    
    def put(self, table_name: str, row_key: str, data: Dict[str, Any]) -> bool:
        """
        Insert or update a row
        
        Args:
            table_name: Table name
            row_key: Row key
            data: Dictionary with column:value pairs
                  e.g., {'cf1:col1': 'value1', 'cf2:col2': 'value2'}
        
        Returns:
            True if successful
        """
        try:
            table = self.connection.table(table_name)
            
            # Convert values to bytes if needed
            encoded_data = {}
            for col, val in data.items():
                if isinstance(val, str):
                    encoded_data[col.encode() if isinstance(col, str) else col] = val.encode()
                elif isinstance(val, bytes):
                    encoded_data[col.encode() if isinstance(col, str) else col] = val
                else:
                    encoded_data[col.encode() if isinstance(col, str) else col] = str(val).encode()
            
            table.put(row_key.encode() if isinstance(row_key, str) else row_key, 
                     encoded_data)
            self.logger.info(f"✓ Put row '{row_key}' in table '{table_name}'")
            return True
        except Exception as e:
            self.logger.error(f"Error putting row: {e}")
            return False
    
    def get(self, table_name: str, row_key: str, 
            columns: Optional[List[str]] = None) -> Optional[Dict]:
        """
        Get a row
        
        Args:
            table_name: Table name
            row_key: Row key
            columns: List of columns to retrieve (optional)
        
        Returns:
            Dictionary with column:value pairs or None
        """
        try:
            table = self.connection.table(table_name)
            
            row_key_bytes = row_key.encode() if isinstance(row_key, str) else row_key
            
            if columns:
                columns_bytes = [col.encode() if isinstance(col, str) else col 
                               for col in columns]
                row = table.row(row_key_bytes, columns=columns_bytes)
            else:
                row = table.row(row_key_bytes)
            
            # Decode result
            decoded_row = {}
            for col, val in row.items():
                col_str = col.decode() if isinstance(col, bytes) else col
                val_str = val.decode() if isinstance(val, bytes) else val
                decoded_row[col_str] = val_str
            
            return decoded_row if decoded_row else None
            
        except Exception as e:
            self.logger.error(f"Error getting row: {e}")
            return None
    
    def scan(self, table_name: str, row_start: Optional[str] = None,
             row_stop: Optional[str] = None, columns: Optional[List[str]] = None,
             limit: Optional[int] = None) -> List[Dict]:
        """
        Scan table rows
        
        Args:
            table_name: Table name
            row_start: Start row key (optional)
            row_stop: Stop row key (optional)
            columns: List of columns to retrieve (optional)
            limit: Maximum number of rows (optional)
        
        Returns:
            List of rows with their data
        """
        try:
            table = self.connection.table(table_name)
            
            scan_kwargs = {}
            if row_start:
                scan_kwargs['row_start'] = row_start.encode() if isinstance(row_start, str) else row_start
            if row_stop:
                scan_kwargs['row_stop'] = row_stop.encode() if isinstance(row_stop, str) else row_stop
            if columns:
                scan_kwargs['columns'] = [col.encode() if isinstance(col, str) else col 
                                         for col in columns]
            if limit:
                scan_kwargs['limit'] = limit
            
            results = []
            for key, data in table.scan(**scan_kwargs):
                row_key = key.decode() if isinstance(key, bytes) else key
                
                decoded_data = {}
                for col, val in data.items():
                    col_str = col.decode() if isinstance(col, bytes) else col
                    val_str = val.decode() if isinstance(val, bytes) else val
                    decoded_data[col_str] = val_str
                
                results.append({
                    'row_key': row_key,
                    'data': decoded_data
                })
            
            self.logger.info(f"✓ Scanned {len(results)} rows from '{table_name}'")
            return results
            
        except Exception as e:
            self.logger.error(f"Error scanning table: {e}")
            return []
    
    def delete(self, table_name: str, row_key: str, 
               columns: Optional[List[str]] = None) -> bool:
        """
        Delete a row or specific columns
        
        Args:
            table_name: Table name
            row_key: Row key
            columns: Columns to delete (if None, deletes entire row)
        
        Returns:
            True if successful
        """
        try:
            table = self.connection.table(table_name)
            
            row_key_bytes = row_key.encode() if isinstance(row_key, str) else row_key
            
            if columns:
                columns_bytes = [col.encode() if isinstance(col, str) else col 
                               for col in columns]
                table.delete(row_key_bytes, columns=columns_bytes)
            else:
                table.delete(row_key_bytes)
            
            self.logger.info(f"✓ Deleted from '{table_name}': {row_key}")
            return True
        except Exception as e:
            self.logger.error(f"Error deleting: {e}")
            return False
    
    def batch_put(self, table_name: str, rows: Dict[str, Dict]) -> bool:
        """
        Batch insert/update multiple rows
        
        Args:
            table_name: Table name
            rows: Dictionary with row_key: data pairs
        
        Returns:
            True if successful
        """
        try:
            table = self.connection.table(table_name)
            batch = table.batch()
            
            for row_key, data in rows.items():
                encoded_data = {}
                for col, val in data.items():
                    if isinstance(val, str):
                        encoded_data[col.encode() if isinstance(col, str) else col] = val.encode()
                    else:
                        encoded_data[col.encode() if isinstance(col, str) else col] = str(val).encode()
                
                batch.put(row_key.encode() if isinstance(row_key, str) else row_key, 
                         encoded_data)
            
            batch.send()
            self.logger.info(f"✓ Batch put {len(rows)} rows into '{table_name}'")
            return True
        except Exception as e:
            self.logger.error(f"Error in batch put: {e}")
            return False
    
    # ========== Utility Operations ==========
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get table information including column families
        
        Args:
            table_name: Table name
        
        Returns:
            Dictionary with table information
        """
        try:
            table = self.connection.table(table_name)
            families = table.families()
            
            info = {
                'table_name': table_name,
                'column_families': {}
            }
            
            for family, config in families.items():
                family_str = family.decode() if isinstance(family, bytes) else family
                info['column_families'][family_str] = config
            
            return info
        except Exception as e:
            self.logger.error(f"Error getting table info: {e}")
            return {}
    
    def count_rows(self, table_name: str) -> int:
        """
        Count rows in a table (approximate)
        
        Args:
            table_name: Table name
        
        Returns:
            Number of rows
        """
        try:
            count = 0
            table = self.connection.table(table_name)
            for _ in table.scan():
                count += 1
            return count
        except Exception as e:
            self.logger.error(f"Error counting rows: {e}")
            return 0
    
    # ========== Context Manager ==========
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()