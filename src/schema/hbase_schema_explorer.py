"""
HBase Schema Explorer
Extracts and analyzes HBase table schemas
"""

import sys
import os
from typing import Dict, List, Any, Optional
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.hbase_connector import HBaseConnector
from utils.logger import setup_logger

class HBaseSchemaExplorer:
    """Explores and analyzes HBase table schemas"""
    
    def __init__(self, connector: HBaseConnector):
        self.connector = connector
        self.logger = setup_logger(__name__)
    
    def get_table_schema(self, table_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """Get complete schema for a table"""
        try:
            # Get table info
            table_info = self.connector.get_table_info(table_name)
            
            # Sample rows
            rows = self.connector.scan(table_name, limit=sample_size)
            
            # Analyze column families
            cf_analysis = defaultdict(lambda: {'columns': set(), 'sample_count': 0})
            
            for row in rows:
                for col, val in row['data'].items():
                    cf = col.split(':')[0]
                    cf_analysis[cf]['columns'].add(col)
                    cf_analysis[cf]['sample_count'] += 1
            
            schema = {
                'table': table_name,
                'column_families': table_info.get('column_families', {}),
                'row_count': self.connector.count_rows(table_name),
                'column_analysis': {
                    cf: {
                        'columns': list(data['columns']),
                        'total_cells': data['sample_count']
                    }
                    for cf, data in cf_analysis.items()
                },
                'sample_rows': rows[:5]
            }
            
            return schema
        except Exception as e:
            self.logger.error(f"Error getting schema: {e}")
            return {}
    
    def get_database_schema(self) -> Dict[str, Any]:
        """Get schema for all tables"""
        try:
            tables = self.connector.list_tables()
            
            schema = {
                'total_tables': len(tables),
                'tables': {}
            }
            
            for table in tables:
                self.logger.info(f"Analyzing table: {table}")
                schema['tables'][table] = self.get_table_schema(table)
            
            return schema
        except Exception as e:
            self.logger.error(f"Error: {e}")
            return {}
    
    def generate_schema_summary(self, schema: Dict[str, Any]) -> str:
        """Generate human-readable summary"""
        lines = []
        
        if 'total_tables' in schema:
            lines.append(f"HBase Database")
            lines.append(f"Total Tables: {schema['total_tables']}")
            lines.append("")
            
            for table_name, table_schema in schema.get('tables', {}).items():
                lines.append(f"Table: {table_name}")
                lines.append(f"  Rows: {table_schema.get('row_count', 0):,}")
                lines.append(f"  Column Families:")
                for cf, analysis in table_schema.get('column_analysis', {}).items():
                    lines.append(f"    - {cf}: {len(analysis['columns'])} columns")
                lines.append("")
        
        return "\n".join(lines)
    
    def generate_llm_context(self, table_name: Optional[str] = None) -> str:
        """Generate LLM-optimized context"""
        if table_name:
            schema = self.get_table_schema(table_name)
        else:
            schema = self.get_database_schema()
        
        lines = ["# HBase Schema", ""]
        
        if 'tables' in schema:
            for tbl, tbl_schema in schema['tables'].items():
                lines.append(f"## Table: {tbl}")
                lines.append(f"Rows: {tbl_schema['row_count']:,}")
                lines.append("Column Families:")
                for cf, analysis in tbl_schema.get('column_analysis', {}).items():
                    cols = ', '.join(analysis['columns'][:10])
                    lines.append(f"  - {cf}: {cols}")
                lines.append("")
        
        return "\n".join(lines)