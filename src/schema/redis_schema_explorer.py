"""
Redis Schema Explorer
Extracts and analyzes Redis key patterns and data structures
"""

import sys
import os
from typing import Dict, List, Any, Optional
from collections import defaultdict
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.redis_connector import RedisConnector
from utils.logger import setup_logger

class RedisSchemaExplorer:
    """Explores and analyzes Redis key patterns and data structures"""
    
    def __init__(self, connector: RedisConnector):
        """
        Initialize schema explorer
        
        Args:
            connector: Redis connector instance
        """
        self.connector = connector
        self.logger = setup_logger(__name__)
    
    def get_key_patterns(self, sample_size: int = 1000) -> Dict[str, List[str]]:
        """
        Detect key naming patterns
        
        Args:
            sample_size: Maximum number of keys to analyze
        
        Returns:
            Dictionary of patterns and their matching keys
        """
        try:
            # Get all keys (or sample)
            all_keys = self.connector.keys('*')
            
            if len(all_keys) > sample_size:
                import random
                all_keys = random.sample(all_keys, sample_size)
            
            # Group keys by pattern
            patterns = defaultdict(list)
            
            for key in all_keys:
                # Extract pattern (replace numbers and variable parts with placeholders)
                pattern = self._extract_pattern(key)
                patterns[pattern].append(key)
            
            return dict(patterns)
            
        except Exception as e:
            self.logger.error(f"Error getting key patterns: {e}")
            return {}
    
    def _extract_pattern(self, key: str) -> str:
        """
        Extract pattern from a key by replacing variable parts
        
        Args:
            key: Redis key
        
        Returns:
            Pattern string
        """
        # Replace UUIDs, hashes, and long IDs
        pattern = re.sub(r'[a-f0-9]{8,}', '{id}', key)
        
        # Replace numbers
        pattern = re.sub(r'\d+', '{num}', pattern)
        
        # Replace long strings (potential dynamic values)
        parts = pattern.split(':')
        cleaned_parts = []
        for part in parts:
            if len(part) > 20 and '{' not in part:
                cleaned_parts.append('{value}')
            else:
                cleaned_parts.append(part)
        
        return ':'.join(cleaned_parts)
    
    def analyze_key_pattern(self, pattern: str, sample_keys: List[str], 
                           sample_size: int = 10) -> Dict[str, Any]:
        """
        Analyze a specific key pattern
        
        Args:
            pattern: Key pattern
            sample_keys: Keys matching this pattern
            sample_size: Number of keys to sample for analysis
        
        Returns:
            Analysis results
        """
        try:
            # Sample keys for analysis
            if len(sample_keys) > sample_size:
                import random
                sample_keys = random.sample(sample_keys, sample_size)
            
            # Analyze data types
            type_counts = defaultdict(int)
            structure_info = defaultdict(lambda: {
                'sample_values': [],
                'field_names': set(),
                'element_types': set()
            })
            
            for key in sample_keys:
                # Determine key type
                key_type = self._get_key_type(key)
                type_counts[key_type] += 1
                
                # Get detailed structure based on type
                if key_type == 'string':
                    value = self.connector.get(key)
                    if value:
                        structure_info['string']['sample_values'].append(value[:50])
                
                elif key_type == 'hash':
                    fields = self.connector.hkeys(key)
                    structure_info['hash']['field_names'].update(fields)
                    
                    # Sample values
                    if fields:
                        sample_field = fields[0]
                        value = self.connector.hget(key, sample_field)
                        structure_info['hash']['sample_values'].append({sample_field: value[:50] if value else None})
                
                elif key_type == 'list':
                    length = self.connector.llen(key)
                    elements = self.connector.lrange(key, 0, 2)  # First 3 elements
                    structure_info['list']['sample_values'].append(elements)
                
                elif key_type == 'set':
                    members = list(self.connector.smembers(key))[:3]  # First 3 members
                    structure_info['set']['sample_values'].append(members)
                
                elif key_type == 'zset':
                    members = self.connector.zrange(key, 0, 2, withscores=True)
                    structure_info['zset']['sample_values'].append(members)
            
            # Compile analysis
            analysis = {
                'pattern': pattern,
                'total_keys': len(sample_keys),
                'types': dict(type_counts),
                'primary_type': max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else 'unknown',
                'structure': {}
            }
            
            # Add structure details
            for data_type, info in structure_info.items():
                analysis['structure'][data_type] = {
                    'sample_values': info['sample_values'][:5],
                    'field_names': list(info['field_names'])[:20] if info['field_names'] else [],
                    'element_types': list(info['element_types']) if info['element_types'] else []
                }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing key pattern: {e}")
            return {}
    
    def _get_key_type(self, key: str) -> str:
        """
        Get Redis data type for a key
        
        Args:
            key: Redis key
        
        Returns:
            Data type string
        """
        try:
            # Use Redis TYPE command through client
            key_type = self.connector.client.type(key)
            return key_type
        except:
            return 'unknown'
    
    def get_database_schema(self, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Get complete schema for Redis database
        
        Args:
            sample_size: Number of keys to sample
        
        Returns:
            Complete database schema
        """
        try:
            # Get basic stats
            total_keys = self.connector.dbsize()
            
            # Get key patterns
            patterns = self.get_key_patterns(sample_size)
            
            # Analyze each pattern
            pattern_schemas = {}
            for pattern, keys in patterns.items():
                self.logger.info(f"Analyzing pattern: {pattern}")
                analysis = self.analyze_key_pattern(pattern, keys)
                pattern_schemas[pattern] = analysis
            
            # Get metadata keys
            metadata_keys = [k for k in self.connector.keys('stats:*') + self.connector.keys('meta:*')]
            metadata = {}
            for key in metadata_keys:
                value = self.connector.get(key)
                if value:
                    metadata[key] = value
            
            schema = {
                'database': self.connector.db,
                'total_keys': total_keys,
                'patterns': pattern_schemas,
                'metadata': metadata,
                'sample_size': sample_size
            }
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting database schema: {e}")
            return {}
    
    def generate_schema_summary(self, schema: Dict[str, Any]) -> str:
        """
        Generate a human-readable schema summary
        
        Args:
            schema: Schema dictionary
        
        Returns:
            Schema summary as string
        """
        try:
            lines = []
            
            lines.append(f"Redis Database: {schema.get('database', 0)}")
            lines.append(f"Total Keys: {schema.get('total_keys', 0):,}")
            lines.append(f"Patterns Detected: {len(schema.get('patterns', {}))}")
            lines.append("")
            
            # Metadata
            metadata = schema.get('metadata', {})
            if metadata:
                lines.append("Metadata:")
                for key, value in metadata.items():
                    lines.append(f"  {key}: {value}")
                lines.append("")
            
            # Patterns
            lines.append("Key Patterns:")
            patterns = schema.get('patterns', {})
            for pattern, analysis in patterns.items():
                lines.extend(self._format_pattern_summary(analysis))
                lines.append("")
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.error(f"Error generating schema summary: {e}")
            return ""
    
    def _format_pattern_summary(self, analysis: Dict[str, Any]) -> List[str]:
        """Format pattern analysis summary"""
        lines = []
        
        lines.append(f"  Pattern: {analysis.get('pattern', 'unknown')}")
        lines.append(f"    Keys: {analysis.get('total_keys', 0):,}")
        lines.append(f"    Primary Type: {analysis.get('primary_type', 'unknown')}")
        
        # Type distribution
        types = analysis.get('types', {})
        if len(types) > 1:
            lines.append(f"    Type distribution: {types}")
        
        # Structure details
        structure = analysis.get('structure', {})
        for data_type, info in structure.items():
            if info.get('field_names'):
                fields = ', '.join(info['field_names'][:10])
                lines.append(f"    {data_type.capitalize()} fields: {fields}")
            
            if info.get('sample_values'):
                samples = info['sample_values'][:2]
                lines.append(f"    Sample values: {samples}")
        
        return lines
    
    def generate_llm_context(self) -> str:
        """
        Generate schema context optimized for LLM query translation
        
        Returns:
            LLM-friendly schema description
        """
        try:
            schema = self.get_database_schema()
            return self._format_llm_schema(schema)
                
        except Exception as e:
            self.logger.error(f"Error generating LLM context: {e}")
            return ""
    
    def _format_llm_schema(self, schema: Dict[str, Any]) -> str:
        """Format Redis schema for LLM"""
        lines = []
        
        lines.append(f"# Redis Database: {schema.get('database')}")
        lines.append("")
        lines.append(f"Total keys: {schema.get('total_keys', 0):,}")
        lines.append("")
        
        # Metadata context
        metadata = schema.get('metadata', {})
        if metadata:
            lines.append("## Dataset Information:")
            for key, value in metadata.items():
                clean_key = key.replace('stats:', '').replace('meta:', '').replace('_', ' ').title()
                lines.append(f"- {clean_key}: {value}")
            lines.append("")
        
        # Key patterns
        lines.append("## Key Patterns:")
        patterns = schema.get('patterns', {})
        for pattern, analysis in patterns.items():
            lines.append(f"### {pattern}")
            lines.append(f"Type: {analysis.get('primary_type')}")
            lines.append(f"Count: {analysis.get('total_keys', 0):,} keys")
            
            # Structure
            structure = analysis.get('structure', {})
            primary_type = analysis.get('primary_type')
            
            if primary_type == 'hash' and 'hash' in structure:
                fields = structure['hash'].get('field_names', [])
                if fields:
                    lines.append(f"Fields: {', '.join(fields[:10])}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def infer_relationships(self) -> List[Dict[str, Any]]:
        """
        Infer relationships between key patterns
        
        Returns:
            List of inferred relationships
        """
        try:
            relationships = []
            
            patterns = self.get_key_patterns()
            pattern_list = list(patterns.keys())
            
            for pattern in pattern_list:
                # Look for foreign key patterns
                # e.g., movie:{id}:cast might reference person:{id}
                parts = pattern.split(':')
                
                for i, part in enumerate(parts):
                    if part == '{id}' and i > 0:
                        entity = parts[i-1]
                        
                        # Look for related patterns
                        potential_target = f"{entity}:{{id}}"
                        if potential_target in pattern_list and potential_target != pattern:
                            relationships.append({
                                'from_pattern': pattern,
                                'to_pattern': potential_target,
                                'type': 'reference',
                                'confidence': 'medium'
                            })
            
            return relationships
            
        except Exception as e:
            self.logger.error(f"Error inferring relationships: {e}")
            return []