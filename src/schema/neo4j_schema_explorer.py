"""
Neo4j Schema Explorer
Extracts and analyzes Neo4j graph schema
"""

import sys
import os
from typing import Dict, List, Any, Optional
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.neo4j_connector import Neo4jConnector
from utils.logger import setup_logger

class Neo4jSchemaExplorer:
    """Explores and analyzes Neo4j graph schema"""
    
    def __init__(self, connector: Neo4jConnector):
        """
        Initialize schema explorer
        
        Args:
            connector: Neo4j connector instance
        """
        self.connector = connector
        self.logger = setup_logger(__name__)
    
    def get_node_labels(self) -> List[str]:
        """
        Get all node labels in the graph
        
        Returns:
            List of node labels
        """
        return self.connector.get_labels()
    
    def get_relationship_types(self) -> List[str]:
        """
        Get all relationship types in the graph
        
        Returns:
            List of relationship types
        """
        return self.connector.get_relationship_types()
    
    def analyze_node_properties(self, label: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Analyze properties of nodes with a specific label
        
        Args:
            label: Node label
            sample_size: Number of nodes to sample
        
        Returns:
            Dictionary with property information
        """
        try:
            query = f"""
            MATCH (n:{label})
            RETURN n
            LIMIT {sample_size}
            """
            
            results = self.connector.execute_query(query)
            
            if not results:
                return {}
            
            # Track property information
            property_info = defaultdict(lambda: {
                'count': 0,
                'types': defaultdict(int),
                'null_count': 0,
                'sample_values': []
            })
            
            # Analyze each node
            for record in results:
                node = record.get('n', {})
                
                # Track which properties exist in this node
                for key, value in node.items():
                    property_info[key]['count'] += 1
                    
                    if value is None:
                        property_info[key]['null_count'] += 1
                        property_info[key]['types']['null'] += 1
                    else:
                        value_type = type(value).__name__
                        property_info[key]['types'][value_type] += 1
                        
                        # Store sample values
                        if len(property_info[key]['sample_values']) < 5:
                            if isinstance(value, str):
                                property_info[key]['sample_values'].append(value[:50])
                            elif isinstance(value, (list, dict)):
                                property_info[key]['sample_values'].append(str(value)[:50])
                            else:
                                property_info[key]['sample_values'].append(value)
            
            # Calculate statistics
            total_nodes = len(results)
            result = {}
            
            for prop, info in property_info.items():
                # Determine primary type
                if info['types']:
                    primary_type = max(info['types'].items(), key=lambda x: x[1])[0]
                else:
                    primary_type = 'unknown'
                
                result[prop] = {
                    'type': primary_type,
                    'all_types': dict(info['types']),
                    'presence': f"{(info['count'] / total_nodes * 100):.1f}%",
                    'count': info['count'],
                    'null_count': info['null_count'],
                    'sample_values': info['sample_values']
                }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error analyzing node properties: {e}")
            return {}
    
    def analyze_relationship_properties(self, rel_type: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Analyze properties of relationships of a specific type
        
        Args:
            rel_type: Relationship type
            sample_size: Number of relationships to sample
        
        Returns:
            Dictionary with property information
        """
        try:
            query = f"""
            MATCH ()-[r:{rel_type}]->()
            RETURN r
            LIMIT {sample_size}
            """
            
            results = self.connector.execute_query(query)
            
            if not results:
                return {}
            
            # Track property information
            property_info = defaultdict(lambda: {
                'count': 0,
                'types': defaultdict(int),
                'sample_values': []
            })
            
            # Analyze each relationship
            for record in results:
                rel = record.get('r', {})
                
                for key, value in rel.items():
                    property_info[key]['count'] += 1
                    
                    if value is not None:
                        value_type = type(value).__name__
                        property_info[key]['types'][value_type] += 1
                        
                        if len(property_info[key]['sample_values']) < 5:
                            property_info[key]['sample_values'].append(value)
            
            # Calculate statistics
            total_rels = len(results)
            result = {}
            
            for prop, info in property_info.items():
                if info['types']:
                    primary_type = max(info['types'].items(), key=lambda x: x[1])[0]
                else:
                    primary_type = 'unknown'
                
                result[prop] = {
                    'type': primary_type,
                    'presence': f"{(info['count'] / total_rels * 100):.1f}%",
                    'sample_values': info['sample_values']
                }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error analyzing relationship properties: {e}")
            return {}
    
    def get_relationship_patterns(self) -> List[Dict[str, Any]]:
        """
        Get all relationship patterns (source label -> rel type -> target label)
        
        Returns:
            List of relationship patterns
        """
        try:
            query = """
            MATCH (a)-[r]->(b)
            WITH labels(a) as source_labels, type(r) as rel_type, labels(b) as target_labels
            UNWIND source_labels as source_label
            UNWIND target_labels as target_label
            RETURN DISTINCT source_label, rel_type, target_label, count(*) as count
            ORDER BY count DESC
            """
            
            results = self.connector.execute_query(query)
            
            patterns = []
            for record in results:
                patterns.append({
                    'source_label': record['source_label'],
                    'relationship_type': record['rel_type'],
                    'target_label': record['target_label'],
                    'count': record['count']
                })
            
            return patterns
            
        except Exception as e:
            self.logger.error(f"Error getting relationship patterns: {e}")
            return []
    
    def get_node_schema(self, label: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Get complete schema information for a node label
        
        Args:
            label: Node label
            sample_size: Number of nodes to sample
        
        Returns:
            Complete node schema information
        """
        try:
            # Get node count
            count = self.connector.count_nodes(label)
            
            # Analyze properties
            properties = self.analyze_node_properties(label, sample_size)
            
            # Get outgoing relationships
            outgoing_query = f"""
            MATCH (n:{label})-[r]->(m)
            RETURN DISTINCT type(r) as rel_type, labels(m) as target_labels, count(*) as count
            ORDER BY count DESC
            """
            outgoing = self.connector.execute_query(outgoing_query)
            
            # Get incoming relationships
            incoming_query = f"""
            MATCH (m)-[r]->(n:{label})
            RETURN DISTINCT type(r) as rel_type, labels(m) as source_labels, count(*) as count
            ORDER BY count DESC
            """
            incoming = self.connector.execute_query(incoming_query)
            
            schema = {
                'label': label,
                'node_count': count,
                'properties': properties,
                'outgoing_relationships': [
                    {
                        'type': rec['rel_type'],
                        'target_labels': rec['target_labels'],
                        'count': rec['count']
                    }
                    for rec in outgoing
                ],
                'incoming_relationships': [
                    {
                        'type': rec['rel_type'],
                        'source_labels': rec['source_labels'],
                        'count': rec['count']
                    }
                    for rec in incoming
                ]
            }
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting node schema: {e}")
            return {}
    
    def get_relationship_schema(self, rel_type: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Get complete schema information for a relationship type
        
        Args:
            rel_type: Relationship type
            sample_size: Number of relationships to sample
        
        Returns:
            Complete relationship schema information
        """
        try:
            # Get relationship count
            count = self.connector.count_relationships(rel_type)
            
            # Analyze properties
            properties = self.analyze_relationship_properties(rel_type, sample_size)
            
            # Get patterns (what connects to what)
            pattern_query = f"""
            MATCH (a)-[r:{rel_type}]->(b)
            RETURN DISTINCT labels(a) as source_labels, labels(b) as target_labels, count(*) as count
            ORDER BY count DESC
            """
            patterns = self.connector.execute_query(pattern_query)
            
            schema = {
                'relationship_type': rel_type,
                'relationship_count': count,
                'properties': properties,
                'patterns': [
                    {
                        'source_labels': rec['source_labels'],
                        'target_labels': rec['target_labels'],
                        'count': rec['count']
                    }
                    for rec in patterns
                ]
            }
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting relationship schema: {e}")
            return {}
    
    def get_graph_schema(self, sample_size: int = 100) -> Dict[str, Any]:
        """
        Get complete schema for entire graph
        
        Args:
            sample_size: Number of nodes/relationships to sample
        
        Returns:
            Complete graph schema
        """
        try:
            labels = self.get_node_labels()
            rel_types = self.get_relationship_types()
            
            graph_schema = {
                'database': self.connector.database,
                'total_nodes': self.connector.count_nodes(),
                'total_relationships': self.connector.count_relationships(),
                'node_labels': len(labels),
                'relationship_types': len(rel_types),
                'nodes': {},
                'relationships': {},
                'patterns': self.get_relationship_patterns()
            }
            
            # Analyze each node label
            for label in labels:
                self.logger.info(f"Analyzing node label: {label}")
                schema = self.get_node_schema(label, sample_size)
                graph_schema['nodes'][label] = schema
            
            # Analyze each relationship type
            for rel_type in rel_types:
                self.logger.info(f"Analyzing relationship type: {rel_type}")
                schema = self.get_relationship_schema(rel_type, sample_size)
                graph_schema['relationships'][rel_type] = schema
            
            return graph_schema
            
        except Exception as e:
            self.logger.error(f"Error getting graph schema: {e}")
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
            
            if 'database' in schema:
                # Graph-level schema
                lines.append(f"Neo4j Database: {schema['database']}")
                lines.append(f"Total Nodes: {schema.get('total_nodes', 0):,}")
                lines.append(f"Total Relationships: {schema.get('total_relationships', 0):,}")
                lines.append(f"Node Labels: {schema.get('node_labels', 0)}")
                lines.append(f"Relationship Types: {schema.get('relationship_types', 0)}")
                lines.append("")
                
                # Node schemas
                lines.append("Node Labels:")
                for label, node_schema in schema.get('nodes', {}).items():
                    lines.extend(self._format_node_summary(node_schema))
                    lines.append("")
                
                # Relationship schemas
                lines.append("Relationship Types:")
                for rel_type, rel_schema in schema.get('relationships', {}).items():
                    lines.extend(self._format_relationship_summary(rel_schema))
                    lines.append("")
                
                # Patterns
                patterns = schema.get('patterns', [])
                if patterns:
                    lines.append("Common Graph Patterns:")
                    for pattern in patterns[:10]:  # Top 10
                        lines.append(f"  ({pattern['source_label']})-[:{pattern['relationship_type']}]->({pattern['target_label']}) [{pattern['count']} instances]")
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.error(f"Error generating schema summary: {e}")
            return ""
    
    def _format_node_summary(self, schema: Dict[str, Any]) -> List[str]:
        """Format node schema summary"""
        lines = []
        
        lines.append(f"  :{schema.get('label', 'unknown')}")
        lines.append(f"    Nodes: {schema.get('node_count', 0):,}")
        
        # Properties
        properties = schema.get('properties', {})
        if properties:
            lines.append(f"    Properties ({len(properties)}):")
            for prop_name, prop_info in list(properties.items())[:10]:
                lines.append(f"      - {prop_name}: {prop_info['type']} (present in {prop_info['presence']})")
        
        # Outgoing relationships
        outgoing = schema.get('outgoing_relationships', [])
        if outgoing:
            lines.append(f"    Outgoing relationships:")
            for rel in outgoing[:5]:
                targets = ', '.join(rel['target_labels'])
                lines.append(f"      - :{rel['type']} -> ({targets}) [{rel['count']} instances]")
        
        # Incoming relationships
        incoming = schema.get('incoming_relationships', [])
        if incoming:
            lines.append(f"    Incoming relationships:")
            for rel in incoming[:5]:
                sources = ', '.join(rel['source_labels'])
                lines.append(f"      - ({sources})-[:{rel['type']}] <- [{rel['count']} instances]")
        
        return lines
    
    def _format_relationship_summary(self, schema: Dict[str, Any]) -> List[str]:
        """Format relationship schema summary"""
        lines = []
        
        lines.append(f"  :{schema.get('relationship_type', 'unknown')}")
        lines.append(f"    Relationships: {schema.get('relationship_count', 0):,}")
        
        # Properties
        properties = schema.get('properties', {})
        if properties:
            lines.append(f"    Properties ({len(properties)}):")
            for prop_name, prop_info in properties.items():
                lines.append(f"      - {prop_name}: {prop_info['type']} (present in {prop_info['presence']})")
        
        # Patterns
        patterns = schema.get('patterns', [])
        if patterns:
            lines.append(f"    Connects:")
            for pattern in patterns[:5]:
                sources = ', '.join(pattern['source_labels'])
                targets = ', '.join(pattern['target_labels'])
                lines.append(f"      - ({sources}) -> ({targets}) [{pattern['count']} instances]")
        
        return lines
    
    def generate_llm_context(self, label: Optional[str] = None) -> str:
        """
        Generate schema context optimized for LLM query translation
        
        Args:
            label: Specific node label (if None, generates for entire graph)
        
        Returns:
            LLM-friendly schema description
        """
        try:
            if label:
                schema = self.get_node_schema(label)
                return self._format_llm_node(schema)
            else:
                schema = self.get_graph_schema()
                return self._format_llm_graph(schema)
                
        except Exception as e:
            self.logger.error(f"Error generating LLM context: {e}")
            return ""
    
    def _format_llm_node(self, schema: Dict[str, Any]) -> str:
        """Format node schema for LLM"""
        lines = []
        
        lines.append(f"# Neo4j Node Label: :{schema.get('label')}")
        lines.append("")
        lines.append(f"Total nodes: {schema.get('node_count', 0):,}")
        lines.append("")
        
        # Properties
        lines.append("## Properties:")
        properties = schema.get('properties', {})
        for prop_name, prop_info in properties.items():
            examples = prop_info.get('sample_values', [])
            example_str = f" (e.g., {examples[0]})" if examples else ""
            lines.append(f"- `{prop_name}`: {prop_info['type']}{example_str}")
        
        # Relationships
        outgoing = schema.get('outgoing_relationships', [])
        if outgoing:
            lines.append("")
            lines.append("## Outgoing Relationships:")
            for rel in outgoing:
                targets = ', '.join(rel['target_labels'])
                lines.append(f"- `:{rel['type']}` -> ({targets})")
        
        incoming = schema.get('incoming_relationships', [])
        if incoming:
            lines.append("")
            lines.append("## Incoming Relationships:")
            for rel in incoming:
                sources = ', '.join(rel['source_labels'])
                lines.append(f"- ({sources})-[`:{rel['type']}`]->")
        
        return "\n".join(lines)
    
    def _format_llm_graph(self, schema: Dict[str, Any]) -> str:
        """Format graph schema for LLM"""
        lines = []
        
        lines.append(f"# Neo4j Graph Database: {schema.get('database')}")
        lines.append("")
        lines.append(f"Total nodes: {schema.get('total_nodes', 0):,}")
        lines.append(f"Total relationships: {schema.get('total_relationships', 0):,}")
        lines.append("")
        
        # Node labels
        lines.append("## Node Labels:")
        for label, node_schema in schema.get('nodes', {}).items():
            lines.append(f"### :{label}")
            lines.append(f"Count: {node_schema.get('node_count', 0):,}")
            
            # Key properties
            properties = node_schema.get('properties', {})
            if properties:
                prop_list = list(properties.keys())[:5]
                lines.append(f"Key properties: {', '.join(prop_list)}")
            
            lines.append("")
        
        # Relationship types
        lines.append("## Relationship Types:")
        for rel_type, rel_schema in schema.get('relationships', {}).items():
            lines.append(f"### :{rel_type}")
            lines.append(f"Count: {rel_schema.get('relationship_count', 0):,}")
            
            # Patterns
            patterns = rel_schema.get('patterns', [])
            if patterns:
                for pattern in patterns[:2]:
                    sources = ', '.join(pattern['source_labels'])
                    targets = ', '.join(pattern['target_labels'])
                    lines.append(f"  ({sources})-[:{rel_type}]->({targets})")
            
            lines.append("")
        
        # Common patterns
        patterns = schema.get('patterns', [])
        if patterns:
            lines.append("## Common Graph Patterns:")
            for pattern in patterns[:10]:
                lines.append(f"- ({pattern['source_label']})-[:{pattern['relationship_type']}]->({pattern['target_label']})")
        
        return "\n".join(lines)