"""
RDF Schema Explorer
Extracts and analyzes RDF store schemas
"""

import sys
import os
from typing import Dict, List, Any, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.rdf_connector import RDFConnector
from utils.logger import setup_logger

class RDFSchemaExplorer:
    """Explores and analyzes RDF graph schemas"""
    
    def __init__(self, connector: RDFConnector):
        self.connector = connector
        self.logger = setup_logger(__name__)
    
    def get_graph_schema(self) -> Dict[str, Any]:
        """Get complete RDF graph schema"""
        try:
            classes = self.connector.get_classes()
            properties = self.connector.get_properties()
            
            schema = {
                'total_triples': self.connector.count_triples(),
                'classes': classes,
                'properties': properties,
                'class_analysis': {}
            }
            
            # Analyze each class
            for cls in classes:
                count_query = f"""
                SELECT (COUNT(?s) as ?count) WHERE {{
                    ?s a <{cls}> .
                }}
                """
                results = self.connector.execute_query(count_query)
                count = int(results[0]['count']) if results else 0
                
                # Get sample properties for this class
                props_query = f"""
                SELECT DISTINCT ?property WHERE {{
                    ?s a <{cls}> .
                    ?s ?property ?o .
                }}
                LIMIT 20
                """
                props_results = self.connector.execute_query(props_query)
                class_props = [r['property'] for r in props_results]
                
                schema['class_analysis'][cls] = {
                    'instance_count': count,
                    'properties': class_props
                }
            
            return schema
        except Exception as e:
            self.logger.error(f"Error: {e}")
            return {}
    
    def generate_schema_summary(self, schema: Dict[str, Any]) -> str:
        """Generate human-readable summary"""
        lines = []
        
        lines.append(f"RDF Graph Schema")
        lines.append(f"Total Triples: {schema.get('total_triples', 0):,}")
        lines.append(f"Classes: {len(schema.get('classes', []))}")
        lines.append(f"Properties: {len(schema.get('properties', []))}")
        lines.append("")
        
        lines.append("Classes:")
        for cls, analysis in schema.get('class_analysis', {}).items():
            lines.append(f"  {cls}")
            lines.append(f"    Instances: {analysis['instance_count']:,}")
            lines.append(f"    Properties: {len(analysis['properties'])}")
        
        return "\n".join(lines)
    
    def generate_llm_context(self) -> str:
        """Generate LLM-optimized context"""
        schema = self.get_graph_schema()
        
        lines = ["# RDF Graph Schema", ""]
        lines.append(f"Total triples: {schema.get('total_triples', 0):,}")
        lines.append("")
        
        lines.append("## Classes:")
        for cls, analysis in schema.get('class_analysis', {}).items():
            lines.append(f"### {cls}")
            lines.append(f"Instances: {analysis['instance_count']:,}")
            props = ', '.join([p.split('/')[-1] for p in analysis['properties'][:10]])
            lines.append(f"Properties: {props}")
            lines.append("")
        
        return "\n".join(lines)