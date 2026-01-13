import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.rdf_connector import RDFConnector
from schema.rdf_schema_explorer import RDFSchemaExplorer

def main():
    print("\n" + "="*60)
    print("RDF SCHEMA EXPLORER TEST")
    print("="*60)
    
    conn = RDFConnector()
    if not conn.connect():
        print("✗ Connection failed")
        return
    
    explorer = RDFSchemaExplorer(conn)
    
    print("\n📊 Analyzing schema...")
    schema = explorer.get_graph_schema()
    
    print("\n" + explorer.generate_schema_summary(schema))
    
    print("\n🤖 LLM Context:")
    print(explorer.generate_llm_context())
    
    conn.disconnect()
    print("\n✅ Complete!")

if __name__ == "__main__":
    main()