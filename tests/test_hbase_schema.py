import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.hbase_connector import HBaseConnector
from schema.hbase_schema_explorer import HBaseSchemaExplorer

def main():
    print("\n" + "="*60)
    print("HBASE SCHEMA EXPLORER TEST")
    print("="*60)
    
    conn = HBaseConnector()
    if not conn.connect():
        print("✗ Connection failed")
        return
    
    explorer = HBaseSchemaExplorer(conn)
    
    print("\n📊 Analyzing schema...")
    schema = explorer.get_database_schema()
    
    print("\n" + explorer.generate_schema_summary(schema))
    
    print("\n🤖 LLM Context:")
    print(explorer.generate_llm_context())
    
    conn.disconnect()
    print("\n✅ Complete!")

if __name__ == "__main__":
    main()