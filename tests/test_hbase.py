"""
HBase Connector Test Script
Tests connection and operations
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.hbase_connector import HBaseConnector

def print_section(title):
    print("\n" + "="*70)
    print(f" {title}")
    print("="*70)

def test_connection():
    """Test 1: Connection"""
    print_section("TEST 1: Connection")
    
    connector = HBaseConnector()
    
    if connector.connect():
        print("✓ Connection successful")
        
        info = connector.get_connection_info()
        print(f"  Host: {info['host']}:{info['port']}")
        print(f"  Connected: {info['connected']}")
        
        return connector
    else:
        print("✗ Connection failed")
        return None

def test_list_tables(connector):
    """Test 2: List tables"""
    print_section("TEST 2: List Tables")
    
    tables = connector.list_tables()
    print(f"\n📊 Tables ({len(tables)}):")
    for table in tables:
        print(f"  - {table}")
        
        # Get table info
        info = connector.get_table_info(table)
        families = info.get('column_families', {})
        print(f"    Column families: {', '.join(families.keys())}")

def test_table_operations(connector):
    """Test 3: Table operations"""
    print_section("TEST 3: Table Operations")
    
    test_table = 'test_movies'
    
    # Create table
    print(f"\n➕ Creating table '{test_table}'...")
    column_families = {
        'info': {'max_versions': 3},
        'ratings': {}
    }
    
    # Delete if exists
    if connector.table_exists(test_table):
        connector.delete_table(test_table)
    
    success = connector.create_table(test_table, column_families)
    if success:
        print(f"  ✓ Created table '{test_table}'")
    
    # Check if exists
    exists = connector.table_exists(test_table)
    print(f"  Table exists: {exists}")

def test_row_operations(connector):
    """Test 4: Row operations (CRUD)"""
    print_section("TEST 4: Row Operations")
    
    test_table = 'test_movies'
    
    # INSERT
    print("\n➕ Inserting rows...")
    
    # Single row
    row_data = {
        'info:title': 'Test Movie',
        'info:year': '2024',
        'ratings:imdb': '8.5'
    }
    connector.put(test_table, 'test_001', row_data)
    print("  ✓ Inserted row 'test_001'")
    
    # Batch insert
    batch_data = {
        'test_002': {
            'info:title': 'Movie Two',
            'info:year': '2023',
            'ratings:imdb': '7.8'
        },
        'test_003': {
            'info:title': 'Movie Three',
            'info:year': '2022',
            'ratings:imdb': '9.0'
        }
    }
    connector.batch_put(test_table, batch_data)
    print("  ✓ Batch inserted 2 rows")
    
    # READ
    print("\n📖 Reading rows...")
    
    # Get single row
    row = connector.get(test_table, 'test_001')
    if row:
        print(f"  Row 'test_001':")
        for col, val in row.items():
            print(f"    {col}: {val}")
    
    # Get specific columns
    row = connector.get(test_table, 'test_001', columns=['info:title', 'ratings:imdb'])
    print(f"\n  Row 'test_001' (specific columns):")
    for col, val in row.items():
        print(f"    {col}: {val}")
    
    # SCAN
    print("\n🔍 Scanning table...")
    rows = connector.scan(test_table, limit=5)
    print(f"  Found {len(rows)} rows:")
    for row in rows:
        print(f"    {row['row_key']}: {row['data'].get('info:title', 'N/A')}")
    
    # UPDATE
    print("\n✏️ Updating row...")
    update_data = {
        'info:year': '2025',
        'ratings:imdb': '9.5'
    }
    connector.put(test_table, 'test_001', update_data)
    print("  ✓ Updated row 'test_001'")
    
    # Verify update
    row = connector.get(test_table, 'test_001')
    print(f"  New year: {row.get('info:year')}")
    print(f"  New rating: {row.get('ratings:imdb')}")
    
    # DELETE
    print("\n🗑️ Deleting rows...")
    
    # Delete specific columns
    connector.delete(test_table, 'test_002', columns=['ratings:imdb'])
    print("  ✓ Deleted column from 'test_002'")
    
    # Delete entire row
    connector.delete(test_table, 'test_003')
    print("  ✓ Deleted row 'test_003'")
    
    # Verify deletion
    rows = connector.scan(test_table)
    print(f"  Remaining rows: {len(rows)}")

def test_scan_operations(connector):
    """Test 5: Advanced scan operations"""
    print_section("TEST 5: Scan Operations")
    
    test_table = 'test_movies'
    
    print("\n🔍 Scan with row range...")
    rows = connector.scan(test_table, row_start='test_001', row_stop='test_003')
    print(f"  Found {len(rows)} rows in range")
    
    print("\n🔍 Scan specific column family...")
    rows = connector.scan(test_table, columns=['info:title', 'info:year'], limit=3)
    for row in rows:
        print(f"  {row['row_key']}: {row['data']}")

def test_movie_data(connector):
    """Test 6: Query loaded movie data"""
    print_section("TEST 6: Query Movie Data")
    
    table_name = 'movies'
    
    if not connector.table_exists(table_name):
        print(f"\n⚠️  Table '{table_name}' not found!")
        print("  Run: python data/load_hbase_movies.py")
        return
    
    # Count rows
    print(f"\n📊 Counting rows in '{table_name}'...")
    count = connector.count_rows(table_name)
    print(f"  Total movies: {count}")
    
    # Get table info
    info = connector.get_table_info(table_name)
    print(f"\n📋 Table structure:")
    print(f"  Column families: {', '.join(info['column_families'].keys())}")
    
    # Sample movies
    print(f"\n🎬 Sample movies:")
    rows = connector.scan(table_name, limit=5)
    for row in rows:
        data = row['data']
        title = data.get('info:title', 'N/A')
        year = data.get('info:year', 'N/A')
        rating = data.get('ratings:imdb_rating', 'N/A')
        print(f"  - {title} ({year}): {rating}")
    
    # Get specific movie
    print(f"\n📄 Movie details (movie_00001):")
    movie = connector.get(table_name, 'movie_00001')
    if movie:
        # Group by column family
        families = {}
        for col, val in movie.items():
            cf = col.split(':')[0]
            if cf not in families:
                families[cf] = {}
            families[cf][col] = val
        
        for cf, cols in families.items():
            print(f"\n  {cf.upper()}:")
            for col, val in list(cols.items())[:5]:
                display_val = val[:50] + '...' if len(val) > 50 else val
                print(f"    {col}: {display_val}")
    
    # Scan by column family
    print(f"\n🔍 Movies with ratings (ratings:imdb_rating):")
    rows = connector.scan(table_name, columns=['info:title', 'ratings:imdb_rating'], limit=5)
    for row in rows:
        data = row['data']
        if 'ratings:imdb_rating' in data:
            print(f"  {data.get('info:title')}: {data.get('ratings:imdb_rating')}")

def test_cleanup(connector):
    """Test 7: Cleanup test table"""
    print_section("TEST 7: Cleanup")
    
    test_table = 'test_movies'
    
    if connector.table_exists(test_table):
        print(f"\n🗑️ Deleting test table '{test_table}'...")
        connector.delete_table(test_table)
        print("  ✓ Test table deleted")

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*70)
    print(" "*22 + "HBASE TEST SUITE")
    print("="*70)
    
    connector = test_connection()
    
    if not connector:
        print("\n✗ Connection failed. Cannot continue tests.")
        print("\nMake sure:")
        print("1. HBase Docker container is running")
        print("2. Thrift server is started:")
        print("   docker exec -d hbase_nosql_project hbase thrift start")
        print("3. Port 9090 is accessible")
        return
    
    try:
        test_list_tables(connector)
        test_table_operations(connector)
        test_row_operations(connector)
        test_scan_operations(connector)
        test_movie_data(connector)
        test_cleanup(connector)
        
        print("\n" + "="*70)
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY")
        print("="*70)
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        connector.disconnect()
        print("\n✓ Disconnected from HBase\n")

if __name__ == "__main__":
    run_all_tests()