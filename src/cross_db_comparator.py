"""
Cross-Database Comparison Module
Compares natural language queries across multiple NoSQL databases
"""

import time
from typing import Dict, List, Any, Optional
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.llm.query_translator import QueryTranslator
from src.llm.query_executor import QueryExecutor
from src.utils.logger import setup_logger

class CrossDatabaseComparator:
    """Compares NLQ execution across multiple NoSQL databases"""

    def __init__(self):
        self.logger = setup_logger(__name__)
        self.translator = QueryTranslator()
        self.executor = QueryExecutor()

        # Supported databases
        self.databases = ['mongodb', 'neo4j', 'redis', 'hbase', 'rdf']

    def compare_query(self, natural_query: str, databases: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Compare a natural language query across multiple databases

        Args:
            natural_query: The natural language query to compare
            databases: List of databases to include (default: all)

        Returns:
            Dictionary with comparison results
        """
        if databases is None:
            databases = self.databases

        results = {
            'natural_query': natural_query,
            'timestamp': time.time(),
            'comparisons': {}
        }

        self.logger.info(f"🔍 Comparing query across {len(databases)} databases: {natural_query}")

        for db_type in databases:
            try:
                self.logger.info(f"📊 Processing {db_type.upper()}...")

                # Get schema context for the database
                schema_context = self._get_schema_context(db_type)

                # Translate query
                start_time = time.time()
                if db_type == 'mongodb':
                    translated = self.translator.translate_to_mongodb(natural_query, schema_context)
                elif db_type == 'neo4j':
                    translated = self.translator.translate_to_neo4j(natural_query, schema_context)
                elif db_type == 'redis':
                    translated = self.translator.translate_to_redis(natural_query, schema_context)
                elif db_type == 'hbase':
                    translated = self.translator.translate_to_hbase(natural_query, schema_context)
                elif db_type == 'rdf':
                    translated = self.translator.translate_to_sparql(natural_query, schema_context)
                else:
                    continue

                translation_time = time.time() - start_time

                # Execute query if translation successful
                execution_result = None
                execution_time = 0

                if 'error' not in translated:
                    exec_start = time.time()
                    try:
                        if db_type == 'mongodb':
                            execution_result = self.executor.execute_mongodb(translated)
                        elif db_type == 'neo4j':
                            execution_result = self.executor.execute_neo4j(translated)
                        elif db_type == 'redis':
                            execution_result = self.executor.execute_redis(translated)
                        elif db_type == 'hbase':
                            execution_result = self.executor.execute_hbase(translated)
                        elif db_type == 'rdf':
                            execution_result = self.executor.execute_sparql(translated)
                    except Exception as e:
                        execution_result = {'success': False, 'error': str(e)}
                    execution_time = time.time() - exec_start

                # Store results
                results['comparisons'][db_type] = {
                    'translation': {
                        'success': 'error' not in translated,
                        'time_seconds': translation_time,
                        'query': translated.get('query') or translated.get('cypher') or
                                translated.get('sparql') or translated.get('commands') or
                                translated.get('operation'),
                        'explanation': translated.get('explanation', ''),
                        'error': translated.get('error'),
                        # Store full translated dict for operation details
                        'operation': translated.get('operation'),
                        'filters': translated.get('filters'),
                        'genre': translated.get('genre'),
                        'year': translated.get('year'),
                        'director': translated.get('director'),
                        'actor': translated.get('actor'),
                        'document': translated.get('document'),
                        'update': translated.get('update')
                    },
                    'execution': {
                        'success': execution_result.get('success', False) if execution_result else False,
                        'time_seconds': execution_time,
                        'result_count': execution_result.get('count', 0) if execution_result else 0,
                        'sample_results': self._extract_sample_results(execution_result),
                        'error': execution_result.get('error') if execution_result else None,
                        # Include generated queries from execution (for filter operations)
                        'cypher': execution_result.get('cypher') if execution_result else None,
                        'sparql': execution_result.get('sparql') if execution_result else None
                    },
                    'total_time': translation_time + execution_time
                }

                self.logger.info(f"✅ {db_type.upper()} completed in {translation_time + execution_time:.2f}s")

            except Exception as e:
                self.logger.error(f"❌ Error processing {db_type}: {e}")
                results['comparisons'][db_type] = {
                    'translation': {'success': False, 'error': str(e)},
                    'execution': {'success': False, 'error': str(e)},
                    'total_time': 0
                }

        # Add summary statistics
        results['summary'] = self._generate_summary(results)

        return results

    def _get_schema_context(self, db_type: str) -> str:
        """Get schema context for a specific database type"""
        try:
            if db_type == 'mongodb':
                from schema.mongodb_schema_explorer import MongoDBSchemaExplorer
                from connectors.mongodb_connector import MongoDBConnector
                conn = MongoDBConnector()
                conn.connect()
                explorer = MongoDBSchemaExplorer(conn)
                schema = explorer.generate_llm_context('movies')
                conn.disconnect()
                return schema

            elif db_type == 'neo4j':
                from schema.neo4j_schema_explorer import Neo4jSchemaExplorer
                from connectors.neo4j_connector import Neo4jConnector
                conn = Neo4jConnector()
                conn.connect()
                explorer = Neo4jSchemaExplorer(conn)
                schema = explorer.generate_llm_context()
                conn.disconnect()
                return schema

            elif db_type == 'redis':
                from schema.redis_schema_explorer import RedisSchemaExplorer
                from connectors.redis_connector import RedisConnector
                conn = RedisConnector()
                conn.connect()
                explorer = RedisSchemaExplorer(conn)
                schema = explorer.generate_llm_context()
                conn.disconnect()
                return schema

            elif db_type == 'hbase':
                from schema.hbase_schema_explorer import HBaseSchemaExplorer
                from connectors.hbase_connector import HBaseConnector
                conn = HBaseConnector()
                conn.connect()
                explorer = HBaseSchemaExplorer(conn)
                schema = explorer.generate_llm_context()
                conn.disconnect()
                return schema

            elif db_type == 'rdf':
                from schema.rdf_schema_explorer import RDFSchemaExplorer
                from connectors.rdf_connector import RDFConnector
                conn = RDFConnector()
                conn.connect()
                explorer = RDFSchemaExplorer(conn)
                schema = explorer.generate_llm_context()
                conn.disconnect()
                return schema

        except Exception as e:
            self.logger.warning(f"Could not get schema for {db_type}: {e}")
            return f"Schema information not available for {db_type}"

        return f"Schema information not available for {db_type}"

    def _get_detailed_schema(self, db_type: str) -> Dict[str, Any]:
        """
        Get detailed schema information for display
        
        Args:
            db_type: Database type
            
        Returns:
            Dictionary with detailed schema information
        """
        try:
            if db_type == 'mongodb':
                from schema.mongodb_schema_explorer import MongoDBSchemaExplorer
                from connectors.mongodb_connector import MongoDBConnector
                conn = MongoDBConnector()
                conn.connect()
                explorer = MongoDBSchemaExplorer(conn)
                schema = explorer.get_database_schema()
                conn.disconnect()
                return schema

            elif db_type == 'neo4j':
                from schema.neo4j_schema_explorer import Neo4jSchemaExplorer
                from connectors.neo4j_connector import Neo4jConnector
                conn = Neo4jConnector()
                conn.connect()
                explorer = Neo4jSchemaExplorer(conn)
                schema = explorer.get_graph_schema()
                conn.disconnect()
                return schema

            elif db_type == 'redis':
                from schema.redis_schema_explorer import RedisSchemaExplorer
                from connectors.redis_connector import RedisConnector
                conn = RedisConnector()
                conn.connect()
                explorer = RedisSchemaExplorer(conn)
                schema = explorer.get_database_schema()
                conn.disconnect()
                return schema

            elif db_type == 'hbase':
                from schema.hbase_schema_explorer import HBaseSchemaExplorer
                from connectors.hbase_connector import HBaseConnector
                conn = HBaseConnector()
                conn.connect()
                explorer = HBaseSchemaExplorer(conn)
                schema = explorer.get_database_schema()
                conn.disconnect()
                return schema

            elif db_type == 'rdf':
                from schema.rdf_schema_explorer import RDFSchemaExplorer
                from connectors.rdf_connector import RDFConnector
                conn = RDFConnector()
                conn.connect()
                explorer = RDFSchemaExplorer(conn)
                schema = explorer.get_graph_schema()
                conn.disconnect()
                return schema

        except Exception as e:
            self.logger.warning(f"Could not get detailed schema for {db_type}: {e}")
            return {'error': str(e)}

        return {'error': f'Detailed schema not available for {db_type}'}

    def _extract_sample_results(self, execution_result: Optional[Dict]) -> List[Any]:
        """Extract sample results for display"""
        if not execution_result or not execution_result.get('success'):
            return []

        results = execution_result.get('results', [])
        if not results:
            return []

        # Return up to 3 sample results - preserve original structure
        # The display formatting will handle standardization
        return results[:3]

    def _generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics for the comparison"""
        comparisons = results.get('comparisons', {})

        summary = {
            'total_databases': len(comparisons),
            'successful_translations': 0,
            'successful_executions': 0,
            'total_time': 0,
            'fastest_database': None,
            'slowest_database': None,
            'most_results': None,
            'least_results': None
        }

        fastest_time = float('inf')
        slowest_time = 0
        max_results = 0
        min_results = float('inf')

        for db_type, data in comparisons.items():
            if data['translation']['success']:
                summary['successful_translations'] += 1
            if data['execution']['success']:
                summary['successful_executions'] += 1

            total_time = data['total_time']
            summary['total_time'] += total_time

            result_count = data['execution']['result_count']

            if total_time < fastest_time:
                fastest_time = total_time
                summary['fastest_database'] = db_type

            if total_time > slowest_time:
                slowest_time = total_time
                summary['slowest_database'] = db_type

            if result_count > max_results:
                max_results = result_count
                summary['most_results'] = db_type

            if result_count < min_results:
                min_results = result_count
                summary['least_results'] = db_type

        return summary

    def print_comparison_report(self, results: Dict[str, Any]):
        """Print a formatted comparison report"""
        print("\n" + "="*80)
        print("🔍 CROSS-DATABASE QUERY COMPARISON REPORT")
        print("="*80)
        print(f"Natural Query: {results['natural_query']}")
        print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(results['timestamp']))}")
        print()

        # Summary
        summary = results.get('summary', {})
        print("📊 SUMMARY STATISTICS")
        print("-" * 40)
        print(f"Databases tested: {summary.get('total_databases', 0)}")
        print(f"Successful translations: {summary.get('successful_translations', 0)}")
        print(f"Successful executions: {summary.get('successful_executions', 0)}")
        print(".2f")
        if summary.get('fastest_database'):
            print(f"Fastest database: {summary['fastest_database'].upper()}")
        if summary.get('most_results'):
            print(f"Most results: {summary['most_results'].upper()}")
        print()

        # Detailed results
        comparisons = results.get('comparisons', {})
        for db_type, data in comparisons.items():
            print(f"🗄️  {db_type.upper()}")
            print("-" * 20)

            # Translation
            trans = data['translation']
            status = "✅" if trans['success'] else "❌"
            print(f"Translation: {status} ({trans['time_seconds']:.2f}s)")
            if trans.get('error'):
                print(f"  Error: {trans['error']}")
            else:
                query_preview = str(trans.get('query', ''))[:100]
                if len(str(trans.get('query', ''))) > 100:
                    query_preview += "..."
                print(f"  Query: {query_preview}")
                if trans.get('explanation'):
                    print(f"  Explanation: {trans['explanation']}")

            # Execution
            exec_data = data['execution']
            status = "✅" if exec_data['success'] else "❌"
            print(f"Execution: {status} ({exec_data['time_seconds']:.2f}s)")
            print(f"Results: {exec_data['result_count']} items")

            if exec_data.get('error'):
                print(f"  Error: {exec_data['error']}")
            elif exec_data.get('sample_results'):
                print("  Sample results:")
                for i, sample in enumerate(exec_data['sample_results'][:2]):
                    print(f"    {i+1}. {str(sample)[:150]}")

            print(f"Total time: {data['total_time']:.2f}s")
            print()

        print("="*80)

def main():
    """Demo the cross-database comparator"""
    print("🚀 Cross-Database Query Comparator Demo")
    print("="*50)

    comparator = CrossDatabaseComparator()

    # Test queries
    test_queries = [
        "Find all movies from 2015",
        "Show me action movies",
        "Find movies with rating above 8",
        "Who directed The Dark Knight?"
    ]

    for query in test_queries:
        print(f"\n🔍 Testing: '{query}'")
        results = comparator.compare_query(query)
        comparator.print_comparison_report(results)

        # Ask to continue
        try:
            input("\nPress Enter to continue to next query...")
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()