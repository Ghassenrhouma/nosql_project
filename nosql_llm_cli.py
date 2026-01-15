#!/usr/bin/env python3
"""
NoSQL-LLM Query Interface
Main CLI application for natural language querying across multiple NoSQL databases
"""

import sys
import os
import argparse
from typing import List, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.live import Live
from rich.spinner import Spinner
from rich.columns import Columns

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from cross_db_comparator import CrossDatabaseComparator
from llm.query_translator import QueryTranslator
from llm.query_executor import QueryExecutor
from schema.mongodb_schema_explorer import MongoDBSchemaExplorer
from schema.neo4j_schema_explorer import Neo4jSchemaExplorer
from schema.redis_schema_explorer import RedisSchemaExplorer
from schema.hbase_schema_explorer import HBaseSchemaExplorer
from schema.rdf_schema_explorer import RDFSchemaExplorer
from connectors.mongodb_connector import MongoDBConnector
from connectors.neo4j_connector import Neo4jConnector
from connectors.redis_connector import RedisConnector
from connectors.hbase_connector import HBaseConnector
from connectors.rdf_connector import RDFConnector

class NoSQLLLMInterface:
    """Main CLI interface for NoSQL-LLM system"""

    def __init__(self):
        self.console = Console()
        self.comparator = CrossDatabaseComparator()
        self.translator = QueryTranslator()
        self.executor = QueryExecutor()

        self.databases = ['mongodb', 'neo4j', 'redis', 'hbase', 'rdf']
        self.db_names = {
            'mongodb': 'MongoDB',
            'neo4j': 'Neo4j',
            'redis': 'Redis',
            'hbase': 'HBase',
            'rdf': 'RDF/SPARQL'
        }

    def show_welcome(self):
        """Display welcome message"""
        welcome_text = Text("🎬 NoSQL-LLM Query Interface", style="bold blue")
        subtitle = Text("Natural Language Queries Across Multiple NoSQL Databases", style="italic cyan")

        panel = Panel.fit(
            f"[bold blue]{welcome_text}[/bold blue]\n[italic cyan]{subtitle}[/italic cyan]\n\n"
            "Query MongoDB, Neo4j, Redis, HBase, and RDF stores using natural language!\n"
            "Supports cross-database comparison and schema exploration.",
            title="🚀 Welcome",
            border_style="blue"
        )

        self.console.print(panel)

    def show_menu(self) -> str:
        """Display main menu and get user choice"""
        menu_table = Table(title="📋 Main Menu", show_header=True, header_style="bold magenta")
        menu_table.add_column("Option", style="cyan", width=8)
        menu_table.add_column("Description", style="white")

        menu_table.add_row("1", "🔍 Single Database Query")
        menu_table.add_row("2", "🔄 Cross-Database Comparison")
        menu_table.add_row("3", "📊 Database Schema Explorer")
        menu_table.add_row("4", "⚡ Quick Test Queries")
        menu_table.add_row("5", "ℹ️  System Information")
        menu_table.add_row("0", "👋 Exit")

        self.console.print(menu_table)

        while True:
            choice = Prompt.ask("\n[bold green]Choose an option[/bold green]", choices=["0", "1", "2", "3", "4", "5"])
            return choice

    def select_databases(self, multi_select: bool = True) -> List[str]:
        """Let user select which databases to use"""
        if not multi_select:
            return [self._select_single_database()]

        self.console.print("\n[bold]Select databases to query:[/bold]")
        self.console.print("(Use space to select/deselect, Enter to confirm)")

        # Show database options
        db_options = []
        for i, (db_key, db_name) in enumerate(self.db_names.items(), 1):
            db_options.append(f"{i}. {db_name}")

        selected = self.console.input("Enter database numbers (e.g., 1 2 4): ").strip()

        if not selected:
            return self.databases  # Default to all

        try:
            indices = [int(x.strip()) - 1 for x in selected.split()]
            selected_dbs = [self.databases[i] for i in indices if 0 <= i < len(self.databases)]
            return selected_dbs if selected_dbs else self.databases
        except (ValueError, IndexError):
            self.console.print("[red]Invalid selection, using all databases[/red]")
            return self.databases

    def _select_single_database(self) -> str:
        """Select a single database"""
        self.console.print("\n[bold]Select database:[/bold]")

        for i, (db_key, db_name) in enumerate(self.db_names.items(), 1):
            self.console.print(f"{i}. {db_name}")

        while True:
            try:
                choice = IntPrompt.ask("Choose database", min_=1, max_=len(self.databases))
                return self.databases[choice - 1]
            except (ValueError, KeyboardInterrupt):
                continue

    def get_query_input(self) -> str:
        """Get natural language query from user"""
        self.console.print("\n[bold cyan]💬 Natural Language Query[/bold cyan]")
        self.console.print("Examples:")
        self.console.print("  • Find all movies from 2015")
        self.console.print("  • Show me action movies with rating above 8")
        self.console.print("  • Who directed The Dark Knight?")
        self.console.print("  • Find comedy movies from the 90s")

        query = Prompt.ask("\n[bold green]Enter your query[/bold green]")
        return query.strip()

    def run_single_database_query(self):
        """Run query on a single database"""
        self.console.print("\n[bold blue]🔍 Single Database Query[/bold blue]")

        # Select database
        db_key = self._select_single_database()
        db_name = self.db_names[db_key]

        # Get query
        query = self.get_query_input()
        if not query:
            return

        # Get schema
        with self.console.status(f"[bold green]Analyzing {db_name} schema...[/bold green]") as status:
            schema_context = self._get_schema_context(db_key)

        # Translate query
        with self.console.status(f"[bold green]Translating query for {db_name}...[/bold green]") as status:
            if db_key == 'mongodb':
                translated = self.translator.translate_to_mongodb(query, schema_context)
            elif db_key == 'neo4j':
                translated = self.translator.translate_to_neo4j(query, schema_context)
            elif db_key == 'redis':
                translated = self.translator.translate_to_redis(query, schema_context)
            elif db_key == 'hbase':
                translated = self.translator.translate_to_hbase(query, schema_context)
            elif db_key == 'rdf':
                translated = self.translator.translate_to_sparql(query, schema_context)

        # Execute query
        with self.console.status(f"[bold green]Executing query on {db_name}...[/bold green]") as status:
            if 'error' not in translated:
                if db_key == 'mongodb':
                    result = self.executor.execute_mongodb(translated)
                elif db_key == 'neo4j':
                    result = self.executor.execute_neo4j(translated)
                elif db_key == 'redis':
                    result = self.executor.execute_redis(translated)
                elif db_key == 'hbase':
                    result = self.executor.execute_hbase(translated)
                elif db_key == 'rdf':
                    result = self.executor.execute_sparql(translated)
            else:
                result = {'success': False, 'error': translated.get('error')}

        # Display results
        self._display_single_result(db_name, query, translated, result)

    def run_cross_database_comparison(self):
        """Run cross-database comparison"""
        self.console.print("\n[bold blue]🔄 Cross-Database Comparison[/bold blue]")

        # Select databases
        databases = self.select_databases(multi_select=True)

        # Get query
        query = self.get_query_input()
        if not query:
            return

        # Run comparison
        with self.console.status(f"[bold green]Comparing across {len(databases)} databases...[/bold green]") as status:
            results = self.comparator.compare_query(query, databases)

        # Display results
        self.comparator.print_comparison_report(results)

    def run_schema_explorer(self):
        """Run database schema exploration"""
        self.console.print("\n[bold blue]📊 Database Schema Explorer[/bold blue]")

        db_key = self._select_single_database()
        db_name = self.db_names[db_key]

        with self.console.status(f"[bold green]Exploring {db_name} schema...[/bold green]") as status:
            schema_info = self._get_detailed_schema(db_key)

        # Display schema
        self._display_schema_info(db_name, schema_info)

    def run_quick_tests(self):
        """Run quick test queries"""
        self.console.print("\n[bold blue]⚡ Quick Test Queries[/bold blue]")

        test_queries = [
            "Find all movies from 2015",
            "Show me action movies",
            "Find movies with rating above 8",
            "Who directed The Dark Knight?"
        ]

        # Select databases
        databases = self.select_databases(multi_select=True)

        for i, query in enumerate(test_queries, 1):
            self.console.print(f"\n[bold cyan]Test {i}/{len(test_queries)}: '{query}'[/bold cyan]")

            if not Confirm.ask("Run this test query?"):
                continue

            with self.console.status(f"[bold green]Testing across {len(databases)} databases...[/bold green]") as status:
                results = self.comparator.compare_query(query, databases)

            # Show summary
            summary = results.get('summary', {})
            success_rate = f"{summary.get('successful_executions', 0)}/{len(databases)}"
            self.console.print(f"✅ Success rate: {success_rate}")

            if not Confirm.ask("Show detailed results?"):
                continue

            self.comparator.print_comparison_report(results)

            if i < len(test_queries) and not Confirm.ask("Continue with next test?"):
                break

    def show_system_info(self):
        """Show system information"""
        self.console.print("\n[bold blue]ℹ️  System Information[/bold blue]")

        info_table = Table(title="System Status", show_header=True, header_style="bold magenta")
        info_table.add_column("Component", style="cyan")
        info_table.add_column("Status", style="green")
        info_table.add_column("Details", style="white")

        # Check database connections
        for db_key, db_name in self.db_names.items():
            try:
                if db_key == 'mongodb':
                    conn = MongoDBConnector()
                    status = "✅ Connected" if conn.connect() else "❌ Failed"
                    conn.disconnect()
                elif db_key == 'neo4j':
                    conn = Neo4jConnector()
                    status = "✅ Connected" if conn.connect() else "❌ Failed"
                    conn.disconnect()
                elif db_key == 'redis':
                    conn = RedisConnector()
                    status = "✅ Connected" if conn.connect() else "❌ Failed"
                    conn.disconnect()
                elif db_key == 'hbase':
                    conn = HBaseConnector()
                    status = "✅ Connected" if conn.connect() else "❌ Failed"
                    conn.disconnect()
                elif db_key == 'rdf':
                    conn = RDFConnector()
                    status = "✅ Connected" if conn.connect() else "❌ Failed"
                    conn.disconnect()

                info_table.add_row(db_name, status, f"Available for queries")

            except Exception as e:
                info_table.add_row(db_name, "❌ Error", str(e)[:50])

        # LLM status
        try:
            # Try a simple translation to test LLM
            test_result = self.translator.translate_to_mongodb("test", "")
            llm_status = "✅ Working" if 'error' not in test_result else "❌ Failed"
        except:
            llm_status = "❌ Failed"

        info_table.add_row("Google Gemini LLM", llm_status, "Query translation engine")

        self.console.print(info_table)

        # Show supported features
        features_table = Table(title="Supported Features", show_header=True, header_style="bold magenta")
        features_table.add_column("Feature", style="cyan")
        features_table.add_column("Description", style="white")

        features_table.add_row("Natural Language Queries", "Query databases using plain English")
        features_table.add_row("Cross-Database Comparison", "Compare same query across multiple databases")
        features_table.add_row("Schema Exploration", "Explore database structure and metadata")
        features_table.add_row("Query Translation", "LLM-powered conversion to database-specific queries")
        features_table.add_row("Multi-Database Support", "MongoDB, Neo4j, Redis, HBase, RDF/SPARQL")

        self.console.print("\n", features_table)

    def _get_schema_context(self, db_type: str) -> str:
        """Get schema context for LLM translation"""
        try:
            if db_type == 'mongodb':
                conn = MongoDBConnector()
                conn.connect()
                explorer = MongoDBSchemaExplorer(conn)
                schema = explorer.generate_llm_context('movies')
                conn.disconnect()
                return schema
            elif db_type == 'neo4j':
                conn = Neo4jConnector()
                conn.connect()
                explorer = Neo4jSchemaExplorer(conn)
                schema = explorer.generate_llm_context()
                conn.disconnect()
                return schema
            elif db_type == 'redis':
                conn = RedisConnector()
                conn.connect()
                explorer = RedisSchemaExplorer(conn)
                schema = explorer.generate_llm_context()
                conn.disconnect()
                return schema
            elif db_type == 'hbase':
                conn = HBaseConnector()
                conn.connect()
                explorer = HBaseSchemaExplorer(conn)
                schema = explorer.generate_llm_context()
                conn.disconnect()
                return schema
            elif db_type == 'rdf':
                conn = RDFConnector()
                conn.connect()
                explorer = RDFSchemaExplorer(conn)
                schema = explorer.generate_llm_context()
                conn.disconnect()
                return schema
        except Exception as e:
            return f"Schema information not available: {e}"

        return "Schema information not available"

    def _get_detailed_schema(self, db_type: str) -> dict:
        """Get detailed schema information for display"""
        try:
            if db_type == 'mongodb':
                conn = MongoDBConnector()
                conn.connect()
                explorer = MongoDBSchemaExplorer(conn)
                schema = explorer.analyze_field_types('movies', 100)
                conn.disconnect()
                return schema
            elif db_type == 'neo4j':
                conn = Neo4jConnector()
                conn.connect()
                explorer = Neo4jSchemaExplorer(conn)
                schema = explorer.get_database_schema()
                conn.disconnect()
                return schema
            elif db_type == 'redis':
                conn = RedisConnector()
                conn.connect()
                explorer = RedisSchemaExplorer(conn)
                schema = explorer.get_database_schema()
                conn.disconnect()
                return schema
            elif db_type == 'hbase':
                conn = HBaseConnector()
                conn.connect()
                explorer = HBaseSchemaExplorer(conn)
                schema = explorer.get_database_schema()
                conn.disconnect()
                return schema
            elif db_type == 'rdf':
                conn = RDFConnector()
                conn.connect()
                explorer = RDFSchemaExplorer(conn)
                schema = explorer.get_database_schema()
                conn.disconnect()
                return schema
        except Exception as e:
            return {"error": str(e)}

        return {"error": "Schema exploration not available"}

    def _display_single_result(self, db_name: str, query: str, translated: dict, result: dict):
        """Display results from single database query"""
        # Query info
        query_panel = Panel(
            f"[bold cyan]Query:[/bold cyan] {query}\n"
            f"[bold cyan]Database:[/bold cyan] {db_name}\n"
            f"[bold cyan]Translated:[/bold cyan] {translated.get('query', translated.get('cypher', translated.get('sparql', 'N/A')))[:100]}...",
            title="🔍 Query Details",
            border_style="blue"
        )
        self.console.print(query_panel)

        # Results
        if result.get('success'):
            success_panel = Panel(
                f"[green]✅ Success![/green]\n"
                f"Found [bold]{result.get('count', 0)}[/bold] results\n"
                f"Execution time: {result.get('execution_time', 'N/A')} seconds",
                title="📊 Results",
                border_style="green"
            )
            self.console.print(success_panel)

            # Show sample results
            if result.get('results'):
                results_table = Table(title="Sample Results", show_header=True, header_style="bold magenta")
                results_table.add_column("Result #", style="cyan", width=8)

                # Get first result to determine columns
                first_result = result['results'][0] if result['results'] else {}
                if isinstance(first_result, dict):
                    for key in first_result.keys():
                        results_table.add_column(key.title(), style="white", max_width=20)

                    for i, res in enumerate(result['results'][:5]):  # Show first 5 results
                        row_data = [str(i+1)]
                        if isinstance(res, dict):
                            for key in first_result.keys():
                                value = res.get(key, '')
                                if isinstance(value, (list, dict)):
                                    value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                                row_data.append(str(value)[:50])
                        else:
                            row_data.extend([''] * len(first_result.keys()))
                        results_table.add_row(*row_data)
                else:
                    results_table.add_column("Value", style="white")
                    for i, res in enumerate(result['results'][:5]):
                        results_table.add_row(str(i+1), str(res)[:100])

                self.console.print(results_table)
        else:
            error_panel = Panel(
                f"[red]❌ Failed[/red]\n"
                f"Error: {result.get('error', 'Unknown error')}",
                title="❌ Error",
                border_style="red"
            )
            self.console.print(error_panel)

    def _display_schema_info(self, db_name: str, schema_info: dict):
        """Display schema information"""
        if 'error' in schema_info:
            error_panel = Panel(
                f"[red]❌ Schema exploration failed[/red]\n"
                f"Error: {schema_info['error']}",
                title=f"📊 {db_name} Schema",
                border_style="red"
            )
            self.console.print(error_panel)
            return

        schema_panel = Panel(
            f"[green]✅ Schema exploration successful[/green]\n"
            f"Database: {db_name}\n"
            f"Details: {len(schema_info)} schema elements found",
            title=f"📊 {db_name} Schema",
            border_style="green"
        )
        self.console.print(schema_panel)

        # Show some schema details
        if schema_info:
            schema_table = Table(title="Schema Elements", show_header=True, header_style="bold magenta")
            schema_table.add_column("Element", style="cyan")
            schema_table.add_column("Type", style="white")
            schema_table.add_column("Details", style="white", max_width=50)

            count = 0
            for key, value in schema_info.items():
                if count >= 10:  # Limit to 10 entries
                    break
                if isinstance(value, dict):
                    schema_table.add_row(key, "Object", f"{len(value)} properties")
                elif isinstance(value, list):
                    schema_table.add_row(key, "Array", f"{len(value)} items")
                else:
                    schema_table.add_row(key, str(type(value).__name__), str(value)[:50])
                count += 1

            self.console.print(schema_table)

    def run(self):
        """Main application loop"""
        self.show_welcome()

        while True:
            try:
                choice = self.show_menu()

                if choice == "0":
                    self.console.print("\n[bold blue]👋 Goodbye! Thanks for using NoSQL-LLM Interface![/bold blue]")
                    break
                elif choice == "1":
                    self.run_single_database_query()
                elif choice == "2":
                    self.run_cross_database_comparison()
                elif choice == "3":
                    self.run_schema_explorer()
                elif choice == "4":
                    self.run_quick_tests()
                elif choice == "5":
                    self.show_system_info()

                # Pause before showing menu again
                if choice != "0":
                    self.console.input("\n[bold green]Press Enter to continue...[/bold green]")

            except KeyboardInterrupt:
                self.console.print("\n[bold yellow]⚠️  Interrupted by user[/bold yellow]")
                break
            except Exception as e:
                self.console.print(f"\n[bold red]❌ Error: {e}[/bold red]")
                continue


def main():
    """Main entry point"""
    try:
        interface = NoSQLLLMInterface()
        interface.run()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()