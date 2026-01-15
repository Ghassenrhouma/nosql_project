# NoSQL-LLM Project

A comprehensive system for natural language querying across multiple NoSQL databases using Large Language Models (LLMs).

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- Google Gemini API key

### Setup
1. **Clone and setup environment:**
   ```bash
   git clone <repository>
   cd nosql-llm-project
   pip install -r requirements.txt
   ```

2. **Set up environment variables:**
   Create a `.env` file with:
   ```
   GEMINI_API_KEY=your_api_key_here
   ```

3. **Start databases:**
   ```bash
   docker-compose up -d
   ```

4. **Load sample data:**
   ```bash
   python data/load_mongodb_movies.py  # (if needed)
   python data/load_redis_movies.py
   python data/load_neo4j_movies.py
   python data/load_hbase_movies.py
   python data/load_rdfs_movies.py
   ```

5. **Launch the interfaces:**
   ```bash
   # CLI Interface
   python nosql_llm_cli.py
   
   # Web Interface (Streamlit)
   streamlit run nosql_llm_streamlit.py
   
   # Or use launchers
   run_cli.bat          # Windows CLI
   run_streamlit.bat    # Windows Web
   ./run_cli.sh         # Linux/Mac CLI
   ./run_streamlit.sh   # Linux/Mac Web
   ```
   
   **Note:** The web interface will be available at `http://localhost:8501` or `http://127.0.0.1:8501`

## 🎯 Features

### ✅ Completed Features
- **Natural Language Query Translation** - Convert plain English to database-specific queries
- **Cross-Database Comparison** - Compare the same query across MongoDB, Neo4j, Redis, HBase, and RDF/SPARQL
- **Schema Exploration** - Explore database structure and metadata
- **Multi-Database Support** - Full support for 5 NoSQL databases
- **CLI Interface** - User-friendly command-line interface with Rich library
- **Web Interface** - Modern Streamlit web application
- **Query Validation** - LLM-powered query validation and error handling

### 🗄️ Supported Databases
- **MongoDB** - Document store with aggregation pipelines
- **Neo4j** - Graph database with Cypher queries
- **Redis** - Key-value store with advanced data structures
- **HBase** - Column-family store with wide-column architecture
- **Apache Jena Fuseki** - RDF/SPARQL triple store

## 📖 Usage Examples

### CLI Interface
The interactive CLI provides several options:

1. **Single Database Query** - Query one database at a time
2. **Cross-Database Comparison** - Compare queries across multiple databases
3. **Schema Explorer** - Explore database structure
4. **Quick Tests** - Run predefined test queries
5. **System Info** - Check database connectivity

### 🌐 Web Interface (Streamlit)
The modern web interface provides:
- **🏠 Home Dashboard** - Overview and quick examples
- **🔍 Single Database Query** - Query individual databases with visual results
- **🔄 Cross-Database Comparison** - Side-by-side comparison across all databases
- **📊 Schema Explorer** - Interactive database structure exploration
- **⚙️ System Status** - Real-time database connectivity monitoring
- **📱 Responsive Design** - Works on desktop and mobile devices

## 🏗️ Architecture

```
├── src/
│   ├── connectors/          # Database connection handlers
│   ├── llm/                 # Query translation and execution
│   ├── schema/              # Database schema explorers
│   └── cross_db_comparator.py # Cross-database comparison
├── data/                    # Data loading scripts
├── tests/                   # Test suites
└── nosql_llm_cli.py         # Main CLI interface
```

## 🔧 Development

### Running Tests
```bash
# Run all tests
pytest

# Run specific test
python tests/test_cross_db_comparator.py

# Run CLI interface
python nosql_llm_cli.py
```

### Adding New Databases
1. Create connector in `src/connectors/`
2. Create schema explorer in `src/schema/`
3. Add translation method in `QueryTranslator`
4. Add execution method in `QueryExecutor`
5. Update `CrossDatabaseComparator`

## 📊 Project Status

This project implements all core requirements from the academic specification:

- ✅ Natural Language Query Translation
- ✅ Database Schema Exploration
- ✅ Query Validation and Explanation
- ✅ Cross-Database Comparison
- ✅ Multi-Database Support (5 databases)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is developed as part of an academic assignment.

## 🙏 Acknowledgments

- Google Gemini for LLM capabilities
- MongoDB, Neo4j, Redis, HBase, and Apache Jena communities
- Rich library for beautiful CLI interface