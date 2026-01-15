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

### ✅ Core Features
- **Natural Language Query Translation** - Convert plain English to database-specific queries using Google Gemini LLM
- **Cross-Database Comparison** - Execute the same query across all 5 databases simultaneously with performance metrics
- **Schema Exploration** - Interactive exploration of database structure, collections, and metadata
- **Multi-Database Support** - Full support for MongoDB, Neo4j, Redis, HBase, and RDF/SPARQL databases
- **Dual Interfaces** - Both CLI (Rich library) and modern Streamlit web application
- **Query Validation** - LLM-powered query validation with helpful error messages and suggestions

### 🔍 Supported Query Types
- **Filter Operations**: `filter_by_genre`, `filter_by_year`, `filter_by_director`, `filter_by_cast`, `filter_by_multiple`
- **CRUD Operations**: Create, Read, Update, Delete operations across all databases
- **Complex Queries**: Multi-condition filtering, case-insensitive text matching, aggregation queries
- **Schema Queries**: Database structure exploration, metadata retrieval, relationship analysis

### 🗄️ Database-Specific Capabilities

#### MongoDB
- Document-based queries with aggregation pipelines
- Nested object queries (e.g., `imdb.rating`)
- Full CRUD operations with document validation
- Schema exploration with collection statistics

#### Neo4j
- Graph-based queries with Cypher generation
- Relationship traversal and pattern matching
- Case-insensitive text matching with `toLower()` and `CONTAINS`
- Node and relationship CRUD operations

#### Redis
- Key-value operations with advanced data structures
- Hash, Set, Sorted Set, and String operations
- JSON document storage and retrieval
- Efficient key-based filtering and searching

#### HBase
- Wide-column architecture with row-key based queries
- Column family organization and scanning
- Efficient range queries and pagination
- Table and column family management

#### RDF/SPARQL
- Semantic web queries with SPARQL generation
- Ontology-based data modeling with custom namespaces
- Complex graph pattern matching
- Triple store operations with inference support

## 📖 Usage Examples

### 🌐 Web Interface (Streamlit)
The modern web interface provides intuitive access to all features:

- **🏠 Home Dashboard** - Overview with quick start examples and system status
- **🔍 Single Database Query** - Query individual databases with visual results and generated query display
- **🔄 Cross-Database Comparison** - Side-by-side comparison across all databases with performance metrics
- **📊 Schema Explorer** - Interactive database structure exploration with sample data
- **⚙️ System Status** - Real-time database connectivity monitoring and health checks
- **📱 Responsive Design** - Optimized for desktop and mobile devices

### CLI Interface
The interactive CLI provides comprehensive options:

1. **Single Database Query** - Query one database at a time with detailed results
2. **Cross-Database Comparison** - Compare queries across multiple databases simultaneously
3. **Schema Explorer** - Explore database structure and metadata
4. **Quick Tests** - Run predefined test queries for validation
5. **System Info** - Check database connectivity and configuration

### Example Queries

#### Filter Operations
```
"show me films from 1914"
"find drama movies"
"movies directed by Hitchcock"
"films starring Marlon Brando"
"show me action movies from 1990 directed by Spielberg"
```

#### CRUD Operations
```
"add film called 'Inception' from 2010"
"find movie with title 'The Matrix'"
"update the rating of 'Inception' to 9.0"
"delete movie 'Test Film'"
```

#### Schema Exploration
```
"what collections exist in the database?"
"show me the structure of movies collection"
"what are the available genres?"
"describe the movie schema"
```

### Data Loading
The project includes comprehensive data loading scripts for all databases:

```bash
# Load sample movie data into all databases
python data/load_mongodb_movies.py  # 23,539 movies from MongoDB sample
python data/load_redis_movies.py    # 100 movies for Redis
python data/load_neo4j_movies.py    # 100 movies for Neo4j
python data/load_hbase_movies.py    # 100 movies for HBase
python data/load_rdfs_movies.py     # 100 movies for RDF/SPARQL
```

**Note**: MongoDB uses the official MongoDB sample dataset, while other databases use a subset of 100 movies for demonstration purposes.

## 🏗️ Project Structure

```
nosql-llm-project/
├── docker-compose.yml              # Multi-database Docker setup
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── nosql_llm_streamlit.py          # Main Streamlit web interface
├── nosql_llm_cli.py               # Main CLI interface
├── run_cli.bat                    # Windows CLI launcher
├── run_streamlit.bat              # Windows web launcher
├── run_cli.sh                     # Linux/Mac CLI launcher
├── run_streamlit.sh               # Linux/Mac web launcher
├── docker-data/                   # Docker volume data
│   └── mongodb-samples/
│       └── movies.json            # Sample movie dataset
├── data/                          # Data loading scripts
│   ├── load_hbase_movies.py       # Load movies into HBase
│   ├── load_hdfs_movies.py        # Load movies into HDFS
│   ├── load_neo4j_movies.py       # Load movies into Neo4j
│   ├── load_rdfs_movies.py        # Load movies into RDF/SPARQL
│   └── load_redis_movies.py       # Load movies into Redis
├── src/                           # Source code
│   ├── __init__.py
│   ├── explore_mflix.py           # MongoDB exploration utilities
│   ├── config/                    # Configuration management
│   │   ├── __init__.py
│   │   ├── database_config.py     # Database connection configs
│   │   └── __pycache__/
│   ├── connectors/                # Database connection handlers
│   │   ├── __init__.py
│   │   ├── hbase_connector.py     # HBase connection & operations
│   │   ├── mongodb_connector.py   # MongoDB connection & operations
│   │   ├── neo4j_connector.py     # Neo4j connection & operations
│   │   ├── rdf_connector.py       # RDF/SPARQL connection & operations
│   │   ├── redis_connector.py     # Redis connection & operations
│   │   └── __pycache__/
│   ├── llm/                       # LLM-powered query processing
│   │   ├── query_executor.py      # Execute queries across all databases
│   │   ├── query_translator.py    # Translate NL to database queries
│   │   └── __pycache__/
│   ├── schema/                    # Database schema exploration
│   │   ├── hbase_schema_explorer.py   # HBase schema analysis
│   │   ├── mongodb_schema_explorer.py # MongoDB schema analysis
│   │   ├── neo4j_schema_explorer.py   # Neo4j schema analysis
│   │   ├── rdf_schema_explorer.py     # RDF schema analysis
│   │   ├── redis_schema_explorer.py   # Redis schema analysis
│   │   └── __pycache__/
│   └── utils/                     # Utility functions
│       ├── __init__.py
│       ├── logger.py              # Centralized logging
│       └── __pycache__/
└── tests/                         # Test suites
    ├── test_hbase_schema.py       # HBase schema tests
    ├── test_hbase.py              # HBase functionality tests
    ├── test_mongodb_schema.py     # MongoDB schema tests
    ├── test_mongodb.py            # MongoDB functionality tests
    ├── test_neo4j_schema.py       # Neo4j schema tests
    ├── test_neo4j.py              # Neo4j functionality tests
    ├── test_nlp_system.py         # NLP system tests
    ├── test_rdf_schema.py         # RDF schema tests
    ├── test_rdf.py                # RDF functionality tests
    ├── test_redis_schema.py       # Redis schema tests
    └── test_redis.py              # Redis functionality tests
```

## 🔧 Development

### Architecture Overview

The system follows a modular architecture with clear separation of concerns:

- **LLM Layer** (`src/llm/`): Handles natural language processing and query translation
  - `query_translator.py`: Converts natural language to database-specific operations
  - `query_executor.py`: Executes translated queries across all database types

- **Database Layer** (`src/connectors/`): Database-specific connection and operation handling
  - Individual connector classes for each database type
  - Unified interface for CRUD operations and query execution

- **Schema Layer** (`src/schema/`): Database structure analysis and exploration
  - Schema explorers for metadata extraction
  - Cross-database schema comparison capabilities

- **Configuration Layer** (`src/config/`): Centralized configuration management
  - Database connection parameters
  - Environment-specific settings

- **Utilities** (`src/utils/`): Shared functionality
  - Logging system with structured output
  - Common helper functions

### Running Tests
```bash
# Run all tests
pytest

# Run specific database tests
pytest tests/test_mongodb.py
pytest tests/test_neo4j.py
pytest tests/test_redis.py
pytest tests/test_hbase.py
pytest tests/test_rdf.py

# Run schema tests
pytest tests/test_*_schema.py

# Run NLP system tests
pytest tests/test_nlp_system.py

# Run with verbose output
pytest -v
```

### Adding New Databases
1. **Create Database Connector** (`src/connectors/newdb_connector.py`)
   - Implement connection management
   - Define CRUD operations interface
   - Handle database-specific error types

2. **Create Schema Explorer** (`src/schema/newdb_schema_explorer.py`)
   - Implement metadata extraction
   - Define schema analysis methods
   - Handle database-specific schema structures

3. **Update Query Translator** (`src/llm/query_translator.py`)
   - Add translation method for new database
   - Define operation mappings
   - Include database-specific query patterns

4. **Update Query Executor** (`src/llm/query_executor.py`)
   - Add execution method for new database
   - Implement operation handlers
   - Return standardized result format

5. **Update Comparator** (`src/cross_db_comparator.py`)
   - Add new database to comparison logic
   - Update result aggregation
   - Handle database-specific metrics

6. **Add Tests** (`tests/`)
   - Create comprehensive test suite
   - Include schema and functionality tests
   - Add integration tests

7. **Update Configuration** (`src/config/database_config.py`)
   - Add connection parameters
   - Define environment variables
   - Update Docker Compose setup

### Environment Variables
Create a `.env` file in the project root:
```
GEMINI_API_KEY=your_gemini_api_key_here
MONGODB_URI=mongodb://localhost:27017
NEO4J_URI=bolt://localhost:7687
REDIS_URL=redis://localhost:6379
HBASE_HOST=localhost
HBASE_PORT=9090
RDF_ENDPOINT=http://localhost:3030/movies
```

## 📊 Project Status

This project implements all core requirements from the academic specification with production-ready features:

### ✅ Completed Features
- **Natural Language Query Translation** - Advanced LLM-powered translation with context awareness
- **Database Schema Exploration** - Comprehensive metadata extraction and visualization
- **Query Validation and Explanation** - Intelligent error handling with actionable suggestions
- **Cross-Database Comparison** - Real-time performance comparison with detailed metrics
- **Multi-Database Support** - Full implementation for 5 NoSQL databases
- **Dual Interface Support** - Both CLI and modern web interfaces
- **Comprehensive Testing** - Extensive test coverage for all components
- **Docker Integration** - Complete containerized deployment
- **Production Logging** - Structured logging with configurable levels

### 🔧 Technical Implementation
- **LLM Integration**: Google Gemini 2.0 Flash with optimized prompting
- **Query Types**: Filter operations, CRUD operations, complex multi-condition queries
- **Error Handling**: Graceful degradation with detailed error reporting
- **Performance**: Optimized query execution with connection pooling
- **Security**: Environment-based configuration with secure credential management
- **Scalability**: Modular architecture supporting additional databases

### 📈 Database Support Matrix

| Feature | MongoDB | Neo4j | Redis | HBase | RDF/SPARQL |
|---------|---------|-------|-------|-------|------------|
| Filter by Genre | ✅ | ✅ | ✅ | ✅ | ✅ |
| Filter by Year | ✅ | ✅ | ✅ | ✅ | ✅ |
| Filter by Director | ✅ | ✅ | ✅ | ✅ | ✅ |
| Filter by Cast | ✅ | ✅ | ✅ | ✅ | ✅ |
| Multi-Condition Filter | ✅ | ✅ | ✅ | ✅ | ✅ |
| Create Operations | ✅ | ✅ | ✅ | ✅ | ✅ |
| Read Operations | ✅ | ✅ | ✅ | ✅ | ✅ |
| Update Operations | ✅ | ✅ | ✅ | ✅ | ✅ |
| Delete Operations | ✅ | ✅ | ✅ | ✅ | ✅ |
| Schema Exploration | ✅ | ✅ | ✅ | ✅ | ✅ |
| Query Performance | ✅ | ✅ | ✅ | ✅ | ✅ |

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

1. **Fork the repository** and create a feature branch
2. **Add comprehensive tests** for new functionality
3. **Follow the existing code style** and architecture patterns
4. **Update documentation** including README and docstrings
5. **Ensure all tests pass** before submitting
6. **Submit a pull request** with a clear description of changes

### Development Setup
```bash
# Clone and setup
git clone <repository>
cd nosql-llm-project
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your API keys and database credentials

# Start databases
docker-compose up -d

# Load test data
python data/load_*_movies.py

# Run tests
pytest

# Start development
streamlit run nosql_llm_streamlit.py
```

## 📄 License

This project is developed as part of an academic assignment and is available for educational and research purposes.

## 🙏 Acknowledgments

- **Google Gemini** for providing powerful LLM capabilities
- **MongoDB, Neo4j, Redis, HBase, and Apache Jena** communities for excellent database technologies
- **Streamlit** for the modern web interface framework
- **Rich** library for beautiful CLI interface components
- **Docker** for containerization and easy deployment
- **pytest** for comprehensive testing framework
- **Academic supervisors** for guidance and requirements specification