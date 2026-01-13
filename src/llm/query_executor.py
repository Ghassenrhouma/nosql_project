"""
Query Executor
Executes translated queries on appropriate databases
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, List, Any
from connectors.mongodb_connector import MongoDBConnector
from connectors.neo4j_connector import Neo4jConnector
from connectors.redis_connector import RedisConnector
from connectors.rdf_connector import RDFConnector
from connectors.hbase_connector import HBaseConnector
from utils.logger import setup_logger

class QueryExecutor:
    """Executes database queries"""
    
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.connectors = {}
    
    def execute_mongodb(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute MongoDB query"""
        try:
            if 'mongodb' not in self.connectors:
                self.connectors['mongodb'] = MongoDBConnector()
                self.connectors['mongodb'].connect()
            
            conn = self.connectors['mongodb']
            collection = query_dict.get('collection')
            operation = query_dict.get('operation', 'find')
            
            if operation == 'find':
                results = conn.find_many(
                    collection,
                    query_dict.get('query', {}),
                    projection=query_dict.get('projection'),
                    limit=query_dict.get('limit', 10),
                    sort=query_dict.get('sort')
                )
            elif operation == 'aggregate':
                results = conn.aggregate(collection, query_dict.get('pipeline', []))
            elif operation == 'count':
                count = conn.count_documents(collection, query_dict.get('query', {}))
                results = [{'count': count}]
            else:
                results = []
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"MongoDB execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_neo4j(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Neo4j query"""
        try:
            if 'neo4j' not in self.connectors:
                self.connectors['neo4j'] = Neo4jConnector()
                self.connectors['neo4j'].connect()
            
            conn = self.connectors['neo4j']
            cypher = query_dict.get('cypher')
            parameters = query_dict.get('parameters', {})
            
            results = conn.execute_query(cypher, parameters)
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"Neo4j execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_redis(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Redis commands"""
        try:
            if 'redis' not in self.connectors:
                self.connectors['redis'] = RedisConnector()
                self.connectors['redis'].connect()
            
            conn = self.connectors['redis']
            commands = query_dict.get('commands', [])
            results = []
            
            for cmd in commands:
                command = cmd.get('command')
                args = cmd.get('args', [])
                
                if command == 'GET':
                    result = conn.get(args[0])
                elif command == 'HGETALL':
                    result = conn.hgetall(args[0])
                elif command == 'ZREVRANGE':
                    withscores = 'WITHSCORES' in [a.upper() for a in args]
                    result = conn.zrevrange(args[0], int(args[1]), int(args[2]), withscores=withscores)
                elif command == 'ZRANGE':
                    withscores = 'WITHSCORES' in [a.upper() for a in args]
                    result = conn.zrange(args[0], int(args[1]), int(args[2]), withscores=withscores)
                elif command == 'SMEMBERS':
                    result = list(conn.smembers(args[0]))
                elif command == 'LRANGE':
                    result = conn.lrange(args[0], int(args[1]), int(args[2]))
                else:
                    result = f"Unknown command: {command}"
                
                results.append({
                    'command': f"{command} {' '.join(map(str, args))}",
                    'result': result
                })
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"Redis execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_sparql(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SPARQL query"""
        try:
            if 'rdf' not in self.connectors:
                self.connectors['rdf'] = RDFConnector()
                self.connectors['rdf'].connect()
            
            conn = self.connectors['rdf']
            sparql = query_dict.get('sparql')
            
            results = conn.execute_query(sparql)
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"SPARQL execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def execute_hbase(self, query_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Execute HBase operation"""
        try:
            if 'hbase' not in self.connectors:
                self.connectors['hbase'] = HBaseConnector()
                self.connectors['hbase'].connect()
            
            conn = self.connectors['hbase']
            operation = query_dict.get('operation', 'scan')
            table = query_dict.get('table')
            
            if operation == 'get':
                row_key = query_dict.get('row_key')
                columns = query_dict.get('columns')
                result = conn.get(table, row_key, columns)
                results = [{'row_key': row_key, 'data': result}] if result else []
            
            elif operation == 'scan':
                columns = query_dict.get('columns')
                limit = query_dict.get('limit', 10)
                results = conn.scan(table, columns=columns, limit=limit)
            
            else:
                results = []
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            self.logger.error(f"HBase execution error: {e}")
            return {'success': False, 'error': str(e)}
    
    def close_all(self):
        """Close all database connections"""
        for name, conn in self.connectors.items():
            try:
                conn.disconnect()
            except:
                pass