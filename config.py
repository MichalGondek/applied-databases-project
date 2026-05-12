# Python config.py
# Stores databse connection settings for MySQL and Neo4j

# MySQL database configuration 
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'YOUR_PASSWORD',
    'database': 'appdbproj'
}

# Neo4j database configuration
NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j',
    'password': 'YOUR_PASSWORD',
    'database': 'neo4j'
}