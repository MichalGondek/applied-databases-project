# Import Neo4j graph database driver
from neo4j import GraphDatabase

# Import Neo4j configuration settings from config.py
from config import NEO4J_CONFIG

# Creates and returns a Neo4j database driver connection
def get_neo4j_driver():
    return GraphDatabase.driver(
        NEO4J_CONFIG['uri'],
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )

# Checks if an attendee node exists in the Neo4j database
def neo4j_attendee_node_exists(tx, attendee_id):
    query = """
        MATCH (a:Attendee {AttendeeID: $attendee_id})
        RETURN a
        """
    result = tx.run(query, attendee_id=attendee_id)
    return result.single() is not None

# Retrives attednee connections from Neo4j
def neo4j_get_connections(tx, attendee_id):
    query = """
        MATCH (a:Attendee {AttendeeID: $attendee_id})-[:CONNECTED_TO]-(b:Attendee)
        RETURN b.AttendeeID AS connected_id
        ORDER BY connected_id
    """
    result = tx.run(query, attendee_id=attendee_id)

    # Returns list of connected attendee IDs
    return [record['connected_id'] for record in result]

# Checks if two attednees are already connected
def neo4j_connection_exists(tx, attendee_id_1, attendee_id_2):
    query = """
        MATCH (a:Attendee {AttendeeID: $id1})-[:CONNECTED_TO]-(b:Attendee {AttendeeID: $id2})
        RETURN a
        """
    
    # Return True if connection already exists
    result = tx.run(query, id1=attendee_id_1, id2=attendee_id_2)
    return result.single() is not None

# Creates attednee node if it does not already exist
def neo4j_create_attendee_node(tx, attendee_id):
    query = """
        MERGE (:Attendee {AttendeeID: $attendee_id})
        """
    tx.run(query, attendee_id=attendee_id)

# Creates CONNECTED_TO relationships between attendees
def neo4j_create_connection(tx, attendee_id_1, attendee_id_2):
    query = """
        MATCH (a:Attendee {AttendeeID: $id1}), (b:Attendee {AttendeeID: $id2})
        MERGE (a)-[:CONNECTED_TO]->(b)
        """
    tx.run(query, id1=attendee_id_1, id2=attendee_id_2)

# Retrieves connected attendee IDs using Neo4j session    
def get_connected_attendee_ids(driver, database, attendee_id):

    # Opens Neo4j database session
    with driver.session(database=database) as session:
        node_exists = session.execute_read(neo4j_attendee_node_exists, attendee_id)

        if not node_exists:
            return None 
            
        return session.execute_read(neo4j_get_connections, attendee_id)

# Creates attendee connection in Neo4j           
def create_attendee_connection(driver, database, attendee_id_1, attendee_id_2):
    with driver.session(database=database) as session:
        already_connected = session.execute_read(
            neo4j_connection_exists,
            attendee_id_1,
            attendee_id_2
        )
        if already_connected:
            return False
        
        # Creates attendee nodes if they do not exist
        session.execute_write(neo4j_create_attendee_node, attendee_id_1)
        session.execute_write(neo4j_create_attendee_node, attendee_id_2)

        # Creates CONNECTED_TO relationship
        session.execute_write(neo4j_create_connection, attendee_id_1, attendee_id_2)
        return True 
    