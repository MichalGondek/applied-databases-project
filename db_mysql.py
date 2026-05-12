# Import MySQL connector library
import mysql.connector

# Imports MySQL database error handling
from mysql.connector import Error

# Imports MySQL configuration settings from config.py
from config import MYSQL_CONFIG

# Creates and returns a MySQL database connection
def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)

# Retrieves speaker session using a partial speaker name search
def get_speakers_sessions(conn, search_text):

    # SQL query joins session and room tables
    query = """
        SELECT s.speakerName, s.sessionTitle, r.roomName
        FROM session s
        JOIN room r ON s.roomID = r.roomID
        WHERE s.speakerName LIKE %s
        ORDER BY s.speakerName, s.sessionTitle
    """

    # Creates a cursor, executes the query with the search text, and fetches all results
    cursor = conn.cursor()
    cursor.execute(query, (f'%{search_text}%',))
    rows = cursor.fetchall()
    cursor.close()
    return rows

# Retrieves company by company ID
def get_company_by_id(conn, company_id):
    query = 'SELECT companyID, companyName FROM company WHERE companyID = %s'
    cursor = conn.cursor()
    cursor.execute(query, (company_id,))

    # Retrieves one mathching company record
    row = cursor.fetchone()
    cursor.close()
    return row

# Retrieves all attendees linked to a company
def get_attendees_by_company(conn, company_id):

    # SQL query joins attendee, registration, session and room tables
    query = """
        SELECT a.attendeeName,
               a.attendeeDOB,
               s.sessionTitle,
               s.speakerName,
               r.roomName
        FROM attendee a
        JOIN registration reg ON a.attendeeID = reg.attendeeID
        JOIN session s ON reg.sessionID = s.sessionID
        JOIN room r ON s.roomID = r.roomID
        WHERE a.attendeeCompanyID = %s
        ORDER BY a.attendeeName, s.sessionTitle
    """
    cursor = conn.cursor()
    cursor.execute(query, (company_id,))
    rows = cursor.fetchall()
    cursor.close()
    return rows

# Checks if an attendee exists in the database
def attendee_exists(conn, attendee_id):
    query = 'SELECT attendeeID, attendeeName FROM attendee WHERE attendeeID = %s'
    cursor = conn.cursor()
    cursor.execute(query, (attendee_id,))

    # Retrives matching attendee
    row = cursor.fetchone()
    cursor.close()
    return row

# Retrieves an attendee's name by ID
def get_attendee_name(conn, attendee_id):

    # SQL query to get attendee name
    query = 'SELECT attendeeName FROM attendee WHERE attendeeID = %s'
    cursor = conn.cursor()
    cursor.execute(query, (attendee_id,))
    row = cursor.fetchone()
    cursor.close()

    # Returns attendee name if found
    return row [0] if row else None

# Adds a new attendee into the attendee table
def add_attendee(conn, attendee_id, name, dob, gender, company_id):
 
    # SQL INSERT statement
    query = """
        INSERT INTO attendee
        (attendeeID, attendeeName, attendeeDOB, attendeeGender, attendeeCompanyID)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor = conn.cursor()

    # Executes insert query
    cursor.execute(query, (attendee_id, name, dob, gender, company_id))

    # Saves changes to database
    conn.commit()
    cursor.close()

# Retrieves all rooms from the room table
def get_rooms(conn):
    query = """
        SELECT roomID, roomName, capacity
        FROM room
        ORDER BY roomID
        """
    cursor = conn.cursor()
    cursor.execute(query)

    # Retrieves all room records
    rows = cursor.fetchall()
    cursor.close()
    return rows