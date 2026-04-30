import mysql.connector
from mysql.connector import Error
from config import MYSQL_CONFIG

def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)

def get_speakers_sessions(conn, search_text):
    query = """
        SELECT s.speakerName, s.sessionTitle, r.roomName
        FROM session s
        JOIN room r ON s.roomID = r.roomID
        WHERE s.speakerName LIKE %s
        ORDER BY s.speakerName, s.sessionTitle
    """
    cursor = conn.cursor()
    cursor.execute(query, (f'%{search_text}%',))
    rows = cursor.fetchall()
    cursor.close()
    return rows

def get_company_by_id(conn, company_id):
    query = 'SELECT companyID, companyName FROM company WHERE companyID = %s'
    cursor = conn.cursor()
    cursor.execute(query, (company_id,))
    row = cursor.fetchone()
    cursor.close()
    return row

def get_attendees_by_company(conn, company_id):
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

def attendee_exists(conn, attendee_id):
    query = 'SELECT attendeeID, attendeeName FROM attendee WHERE attendeeID = %s'
    cursor = conn.cursor()
    cursor.execute(query, (attendee_id,))
    row = cursor.fetchone()
    cursor.close()
    return row

def get_attendee_name(conn, attendee_id):
    query = 'SELECTE attendeeName FROM attendee WHERE attendeeID = %s'
    cursor = conn.cursor()
    cursor.execute(query, (attendee_id,))
    row = cursor.fetchone()
    cursor.close()
    return row [0] if row else None

def add_attendee(conn, attendee_id, name, dob, gender, company_id):
    query = """
        INSERT INTO attendee
        (attendeeID, attendeeName, attendeeDOB, attendeeGender, attendeeCompanyID)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor = conn.cursor()
    cursor.execute(query, (attendee_id, name, dob, gender, company_id))
    conn.commit()
    cursor.close()

def get_rooms(conn):
    query = """
        SELECT roomID, roomName, capacity
        FROM room
        ORDER BY roomID
        """
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    return rows