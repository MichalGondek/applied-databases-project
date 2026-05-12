# Applied DataBases Project
# Author: Michal Gondek

# Import MqSQL error handling
from mysql.connector import Error

# Import Neo4j database settings from config file
from config import NEO4J_CONFIG

# Import MySQL functions used by the menu options
from db_mysql import (
    get_mysql_connection,
    get_speakers_sessions,
    get_company_by_id,
    get_attendees_by_company,
    attendee_exists,
    get_attendee_name,
    add_attendee,
    get_rooms,      
)

# Import Neo4j functions used for attendee connections
from db_neo4j import (
    get_neo4j_driver,
    get_connected_attendee_ids,
    create_attendee_connection,
)

# Displays the main application menu
def print_menu():
    print('\n==== Conference Application ====')
    print('1. View Speaker Sessions')
    print('2. View Company Attendees')
    print('3. Add New Attendee')
    print('4. View Connected Attendees')
    print('5. Add Attendee Connection')
    print('6. View Rooms')
    print('x. Exit')

# Checks that user input is a positive number
def is_positive_int(value):
    return value.isdigit() and int(value) > 0

# Option 1: Seatch for a speaker and display their sessions
def option_1_view_speaker(mysql_conn):
    speaker_search = input('Enter Speaker Name (partial or full): ').strip()
    rows = get_speakers_sessions(mysql_conn, speaker_search)

    if not rows:
        print('No speakers match the search criteria.')
        return
    
    print('\nSpeaker Sessions')
    print('--' * 50)

    # Displays each matching speaker, sesssion and room
    for speaker_name, session_title,room_name in rows:
        print(f'Speaker: {speaker_name}')
        print(f'  Session: {session_title}')
        print(f'  Room: {room_name}')
        print('--' * 50)
    
# Option 2: View attendees linked to a selected company   
def option_2_view_attendees_by_company(mysql_conn):
    while True:
        company_id_input = input('Enter Company ID: ').strip()

        # Validates company ID input
        if not is_positive_int(company_id_input):
            print('Invalid company ID, Enter a number greater than 0.')
            continue


        company_id = int(company_id_input)

        # Checks if company exists and retrieves company name
        company = get_company_by_id(mysql_conn, company_id)

        if not company:
            print(f'No company found with ID {company_id}. Please try again.')
            continue
        
        # Retrieves attendees for the selected company
        rows = get_attendees_by_company(mysql_conn, company_id)

        if not rows:
            print(f'Company: {company[1]}')
            print('This company exists but has no attendees for any sessions.')
            continue

        print(f'\nCompany: {company[1]}')
        print('-' * 60)

        # Displays attendee and session details
        for attendee_name, attendee_dob, session_title, speaker_name, room_name in rows:
            print(f'Attendee: {attendee_name}')
            print(f'DOB: {attendee_dob}')
            print(f'  Session: {session_title}')
            print(f'  Speaker: {speaker_name}')
            print(f'  Room: {room_name}')
            print('-' * 60)
        break

# Option 3: Add a new attendee to the MySQL database
def option_3_add_attendee(mysql_conn):
    try:
        attendee_id_input = input('Enter Attendee ID:').strip()
        attendee_name = input('Enter Attendee Name: ').strip()
        attendee_dob = input('Enter Attendee DOB (YYYY-MM-DD): ').strip()
        attendee_gender = input('Enter gender (Male/Female): ').strip()
        attendee_company_id_input = input('Enter company ID:').strip()

        # Validate attendee ID
        if not is_positive_int(attendee_id_input):
            print('Invalid attendee ID.')
            return
        
        # Validate gender input
        if attendee_gender not in ('Male', 'Female'):
            print('Invalid gender.')
            return
        
        # Validate company ID
        if not is_positive_int(attendee_company_id_input):
            print('Invalid company ID.')        
            return
        
        attendee_id = int(attendee_id_input)
        attendee_company_id = int(attendee_company_id_input)

        # Prevents duplicate attednee IDs
        if attendee_exists(mysql_conn, attendee_id):
            print('Attendee ID already exists.')
            return

        # Checks if company exists before adding attendee
        if not get_company_by_id(mysql_conn, attendee_company_id):
            print('Invalid company ID.')
            return
        
        # Inserts new attednee into MySQL
        add_attendee(
            mysql_conn,
            attendee_id,
            attendee_name,
            attendee_dob,
            attendee_gender,
            attendee_company_id

        )

        print('Attendee added successfully.')

    except Error as e:
        print(f'Database error: {e}')

# Option 4: View attendees connected through Neo4j
def option_4_view_connected_attendees(mysql_conn, neo4j_driver):
    while True:
        attendee_id_input = input('Enter Attendee ID: ').strip()

        # Validates attednee ID
        if not is_positive_int(attendee_id_input):
            print('Invalid attendeeID. Please enter a numeric value')
            continue

        attendee_id = int(attendee_id_input)

        # Gets attednee name from NySQL
        attendee_name = get_attendee_name(mysql_conn, attendee_id)

        if not attendee_name:
            print(f'Attendee does not exist in MySQL.')
            break

        # GEts conencted attendee IDs from Neo4j
        connected_ids = get_connected_attendee_ids(
        neo4j_driver,
        NEO4J_CONFIG['database'],
        attendee_id 
        )

        if connected_ids is None or len(connected_ids) == 0:
            print(f'Attendee: {attendee_name}')
            print('No Connections')
            break

        print(f'Attendee: {attendee_name}')
        print('Connected Attendees:')

        # Uses MySQL to dispplay names for connected attednee IDs
        for connected_id in connected_ids:
            connected_name = get_attendee_name(mysql_conn, connected_id)
            if connected_name:
                print(f'{connected_id} - {connected_name}')
        break

# Option 5: Create a connection between two attendees in Neo4j
def option_5_add_attendee_connection(mysql_conn, neo4j_driver):
    while True:
        attendee_id_1_input = input('Enter first Attendee ID: ').strip()
        attendee_id_2_input = input('Enter second Attendee ID: ').strip()

        # Validates both attendee IDs
        if not is_positive_int(attendee_id_1_input) or not is_positive_int(attendee_id_2_input):
            print('Invalid attendee ID. Please enter numeric values.')
            continue

        id1 = int(attendee_id_1_input)
        id2 = int(attendee_id_2_input)

        # Prevents attendee connecting to themselves
        if id1 == id2:
            print('An attendee cannot be CONNECTED_TO themselves.')
            continue
        
        # Checks both attendees exist in MySQL first
        attendee1 = attendee_exists(mysql_conn, id1)
        attendee2 = attendee_exists(mysql_conn, id2)

        if not attendee1 or not attendee2:
            print('One or both attendees do not exist in MySQL.')
            continue
        
        # Creates relationship in Neo4j
        created = create_attendee_connection(
            neo4j_driver,
            NEO4J_CONFIG['database'],
            id1,
            id2
    )
        if not created:
            print('These attendees are already connected.')
            continue 
    
        print(f"Attendee {id1} is now CONNECTED_TO attendee {id2}.")
        break

# Option 6: Display room information from MySQL
def option_6_view_rooms(rooms_cache):
    print('\nRooms:')
    print('-' * 40)

    # Displays cached room results
    for room_id, room_name, capacity in rooms_cache:
        print(f'Room ID: {room_id}')
        print(f' Room Name: {room_name}')
        print(f'  Capacity: {capacity}')
        print('-' * 40)

# Main function controls database connections and menu loop
def main():
    rooms_cache = None
    mysql_conn = None
    neo4j_driver = None

    try:
        # Opens MySQL and Neo4j connection
        mysql_conn = get_mysql_connection()
        neo4j_driver = get_neo4j_driver()

        # Keeps application running until user exits
        while True:
            print_menu()
            choice = input('Select an option: ').strip().lower()

            # Run selected menu option
            if choice == '1':
                option_1_view_speaker(mysql_conn)
            elif choice == '2':
                option_2_view_attendees_by_company(mysql_conn)
            elif choice == '3':
                option_3_add_attendee(mysql_conn)
            elif choice == '4':
                option_4_view_connected_attendees(mysql_conn, neo4j_driver)
            elif choice == '5':
                option_5_add_attendee_connection(mysql_conn, neo4j_driver)
            elif choice == '6':

                # Loads rooms once and stores them in cache
                if rooms_cache is None:
                    rooms_cache = get_rooms(mysql_conn)
                option_6_view_rooms(rooms_cache)
            elif choice == 'x':
                print('Exiting application.')
                break
            else: 
                continue
                
    except Error as e:
        print(f'MySQL error: {e}')
    except Exception as e:
        print(f'Application Error: {e}')
    finally:
        # Closes MySQL connecton safely
        try:
            if mysql_conn and mysql_conn.is_connected():
                mysql_conn.close()
        except Exception:
            pass

        # Closes Neo4j connection safely
        try:
            if neo4j_driver:
                neo4j_driver.close()
        except Exception:
            pass

# Starts the application
if __name__ == '__main__':
    main()

