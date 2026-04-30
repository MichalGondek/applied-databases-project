# Applied DataBases Project
# Authoer: Michal Gondek

from mysql.connector import Error
from config import NEO4J_CONFIG
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
from db_neo4j import (
    get_neo4j_driver,
    get_connected_attendee_ids,
    create_attendee_connection,
)

def print_menu():
    print('\n==== Conference Application ====')
    print('1. View Speaker Sessions')
    print('2. View Company Attendees')
    print('3. Add New Attendee')
    print('4. View Connected Attendees')
    print('5. Add Attendee Connection')
    print('6. View Rooms')
    print('x. Exit')

def is_positive_int(value):
    return value.isdigit() and int(value) > 0

def option_1_view_speaker(mysql_conn):
    speaker_search = input('Enter Speaker Name (partial or full): ').strip()
    rows = get_speakers_sessions(mysql_conn, speaker_search)

    if not rows:
        print('No speakers match the search criteria.')
        return
    
    print('\nSpeaker Sessions')
    print('--' * 50)
    for speaker_name, session_title,room_name in rows:
        print(f'Speaker: {speaker_name}')
        print(f'  Session: {session_title}')
        print(f'  Room: {room_name}')
        print('--' * 50)
    
def option_2_view_attendees_by_company(mysql_conn):
    while True:
        company_id_input = input('Enter Company ID: ').strip()

        if not is_positive_int(company_id_input):
            print('Invalid company ID, Enter a number greater than 0.')
            continue


        company_id = int(company_id_input)
        company = get_company_by_id(mysql_conn, company_id)

        if not company:
            print(f'No company found with ID {company_id}. Please try again.')
            continue

        rows = get_attendees_by_company(mysql_conn, company_id)

        if not rows:
            print(f'Company: {company[1]}')
            print('This company exists but has no attendees for any sessions.')
            continue

        print(f'\nCompany: {company[1]}')
        print('-' * 60)
        for attendee_name, attendee_dob, session_title, speaker_name, room_name in rows:
            print(f'Attendee: {attendee_name}')
            print(f'DOB: {attendee_dob}')
            print(f'  Session: {session_title}')
            print(f'  Speaker: {speaker_name}')
            print(f'  Room: {room_name}')
            print('-' * 60)
        break

def option_3_add_attendee(mysql_conn):
    try:
        attendee_id_input = input('Enter Attendee ID:').strip()
        attendee_name = input('Enter Attendee Name: ').strip()
        attendee_dob = input('Enter Attendee DOB (YYYY-MM-DD): ').strip()
        attendee_gender = input('Enter gender (Male/Female): ').strip()
        attendee_company_id_input = input('Enter company ID:').strip()

        if not is_positive_int(attendee_id_input):
            print('Invalid attendee ID.')
            return
        
        if attendee_gender not in ('Male', 'Female'):
            print('Invalid gender.')
            return
        
        if not is_positive_int(attendee_company_id_input):
            print('Invalid company ID.')        
            return
        
        attendee_id = int(attendee_id_input)
        attendee_company_id = int(attendee_company_id_input)

        if attendee_exists(mysql_conn, attendee_id):
            print('Attendee ID already exists.')
            return

        if not get_company_by_id(mysql_conn, attendee_company_id):
            print('Invalid company ID.')
            return
        
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

def option_4_view_connected_attendees(mysql_conn, neo4j_driver):
    while True:
        attendee_id_input = input('Enter Attendee ID: ').strip()

        if not is_positive_int(attendee_id_input):
            print('Invalid attendeeID. Please enter a numeric value')
            continue

        attendee_id = int(attendee_id_input)
        attendee_name = get_attendee_name(mysql_conn, attendee_id)

        if not attendee_name:
            print(f'Attendee does not exist in MySQL or Neo4j.')
            break

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

        for connected_id in connected_ids:
            connected_name = get_attendee_name(mysql_conn, connected_id)
            if connected_name:
                print(f'{connected_id} - {connected_name}')
        break

def option_5_add_attendee_connection(mysql_conn, neo4j_driver):
    while True:
        attendee_id_1_input = input('Enter first Attendee ID: ').strip()
        attendee_id_2_input = input('Enter second Attendee ID: ').strip()

        if not is_positive_int(attendee_id_1_input) or not is_positive_int(attendee_id_2_input):
            print('Invalid attendee ID. Please enter numeric values.')
            continue

        id1 = int(attendee_id_1_input)
        id2 = int(attendee_id_2_input)

        if id1 == id2:
            print('An atendee cannot be CONNECTED_TO themselves.')
            continue

        attendee1 = attendee_exists(mysql_conn, id1)
        attendee2 = attendee_exists(mysql_conn, id2)

        if not attendee1 or not attendee2:
            print('One or both attendees do not exist in MySQL.')
            continue

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

def option_6_view_rooms(rooms_cache):
    print('\nRooms:')
    print('-' * 40)

    for room_id, room_name, capacity in rooms_cache:
        print(f'Room ID: {room_id}')
        print(f' Room Name: {room_name}')
        print(f'  Capacity: {capacity}')
        print('-' * 40)

def main():
    rooms_cache = None
    mysql_conn = None
    neo4j_driver = None

    try:
        mysql_conn = get_mysql_connection()
        neo4j_driver = get_neo4j_driver()

        while True:
            print_menu()
            choice = input('Select an option: ').strip().lower()

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
        try:
            if mysql_conn and mysql_conn.is_connected():
                mysql_conn.close()
        except Exception:
            pass
        try:
            if neo4j_driver:
                neo4j_driver.close()
        except Exception:
            pass
            
if __name__ == '__main__':
    main()

