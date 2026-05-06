# Applied Databases Project 2026
## Author: Michal Gondek

## Assignment Description
This assignment is a conference managment application build using Python, MySQL and Neo4j.

The application allows users to:
- View speaker sessions
- View company attendees
- Add new attendees
- View attendee connections
- Add attendee connections
- View conference rooms

MySQL is used for relational conference data and Neo4j is used for attendee network relationships.

## Applications Used
- Python 3
- MySQL
- Neo4j
- VS Code

## Project Files
- 'main.py' - Main Application
- 'db_mysql.py' - MySQL database functions
- 'db_neo4j.py' - Neo4j database functions
- 'config.py' - Database configuration
- 'appdbproj.sql.txt' - MySQL database script
- 'appdpprojNeo4j.json - Neo4j Cypher script

## How To Run

### 1. Clone Repository
git clone https://github.com/MichalGondek/applied-databases-project.git

### 2. Install packages
pip install mysql-connector-python neo4j

### 3. Import MySQL database
mysql -u root -p < appdbproj.sql.txt

### 4. Load Neo4j database and run Cypher queries
attendeeNetwork
appdbprojNeo4j.json

### 5. Update Passwords
Edit config.py and enter the correct MySQL and Neo4j passwords

### 6. Run Application
python main.py