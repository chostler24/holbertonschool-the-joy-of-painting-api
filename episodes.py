#!/usr/bin/env python3

import mysql.connector

# Database connection parameters
db_config = {
    "host": "localhost",
    "user": "con",
    "password": "root",
    "database": "bobross",
}

# Read data from dataset3.txt and prepare it for insertion
data_file = "dataset3.txt"

episodes = []

with open(data_file, "r") as file:
    lines = file.readlines()

for line in lines:
    line = line.strip()
    if line:
        episodes.append(line)

# Connect to the MySQL database
db_connection = mysql.connector.connect(**db_config)
cursor = db_connection.cursor()

# Create the Episodes table if it doesn't exist
create_table_query = """
    CREATE TABLE IF NOT EXISTS Episodes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        air_date DATE NOT NULL
    );
"""
cursor.execute(create_table_query)

# Insert data into the Episodes table
insert_query = "INSERT INTO Episodes (title, air_date) VALUES (%s, %s)"

for episode in episodes:
    title, air_date = episode.split(" (")
    air_date = air_date.strip(")").split(", ")[1]
    cursor.execute(insert_query, (title, air_date))

db_connection.commit()

# Close the database connection
cursor.close()
db_connection.close()

print("Data inserted successfully!")
