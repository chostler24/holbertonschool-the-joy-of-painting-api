#!/usr/bin/env python3

import pandas as pd
import mysql.connector

# MySQL connection configuration
db_config = {
    'host': 'localhost',
    'user': 'con',
    'password': 'root',
    'database': 'bobross',
}

# CSV file path and name
csv_file = 'dataset1.csv'

# Table name in the database
table_name = 'Colors'

# Read CSV file into a pandas DataFrame
df = pd.read_csv(csv_file, usecols=lambda column: column != '')

# Connect to MySQL database
try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Create table (only if it doesn't exist)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        painting_index INT PRIMARY KEY,
        img_src VARCHAR(255),
        painting_title VARCHAR(255),
        season VARCHAR(50),
        episode VARCHAR(50),
        num_colors INT,
        youtube_src VARCHAR(255),
        Black_Gesso VARCHAR(50),
        Bright_Red VARCHAR(50),
        Burnt_Umber VARCHAR(50),
        Cadmium_Yellow VARCHAR(50),
        Dark_Sienna VARCHAR(50),
        Indian_Red VARCHAR(50),
        Indian_Yellow VARCHAR(50),
        Liquid_Black VARCHAR(50),
        Liquid_Clear VARCHAR(50),
        Midnight_Black VARCHAR(50),
        Phthalo_Blue VARCHAR(50),
        Phthalo_Green VARCHAR(50),
        Prussian_Blue VARCHAR(50),
        Sap_Green VARCHAR(50),
        Titanium_White VARCHAR(50),
        Van_Dyke_Brown VARCHAR(50),
        Yellow_Ochre VARCHAR(50),
        Alizarin_Crimson VARCHAR(50)
    );
    """
    cursor.execute(create_table_query)

    # Convert DataFrame to list of tuples for bulk insert
    data_to_insert = df.to_records(index=False).tolist()

    # Prepare the SQL query for bulk insert
    insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(data_to_insert[0]))})"

    # Execute the bulk insert
    cursor.executemany(insert_query, data_to_insert)

    # Commit the changes
    connection.commit()

    print("Data inserted successfully!")

except mysql.connector.Error as error:
    print("Error: ", error)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
