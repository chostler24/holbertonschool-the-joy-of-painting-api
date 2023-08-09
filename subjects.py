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
csv_file = 'dataset2.csv'

# Table name in the database
table_name = 'Episodes'

# Read CSV file into a pandas DataFrame, skipping the first unnamed column
df = pd.read_csv(csv_file)

# Convert boolean columns to boolean values
boolean_columns = df.columns[2:]  # Assuming boolean columns start from the third column
df[boolean_columns] = df[boolean_columns].astype(bool)

# Connect to MySQL database
try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        EPISODE VARCHAR(50) PRIMARY KEY,
        TITLE VARCHAR(255),
        APPLE_FRAME BOOLEAN,
        AURORA_BOREALIS BOOLEAN,
        BARN BOOLEAN,
        BEACH BOOLEAN,
        BOAT BOOLEAN,
        BRIDGE BOOLEAN,
        BUILDING BOOLEAN,
        BUSHES BOOLEAN,
        CABIN BOOLEAN,
        CACTUS BOOLEAN,
        CIRCLE_FRAME BOOLEAN,
        CIRRUS BOOLEAN,
        CLIFF BOOLEAN,
        CLOUDS BOOLEAN,
        CONIFER BOOLEAN,
        CUMULUS BOOLEAN,
        DECIDUOUS BOOLEAN,
        DIANE_ANDRE BOOLEAN,
        DOCK BOOLEAN,
        DOUBLE_OVAL_FRAME BOOLEAN,
        FARM BOOLEAN,
        FENCE BOOLEAN,
        FIRE BOOLEAN,
        FLORIDA_FRAME BOOLEAN,
        FLOWERS BOOLEAN,
        FOG BOOLEAN,
        FRAMED BOOLEAN,
        GRASS BOOLEAN,
        GUEST BOOLEAN,
        HALF_CIRCLE_FRAME BOOLEAN,
        HALF_OVAL_FRAME BOOLEAN,
        HILLS BOOLEAN,
        LAKE BOOLEAN,
        LAKES BOOLEAN,
        LIGHTHOUSE BOOLEAN,
        MILL BOOLEAN,
        MOON BOOLEAN,
        MOUNTAIN BOOLEAN,
        MOUNTAINS BOOLEAN,
        NIGHT BOOLEAN,
        OCEAN BOOLEAN,
        OVAL_FRAME BOOLEAN,
        PALM_TREES BOOLEAN,
        PATH BOOLEAN,
        PERSON BOOLEAN,
        PORTRAIT BOOLEAN,
        RECTANGLE_3D_FRAME BOOLEAN,
        RECTANGULAR_FRAME BOOLEAN,
        RIVER BOOLEAN,
        ROCKS BOOLEAN,
        SEASHELL_FRAME BOOLEAN,
        SNOW BOOLEAN,
        SNOWY_MOUNTAIN BOOLEAN,
        SPLIT_FRAME BOOLEAN,
        STEVE_ROSS BOOLEAN,
        STRUCTURE BOOLEAN,
        SUN BOOLEAN,
        TOMB_FRAME BOOLEAN,
        TREE BOOLEAN,
        TREES BOOLEAN,
        TRIPLE_FRAME BOOLEAN,
        WATERFALL BOOLEAN,
        WAVES BOOLEAN,
        WINDMILL BOOLEAN,
        WINDOW_FRAME BOOLEAN,
        WINTER BOOLEAN,
        WOOD_FRAMED BOOLEAN
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
