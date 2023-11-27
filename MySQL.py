import mysql.connector
from mysql.connector import Error
import csv

def create_connection(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
        )
        if connection.is_connected():
            cursor = connection.cursor()

            # Create the database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")

            # Switch to the specified database
            cursor.execute(f"USE {database}")

            # Create the 'first' table if it doesn't exist
            create_table_query = """
                CREATE TABLE IF NOT EXISTS first ( 
                    rental_id INT,
                    duration INT,
                    bike_id INT,
                    end_rental_date_time DATETIME,
                    end_station_id INT,
                    end_station_name VARCHAR(255),
                    start_rental_date_time DATETIME,
                    start_station_id INT,
                    start_station_name VARCHAR(255),
                    PRIMARY KEY (rental_id)
                )
            """
    

            cursor.execute(create_table_query)

            print(f"Connected to MySQL Database: {database}")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def import_data_from_csv(connection, csv_file_path, table_name):
    try:
        cursor = connection.cursor()

        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header row

            for row in csv_reader:
                # Assuming the CSV columns match the table columns in order
                query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in row])})"
                cursor.execute(query, row)

        connection.commit()
        print(f"Data imported into table: {table_name}")

    except Error as e:
        print(f"Error importing data: {e}")

    finally:
        if cursor:
            cursor.close()

if __name__ == "__main__":
    # Replace these values with your MySQL server credentials
    host = "localhost"
    user = "root"
    password = ""
    database = "Project1"

    # Replace these values with your CSV file path and table name
    csv_file_path = "london.csv"
    table_name = "first"

    # Create a connection to MySQL, create the database and 'first' table if they don't exist
    connection = create_connection(host, user, password, database)

    if connection:
        # Import data from CSV into MySQL
        import_data_from_csv(connection, csv_file_path, table_name)

        # Close the connection
        connection.close()
