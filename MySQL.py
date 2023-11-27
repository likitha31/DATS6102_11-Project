import mysql.connector
from mysql.connector import Error
import csv

def create_connection(host, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
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

def delete_data(connection, table_name, condition):
    try:
        cursor = connection.cursor()
        query = f"DELETE FROM {table_name} WHERE {condition}"
        cursor.execute(query)
        connection.commit()
        print(f"Data deleted from table: {table_name}")

    except Error as e:
        print(f"Error deleting data: {e}")

    finally:
        if cursor:
            cursor.close()

def update_data(connection, table_name, update_values, condition):
    try:
        cursor = connection.cursor()
        set_clause = ', '.join([f"{column} = %s" for column in update_values.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
        cursor.execute(query, list(update_values.values()))
        connection.commit()
        print(f"Data updated in table: {table_name}")

    except Error as e:
        print(f"Error updating data: {e}")

    finally:
        if cursor:
            cursor.close()

if __name__ == "__main__":
    # Replace these values with your MySQL server credentials
    host = "localhost"
    user = "your_username"
    password = "your_password"
    database = "your_database"
    
    # Replace these values with your CSV file path and table name
    csv_file_path = "path/to/your/file.csv"
    table_name = "your_table"

    # Create a connection to MySQL
    connection = create_connection(host, user, password, database)

    if connection:
        # Import data from CSV into MySQL
        import_data_from_csv(connection, csv_file_path, table_name)

        # Delete data from table
        delete_condition = "column_name = 'some_value'"  # Replace with your actual condition
        delete_data(connection, table_name, delete_condition)

        # Update data in table
        update_values = {"column_name": "new_value"}  # Replace with your actual update values
        update_condition = "another_column = 'some_value'"  # Replace with your actual condition
        update_data(connection, table_name, update_values, update_condition)

        # Close the connection
        connection.close()
