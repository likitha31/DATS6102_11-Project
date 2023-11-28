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
                        ID INT PRIMARY KEY,
                       Clean_Alternative_Fuel_Vehicle_Type VARCHAR(255),
                        VIN VARCHAR(20),
                        DOL_Vehicle_ID INT,
                        Model_Year INT,
                        Make VARCHAR(255),
                        Model VARCHAR(255),
                        Vehicle_Primary_Use VARCHAR(255),
                        Electric_Range INT,
                        Odometer_Reading INT,
                        Odometer_Code VARCHAR(255),
                        New_or_Used_Vehicle VARCHAR(255),
                        Sale_Price INT,
                        Sale_Date VARCHAR(255),
                        Base_MSRP INT,
                        Transaction_Type VARCHAR(255),
                        DOL_Transaction_Date VARCHAR(255),
                        Transaction_Year INT,
                        County VARCHAR(255),
                        City VARCHAR(255),
                        State_of_Residence VARCHAR(255),
                        Postal_Code INT,
                        HB_2778_Exemption_Eligibility VARCHAR(255),
                        HB_2042_Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility VARCHAR(255),
                        Meets_2019_HB_2042_Electric_Range_Requirement BOOLEAN,
                        Meets_2019_HB_2042_Sale_Date_Requirement BOOLEAN,
                        Meets_2019_HB_2042_Sale_Price_Value_Requirement BOOLEAN,
                        HB_2042_Battery_Range_Requirement VARCHAR(255),
                        HB_2042_Purchase_Date_Requirement VARCHAR(255),
                        HB_2042_Sale_Price_Value_Requirement VARCHAR(255),
                        Electric_Vehicle_Fee_Paid VARCHAR(255),
                        Transportation_Electrification_Fee_Paid VARCHAR(255),
                        Hybrid_Vehicle_Electrification_Fee_Paid VARCHAR(255),
                        Census_Tract INT,
                        Legislative_District INT,
                        Electric_Utility VARCHAR(255)
                )
            """

            cursor.execute(create_table_query)

            print(f"Connected to MySQL Database: {database}")
            print("Table created")

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

            print("inserting data")

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

def insert_data(connection, table_name, values):
    try:
        cursor = connection.cursor()
        placeholders = ', '.join(['%s' for _ in values])
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(query, values)
        connection.commit()
        print(f"Data inserted into table: {table_name}")

    except Error as e:
        print(f"Error inserting data: {e}")

    finally:
        if cursor:
            cursor.close()

def perform_aggregate_query(connection, table_name, make):
    try:
        cursor = connection.cursor()

        # Query to find the maximum sale price for the given make
        max_query = f"SELECT MAX(Sale_Price) FROM {table_name} WHERE Make = '{make}'"
        cursor.execute(max_query)
        max_result = cursor.fetchone()
        max_sale_price = max_result[0] if max_result[0] is not None else 0

        # Query to find the minimum sale price for the given make
        min_query = f"SELECT MIN(Sale_Price) FROM {table_name} WHERE Make = '{make}'"
        cursor.execute(min_query)
        min_result = cursor.fetchone()
        min_sale_price = min_result[0] if min_result[0] is not None else 0

        print(f"Statistics for {make}:")
        print(f"Maximum Sale Price: {max_sale_price}")
        print(f"Minimum Sale Price: {min_sale_price}")

    except Error as e:
        print(f"Error finding sale price stats: {e}")

    finally:
        if cursor:
            cursor.close()


def perform_complex_query(connection, table_name, limit_value=10):
    try:
        cursor = connection.cursor()

        # Query to select specific columns and apply conditions with LIMIT
        query = f"""
            SELECT Make, COUNT(*) as VehicleCount, AVG(Sale_Price) as AvgSalePrice
            FROM {table_name}
            WHERE
                Clean_Alternative_Fuel_Vehicle_Type = 'Battery Electric Vehicle (BEV)'
                AND Model_Year >= 2020
                AND Sale_Price > 25000
                AND State_of_Residence = 'WA'
            GROUP BY Make
            ORDER BY AvgSalePrice DESC
            LIMIT {limit_value};
        """

        cursor.execute(query)
        result = cursor.fetchall()

        # Print the results
        print(f"Results of the query with LIMIT: {limit_value}:")
        for row in result:
            print(row)

    except Error as e:
        print(f"Error performing complex query: {e}")

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
    csv_file_path = "Electric_Vehicle_Title_and_Registration_Activity.csv"
    table_name = "first"

    # Create a connection to MySQL, create the database and 'first' table if they don't exist
    connection = create_connection(host, user, password, database)

    if connection:
        # Import data from CSV into MySQL
        '''
        import_data_from_csv(connection, csv_file_path, table_name)
        
        
        # Delete data from table
        delete_condition = "ID = '6'"  # Replace with your actual condition
        delete_data(connection, table_name, delete_condition)

        # Update data in table
        update_values = {"Postal_Code": '98036'}  # Replace with your actual update values
        update_condition = "Postal_Code = '98034'"  # Replace with your actual condition
        update_data(connection, table_name, update_values, update_condition)
        
        new_values = (6, 'Electric', 'ABC123', 12345, 2023, 'Tesla', 'Model S', 'Personal', 300, 15000, 'Normal', 'New', 80000, '2023-01-01', 90000, 'Sale', '2023-01-02', 2023, 'King', 'Seattle', 'WA', 98034, 'Exempt', 'Eligible', True, True, True, 'High', '2023-01-01', '90000', 'Yes', 'Yes', 'No', 123, 11, 'Power Company')
        insert_data(connection, table_name, new_values)
        

        perform_aggregate_query(connection, table_name, "Tesla")
        '''

        perform_complex_query(connection, table_name, limit_value=5)

        # Close the connection
        connection.close()
