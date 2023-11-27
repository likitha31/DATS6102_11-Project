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
                       ReportNumber VARCHAR(255),
                        LocalCaseNumber INT,
                        AgencyName VARCHAR(255),
                        ACRSReportType VARCHAR(255),
                        CrashDateTime DATETIME,
                        RouteType VARCHAR(255),
                        RoadName VARCHAR(255),
                        CrossStreetType VARCHAR(255),
                        CrossStreetName VARCHAR(255),
                        OffRoadDescription VARCHAR(255),
                        Municipality VARCHAR(255),
                        RelatedNonMotorist VARCHAR(255),
                        CollisionType VARCHAR(255),
                        Weather VARCHAR(255),
                        SurfaceCondition VARCHAR(255),
                        Light VARCHAR(255),
                        TrafficControl VARCHAR(255),
                        DriverSubstanceAbuse VARCHAR(255),
                        NonMotoristSubstanceAbuse VARCHAR(255),
                        PersonID VARCHAR(255),
                        DriverAtFault VARCHAR(3),
                        InjurySeverity VARCHAR(255),
                        Circumstance VARCHAR(255),
                        DriverDistractedBy VARCHAR(255),
                        DriversLicenseState VARCHAR(255),
                        VehicleID VARCHAR(255),
                        VehicleDamageExtent VARCHAR(255),
                        VehicleFirstImpactLocation VARCHAR(255),
                        VehicleSecondImpactLocation VARCHAR(255),
                        VehicleBodyType VARCHAR(255),
                        VehicleMovement VARCHAR(255),
                        VehicleContinuingDir VARCHAR(255),
                        VehicleGoingDir VARCHAR(255),
                        SpeedLimit INT,
                        DriverlessVehicle VARCHAR(3),
                        ParkedVehicle VARCHAR(3),
                        VehicleYear INT,
                        VehicleMake VARCHAR(255),
                        VehicleModel VARCHAR(255),
                        EquipmentProblems VARCHAR(255),
                        Latitude FLOAT,
                        Longitude FLOAT,
                        Location VARCHAR(255)
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

if __name__ == "__main__":
    # Replace these values with your MySQL server credentials
    host = "localhost"
    user = "root"
    password = ""
    database = "Project1"

    # Replace these values with your CSV file path and table name
    csv_file_path = "Crash_Reporting_-_Drivers_Data.csv"
    table_name = "first"

    # Create a connection to MySQL, create the database and 'first' table if they don't exist
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
