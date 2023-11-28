import csv
from pymongo import MongoClient

def create_connection():
    try:
        # Connect to the MongoDB server
        client = MongoClient('mongodb://localhost:27017/')

        # Create or get the database
        database = client['Project1']

        # Create or get the collection (equivalent to table in MongoDB)
        collection = database['first']

        print("Connected to MongoDB Database: Project1")
        print("Collection created")

        return collection

    except Exception as e:
        print(f"Error: {e}")
        return None

def import_data_from_csv(collection, csv_file_path):
    try:
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            print("Inserting data")

            # Insert each row as a document in the collection
            for row in csv_reader:
                collection.insert_one(row)

        print("Data imported into collection")

    except Exception as e:
        print(f"Error importing data: {e}")

def delete_data(collection, condition):
    try:
        # Delete documents that match the specified condition
        collection.delete_many(condition)
        print("Data deleted from collection")

    except Exception as e:
        print(f"Error deleting data: {e}")

def update_data(collection, update_values, condition):
    try:
        # Update documents that match the specified condition
        collection.update_many(condition, {"$set": update_values})
        print("Data updated in collection")

    except Exception as e:
        print(f"Error updating data: {e}")

def insert_data(collection, values):
    try:
        # Insert a new document into the collection
        collection.insert_one(values)
        print("Data inserted into collection")

    except Exception as e:
        print(f"Error inserting data: {e}")

def perform_aggregate_query(collection, make):
    try:
        # Aggregate pipeline to find the maximum and minimum sale price for the given make
        pipeline = [
            {"$match": {"Make": make}},
            {"$group": {"_id": None, "max_sale_price": {"$max": "$Sale_Price"}, "min_sale_price": {"$min": "$Sale_Price"}}}
        ]

        result = list(collection.aggregate(pipeline))

        if result:
            max_sale_price = result[0]["max_sale_price"] if result[0]["max_sale_price"] is not None else 0
            min_sale_price = result[0]["min_sale_price"] if result[0]["min_sale_price"] is not None else 0

            print(f"Statistics for {make}:")
            print(f"Maximum Sale Price: {max_sale_price}")
            print(f"Minimum Sale Price: {min_sale_price}")
        else:
            print(f"No data found for {make}")

    except Exception as e:
        print(f"Error finding sale price stats: {e}")

def perform_complex_query(collection, limit_value=10):
    try:
        # Query to select specific fields and apply conditions with LIMIT
        query = [
            {"$match": {
                "Clean_Alternative_Fuel_Vehicle_Type": "Battery Electric Vehicle (BEV)",
                "Model_Year": {"$gte": 2020},
                "Sale_Price": {"$gt": 25000},
                "State_of_Residence": "WA"
            }},
            {"$group": {
                "_id": "$Make",
                "VehicleCount": {"$sum": 1},
                "AvgSalePrice": {"$avg": "$Sale_Price"}
            }},
            {"$sort": {"AvgSalePrice": -1}},
            {"$limit": limit_value}
        ]

        result = list(collection.aggregate(query))

        # Print the results
        print(f"Results of the query with LIMIT: {limit_value}:")
        for row in result:
            print(row)

    except Exception as e:
        print(f"Error performing complex query: {e}")

if __name__ == "__main__":
    # Replace these values with your CSV file path
    csv_file_path = "Electric_Vehicle_Title_and_Registration_Activity.csv"

    # Create a connection to MongoDB, create the database and 'first' collection if they don't exist
    collection = create_connection()

    if collection:
        # Import data from CSV into MongoDB
        
        import_data_from_csv(collection, csv_file_path)

        # Delete data from collection
        delete_condition = {"ID": 6}  # Replace with your actual condition
        delete_data(collection, delete_condition)

        # Update data in collection
        update_values = {"Postal_Code": 98036}  # Replace with your actual update values
        update_condition = {"Postal_Code": 98034}  # Replace with your actual condition
        update_data(collection, update_values, update_condition)
        
        new_values = {
            "ID": 6,
            "Clean_Alternative_Fuel_Vehicle_Type": "Electric",
            "VIN": "ABC123",
            # Add other fields here...
            "Electric_Utility": "Power Company"
        }
        insert_data(collection, new_values)
        

        perform_aggregate_query(collection, "Tesla")

        perform_complex_query(collection, limit_value=5)
