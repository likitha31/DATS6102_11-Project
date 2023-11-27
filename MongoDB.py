# python import_data_mongodb.py
# pip install pymongo
import csv
from pymongo import MongoClient

def create_mongo_connection(host, port, username, password, database):
    try:
        client = MongoClient(host=host, port=port, username=username, password=password)
        db = client[database]
        print(f"Connected to MongoDB Database: {database}")
        return db
    except Exception as e:
        print(f"Error: {e}")
        return None

def import_data_from_csv(mongo_db, collection_name, csv_file_path):
    try:
        collection = mongo_db[collection_name]

        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            csv_data = list(csv_reader)

            # Insert the CSV data into MongoDB
            result = collection.insert_many(csv_data)
            print(f"Data imported into collection: {collection_name}. Inserted {len(result.inserted_ids)} documents.")

    except Exception as e:
        print(f"Error importing data: {e}")

def delete_data(mongo_db, collection_name, delete_condition):
    try:
        collection = mongo_db[collection_name]
        result = collection.delete_many(delete_condition)
        print(f"Data deleted from collection: {collection_name}. Deleted {result.deleted_count} documents.")

    except Exception as e:
        print(f"Error deleting data: {e}")

def update_data(mongo_db, collection_name, update_condition, update_values):
    try:
        collection = mongo_db[collection_name]
        result = collection.update_many(update_condition, {"$set": update_values})
        print(f"Data updated in collection: {collection_name}. Matched {result.matched_count} documents, modified {result.modified_count} documents.")

    except Exception as e:
        print(f"Error updating data: {e}")

if __name__ == "__main__":
    # Replace these values with your MongoDB connection details
    mongodb_host = "localhost"
    mongodb_port = 27017
    mongodb_username = "your_username"
    mongodb_password = "your_password"
    mongodb_database = "your_database"

    # Replace these values with your CSV file path and MongoDB collection name
    csv_file_path = "path/to/your/file.csv"
    mongodb_collection = "your_collection"

    # Create a connection to MongoDB
    mongo_db = create_mongo_connection(mongodb_host, mongodb_port, mongodb_username, mongodb_password, mongodb_database)

    if mongo_db:
        # Import data from CSV into MongoDB
        import_data_from_csv(mongo_db, mongodb_collection, csv_file_path)

        # Delete data from MongoDB collection
        delete_condition = {"your_field": "your_value"}  # Replace with your actual condition
        delete_data(mongo_db, mongodb_collection, delete_condition)

        # Update data in MongoDB collection
        update_condition = {"your_field": "your_value"}  # Replace with your actual condition
        update_values = {"$set": {"field_to_update": "new_value"}}  # Replace with your actual update values
        update_data(mongo_db, mongodb_collection, update_condition, update_values)

        # Close the MongoDB connection
        mongo_db.client.close()
