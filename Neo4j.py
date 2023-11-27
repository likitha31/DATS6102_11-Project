# python import_data_neo4j.py
# pip install neo4j
from neo4j import GraphDatabase
import csv

class Neo4jImporter:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        self._driver.close()

    def create_node(self, tx, label, properties):
        query = (
            f"CREATE (n:{label} {self._properties_string(properties)})"
        )
        tx.run(query, properties)

    def delete_node(self, tx, label, properties):
        query = (
            f"MATCH (n:{label} {self._properties_string(properties)}) "
            "DELETE n"
        )
        tx.run(query, properties)

    def update_node(self, tx, label, properties, update_properties):
        query = (
            f"MATCH (n:{label} {self._properties_string(properties)}) "
            "SET n += $update_properties"
        )
        tx.run(query, properties=properties, update_properties=update_properties)

    def _properties_string(self, properties):
        return "{" + ", ".join([f"{key}: '{value}'" for key, value in properties.items()]) + "}"

def import_data_from_csv(neo4j_importer, csv_file_path, label):
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        csv_data = list(csv_reader)

        with neo4j_importer._driver.session() as session:
            for row in csv_data:
                neo4j_importer.create_node(session, label, row)

def delete_node(neo4j_importer, label, properties):
    with neo4j_importer._driver.session() as session:
        neo4j_importer.delete_node(session, label, properties)

def update_node(neo4j_importer, label, properties, update_properties):
    with neo4j_importer._driver.session() as session:
        neo4j_importer.update_node(session, label, properties, update_properties)

if __name__ == "__main__":
    # Replace these values with your Neo4j connection details
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "your_username"
    neo4j_password = "your_password"

    # Replace these values with your CSV file path and Neo4j node label
    csv_file_path = "path/to/your/file.csv"
    neo4j_label = "your_label"

    # Create a connection to Neo4j
    neo4j_importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)

    try:
        # Import data from CSV into Neo4j
        import_data_from_csv(neo4j_importer, csv_file_path, neo4j_label)

        # Delete a node in Neo4j
        delete_properties = {"property_key": "property_value"}  # Replace with your actual properties
        delete_node(neo4j_importer, neo4j_label, delete_properties)

        # Update a node in Neo4j
        update_properties = {"update_key": "update_value"}  # Replace with your actual update properties
        update_conditions = {"condition_key": "condition_value"}  # Replace with your actual conditions
        update_node(neo4j_importer, neo4j_label, update_conditions, update_properties)
    finally:
        # Close the Neo4j connection
        neo4j_importer.close()

