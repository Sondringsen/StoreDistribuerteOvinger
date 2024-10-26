from utils.DbConnector import DbConnector

def main():
    connection = DbConnector()
    db = connection.db

    collections_to_clear = ["User", "Activity", "TrackPoint"]

    # Delete all records in each collection
    for collection_name in collections_to_clear:
        collection = db[collection_name]
        result = collection.delete_many({})  # Deletes all documents
        print(f"Deleted {result.deleted_count} documents from {collection_name} collection.")

    connection.close_connection()

if __name__ == "__main__":
    main()