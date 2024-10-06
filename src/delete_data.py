from utils.DbConnector import DbConnector

def delete_data(connector, table_name):
    query = f"DELETE FROM {table_name}"
    connector.cursor.execute(query)
    connector.db_connection.commit()

def main():
    connector = DbConnector()
    delete_data(connector, "TrackPoint")
    delete_data(connector, "Activity")
    delete_data(connector, "User")

if __name__ == "__main__":
    main()