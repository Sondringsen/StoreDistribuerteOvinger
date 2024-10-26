from utils.DbConnector import DbConnector

def main():
    connection = DbConnector()
    db = connection.db
    
    db.create_collection("User")
    print("User collection created successfully.")

    db.create_collection("Activity")
    print("Activity collection created successfully.")

    db.create_collection("TrackPoint")
    print("Trackpoint collection created successfully.")

    connection.close_connection()



if __name__ == "__main__":
    main()