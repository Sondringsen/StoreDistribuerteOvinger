from utils.DbConnector import DbConnector
from utils.dataset_utils import create_dfs

def insert_user_df(df, connector):
    insert_query = """
        INSERT INTO User (id, has_labels)
        VALUES (%s, %s);
    """
    print("Start inserting User table")
    for _, row in df.iterrows():
        connector.cursor.execute(insert_query, tuple(row))

    connector.db_connection.commit()
    print("Finished inserting User table")

def insert_activity_df(df, connector):
    insert_query = """
        INSERT INTO Activity (id, user_id, walk, bike, bus, taxi, car, subway, train, airplane, boat, run, motorcycle, start_date_time, end_date_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    print("Start inserting Activity table")
    for _, row in df.iterrows():
        row["user_id"] = str(row["user_id"])
        connector.cursor.execute(insert_query, tuple(row))
    
    connector.db_connection.commit()
    print("Finished inserting Activity table")

def insert_trackpoint_df(df, connector):
    insert_query = """
        INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    print("Start inserting TrackPoint table")
    for _, row in df.iterrows():
        connector.cursor.execute(insert_query, tuple(row))
    
    connector.db_connection.commit()
    print("Finished inserting TrackPoint table")

def main():
    user_df, activity_df, trackpoint_df = create_dfs()

    connector = DbConnector()

    insert_user_df(user_df, connector)
    insert_activity_df(activity_df, connector)
    insert_trackpoint_df(trackpoint_df, connector)

    connector.close_connection()

if __name__ == "__main__":
    main()
