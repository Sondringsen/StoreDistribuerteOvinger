from utils.DbConnector import DbConnector

def create_user_table(connector):
    """
    Creates the User table in the database.

    Args:
        connector (DbConnector): used to execute the query.
    """
    query = """
        CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(4) NOT NULL PRIMARY KEY,
            has_labels BIT(1)
        );
    """
    connector.cursor.execute(query)
    connector.db_connection.commit()

def create_activity_table(connector):
    """
    Creates the Activity table in the database.

    Args:
        connector (DbConnector): used to execute the query.
    """
    query = """
        CREATE TABLE IF NOT EXISTS Activity (
            id SMALLINT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY,
            user_id VARCHAR(4),
            transportation_mode VARCHAR(30),
            start_date_time DATETIME,
            end_date_time DATETIME,
            CONSTRAINT f_key1 FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
        );
    """
    connector.cursor.execute(query)
    connector.db_connection.commit()

def create_trackpoint_table(connector):
    """
    Creates the TrackPoint table in the database.

    Args:
        connector (DbConnector): used to execute the query.
    """
    query = """
        CREATE TABLE IF NOT EXISTS TrackPoint (
            id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
            activity_id SMALLINT UNSIGNED,
            lat DOUBLE(8, 6), 
            lon DOUBLE(9, 6),
            altitude MEDIUMINT,
            date_days DOUBLE(15, 10),
            date_time DATETIME,
            CONSTRAINT f_key2 FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
        );
    """
    connector.cursor.execute(query)
    connector.db_connection.commit()


def main():
    connector = DbConnector()

    create_user_table(connector)
    create_activity_table(connector)
    create_trackpoint_table(connector)

    connector.close_connection()


if __name__ == "__main__":
    main()