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
            walk BIT(1),
            bike BIT(1),
            bus BIT(1),
            taxi BIT(1),
            car BIT(1),
            subway BIT(1),
            train BIT(1),
            airplane BIT(1),
            boat BIT(1),
            run BIT(1),
            motorcycle BIT(1),
            valid_activity BIT(1),
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
            transportation_mode VARCHAR(30),
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