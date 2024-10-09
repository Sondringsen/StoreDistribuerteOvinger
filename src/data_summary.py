from utils.DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
from utils.data_utils import calculate_distance, calculate_altitudes, calculate_invalid

def table1(connector):
    query = f"""
        SELECT * FROM User
        LIMIT 11;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("First 10 entries of User")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))

def table2(connector):
    query = f"""
        SELECT * FROM Activity
        LIMIT 10;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("First 10 entries of Activity")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))

def table3(connector):
    query = f"""
        SELECT * FROM TrackPoint
        LIMIT 10;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("First 10 entries of TrackPoint")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))

def query1(connector):
    for table_name in ["User", "Activity", "TrackPoint"]:
        query = f"""SELECT COUNT(ID) FROM {table_name}"""
        connector.cursor.execute(query)
        result = connector.cursor.fetchall()
        print(f"Table {table_name} has {result[0][0]} rows")

def query2(connector):
    query = f"""
        SELECT AVG(activity_count) as  avg_activity_count FROM (
            SELECT COUNT(id) AS activity_count 
            FROM Activity
            GROUP BY user_id
        ) AS counts;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("Average activities per user")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))

def query3(connector):
    query = f"""
        SELECT user_id, COUNT(id) AS activity_count 
        FROM Activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("Users with the most activities")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))

def query4(connector):
    query = f"""
        SELECT DISTINCT user.id 
        FROM User as user RIGHT JOIN Activity as activity
        ON user.id = activity.user_id
        WHERE taxi = 1;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("Users who have taken taxi")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))

def query5(connector):
    query = f"""
        SELECT 
            SUM(walk) AS walk,
            SUM(bike) AS bike,
            SUM(bus) AS bus,
            SUM(taxi) AS taxi,
            SUM(car) AS car,
            SUM(subway) AS subway,
            SUM(train) AS train,
            SUM(airplane) AS airplane,
            SUM(boat) AS boat,
            SUM(run) AS run,
            SUM(motorcycle) AS motorcycle
        FROM Activity;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()

    print("Number of times each transportation mode has been used")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))


def query6a(connector):
    query = f"""
        SELECT YEAR(start_date_time) as year, COUNT(id) AS activity_count
        FROM Activity
        GROUP BY YEAR(start_date_time)
        ORDER BY activity_count DESC;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("Number of activities per year")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))

def query6b(connector):
    query = f"""
        SELECT YEAR(start_date_time) as year, SUM(TIMESTAMPDIFF(second, start_date_time, end_date_time) / 3600) AS time_spent
        FROM Activity
        GROUP BY YEAR(start_date_time)
        ORDER BY time_spent DESC;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("Time spent on activities per year")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))


def query7(connector):
    query = f"""
        SELECT tp.lat, tp.lon, tp.activity_id
        FROM Activity as a RIGHT JOIN TrackPoint as tp ON a.id = tp.activity_id
        WHERE a.user_id = "112" AND YEAR(a.start_date_time) = 2008 AND tp.transportation_mode = "walk";
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    df = pd.DataFrame(columns=["lat", "lon", "activity_id"], data=result)
    df = calculate_distance(df)
    
    print("Total kilometers traveled by user 112 in 2008:")
    print(df["distance"].sum())


def query8(connector):
    query = f"""
        SELECT a.user_id, a.id, tp.altitude
        FROM TrackPoint AS tp LEFT JOIN Activity AS a ON tp.activity_id = a.id
        WHERE tp.altitude != -777;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    df = pd.DataFrame(columns=["user_id", "activity_id", "alt"], data=result)
    df = calculate_altitudes(df)
    print("Users who have gained the most meters")
    print(df.sort_values(ascending=False).head(20)*0.3048)
    
def query9(connector):
    query = f"""
        SELECT a.user_id, a.id, tp.date_time
        FROM Activity AS a RIGHT JOIN TrackPoint AS tp ON a.id = tp.activity_id;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    df = pd.DataFrame(columns=["user_id", "activity_id", "date_time"], data=result)
    df = calculate_invalid(df)
    print("Invalid activities per user (only includes users who has invalid activities)")
    print(df)


def query10(connector):
    query = f"""
        SELECT DISTINCT a.user_id
        FROM Activity AS a RIGHT JOIN TrackPoint AS tp ON a.id = tp.activity_id
        WHERE tp.lat > 39.915 AND tp.lat < 39.917 AND tp.lon > 116.396 AND tp.lon < 116.398;
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    print("Users who have registered activities in the Forbidden City")
    headers = [description[0] for description in connector.cursor.description]
    print(tabulate(result, headers))


def query11(connector):
    query = f"""
        SELECT 
            u.id,
            SUM(walk) AS walk,
            SUM(bike) AS bike,
            SUM(bus) AS bus,
            SUM(taxi) AS taxi,
            SUM(car) AS car,
            SUM(subway) AS subway,
            SUM(train) AS train,
            SUM(airplane) AS airplane,
            SUM(boat) AS boat,
            SUM(run) AS run,
            SUM(motorcycle) AS motorcycle
        FROM User AS u LEFT JOIN Activity AS a ON u.id = a.user_id
        WHERE u.has_labels = 1
        GROUP BY u.id
    """
    connector.cursor.execute(query)
    result = connector.cursor.fetchall()
    columns = [desc[0] for desc in connector.cursor.description]
    df = pd.DataFrame(columns=columns, data=result)
    df = df.set_index("id")
    df = df.idxmax(axis=1)
    print("Users most used transportation mode")
    print(df)
    

def main():
    connector = DbConnector()
    
    # table1(connector)
    # print()
    # table2(connector)
    # print()
    # table3(connector)
    # print()
    # query1(connector)
    # print()
    # query2(connector)
    # print()
    # query3(connector)
    # print()
    # query4(connector)
    # print()
    # query5(connector)
    # print()
    # query6a(connector)
    # print()
    # query6b(connector)
    # print()
    # query7(connector)
    # print()
    # query8(connector)
    # print()
    query9(connector) 
    print()
    query10(connector)
    print()
    query11(connector)


    connector.close_connection()

if __name__ == "__main__":
    main()