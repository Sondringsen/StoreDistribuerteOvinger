from utils.DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
from utils.data_utils import calculate_distance, calculate_altitudes, calculate_invalid

def table1(db):
    collection = db["User"]

    result = collection.find().limit(10)
    print("First 10 entries of User")
    df = pd.DataFrame(list(result))
    print(df)

def table2(db):
    collection = db["Activity"]

    result = collection.find().limit(10)
    print("First 10 entries of Activity")
    df = pd.DataFrame(list(result))
    print(df)


def table3(db):
    collection = db["TrackPoint"]

    result = collection.find().limit(10)
    print("First 10 entries of TrackPoint")
    df = pd.DataFrame(list(result))
    print(df)

def query1(db):
    for collection_name in ["User", "Activity", "TrackPoint"]:
        collection = db[collection_name]
        count = collection.count_documents({})
        print(f"Collection {collection_name} has {count} documents")


def query2(db):
    collection = db["Activity"]
    pipeline = [
        {"$group": {"_id": "$user_id", "activity_count": {"$count": {}}}},
        {"$group": {"_id": 0, "avg_activity_count": {"$avg": "$activity_count"}}}
    ]
    result = list(collection.aggregate(pipeline))
    print("Average activities per user:")
    print(result[0]["avg_activity_count"])

def query3(db):
    collection = db["Activity"]
    pipeline = [
        {"$match": {"valid_activity": 1}},
        {"$group": {"_id": "$user_id", "activity_count": {"$count": {}}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 20}
    ]

    result = list(collection.aggregate(pipeline))
    print("Users with the most activities:")
    df = pd.DataFrame(result)
    print(df)

def query4(db):
    collection = db["Activity"]
    filter = {"taxi": 1, "valid_activity": 1}
    result = list(collection.distinct("user_id", filter))
    print("Users who have taken taxi")
    df = pd.DataFrame(result)
    print(df)

def query5(db):
    collection = db["Activity"]
    pipeline = [
        {"$match": {"valid_activity": 1}},
        {"$group": {
            "_id": None,
            "walk": {"$sum": "$walk"},
            "bike": {"$sum": "$bike"},
            "bus": {"$sum": "$bus"},
            "taxi": {"$sum": "$taxi"},
            "car": {"$sum": "$car"},
            "subway": {"$sum": "$subway"},
            "train": {"$sum": "$train"},
            "airplane": {"$sum": "$airplane"},
            "boat": {"$sum": "$boat"},
            "run": {"$sum": "$run"},
            "motorcycle": {"$sum": "$motorcycle"}
            }
        }
    ]
    
    result = list(collection.aggregate(pipeline))

    print("Number of times each transportation mode has been used")
    df = pd.DataFrame(result)
    print(df)


def query6a(db):
    collection = db["Activity"]
    pipeline = [
        {"$match": {"valid_activity": 1}},
        {"$project": {"year": {"$year": {"$dateFromString": {"dateString": "$start_date_time", "format": "%Y:%m:%d %H:%M:%S"}}}}},
        {"$group": {"_id": "$year", "activity_count": {"$count": {}}}},
        {"$sort": {"activity_count": -1}},
    ]

    result = list(collection.aggregate(pipeline))
    print("Number of activities per year")
    df = pd.DataFrame(result)
    print(df)


def query6b(db):
    collection = db["Activity"]
    pipeline = [
        {"$match": {"valid_activity": 1}},
        {"$project": {
            "year": {"$year": {"$dateFromString": {"dateString": "$start_date_time", "format": "%Y:%m:%d %H:%M:%S"}}}, 
            "start_date_time": {"$toDate": "$start_date_time"}, 
            "end_date_time": {"$toDate": "$end_date_time"}
            }
        },
        {"$group": {"_id": "$year", "time_spent": {"$sum": {"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600*1000]}}}},
        {"$sort": {"time_spent": -1}},
    ]
    result = list(collection.aggregate(pipeline))
    print("Time spent on activities per year")
    df = pd.DataFrame(result)
    print(df)


def query7(db):
    collection = db["TrackPoint"]
    pipeline = [
        {"$project": {
            "year": {"$year": {"$dateFromString": {"dateString": "$date_time", "format": "%Y:%m:%d %H:%M:%S"}}}, 
            "activity_id": 1,
            "lat": 1, 
            "lon": 1,
            "transportation_mode": 1
            }, 
        },
        {"$match": {"transportation_mode": "walk", "year": 2008}},
    ]
    result = list(collection.aggregate(pipeline))

    collection = db["User"]
    user_activities = list(collection.find({"_id": "010"}, {"activities": 1}))[0]["activities"]

    df = pd.DataFrame(result)
    df = df[["lat", "lon", "activity_id"]]
    df = df[df["activity_id"].isin(user_activities)]
    df = calculate_distance(df)
    
    print("Total kilometers traveled by user 112 in 2008:")
    print(df["distance"].sum())


def query8(db):
    collection = db["TrackPoint"]
    result_tp = list(collection.find({"altitude": {"$ne": -777}}, {"_id": 0, "activity_id": 1, "altitude": 1}))
    df_tp = pd.DataFrame(result_tp)

    collection = db["Activity"]
    result_act = list(collection.find({}, {"_id": 1, "user_id": 1}))
    df_act = pd.DataFrame(result_act)

    df = pd.merge(df_tp, df_act, how="left", left_on="activity_id", right_on="_id")
    df = df.drop(columns=["_id"])
    df = df.rename(columns={"altitude": "alt"})

    df = calculate_altitudes(df)
    print("Users who have gained the most meters")
    print(df.sort_values(ascending=False).head(20)*0.3048)
    
def query9(db):
    collection = db["TrackPoint"]
    result_tp = list(collection.find({}, {"_id": 0, "activity_id": 1, "date_time": 1}))
    df_tp = pd.DataFrame(result_tp)
    
    collection = db["Activity"]
    result_act = list(collection.find({}, {"_id": 1, "user_id": 1}))
    df_act = pd.DataFrame(result_act)

    df = pd.merge(df_tp, df_act, how="left", left_on="activity_id", right_on="_id")
    df = df.drop(columns=["_id"])

    df = calculate_invalid(df)
    print("Invalid activities per user (only includes users who has invalid activities)")
    print(df)


def query10(db):
    collection = db["TrackPoint"]
    result_tp = list(collection.find({"lat": {"$gt": 39.915, "$lt": 39.917}, "lon": {"$gt": 116.396, "$lt": 116.398}}, {"activity_id": 1}))
    df_tp = pd.DataFrame(result_tp)

    collection = db["Activity"]
    result_act = list(collection.find({}, {"_id": 1, "user_id": 1}))
    df_act = pd.DataFrame(result_act)

    df = pd.merge(df_tp, df_act, how="left", left_on="activity_id", right_on="_id")
    df = df.drop(columns=["_id", "ativity_id"])
    df = df.drop_duplicates(subset=["user_id"])

    print("Users who have registered activities in the Forbidden City")
    print(df)


def query11(db):
    collection = db["User"]
    results_user = list(collection.find({"has_labels": 1}, {"_id": 1}))
    df_user = pd.DataFrame(results_user)

    collection = db["Activity"]
    results_act = list(collection.find({"valid_activity": 1}, {"start_date_time": 0, "end_date_time": 0, "valid_activity": 0, "_id": 0}))
    df_act = pd.DataFrame(results_act)

    df = df_act[df_act["user_id"].isin(df_user["_id"])]
    df = df.groupby(by=["user_id"]).sum()
    df = df.idxmax(axis=1)
    
    print("Users most used transportation mode")
    print(df)
    

# def test_query(connector):
#     query = """
#         SELECT DISTINCT activity_id 
#         FROM TrackPoint
#         WHERE YEAR(date_time) = 2000;
#     """
#     connector.cursor.execute(query)
#     result = connector.cursor.fetchall()
#     headers = [description[0] for description in connector.cursor.description]
#     print(tabulate(result, headers))

# def test_query2(connector):
#     query = """
#         SELECT activity_id, date_time, lat, lon, altitude
#         FROM TrackPoint
#         WHERE activity_id = 190
#         LIMIT 10
#     """
#     connector.cursor.execute(query)
#     result = connector.cursor.fetchall()
#     headers = [description[0] for description in connector.cursor.description]
#     print(tabulate(result, headers))



# def test_query3(connector):
#     query = """
#         SELECT COUNT(id) as count_activity
#         FROM Activity
#         WHERE valid_activity = 1
#     """
#     connector.cursor.execute(query)
#     result = connector.cursor.fetchall()
#     headers = [description[0] for description in connector.cursor.description]
#     print(tabulate(result, headers))

def main():
    connector = DbConnector()
    db = connector.db
    
    table1(db)
    print()
    table2(db)
    print()
    table3(db)
    print()
    query1(db)
    print()
    query2(db)
    print()
    query3(db)
    print()
    query4(db)
    print()
    query5(db)
    print()
    query6a(db)
    print()
    query6b(db)
    print()
    query7(db)
    print()
    query8(db)
    print()
    query9(db) 
    print()
    query10(db)
    print()
    query11(db)
    # test_query(db)
    # test_query2(db)
    # test_query3(db)


    connector.close_connection()

if __name__ == "__main__":
    main()