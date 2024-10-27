import os
from os.path import join
import pandas as pd
import numpy as np
from haversine import haversine, Unit
from datetime import timedelta, datetime

def calculate_distance(df):
    def subfunction(sub_df):
        distances = []
        for i in range(len(sub_df)-1):
            distance = haversine((sub_df.iloc[i, 0], sub_df.iloc[i, 1]), (sub_df.iloc[i + 1, 0], sub_df.iloc[i + 1, 1]), unit=Unit.KILOMETERS)
            distances.append(distance)
        distances.append(0)
        distances = np.array(distances)
        distances = np.where(distances<=10, distances, 0)
        return np.sum(distances)
    df = df.groupby(by="activity_id").apply(subfunction).reset_index()
    df = df.rename(columns={0: "distance"})
    return df

def calculate_altitudes(df):
    def sub_function(alt_series):
        alts = alt_series[(alt_series >= -300) & (alt_series <= 50_000)]
        alts = (alt_series - alt_series.shift()).dropna()
        alts = np.where((alts > 0) & (alts <= 300), alts, 0)
        return np.sum(alts)

    df = df.groupby(by=["user_id", "activity_id"]).apply(sub_function)
    df = df.groupby(by=["user_id"]).apply("sum")
    return df

def calculate_invalid(df):
    def sub_function(activity_series):
        # activity_series = activity_series["date_time"]
        activity_series = pd.to_datetime(activity_series, format="%Y:%m:%d %H:%M:%S")
        activity_series = activity_series.diff().dropna()
        if (activity_series > timedelta(minutes=5)).any():
            return 1
        else:
            return 0
        
    df = df.groupby(by=["user_id", "activity_id"])["date_time"].apply(sub_function).reset_index()
    df = df.rename(columns={"date_time": "invalid"})

    invalid_activity_counts = df.groupby("user_id")["invalid"].apply("sum").reset_index()
    invalid_activity_counts.columns = ["user_id", "invalid_activity_count"]

    return invalid_activity_counts

    

def get_labeled_users():
    with open("dataset/dataset/labeled_ids.txt") as id_file:
        label_ids = np.array(id_file.read().splitlines())

    return label_ids

def create_user_ids():
    user_ids = []
    for digit1 in range(2):
        for digit2 in range(10):
            for digit3 in range(10):
                user_id = str(digit1) + str(digit2) + str(digit3)
                if int(user_id) > 181:
                    break
                user_ids.append(str(digit1) + str(digit2) + str(digit3))

    return np.array(user_ids)

def create_transportation_list(transportation_list):
    possible_transportation_modes = ["walk", "bike", "bus", "taxi", "car", "subway", "train", "airplane", "boat", "run", "motorcycle"]
    binary_list = []

    for possible_transportation in possible_transportation_modes:
        if possible_transportation in transportation_list:
            binary_list.append(1)
        else:
            binary_list.append(0)
    return binary_list

def add_transportation_mode_tp(tp_df, transportation_mode_df):
    tp_df["transportation_mode"] = "N/A"

    for _, row in transportation_mode_df.iterrows():
        start_time, end_time, transportation_mode = row["start_time"], row["end_time"], row["transportation_mode"]
        tp_df.loc[(tp_df.loc[:, "date_time"] >= start_time) & (tp_df.loc[:, "date_time"] <= end_time), "transportation_mode"] = transportation_mode

    return tp_df

def create_doc_lists():
    labeled_ids = get_labeled_users()
    all_user_ids = create_user_ids()

    user_df = pd.DataFrame(columns=["_id", "has_labels"])
    user_df.loc[:, "_id"] = all_user_ids
    user_df.loc[:, "has_labels"] = np.isin(all_user_ids, labeled_ids).astype(int)
    user_docs = user_df.to_dict("records")


    activity_df = pd.DataFrame(columns=["_id", "user_id", "walk", "bike", "bus", "taxi", "car", "subway", "train", "airplane", "boat", "run", "motorcycle", "valid_activity", "start_date_time", "end_date_time"])
    trackpoint_df = pd.DataFrame(columns=["activity_id", "lat", "lon", "altitude", "transportation_mode", "date_days", "date_time"])

    activity_id = 0
    for user_num, user_id in enumerate(all_user_ids):
        # if user_id != "010" and user_id != "001":
        #     continue
        print(f"Currently extracting data on user {user_id}")
        directory = f"dataset/dataset/Data/{user_id}/Trajectory"

        # Checking if user has labeled activities, and if so loads the labels
        if user_id in labeled_ids:
            labeled_id = True
            labeled_df = pd.read_csv(directory + "/../labels.txt", delimiter="\t", skiprows=1, header=None)
            labeled_df = labeled_df.set_axis(["start_time", "end_time", "transportation_mode"], axis = 1)
            labeled_df["start_time"] = pd.to_datetime(labeled_df["start_time"], format="%Y/%m/%d %H:%M:%S")
            labeled_df["end_time"] = pd.to_datetime(labeled_df["end_time"], format="%Y/%m/%d %H:%M:%S")

            labeled_df["start_time"] = labeled_df["start_time"].dt.strftime('%Y:%m:%d %H:%M:%S')
            labeled_df["end_time"] = labeled_df["end_time"].dt.strftime('%Y:%m:%d %H:%M:%S')
        else:
            labeled_id = False

        activity_ids_to_user = []
        # Looping through all the activities for a user
        for file in os.listdir(directory):
            df = pd.read_csv(join(directory, file), skiprows=6, header=None)
            if df.shape[0] > 2500:
                continue
            else:
                activity_id += 1
                activity_ids_to_user.append(activity_id)
            df = df.set_axis(["lat", "lon", "field3", "altitude", "date_days", "date_time1", "date_time2"], axis = 1)
            
            df["date_time"] = df["date_time1"] + ' ' + df["date_time2"]
            df["date_time"] = pd.to_datetime(df["date_time"], format="%Y-%m-%d %H:%M:%S")
            df["date_time"] = df["date_time"].dt.strftime("%Y:%m:%d %H:%M:%S")

            df = df.drop(columns=["field3", "date_time1", "date_time2"])

            df["activity_id"] = activity_id

            df = df.loc[(df.loc[:, "lat"] <= 90) & (df.loc[:, "lat"] >= -90), :]
            df = df.loc[(df.loc[:, "lon"] <= 180) & (df.loc[:, "lon"] >= -180), :]
            df = df.loc[((df.loc[:, "altitude"] >= -300) & (df.loc[:, "altitude"] <= 50_000)) | (df.loc[:, "altitude"] == -777), :]

            activity_start_time_tp = df["date_time"].min()
            activity_end_time_tp = df["date_time"].max()

            # Finding which transportation modes apply to an activity (possibly many) and a trackpoint (only one)
            if labeled_id:
                transportation_mode_df = labeled_df.loc[(labeled_df.loc[:, "start_time"] >= activity_start_time_tp) & (labeled_df.loc[: , "end_time"] <= activity_end_time_tp), :]
                labeled_df = labeled_df.drop(index=transportation_mode_df.index) # removing valid activities from the labeled df, so we can deal with invalid activities later
                df = add_transportation_mode_tp(df, transportation_mode_df)
                transportation_mode = transportation_mode_df["transportation_mode"].to_list()
            else:
                df["transportation_mode"] = np.nan
                transportation_mode = []
            transportation_mode = create_transportation_list(transportation_mode)

            activity_row = [[activity_id, user_id, *transportation_mode, 1, activity_start_time_tp, activity_end_time_tp]]
            activity_row = pd.DataFrame(columns=["_id", "user_id", "walk", "bike", "bus", "taxi", "car", "subway", "train", "airplane", "boat", "run", "motorcycle", "valid_activity", "start_date_time", "end_date_time"], data=activity_row)
            activity_df = pd.concat([activity_df, activity_row], ignore_index=True)

            trackpoint_df = pd.concat([trackpoint_df, df], ignore_index=True)
        

        # Insert the invalid activities
        if labeled_id:
            for _, invalid_activity in labeled_df.iterrows():
                activity_id += 1
                activity_ids_to_user.append(activity_id)
                transportation_mode = create_transportation_list([invalid_activity["transportation_mode"]])
                activity_row = [[activity_id, user_id, *transportation_mode, 0, invalid_activity["start_time"], invalid_activity["end_time"]]]
                activity_row = pd.DataFrame(columns=["_id", "user_id", "walk", "bike", "bus", "taxi", "car", "subway", "train", "airplane", "boat", "run", "motorcycle", "valid_activity", "start_date_time", "end_date_time"], data=activity_row)
                activity_df = pd.concat([activity_df, activity_row], ignore_index=True)

        # Add reference to activities for users
        user_docs[user_num]["activities"] = activity_ids_to_user
    return user_docs, activity_df.to_dict("records"), trackpoint_df.to_dict("records")

    
if __name__ == "__main__":
    user_df, activity_df, trackpoint_df = create_doc_lists()
    # print(user_df)
    # print(activity_df)
    # print(trackpoint_df)