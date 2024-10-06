import os
from os.path import join
import pandas as pd
import numpy as np

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

def create_dfs():
    labeled_ids = get_labeled_users()
    all_user_ids = create_user_ids()

    user_df = pd.DataFrame(columns=["id", "has_labels"])
    user_df.loc[:, "id"] = all_user_ids
    user_df.loc[:, "has_labels"] = np.isin(all_user_ids, labeled_ids).astype(int)

    activity_df = pd.DataFrame(columns=["id", "user_id", "walk", "bike", "bus", "taxi", "car", "subway", "train", "airplane", "boat", "run", "motorcycle", "start_date_time", "end_date_time"])
    trackpoint_df = pd.DataFrame(columns=["activity_id", "lat", "lon", "altitude", "date_days", "date_time"])

    
    activity_id = 0
    for user_id in all_user_ids:
        # if user_id != "010" and user_id != "001":
        #     continue
        print(f"Currently extracting data on user {user_id}")
        directory = f"dataset/dataset/Data/{user_id}/Trajectory"
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

        for file in os.listdir(directory):
            df = pd.read_csv(join(directory, file), skiprows=6, header=None)
            if df.shape[0] > 2500:
                continue
            else:
                activity_id += 1
            df = df.set_axis(["lat", "lon", "field3", "altitude", "date_days", "date_time1", "date_time2"], axis = 1)
            
            df["date_time"] = df["date_time1"] + ' ' + df["date_time2"]
            df["date_time"] = pd.to_datetime(df["date_time"], format="%Y-%m-%d %H:%M:%S")
            df['date_time'] = df['date_time'].dt.strftime('%Y:%m:%d %H:%M:%S')

            df = df.drop(columns=["field3", "date_time1", "date_time2"])

            df["activity_id"] = activity_id

            activity_start_time = df["date_time"].min()
            activity_end_time = df["date_time"].max()
            
            # if labeled_id:
            #     transportation_mode = labeled_df.loc[(labeled_df.loc[:, "start_time"] >= activity_start_time) & (labeled_df.loc[: , "end_time"] <= activity_end_time), "transportation_mode"]
            #     if transportation_mode.shape[0] == 0:
            #         transportation_mode = np.nan
            #     else:
            #         print(transportation_mode)
            #         transportation_mode = create_transportation_list(transportation_mode)
            #         print(transportation_mode)
            # else:
            #     transportation_mode = create_transportation_list(transportation_mode)
            
            # TODO: what to do when there are multiple transportation methods during the same activity
            # TODO: check illegal activity reporting? How many are there? Is it something wrong with the code?
            if labeled_id:
                transportation_mode = labeled_df.loc[(labeled_df.loc[:, "start_time"] >= activity_start_time) & (labeled_df.loc[: , "end_time"] <= activity_end_time), "transportation_mode"]
                transportation_mode = transportation_mode.to_list()
            else:
                transportation_mode = []
            transportation_mode = create_transportation_list(transportation_mode)

            activity_row = [[activity_id, user_id, *transportation_mode, activity_start_time, activity_end_time]]
            activity_row = pd.DataFrame(columns=["id", "user_id", "walk", "bike", "bus", "taxi", "car", "subway", "train", "airplane", "boat", "run", "motorcycle", "start_date_time", "end_date_time"], data=activity_row)
            activity_df = pd.concat([activity_df, activity_row], ignore_index=True)

            trackpoint_df = pd.concat([trackpoint_df, df], ignore_index=True)

    # activity_df["start_date_time"] = pd.to_datetime(activity_df["start_date_time"], format="%Y:%m:%d %H:%M:%S")
    # activity_df["end_date_time"] = pd.to_datetime(activity_df["end_date_time"], format="%Y:%m:%d %H:%M:%S")

    return user_df, activity_df, trackpoint_df

    
if __name__ == "__main__":
    user_df, activity_df, trackpoint_df = create_dfs()
    # print(user_df)
    print(activity_df)
    print(activity_df.dtypes)
    # print(trackpoint_df)