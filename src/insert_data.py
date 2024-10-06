from utils.DbConnectorBatch import DbConnectorBatch
from utils.dataset_utils import create_dfs

def main():
    user_df, activity_df, trackpoint_df = create_dfs()

    print(f"user_df shape: {user_df.shape}")
    print(f"activity_df shape: {activity_df.shape}")
    print(f"trackpoint_df shape: {trackpoint_df.shape}")

    batch_connector = DbConnectorBatch()
    batch_connector.insert_dataframe(user_df, "User")
    batch_connector.insert_dataframe(activity_df, "Activity")
    batch_connector.insert_dataframe(trackpoint_df, "TrackPoint", 10_000)

if __name__ == "__main__":
    main()
