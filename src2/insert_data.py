from utils.DbConnector import DbConnector
from utils.data_utils import create_doc_lists


def main():
    user_docs, activity_docs, trackpoint_docs = create_doc_lists()

    print(f"user_docs length: {len(user_docs)}")
    print(f"activity_docs length: {len(activity_docs)}")
    print(f"trackpoint_docs length: {len(trackpoint_docs)}")

    connection = DbConnector()
    db = connection.db

    db["User"].insert_many(user_docs)
    db["Activity"].insert_many(activity_docs)
    db["TrackPoint"].insert_many(trackpoint_docs)

    connection.close_connection()

if __name__ == "__main__":
    main()
