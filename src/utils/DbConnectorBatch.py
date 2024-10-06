from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from decouple import config

class DbConnectorBatch:
    """
    Connects to the MySQL server on the Ubuntu virtual machine using SQLAlchemy.
    Connector needs HOST, DATABASE, USER and PASSWORD to connect,
    while PORT is optional and should be 3306.
    
    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    DATABASE = "testdb" // Database name, if you just want to connect to MySQL server, leave it empty
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 HOST=f"tdt4225-{config('group_number')}.idi.ntnu.no",
                 DATABASE=config("database_name"),
                 USER=config("user_name"),
                 PASSWORD=config("password")):
        # SQLAlchemy connection string
        connection_string = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}"

        # Create SQLAlchemy engine
        try:
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            self.Session = sessionmaker(bind=self.engine)  # Session factory for ORM-style operations
            print("Connected to:", self.engine)
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)


    def insert_dataframe(self, df: pd.DataFrame, table_name: str, batch_size=1000):
        """
        Inserts a pandas DataFrame into the specified table in batches.
        
        :param df: Pandas DataFrame to insert.
        :param table_name: The name of the target SQL table.
        :param batch_size: Size of each batch for batch insert.
        """
        try:
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False, chunksize=batch_size)
            print(f"Data inserted successfully into {table_name} in batches of {batch_size}.")
        except Exception as e:
            print(f"ERROR: Failed to insert data into {table_name}: {e}")
