import pandas as pd


df = pd.DataFrame(data=[[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col1", "col2", "col3"])
print(df)
print(df.to_dict("records"))