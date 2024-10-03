import pandas as pd

id = "010"
directory = f"dataset/dataset/Data/{id}/Trajectory/"
df = pd.read_csv(directory + "../labels.txt", delimiter="\t", skiprows=1, header=None)
df = df.set_axis(["start_time", "end_time", "transportation_mode"], axis = 1)
print(df)
