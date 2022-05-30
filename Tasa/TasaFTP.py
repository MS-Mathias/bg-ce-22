import pandas as pd

df = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/TasaFTP.xlsx")

df1 = pd.DataFrame(df)

df1["120 month"] = df1["120 month"] / df1["72 month"]
df1["72 month"] = df1["72 month"] / df1["60 month"]
df1["60 month"] = df1["60 month"] / df1["36 month"]
df1["36 month"] = df1["36 month"] / df1["24 month"]
df1["24 month"] = df1["24 month"] / df1["18 month"]
df1["18 month"] = df1["18 month"] / df1["12 month"]
df1["12 month"] = df1["12 month"] / df1["6 month"]
df1["6 month"] = df1["6 month"] / df1["3 month"]
df1["3 month"] = df1["3 month"] / df1["1 month"]

statsdf1 = df1.describe()