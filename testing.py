import pandas as pd

from src import raw_path
from src.extract import extract_csv
pd.set_option("display.max.rows", None)
pd.set_option("display.width", None)


df = extract_csv(raw_path, "olistPW_2017_orders.csv", ",")
#df1 = extract_csv(raw_path, "olistPW_2017_categories.csv", ",")
#df["stato_ordine"].unique()
u = df["order_status"].unique()
#u1 = df1["product_category_name_english"].unique()
#diff = set(u) - set(u1)
print(u)