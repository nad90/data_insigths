"""
Top 100 french surnames by gender
Process and aggregate the data by firstname.

Result
2 files
agg_top100_fr_firstnames_nat: firstname, sexe, nombre(number of kids)
"""
from config import dir_infos
import pandas as pd

data_src_file = dir_infos.SRC_DATA_DIR_FN + "firstnames_nat2019.csv"
data_agg_dir = dir_infos.AGG_DATA_DIR_FN
agg_top100_file = "agg_top100_fr_firstnames_nat.csv"
all_top100_file = "all_top100_fr_firstnames_nat.csv"

print("PROCESS BEGINS")
df = pd.read_csv(data_src_file, sep=";")
df.query('preusuel!="_PRENOMS_RARES"', inplace=True)

# get the top 100 firstnames for each gender
df_males = df.query("sexe==1").groupby(["preusuel", "sexe"]).sum("nombre").sort_values("nombre", ascending=False).head(
    100)
df_males.to_csv(data_agg_dir + agg_top100_file, sep=";")

df_females = df.query("sexe==2").groupby(["preusuel", "sexe"]).sum("nombre").sort_values("nombre",
                                                                                         ascending=False).head(100)
df_females.to_csv(data_agg_dir + agg_top100_file, mode="a", header=False, sep=";")

df.merge(df_males, how="inner", on=["preusuel", "sexe"]).drop(columns="nombre_y").rename(
    columns={"nombre_x": "nombre"}).to_csv(data_agg_dir + all_top100_file, index=False, sep=";")
df.merge(df_females, how="inner", on=["preusuel", "sexe"]).drop(columns="nombre_y").to_csv(
    data_agg_dir + all_top100_file, index=False, sep=";", mode="a", header=False)

print("PROCESS ENDS")
