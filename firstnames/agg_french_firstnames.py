"""
Process and aggregate the data by firstname
Result
agg_french_firstnames.csv: firstname, sexe, nombre(number of kids)
"""
from config import dir_infos
import pandas as pd

data_src_file=dir_infos.SRC_DATA_DIR_FN+"firstnames_nat2019.csv"
data_agg_dir=dir_infos.AGG_DATA_DIR_FN


print("PROCESS BEGINS")
df=pd.read_csv(data_src_file, sep=";")
df.groupby(["preusuel", "sexe"]).sum("nombre").to_csv(data_agg_dir+"agg_french_firstnames_nat.csv")
print("PROCESS ENDS")