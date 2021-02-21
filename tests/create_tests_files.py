"""Creates test files
Takes the first 10000 lines from the scrapped files and put it in a file with the same name"""
import pandas as pd
from config import dir_infos
import os
import csv

src_data_dir= dir_infos.SRC_DATA_DIR_CB
tst_data_dir=dir_infos.TST_DATA_DIR_CB

i=0
for file in os.listdir(src_data_dir)[0:10]:
    i+=1
    print(i,"/10 ", file)
    df=pd.read_csv(src_data_dir+file)
    df.head(10000).to_csv(tst_data_dir + file, index=False, quoting=csv.QUOTE_ALL)

