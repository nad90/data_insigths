"""script to test the citibike insighs
Put all the data in a mysql DB
Use a mysql requests to obtain the insights results
Compare the 2 dataframes

"""
import pandas as pd
from sqlalchemy import create_engine
from config import dir_infos
from config import db_infos

db_user = db_infos.DB_USER
db_pass = db_infos.DB_PASSWORD
db_host = db_infos.DB_HOST
db_name = "test_data_insights"
table_name = "citibikes"
tst_data_dir = dir_infos.TST_DATA_DIR_CB
agg_data_dir = dir_infos.AGG_DATA_DIR_CB

mysql_engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}")

columns_list = ["trip_duration", "start_time", "stop_time", "start_station_id", "start_station_name",
                "start_station_latitude",
                "start_station_longitude", "end_station_id", "end_station_name", "end_station_latitude",
                "end_station_longitude", "bike_id", "user_type", "birth_year", "gender"]

print("PROCESS BEGINS")
"""
print("CREATING TABLE IF NOT EXISTS")

try:
    df=pd.DataFrame(columns=columns_list)

    df.to_sql(name=table_name, con=mysql_engine, index=False)
except ValueError as e:
    print(e)
    exit(1)


print("WRITING DATA IN THE DB")
list_files=os.listdir(tst_data_dir)
i=0
for file in list_files:
    i+=1
    print(file, i,"/", len(list_files))
    df_el=pd.read_csv(tst_data_dir + file)
    df_el.columns=columns_list
    df_el.head(10000).to_sql(table_name, mysql_engine, index=False, if_exists="append")
"""
print("GETTING INSIGHTS THROUGH SQL")
query_user = "SELECT year, user_age_at_trip, gender, COUNT(1)  as number_trips " \
             "FROM ( " \
             "SELECT  year(start_time) as year , if(birth_year LIKE'%N%',NULL, (start_time)-birth_year)  as user_age_at_trip, birth_year, CAST(gender as SIGNED) as gender " \
    f"FROM {table_name})tab " \
             "GROUP BY year, user_age_at_trip, gender " \
    # "ORDER BY year, user_age_at_trip desc, gender;"

df_user_test = pd.read_sql(query_user, mysql_engine)

print("COMPARING INSIGHTS")

df_user_agg = pd.read_csv(agg_data_dir + "user_data.csv")

df_user_agg.fillna(-1, inplace=True)
df_user_test.fillna(-1, inplace=True)
#df_user_test.astype({"gender":"int64"}).dtypes
#pd.to_numeric(df_user_test["gender"])

df_user_agg.sort_values(by=["year", "user_age_at_trip", "gender"], ascending=[True, False, True], inplace=True)
df_user_test.sort_values(by=["year", "user_age_at_trip", "gender"], ascending=[True, False, True], inplace=True)
print ("YOUPI", df_user_test.equals(df_user_agg))
#print(pd.concat([df_user_test, df_user_agg]).drop_duplicates(keep=False).head())
print(pd.concat([df_user_test, df_user_agg]).drop_duplicates(keep=False).shape)
if(pd.concat([df_user_test, df_user_agg]).drop_duplicates(keep=False).shape)[0]==0:
    print("TEST USERS INSIGHTS: OK ")
else:
    pd.concat([df_user_test.add_prefix("TEST_"),df_user_agg.add_prefix("SCRIPT_")], axis=1).to_csv(agg_data_dir+"TEST_doublons.csv", index=False)
"""

print("df_user_test.dtypes")
print(df_user_test.dtypes)
print("df_user_agg.dtypes")
print(df_user_agg.dtypes)






df_user_agg.reset_index( inplace=True, drop=True)


#pd.concat([df_user_test,df_user_agg], axis=1).to_csv(agg_data_dir+"TEST_doublons.csv", index=False)
test_agg= df_user_agg
test_agg["user_age_at_trip"]=test_agg["user_age_at_trip"].fillna(-1)
print(df_user_agg.head())
print(test_agg.head())
print(pd.concat([df_user_test,test_agg ]).drop_duplicates(keep=False).shape)
"""

print("PROCESS ENDS")
