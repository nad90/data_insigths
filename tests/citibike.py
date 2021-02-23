"""script to test the citibike insighs
Put all the data in a mysql DB
Use a mysql requests to obtain the insights results
Compare the 2 dataframes

"""
import pandas as pd
from sqlalchemy import create_engine
from config import dir_infos
from config import db_infos
import os

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
    df_el.to_sql(table_name, mysql_engine, index=False, if_exists="append")

print("GETTING INSIGHTS THROUGH SQL")
query_user = "SELECT year, user_age_at_trip, gender, COUNT(1)  as number_trips " \
             "FROM ( " \
             "SELECT  year(start_time) as year , if(birth_year LIKE'%N%',NULL, (start_time)-birth_year)  as user_age_at_trip, birth_year, CAST(gender as SIGNED) as gender " \
    f"FROM {table_name})tab " \
             "GROUP BY year, user_age_at_trip, gender " \


df_user_test = pd.read_sql(query_user, mysql_engine)

print("COMPARING INSIGHTS")

df_user_agg = pd.read_csv(agg_data_dir + "user_data.csv")

df_user_agg.fillna(-1, inplace=True)
df_user_test.fillna(-1, inplace=True)

df_user_agg.sort_values(by=["year", "user_age_at_trip", "gender"], ascending=[True, False, True], inplace=True)
df_user_test.sort_values(by=["year", "user_age_at_trip", "gender"], ascending=[True, False, True], inplace=True)

if (pd.concat([df_user_test, df_user_agg]).drop_duplicates(keep=False).shape)[0] == 0:
    print("TEST USERS INSIGHTS: OK ")
else:
    print("TEST USERS INSIGHTS: KO. See log file ")
    pd.concat([df_user_test.add_prefix("TEST_"), df_user_agg.add_prefix("SCRIPT_")], axis=1).to_csv(
        agg_data_dir + "TEST_doublons.csv", index=False)
"""


query_geo="SELECT year, start_station_name as name_station,  lat_start_station, long_start_station, count(1) number_trips "\
    "FROM(SELECT   year(start_time) as year , start_station_name, cast(start_station_latitude as signed) lat_start_station, cast(start_station_longitude as signed) as long_start_station  "\
    f"FROM {table_name})tab "\
    "GROUP BY year, start_station_name, lat_start_station, long_start_station "

df_geo_test=pd.read_sql(query_geo, mysql_engine)


df_geo_agg=pd.read_csv(agg_data_dir+"geo_data.csv")
print("df_geo_agg.dtypes")
df_geo_agg.astype({"lat_start_station":"object", "long_start_station":"float"}).dtypes
print(df_geo_agg.dtypes)

print("df_geo_test.dtypes")
#df_geo_test=df_geo_test.astype({"lat_start_station":"object", "long_start_station":"object"}, copy=True).dtypes

#df_geo_test.astype({"lat_start_station":"float", "long_start_station":"float"}).dtypes
#df_geo_agg.sort_values(by=["year", "name_station", "lat_start_station", "long_start_station"], ascending=[True, False, True, True], inplace=True)
#df_geo_test.sort_values(by=["year", "name_station", "lat_start_station", "long_start_station"], ascending=[True, False, True, True], inplace=True)
#df_geo_test.sort_values(by=["year", "name_station", "lat_start_station", "long_start_station"], ascending=[True, False, True, True], inplace=True)

print(df_geo_test.equals(df_geo_agg))

print(df_geo_test)
print(pd.concat([df_geo_test, df_geo_agg]).drop_duplicates(keep=False).shape)
print("PROCESS ENDS")
