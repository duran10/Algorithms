import dateutil
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import numpy as np
from itertools import chain, zip_longest

start_time = datetime.now()
engine = create_engine('postgresql+psycopg2://postgres:T9fbeAz4#hYKHV@67xM%q7bw9@35.198.66.183:5432/app_analytics')
conn = psycopg2.connect(
    "dbname='app_analytics' user='postgres' host='35.198.66.183' password='T9fbeAz4#hYKHV@67xM%q7bw9'")
cur = conn.cursor()

sql = f"""
select r."assigneeId",r."shiftId", s."venueId",r."startTime",r."endTime", r.management, r.overall, r.teamwork 
from ratings r
inner join shift s on r."shiftId"=s."shiftid"
group by r."assigneeId",r."shiftId", s."venueId",r."startTime",r."endTime", r.management, r.overall, r.teamwork 
order by  s."venueId", r."startTime" 

 """
ratings_df = pd.read_sql_query(sql, conn)
start_time2 = datetime.now()

ratings_df1= ratings_df.copy()
start = ratings_df1['startTime']
end = ratings_df1['endTime']
ratings_df1['Difference'] = (end - start)
diff = ratings_df1['Difference']
ratings_df1['Difference'] = diff.dt.total_seconds()
diff_seconds = ratings_df1['Difference']


# This finds overlapping times for at least 50% and matches indexes
def common_row(x):
    rows = ratings_df1.loc[(ratings_df1.index != x.name) & (ratings_df1.venueId == x.venueId), :]
    overlap = [min(x.endTime - y.startTime, y.endTime - x.startTime).total_seconds() >= x.Difference * 0.5 for
               y in rows.itertuples()]
    shared = rows.index[overlap].values
    # print(shared)
    if shared.size > 0:
        return list(shared)


ratings_df1['Overlap_Indexes'] = ratings_df1.apply(lambda x: common_row(x), axis = 1)

# This finds overlapping times and matches management ratings
def management(x):
    rows = ratings_df1.loc[(ratings_df1.index != x.name) & (ratings_df1.venueId == x.venueId), :]
    overlap = [min(x.endTime - y.startTime, y.endTime - x.startTime).total_seconds() >= x.Difference * 0.5 for
               y in rows.itertuples()]
    shared = rows.index[overlap].values
    # print(shared)
    if shared.size > 0:
        return list(ratings_df1.loc[shared[0:], 'management'])


ratings_df1['management_overlap'] = (ratings_df1.apply(lambda x: management(x), axis=1))

# Create an empty list
Row_list = []
# Iterate over each row
for index, rows in ratings_df1.iterrows():
    my_list = [rows.management]
    Row_list.append(my_list)
ratings_df1['management'] = Row_list

# This joins ratings with management matching ratings and creates an average for every list
management_list = ratings_df1['management'].tolist()
management_overlap_list = ratings_df1['management_overlap'].tolist()
ratings_df1['management_average'] = list(zip_longest(management_list, management_overlap_list))
management_average_list = [list(xi for xi in x if xi is not None) for x in ratings_df1['management_average']]
ratings_df1['management_average'] = management_average_list
management_average_list = [sum(ll, []) for ll in ratings_df1['management_average']]
ratings_df1['management_average'] = management_average_list
ratings_df1['management_average'] = list(map(lambda x: sum(x) / len(x), management_average_list))
#ratings_df1['management_average_list'] = management_average_list


# This finds overlapping times and matches overall ratings
def overall(x):
    rows = ratings_df1.loc[(ratings_df1.index != x.name) & (ratings_df1.venueId == x.venueId), :]
    overlap = [min(x.endTime - y.startTime, y.endTime - x.startTime).total_seconds() >= x.Difference * 0.5 for
               y in rows.itertuples()]
    shared = rows.index[overlap].values
    # print(shared)
    if shared.size > 0:
        return list(ratings_df1.loc[shared[0:], 'overall'])


ratings_df1['overall_overlap'] = (ratings_df1.apply(lambda x: overall(x), axis=1))

# Create an empty list
Row_list = []
# Iterate over each row
for index, rows in ratings_df1.iterrows():
    my_list = [rows.overall]
    Row_list.append(my_list)
ratings_df1['overall'] = Row_list

# This joins overall ratings with overall matching ratings and creates an average for every list
overall_list = ratings_df1['overall'].tolist()
overall_overlap_list = ratings_df1['overall_overlap'].tolist()
ratings_df1['overall_average'] = list(zip_longest(overall_list, overall_overlap_list))
overall_average_list = [list(xi for xi in x if xi is not None) for x in ratings_df1['overall_average']]
ratings_df1['overall_average'] = overall_average_list
overall_average_list = [sum(ll, []) for ll in ratings_df1['overall_average']]
ratings_df1['overall_average'] = overall_average_list
ratings_df1['overall_average'] = list(map(lambda x: sum(x) / len(x), overall_average_list))
#ratings_df1['overall_average_list'] = overall_average_list

# This finds overlapping times and matches teamwork ratings
def teamwork(x):
    rows = ratings_df1.loc[(ratings_df1.index != x.name) & (ratings_df1.venueId == x.venueId), :]
    overlap = [min(x.endTime - y.startTime, y.endTime - x.startTime).total_seconds() >= x.Difference * 0.5 for
               y in rows.itertuples()]
    shared = rows.index[overlap].values
    # print(shared)
    if shared.size > 0:
        return list(ratings_df1.loc[shared[0:], 'teamwork'])


ratings_df1['teamwork_overlap'] = ratings_df1.apply(lambda x: teamwork(x), axis=1)

# Create an empty list
Row_list = []
# Iterate over each row
for index, rows in ratings_df1.iterrows():
    my_list = [rows.teamwork]
    Row_list.append(my_list)
ratings_df1['teamwork'] = Row_list

# This joins ratings with teamwork matching ratings and creates an average for every list
teamwork_list = ratings_df1['teamwork'].tolist()
teamwork_overlap_list = ratings_df1['teamwork_overlap'].tolist()
ratings_df1['teamwork_average'] = list(zip_longest(teamwork_list, teamwork_overlap_list))
teamwork_average_list = [list(xi for xi in x if xi is not None) for x in ratings_df1['teamwork_average']]
ratings_df1['teamwork_average'] = teamwork_average_list
teamwork_average_list = [sum(ll, []) for ll in ratings_df1['teamwork_average']]
ratings_df1['teamwork_average'] = teamwork_average_list
ratings_df1['teamwork_average'] = list(map(lambda x: sum(x) / len(x), teamwork_average_list))
#ratings_df1['teamwork_average_list'] = teamwork_average_list


# Drop unwanted columns
ratings_df1 = ratings_df1.drop(['Difference', 'Overlap_Indexes', 'management_overlap', 'overall_overlap', 'teamwork_overlap'], axis = 1)
# Replace column with old dataframe column
ratings_df1['management'] = ratings_df['management']
ratings_df1['overall'] = ratings_df['overall']
ratings_df1['teamwork'] = ratings_df['teamwork']

# Export dataframe to excel
##overlap_ratings_excel = ratings_df1.to_excel('Overlap_ratings.xlsx', index=True, header=True)

print("Time taken before sql read", (datetime.now() - start_time).total_seconds())
print("Time taken after sql read", (datetime.now() - start_time2).total_seconds())

# Export dataframe to postgres
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS "overlap_ratings";')
conn.commit()

ratings_df1.to_sql('overlap_ratings', engine)

if (conn):
    cur.close()
    conn.close()

print("Script done")