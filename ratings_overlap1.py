import pandas as pd
import numpy as np

# Read in data from csv
ratings_df = pd.read_csv('data.csv')

# Convert startTime and endTime columns to datetime objects
ratings_df['startTime'] = pd.to_datetime(ratings_df['startTime'])
ratings_df['endTime'] = pd.to_datetime(ratings_df['endTime'])

# Calculate the difference between startTime and endTime in seconds
ratings_df['Difference'] = (ratings_df['endTime'] - ratings_df['startTime']).dt.total_seconds()

# Find rows that have at least 50% overlap with other rows for the same venueId
overlap = ratings_df.groupby('venueId').apply(lambda x: x[(x['Difference'] / 2) <= (x['endTime'] - x['startTime'])])

# Merge ratings_df with overlap dataframe on venueId and startTime
ratings_df = pd.merge(ratings_df, overlap, on=['venueId', 'startTime'])

# Calculate the mean of the management and overall columns
ratings_df['management_average'] = ratings_df['management'].mean()
ratings_df['overall_average'] = ratings_df['overall'].mean()

# Filter rows where the management_average is not null
ratings_df = ratings_df[ratings_df['management_average'].notnull()]

# Calculate the mean of the management_average and overall_average columns
ratings_df['average'] = ratings_df[['management_average', 'overall_average']].mean(axis=1)
