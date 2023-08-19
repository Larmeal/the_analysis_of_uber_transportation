
import pandas as pd
data = pd.read_csv("C:/Users/armlo/OneDrive/เดสก์ท็อป/Project/Personal_project/the_analysis_of_uber_transportation/script/cleaning data/transformed_uber_data.csv")
df = pd.DataFrame(data)

# Create passenger-count_dim table
passenger_dict = {
    'passenger_count_id': [1, 2, 3, 4, 5, 6, 7],
    'passenger_count': [0, 1, 2, 3, 4, 5, 6]
}

df_passenger = pd.DataFrame(passenger_dict)
df_passenger.to_csv("passenger_count_dim.csv", index=False)
df_passenger.info()

# Create rate_code_dim table
df_ratecode_dict = {
    'rate_code_id' : [1, 2, 3, 4, 5, 6], 
    'ratecodeid' : [1, 2, 3, 4, 5, 6], 
    'rate_code_name' : ['Standard rate', 'JFK', 'Newark', 'Nassau or Westchester', 'Negotiated fare', 'Group ride']
}

df_ratecode = pd.DataFrame(df_ratecode_dict)
df_ratecode['rate_code_name'] = df_ratecode['rate_code_name'].astype('string')
df_ratecode.to_csv("rate_code_dim.csv", index=False)
df_ratecode.info()

# Create payment_type_dim table
payment_type_dict = {
    'payment_type_id': [1, 2, 3, 4, 5, 6], 
    'payment_type': [1, 2, 3, 4, 5, 6], 
    'payment_type_name': ['Credit card', 'Cash', 'No charge', 'Dispute', 'Unknown', 'Voided trip']
}
df_payment_type = pd.DataFrame(payment_type_dict)
df_payment_type['payment_type_name'] = df_payment_type['payment_type_name'].astype('string')
df_payment_type.to_csv("payment_type_dim.csv", index = False)
df_payment_type.info()

# Create pickup_location_dim table
df_pickup_location = df[['pickup_latitude', 'pickup_longitude']]
df_pickup_location = df_pickup_location.drop_duplicates()
df_pickup_location['pickup_location_id'] = range(1, len(df_pickup_location) + 1)
df_pickup_location = df_pickup_location.reindex(columns=['pickup_location_id', 'pickup_latitude', 'pickup_longitude'])
df_pickup_location.to_csv('pickup_location_dim.csv', index=False)
df_pickup_location.info()

# Create dropoff_location_dim table
df_dropoff_location = df[['dropoff_latitude', 'dropoff_longitude']]
df_dropoff_location = df_dropoff_location.drop_duplicates()
df_dropoff_location['dropoff_location_id'] = range(1, len(df_dropoff_location) + 1)
df_dropoff_location = df_dropoff_location.reindex(columns=['dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude'])
df_dropoff_location.to_csv('dropoff_location_dim.csv', index=False)
df_dropoff_location.info()

# Create datetime_dim table

# Convert data type: object to datetime 
columns_to_convert_datetime = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
df[columns_to_convert_datetime] = df[columns_to_convert_datetime].apply(pd.to_datetime)

# Create df_datetime dataframe
df_datetime = df[['tpep_pickup_datetime','tpep_dropoff_datetime']]
df_datetime = df_datetime.drop_duplicates()

# Change format datetime
df_datetime['tpep_pickup_datetime'] = df_datetime['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df_datetime['tpep_dropoff_datetime'] = df_datetime['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %I:%M:%S')

# Create datetime_id column
df_datetime['datetime_id'] = range(1, len(df_datetime) + 1)

# Extract hour, day, month, year, weekday from timestamp
df_datetime['pick_hour'] = df['tpep_pickup_datetime'].dt.hour
df_datetime['pick_day'] = df['tpep_pickup_datetime'].dt.day
df_datetime['pick_month'] = df['tpep_pickup_datetime'].dt.month
df_datetime['pick_year'] = df['tpep_pickup_datetime'].dt.year
df_datetime['pick_weekday'] = df['tpep_pickup_datetime'].dt.dayofweek +1


df_datetime['drop_hour'] = df['tpep_dropoff_datetime'].dt.hour
df_datetime['drop_day'] = df['tpep_dropoff_datetime'].dt.day
df_datetime['drop_month'] = df['tpep_dropoff_datetime'].dt.month
df_datetime['drop_year'] = df['tpep_dropoff_datetime'].dt.year
df_datetime['drop_weekday'] = df['tpep_dropoff_datetime'].dt.dayofweek +1 


# Reundex column
df_datetime = df_datetime.reindex(columns=['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday', 'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday'])

# Export to csv dile
df_datetime.to_csv('datetime_dim.csv', index=False)

# Convert object to datetime 
convert_datetime = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
df_datetime[convert_datetime] = df_datetime[convert_datetime].apply(pd.to_datetime)

df_datetime
df_datetime.info()

# # Merge datetime_id column in fact_table
# merge_data = pd.merge(df, df_datetime[['datetime_id','tpep_pickup_datetime', 'tpep_dropoff_datetime']], on=['tpep_pickup_datetime', 'tpep_dropoff_datetime'], how='left')
# merge_data['datetime_id_new'] = merge_data['datetime_id']
# merge_data.drop(['datetime_id','tpep_pickup_datetime', 'tpep_dropoff_datetime'], axis=1, inplace=True)
# merge_data = merge_data.rename(columns={'datetime_id_new': 'datetime_id'})

# # Merge passenger_count_id column in fact_table
# merge_data = pd.merge(merge_data, df_passenger[['passenger_count_id', 'passenger_count']], on=['passenger_count'], how='left')
# merge_data['passenger_count_id_new'] = merge_data['passenger_count_id']
# merge_data.drop(['passenger_count_id', 'passenger_count'], axis=1, inplace=True)
# merge_data = merge_data.rename(columns={'passenger_count_id_new': 'passenger_count_id'})

# # Merge rate_code_id column in fact_table
# merge_data = pd.merge(merge_data, df_ratecode[['rate_code_id', 'ratecodeid']], on=['ratecodeid'], how='left')
# merge_data['rate_code_id_new'] = merge_data['rate_code_id']
# merge_data.drop(['rate_code_id','ratecodeid'], axis=1, inplace=True)
# merge_data = merge_data.rename(columns={'rate_code_id_new': 'rate_code_id'})

# # Merge pickup_location_id column in fact_table
# merge_data = pd.merge(merge_data, df_pickup_location[['pickup_location_id','pickup_latitude', 'pickup_longitude']], on=['pickup_latitude', 'pickup_longitude'], how='left')
# merge_data['pickup_location_id_new'] = merge_data['pickup_location_id']
# merge_data.drop(['pickup_location_id','pickup_latitude', 'pickup_longitude'], axis=1, inplace=True)
# merge_data = merge_data.rename(columns={'pickup_location_id_new': 'pickup_location_id'})

# # Merge dropoff_location_id column in fact_table
# merge_data = pd.merge(merge_data, df_dropoff_location[['dropoff_location_id','dropoff_latitude', 'dropoff_longitude']], on=['dropoff_latitude', 'dropoff_longitude'], how='left')
# merge_data['dropoff_location_id_new'] = merge_data['dropoff_location_id']
# merge_data.drop(['dropoff_location_id','dropoff_latitude', 'dropoff_longitude'], axis=1, inplace=True)
# merge_data = merge_data.rename(columns={'dropoff_location_id_new': 'dropoff_location_id'})

# Merge payment_type_id column in fact_table
# merge_data = pd.merge(merge_data, df_payment_type[['payment_type_id', 'payment_type']], on=['payment_type'], how='left')
# merge_data['payment_type_id_new'] = merge_data['payment_type_id']
# merge_data.drop(['payment_type_id', 'payment_type'], axis=1, inplace=True)
# merge_data = merge_data.rename(columns={'payment_type_id_new': 'payment_type_id'})

def merge_and_rename(merge_data, df, merge_columns, id_column, id_column_new):
    merge_data = pd.merge(merge_data, df[[id_column, *merge_columns]], on=merge_columns, how='left')
    merge_data[id_column_new] = merge_data[id_column]
    merge_data.drop([id_column, *merge_columns], axis=1, inplace=True)
    merge_data = merge_data.rename(columns={id_column_new: id_column})
    return merge_data

# List of dictionaries containing merge information
merge_info = [
    {   
        'df': df_datetime, 
        'merge_columns': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'], 
        'id_column': 'datetime_id', 
        'id_column_new': 'datetime_id_new'
    },
    {
        'df': df_passenger, 
        'merge_columns': ['passenger_count'], 
        'id_column': 'passenger_count_id', 
        'id_column_new': 'passenger_count_id_new'
    },
    {
        'df': df_ratecode, 
        'merge_columns': ['ratecodeid'], 
        'id_column': 'rate_code_id', 
        'id_column_new': 'rate_code_id_new'
    },
    {
        'df': df_pickup_location,
        'merge_columns': ['pickup_latitude', 'pickup_longitude'],
        'id_column': 'pickup_location_id', 
        'id_column_new': 'pickup_location_id_new'
    },
    {
        'df': df_dropoff_location,
        'merge_columns': ['dropoff_latitude', 'dropoff_longitude'], 
        'id_column': 'dropoff_location_id', 
        'id_column_new': 'dropoff_location_id_new'
    },
    {
        'df': df_payment_type, 
        'merge_columns': ['payment_type'], 
        'id_column': 'payment_type_id', 
        'id_column_new': 'payment_type_id_new'
    }
]

# Initial DataFrame
merge_data = df.copy()

# Perform the merge operations
for merge_dict in merge_info:
    merge_data = merge_and_rename(merge_data, **merge_dict)

# Add trip_id in fact_table
merge_data['trip_id'] = range(1, len(merge_data) + 1)


# Change data type
merge_data['store_and_fwd_flag']  = merge_data['store_and_fwd_flag'].astype('string')
merge_data['trip_distance'] = merge_data['trip_distance'].astype('int64')

# Rename some column
merge_data = merge_data.rename(columns={'vendorid': 'vendor_id', 'trip_distance': 'trip_distance_id'})

# Reindex data
merge_data = merge_data.reindex(columns=[
                                    'trip_id', 
                                    'vendor_id', 
                                    'datetime_id', 
                                    'passenger_count_id', 
                                    'rate_code_id', 
                                    'store_and_fwd_flag', 
                                    'pickup_location_id', 
                                    'dropoff_location_id', 
                                    'payment_type_id', 
                                    'trip_distance_id', 
                                    'fare_amount', 
                                    'extra', 
                                    'mta_tax', 
                                    'tip_amount', 
                                    'tolls_amount', 
                                    'improvement_surcharge', 
                                    'total_amount']
                                )

merge_data.to_csv('fact_table.csv', index='False')

merge_data
merge_data.info()


