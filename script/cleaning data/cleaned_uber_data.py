# %%
import pandas as pd

data = pd.read_csv("C:/Users/matur/OneDrive/Project/the_analysis_of_uber_transportation/data/uber_data.csv")
df = pd.DataFrame(data)

# Delete all rows with null values
df = df.dropna()


# Convert all column names to lowercase
df.columns = df.columns.str.lower()

# Convert data type: object to datetime 
columns_to_convert_datetime = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
df[columns_to_convert_datetime] = df[columns_to_convert_datetime].apply(pd.to_datetime)

# Convert data type: float to integer 
columns_covert_to_int = ["passenger_count", "ratecodeid", "payment_type"]
df[columns_covert_to_int] = df[columns_covert_to_int].astype("int64")

# Covert data type: object to string
column_covert_to_str = "store_and_fwd_flag"
df[column_covert_to_str] = df[column_covert_to_str].astype("string")

# Delete rows with zero values
df = df[(df['pickup_longitude'] != 0) & (df['pickup_latitude'] != 0) & (df['dropoff_longitude'] != 0) & (df['dropoff_latitude'] != 0)]
position_columns = ['pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude']
str_datetime_columns = ['tpep_pickup_date', 'tpep_dropoff_date', 'store_and_fwd_flag']

# Iterate through each column
for col in df.select_dtypes(include=['int', 'float']).columns:
    print(col)
    if col in position_columns or col in str_datetime_columns:
        continue
    else:
        # Remove negative values
        df = df[df[col].apply(lambda x: x >= 0)]

df.to_csv('transformed_uber_data.csv', index=False)



