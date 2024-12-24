import pandas as pd
import numpy as np

# Function to read the CSV data
def read_data(file_path):
    try:
        data = pd.read_csv(file_path)
        print("Data loaded successfully!")
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Function to convert timestamp to readable datetime
def convert_timestamp_to_datetime(data):
    try:
        data['Datetime'] = pd.to_datetime(data['Timestamp'], unit='s')
        print("Timestamp converted to datetime!")
        return data
    except Exception as e:
        print(f"Error converting timestamp: {e}")
        return data

# Function to set the datetime as index
def set_datetime_as_index(data):
    try:
        data.set_index('Datetime', inplace=True)
        print("Datetime set as index!")
        return data
    except Exception as e:
        print(f"Error setting datetime as index: {e}")
        return data

# Function to drop a specified column
def drop_column(data, column_name):
    try:
        data.drop(columns=[column_name], inplace=True)
        print(f"Column '{column_name}' dropped!")
        return data
    except Exception as e:
        print(f"Error dropping column '{column_name}': {e}")
        return data

# Function to fill missing values using forward-fill method
def fill_missing_values(data):
    try:
        data.fillna(method='ffill', inplace=True)
        print("Missing values filled using forward-fill!")
        return data
    except Exception as e:
        print(f"Error filling missing values: {e}")
        return data

# Function to calculate price range (High - Low)
def calculate_price_range(data):
    try:
        data['Price_Range'] = data['High'] - data['Low']
        print("Price range calculated!")
        return data
    except Exception as e:
        print(f"Error calculating price range: {e}")
        return data

# Function to calculate moving averages
def calculate_moving_average(data, window_size, column_name):
    try:
        moving_average_column = f"MA_{column_name}_{window_size}"
        data[moving_average_column] = data[column_name].rolling(window=window_size).mean()
        print(f"{window_size}-period moving average of {column_name} calculated!")
        return data
    except Exception as e:
        print(f"Error calculating moving average: {e}")
        return data

# Function to calculate daily returns
def calculate_daily_returns(data):
    try:
        data['Daily_Return'] = data['Close'].pct_change() * 100
        print("Daily returns calculated!")
        return data
    except Exception as e:
        print(f"Error calculating daily returns: {e}")
        return data

# Function to add a column indicating if the Close price increased or decreased
def calculate_close_increased(data):
    try:
        data['Close_Increased'] = (data['Close'].diff() > 0).astype(int)
        print("Close increased/decreased column added!")
        return data
    except Exception as e:
        print(f"Error calculating Close_Increased: {e}")
        return data

# Function to resample the data to daily frequency and calculate mean of Close price
def resample_data_to_daily(data):
    try:
        daily_data = data['Close'].resample('D').mean().to_frame(name='Daily_Close_Mean')
        print("Data resampled to daily frequency!")
        return daily_data
    except Exception as e:
        print(f"Error resampling data: {e}")
        return None

# Function to calculate cumulative volume
def calculate_cumulative_volume(data):
    try:
        data['Cumulative_Volume'] = data['Volume'].cumsum()
        print("Cumulative volume calculated!")
        return data
    except Exception as e:
        print(f"Error calculating cumulative volume: {e}")
        return data

# Function to check data types of columns
def check_data_types(data):
    try:
        print(data.dtypes)
    except Exception as e:
        print(f"Error checking data types: {e}")

# Main function to execute all steps
def process_data(file_path):
    data = read_data(file_path)
    if data is not None:
        data = convert_timestamp_to_datetime(data)
        data = set_datetime_as_index(data)
        data = drop_column(data, 'Timestamp')
        data = fill_missing_values(data)
        data = calculate_price_range(data)
        data = calculate_moving_average(data, 10, 'Close')
        data = calculate_moving_average(data, 30, 'Close')
        data = calculate_daily_returns(data)
        data = calculate_close_increased(data)
        daily_data = resample_data_to_daily(data)
        data = calculate_cumulative_volume(data)
        check_data_types(data)

        return data, daily_data
    else:
        return None, None

# File path to the CSV
file_path = r"C:/Users/44754/Downloads/btcusd.csv"

# Process the data
data, daily_data = process_data(file_path)

# To view the processed data
if data is not None:
    print(data.head())
    print(daily_data.head())
