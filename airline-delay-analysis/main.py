import pandas as pd
import numpy as np

import datetime

df=pd.read_csv('August 2018 Nationwide.csv')

columns_to_drop = ['ORIGIN_AIRPORT_SEQ_ID', 'ORIGIN_CITY_MARKET_ID',
                   'DEST_AIRPORT_SEQ_ID', 'DEST_CITY_MARKET_ID',
                   'ARR_DELAY', 'DEP_DELAY','ACTUAL_ELAPSED_TIME']

df = df.drop(columns=columns_to_drop)







# Fill missing values with zero for all columns
df.fillna(0, inplace=True)

# Display the updated DataFrame
print(df)
missing_values_count = df.isnull().sum()
missing_values_count[0:10]
print(missing_values_count)

total_cells = np.product(df.shape)
total_missing = df.isnull().sum().sum()
percent_missing = (total_missing/total_cells) * 100
print("Percentage of missing values in the dataset: {:.2f}%".format(percent_missing))

duplicate_rows = df[df.duplicated()]
print(duplicate_rows)



def convert_float_time(float_time):
    if not pd.isna(float_time):
        # Convert the float to an integer
        int_time = int(float_time)
        # Extract hours and minutes
        hours = int(int_time / 100)
        minutes = int_time % 100
        # Format as "HH:MM:00"
        return f"{hours:02d}:{minutes:02d}:00"
    else:
        return float_time
# Apply the conversion function to the "dep_time" column
df['ARR_TIME'] = df['ARR_TIME'].apply(convert_float_time)
df['DEP_TIME'] = df['DEP_TIME'].apply(convert_float_time)
df['CRS_DEP_TIME'] = df['CRS_DEP_TIME'].apply(convert_float_time)


# Display the DataFrame with the modified "dep_time" column
print(df)

df['ARR_TIME'] = pd.to_timedelta(df['ARR_TIME'])

df['CRS_ARR_TIME'] = df['ARR_TIME'] - pd.to_timedelta(df['ARR_DELAY_NEW'], unit='m')

# Convert to string format hh:mm:ss
df['CRS_ARR_TIME'] = df['CRS_ARR_TIME'].apply(lambda td: '{:02d}:{:02d}:{:02d}'.format(td.components.hours,
                                                                                       td.components.minutes,
                                                                                   td.components.seconds))
print(df)
# Convert 'ARR_TIME' to timedelta format
df['ARR_TIME'] = pd.to_timedelta(df['ARR_TIME'], errors='coerce')

# Convert timedelta to datetime and then format as "hh:mm"
df['ARR_TIME'] = (pd.to_datetime('00:00:00') + df['ARR_TIME']).dt.strftime('%H:%M')

# Display the updated DataFrame
print(df[['ARR_TIME']])

# Convert 'CRS_ELAPSED_TIME' and 'ARR_DELAY_NEW' to timedelta format

# Add a new column 'ACTUAL_ELAPSED_TIME' by calculating the sum of 'CRS_Elapsed_Time' and 'ARR_Delay_New'
df['ACTUAL_ELAPSED_TIME'] = df['CRS_ELAPSED_TIME'] + df['ARR_DELAY_NEW']



# Print the DataFrame
print(df)



# Create a dictionary to map Airline IDs to Airline Codes
airline_id_mapping = {
    19393: 'UA', 19790: 'DL', 19805: 'WN', 20304: 'AA', 19977: 'B6',
    20452: 'NK', 20409: 'VX', 20398: 'AS', 20397: 'OH', 20363: 'Y4',
    19930: 'F9', 20378: 'OO', 20416: 'G4', 20366: 'CZ', 20436: '3K',
    20368: '5Y', 19690: 'HA', 19687: 'MQ', 21167: 'UX', 20500: 'VX',
    20046: 'B6', 20427: 'UA', 20237: 'DL', 20445: 'AA', 20263: 'WN',
    20225: 'B6'
}

# Add a new column 'Airline_Code' based on the mapping
df['Airline_Code'] = df['OP_CARRIER_AIRLINE_ID'].map(airline_id_mapping)

# Display the resulting DataFrame
print(df)

airline_name_mapping = {
    'AA': 'American Airlines',
    'DL': 'Delta Air Lines',
    'UA': 'United Airlines',
    'WN': 'Southwest Airlines',
    'AS': 'Alaska Airlines',
    'B6': 'JetBlue Airways',
    'F9': 'Frontier Airlines',
    'HA': 'Hawaiian Airlines',
    'NK': 'Spirit Airlines',
    'G4': 'Allegiant Air',
    'VX': 'Virgin America',
    'SY': 'Sun Country Airlines',
    'OO': 'SkyWest Airlines',
    'YX': 'Republic Airways',
    '9E': 'Endeavor Air',
    'MQ': 'Envoy Air',
    'CP': 'Compass Airlines',
    'OH': 'PSA Airlines',
    'PT': 'Piedmont Airlines',
    'C5': 'CommutAir',
    'EV': 'ExpressJet',
    'G7': 'GoJet Airlines',
    'AX': 'Trans States Airlines',
    'ZW': 'Air Wisconsin',
    'AL': 'SkyWest Airlines',
}

# Add a new column 'Airline_Name' based on the mapping
df['Airline_Name'] = df['Airline_Code'].map(airline_name_mapping)

# Display the resulting DataFrame
print(df)



df['FL_DATE'] = pd.to_datetime(df['FL_DATE'], errors='coerce')



# Add a new column 'DAY_OF_WEEK' with the day of the week
df['DAY_OF_WEEK'] = df['FL_DATE'].dt.day_name()


file_path = 'C:/Users/Student/PycharmProjects/cleaned_data_aug18.csv'

# Save the DataFrame to the specified file path
df.to_csv(file_path, index=False)