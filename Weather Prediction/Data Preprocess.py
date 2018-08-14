# -*- coding: utf-8 -*-
"""
This code is used to clean the weather datasets before they are built into training, validation and test sets. This code
    is not intended to be run all at once but rather used piece by piece as various problems arise.
    
    Written Nov 2017
"""

# Pandas allows us to create dataframes.
import pandas as pd
# os allows us to interact with the operating system
import os
# sys allows access to system variable attributes
import sys
# Use numpy to find missing values
import numpy as np

#%%
# Specify the location of the folder containing all the data files
directory = r"C:\Users\Gregory.Renner\Documents\Personal\Programs\Data-Analysis\Weather Data\Aardvark"


# Format every .csv file and add it to a single master file.
def loadCSVs(directory):
    #Initialize a master file to merge the datasets into
    master = pd.DataFrame({'Date_Time' : ["2014-01-22 23:42:00"]})
    for root,dirs,files in os.walk(directory):
        print (str(len(files)) + " files will be formatted and added to the final dataset.")
        j = 0 # Keep track of how many files have loaded.
        for file in files:
            j = j + 1
            if file.endswith(".csv"):
                f = open(root+file, 'r')
                # Print the file name
                print ("Reading file " + str(j) + "... " + os.path.splitext(file)[0])
                data = pd.read_csv(root + file, header=0)
# =============================================================================
#                 data = pd.read_csv(root + file, header=0,
#                                    dtype={"Date_Time": str, "Wind_Direction": int, "Wind_Speed": float, 
#                                           "Wind_Gust": float, "Air_Temperature": float, "Relative_Humidity": int,
#                                           "Barometric_Pressure": float, "Precipitation_3Hr": float, 
#                                           "Precipitation_6Hr": float, "Windchill": float,
#                                           "Heat_Stress": float, "fits": float})   #read the csv file
# =============================================================================
                # Couldn't figure out how to import as Date_Time format so do the conversion now.
                data['Date_Time'] = pd.to_datetime(data['Date_Time'])
                #data = pd.read_csv(root + file, header=0)
                station = file[:file.find(" ")]
                # Add the station name to every variable in the dataset, excluding time
                for i in list(data)[1:]: #skip the first variable, time
                    data = data.rename(columns={i : station + '_' + i})
                f.close()
                # Append the formatted file to the master.
                master = pd.merge(master, data, how='outer', on="Date_Time")
    print ("Load complete.")
    return master

        
master = loadCSVs(directory=directory)
 
#%%      
#check column data types in dataframe with .dtypes
            #rampart 12-01-01 has the first instance of the error


error_file = "Aardvark 2011-06-01 - 12-31-2011.csv"

#import errrors due to dtype. Lets troubleshoot
data2 = pd.read_csv("C:/Users/Gregory.Renner/Documents/Personal/Programs/Data-Analysis/Weather Data/Aardvark/"+ error_file, header=0,
                   dtype={"Date_Time": str, "Wind_Direction": int, "Wind_Speed": float, 
                          "Wind_Gust": float, "Air_Temperature": float, "Relative_Humidity": int,
                          "Barometric_Pressure": float, "Precipitation_3Hr": float, 
                          "Precipitation_6Hr": float, "Windchill": float,
                          "Heat_Stress": float, "fits": float})   #read the csv file

data2 = pd.read_csv("C:/Users/Gregory.Renner/Documents/Personal/Programs/Data-Analysis/Weather Data/Aardvark/"+ error_file, header=0)
    
print(data2.info()) #size of dataframe in Mbytes
print(data2.dtypes) 
# Change the varible types from objects to less memory intense dtypes
data2[['Wind_Direction', 'Air_Temperature', 'Relative_Humidity', 'Windchill']] = data2[['Wind_Direction', 'Air_Temperature', 'Relative_Humidity', 'Windchill']].astype(int)
data2[['Wind_Speed', 'Wind_Gust', 'Barometric_Pressure', 'Precipitation_3Hr', 'Precipitation_6Hr', 'Heat_Stress', 'fits']] = data2[['Wind_Speed', 'Wind_Gust', 'Barometric_Pressure', 'Precipitation_3Hr', 'Precipitation_6Hr', 'Heat_Stress', 'fits']].astype(float)
data2['Date_Time'] = pd.to_datetime(data2['Date_Time'])
# Changing the varible types reduced memory usage by 87%

# Find specific values in a column
data2[(data2.Date_Time == '1/22/2014 23:38')]

# Find specific values throughout the dataframe
np.where(data2.applymap(lambda x: x == '\\N'))

# Check for null values.
np.where(pd.isnull(data2))

# View a specific row or column of data.
data2.iloc[400578, :]

# View an array of unique values in a column.
data2.Barometric_Pressure.unique()

# Save the dataframe to a csv.
data2.to_csv(path_or_buf=directory + error_file, index = False)

#%%
# Memory usage is a problem. We can't load the dataset into RAM so let's try downcasting variable types to save space.
def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)


#%%
directory = r"C:\Users\Gregory.Renner\Documents\Personal\Programs\Data-Analysis\Weather Data\Aardvark"

def assembleStation(directory):
    # Some weather stations have data split across multiple files. Append those files so that each
    #   weather station has a single csv.
    #Initialize a master file to merge the datasets into
    master = pd.DataFrame({'Date_Time' : ["2011-06-06 19:42:00"]})
    for root,dirs,files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                print(file)
                data = pd.read_csv(root + '\\' + file, header=0)
                master = master.append(data)
    master = master[1:]
    return master

master = assembleStation(directory=directory)

# Conver the data types.
master['Date_Time'] = pd.to_datetime(master['Date_Time'])
master['Air_Temperature'] = pd.to_numeric(master['Air_Temperature'], errors='coerce', downcast='float')
master['Barometric_Pressure'] = pd.to_numeric(master['Barometric_Pressure'], errors='coerce', downcast='float')
master['Heat_Stress'] = pd.to_numeric(master['Heat_Stress'], errors='coerce', downcast='float')
master['Precipitation_3Hr'] = pd.to_numeric(master['Precipitation_3Hr'], errors='coerce', downcast='float')
master['Precipitation_6Hr'] = pd.to_numeric(master['Precipitation_6Hr'], errors='coerce', downcast='float')
master['Relative_Humidity'] = pd.to_numeric(master['Relative_Humidity'], errors='coerce', downcast='float')
master['Wind_Direction'] = pd.to_numeric(master['Wind_Gust'], errors='coerce', downcast='float')
master['Wind_Gust'] = pd.to_numeric(master['Wind_Gust'], errors='coerce', downcast='float')
master['Wind_Speed'] = pd.to_numeric(master['Wind_Speed'], errors='coerce', downcast='float')
master['Windchill'] = pd.to_numeric(master['Windchill'], errors='coerce', downcast='float')
master['fits'] = pd.to_numeric(master['fits'], errors='coerce', downcast='float')

# Export
out_dir = r"C:\Users\Gregory.Renner\Documents\Personal\Programs\Data-Analysis\Weather Data Clean"
master.to_csv(path_or_buf=out_dir + r'\Aardvark.csv', index = False)


