# -*- coding: utf-8 -*-
"""
This script builds a single dataset of weather data collected from stations around the Colorado 
    Springs and Monument areas. This dataset will be used to build ML algorithms to short term predictions
    of out of limits flying conditions at the USAFA Academy airfield.
    
    Written Nov 2017
"""

# Pandas allows us to create dataframes.
import pandas as pd
# os allows us to interact with the operating system
import os

# Data is too lage to load into memory so downcast the variables to save space.
def downcast(data): 
    # Downcast all integer variables
    data_int = data.select_dtypes(include=['int'])
    converted_int = data_int.apply(pd.to_numeric,downcast='unsigned')
    # Downcast all float variables
    data_float = data.select_dtypes(include=['float'])
    converted_float = data_float.apply(pd.to_numeric,downcast='float')
    optimized_data = data.copy()
    optimized_data[converted_int.columns] = converted_int
    optimized_data[converted_float.columns] = converted_float
    return optimized_data

# Specify the location of the folder containing all the data files
directory = "/home/greg/Documents/Projects/Data-Analysis/Weather Prediction/Training Data/"

# Format every .csv file and add it to a single master file.
def loadCSVs(directory):
    #Initialize a master file to merge the datasets into
    master = pd.DataFrame({'Date_Time' : ["2011-06-06 19:42:00"]})
    master["Date_Time"] = pd.to_datetime(master["Date_Time"])
    for root,dirs,files in os.walk(directory):
        print (str(len(files)) + " files will be formatted and added to the final dataset.")
        j = 0 # Keep track of how many files have been processed.
        for file in files:
            if file.endswith(".csv"):
                j = j +1
                f=open(root+file, 'r')
                # Print the file name.
                print ("Reading file " +str(j) + "... " + os.path.splitext(file)[0])
                data = pd.read_csv(root + file, header=0,
                                   dtype={"Date_Time": str, "Wind_Direction": int, "Wind_Speed": float, 
                                          "Wind_Gust": float, "Air_Temperature": float, "Relative_Humidity": int,
                                          "Barometric_Pressure": float, "Precipitation_3Hr": float, 
                                          "Precipitation_6Hr": float, "Windchill": float,
                                          "Heat_Stress": float, "fits": float})   #read the csv file
                # Couldn't figure out how to import as Date_Time format so do the conversion now.
                data['Date_Time'] = pd.to_datetime(data['Date_Time'])
                # Downcast variables to save memory.
                data = downcast(data)
                station = file[:file.find(" ")]
                # Add the station name to every variable in the dataset, excluding time
                for i in list(data)[1:]: #skip the first variable, time
                    data = data.rename(columns={i : station + '_' + i})
                f.close()
                # Append the formatted file to the master.
                master = pd.merge(master, data, how='outer', on="Date_Time")
                print("Merged")
    print ("Load complete.")
    return master

        
master = loadCSVs(directory=directory)

# Use only 2014 data as the training set.
master = master[((master.Date_Time >= "2014-01-01 00:00:00") & (master.Date_Time < "2015-01-01 00:00:00"))]



