# -*- coding: utf-8 -*-
"""
Created on Sat Jan 30 17:13:33 2021

@author: Thomas     <motthomasn@gmail.com>

Script used to automate the process of exporting a *.LRD file using LifeView software
Script is heavily functionised to facilitate use from other scripts
The exported file will be saved as a comma delimited *.txt file in the below location: 
Google Drive\Vehicles\Honda CBR250RR\Technical\015 - ELECTRICAL SYSTEM\01_ENGINE_MANAGEMENT\LIFE_RACING_F88R\CALIBRATIONS

The standalone script allows the user to select a number of LRD files and export them
sequentially. The user is also asked to specify the export sample rate

Session info such as Cal Name, HW Status, Firmware version, Date/Time of recording, original LRD File Name & session comment
are added to the export file. Because of the process of adding this data to the file, 
exports using the automated method differ slightly from those exported manually from LifeView.
The header info contained on lines 1 & 2 have been removed and the various ### for null values 
have been replaced with empty cells.

Version         Date            Notes
1.0             2021-01-31      Original version
2.0             2024-03-22      Conversion to a class. 
                                Used to be called LifeView_Auto_Export
3.0             2025-06-18      Merged in LVdata class from prep_data v1.0
                                Updated file handling to pathlib.Path
                                Simplified number of functions interacting with LifeView
                                Issues: FutureWarning: Downcasting behavior in `replace` is deprecated and will be removed in a future version. To retain the old behavior, explicitly call `result.infer_objects(copy=False)`. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`

"""

from pywinauto import Application
import os
import win32clipboard
import time
from pathlib import Path
import re
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype
import json


def read_clipboard():
    win32clipboard.OpenClipboard()
    data = win32clipboard.GetClipboardData()
    win32clipboard.CloseClipboard()
    
    return data

def col_to_numeric(col):
    # Mimic of pd.to_numeric(errors="ignore")
    try:
        return pd.to_numeric(col)
    except ValueError:
        return col

class LV:
    
    def __init__(self, file, export=True, read_data=True, output=None, sampleRate=20, dictFile="LifeView_Export_Denumeration.json"):
        """
        

        Parameters
        ----------
        file : string or pathlib
            File path to LRD file
        export : bool, optional
            Flag to indicate if data is to be exported to csv. 
            default is True
        output : string, optional
            File name for export. If None is passed, the original LRD file name is used. 
            default is None.
        sampleRate : int, optional
            Sample rate for csv export in Hz.
            default is 20Hz

        Returns
        -------
        None.

        """
        
        if isinstance(file, Path):
            self.file_path = file
        else:
            self.file_path = Path(file)
        
        if self.file_path.suffix not in [".LRD", ".lrd",  ".SD", ".sd"]:
            # format not correct
            raise SyntaxError("Incorrect file type passed. Valid file extensions are .LRD, .lrd, .SD or .sd")
        
        self.fileName = self.file_path.name
        if Path(dictFile).exists():
            self.dictFile = Path(dictFile)
        else:
            raise ValueError("Invalid denumeration dictionary file path specified")
        
        pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}-\d{2}-\d{2}) plus (\d+h\d+m\d+s) '

        match = re.match(pattern, self.fileName)
        if match:
            date_time, offset = match.groups()
            dt = datetime.strptime(date_time, "%Y-%m-%d %H-%M-%S")
            # delta is only non zero if the start of the log has been written over
            t = datetime.strptime(offset,"%Hh%Mm%Ss")
            # ...and use datetime's hour, min and sec properties to build a timedelta
            delta = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
            self.startTime = dt + delta
        else:
            raise ValueError(f"Invalid filename format: {self.file_path}")
        
        
        self.get_session_info()
        
        if export or read_data:
            # first check the output path
            if output is None:
                output = self.file_path.with_suffix('.csv')
            elif isinstance(output, str):
                output = Path(output)
            
            if output.suffix==".csv":
                self.csvFile = output
            else:
                self.csvFile = output.with_suffix('.csv')
                print("Output file extension changed to .csv")
                
            # then check if a csv matching the criteria already exists at the path
            if not self.csvFile.exists():
                self.export_LRD(self.csvFile.stem, sampleRate)
            else:
                if read_data:
                    print(f"{self.csvFile} already exists. Existing file will be read.")
                else:
                    print(f"{self.csvFile} already exists. Export will not continue.")
            
        self.close_window()
        
        if read_data:
            self.read_exported_csv()
            
            # delete csv after loading
            # self.csvFile.unlink()
            
        
    def get_session_info( self ):
        # function gets the current LifeView Session info and returns it as a dictionary
        # open an LRD with default program. pass the same string as you would to Command Prompt
        # The path should be encased in "" so that whitespaces are ignored
        os.system(f'start "" "{self.file_path}"')
        
        app = Application().connect(path=r"C:\Program Files (x86)\Life Racing\LifeView.exe") # Default backend raises warning that 32-bit program should be automated by 32-bit Python. Passing backend="uia" to Application() suppresses error but key types then do not work.
        self.lifeView = app.window(title_re="^LifeView*")
        
        # https://pywinauto.readthedocs.io/en/latest/code/pywinauto.keyboard.html
        # need to hit ENTER to get into program
        self.lifeView.type_keys("{ENTER}")
        
        # print("Loading file into LifeView...")
        # as it takes time to load the file, wait until LifeView is active before proceeding
        self.lifeView.wait("active", timeout=None, retry_interval=None)
        # print("File Loaded.")
        
        self.lifeView.type_keys("s", vk_packet=False)
        self.lifeView.type_keys("i", vk_packet=False)
        self.lifeView.type_keys("s", vk_packet=False)
        time.sleep(0.1) # wait for 0.1s to allow data to be copied to clipboard
        infoString = read_clipboard().split(")")
        
        for item in infoString:
            if item.strip().startswith("Cal("):
                self.calName = item.split("(")[1]
                hw, cal = self.calName.split("_")[2].split("-")
                self.hwLevel = int(hw) 
                self.calLevel = float(cal)
            elif item.strip().startswith("Cfg("):
                self.cfgName = item.split("(")[1]
            elif item.strip().startswith("Main("):
                self.swLevel = item.split("(")[1]
        self.comment = '. '.join(list(filter(None, infoString[-1].replace("\r","").split("\n")))) # lot going on here. Select the last item in the list, remove carraige returns, split by newline, remove empty items and then join back up again.
    

    def export_LRD( self, output, sampleRate ):
        # arguments:
        # outputFile    -   Name of exported filename without extension or path
        # sampleRate    -   Sample Rate of export file in Hz. Default is 50Hz
        
        # use keyboard shortcuts to add all channels
        # need to include vk_packet = False for letters or it won't work
        self.lifeView.type_keys("c", vk_packet=False)
        self.lifeView.type_keys("a", vk_packet=False)
        self.lifeView.type_keys("{F10}")
        
        # wait for all channels to be loaded
        # print("Loading channels in LifeView. Please wait...")
        self.lifeView.wait("active", timeout=60, retry_interval=None) # default timeout is too short. Most of the time this will take 6-8s but have seen it take up to 30s.
        # print("Channels Loaded.")
        
        # export as csv
        self.lifeView.type_keys("f", vk_packet=False)
        self.lifeView.type_keys("t", vk_packet=False)
        self.lifeView.type_keys("{VK_DOWN}") # choose test file (csv)
        self.lifeView.type_keys("{VK_DOWN}") # choose test file (csv)
        self.lifeView.type_keys("{ENTER}")
        
        # if output is None:
        #     output = self.fileName.split(".")[0]
        self.lifeView.type_keys(output.replace(" ", "{SPACE}")) # enter filename in box
        self.lifeView.type_keys("{ENTER}")
        self.lifeView.type_keys("{ENTER}") # this selects the default directory which is D:\Google Drive\Vehicles\Honda CBR250RR\Technical\015 - ELECTRICAL SYSTEM\01_ENGINE_MANAGEMENT\LIFE_RACING_F88R\CALIBRATIONS
        
        self.lifeView.type_keys("{VK_DOWN}") # choose User defined sample rate
        self.lifeView.type_keys("{VK_SPACE}") # space selects button
        self.lifeView.type_keys("{TAB}") # press tab to enable to typing field
        self.lifeView.type_keys( str(sampleRate) ) # export sample rate
        self.lifeView.type_keys("{ENTER}")
        # print("Exporting data as csv. This may take several minutes. \nPlease Wait...")
        self.lifeView.wait("active", timeout=600, retry_interval=None) # default timeout is too short
        # print("Data exported to csv")
    
    
    def close_window( self ):
        self.lifeView.type_keys("f", vk_packet=False) # file
        self.lifeView.type_keys("x", vk_packet=False) # exit
        self.lifeView.type_keys("y", vk_packet=False) # confirm exit
        
        
    def read_exported_csv(self):
        # this function loads a datafile which has been exported as csv by LifeView
        # and returns the data as a pandas dataframe
        
        # LifeView export is comma delimited, channel names are in row 4 and data afterwards
        # sometimes we error out due to permissions on the csv file after generating it so allow for 10 retries
        for cnt in range(10):
            try:
                dataDf = pd.read_csv( self.csvFile, 
                                   sep=',', 
                                   skiprows=3, 
                                   header=0, 
                                   index_col=None, 
                                   skipinitialspace=True, 
                                   engine='python')
                break
            except PermissionError:
                time.sleep(0.5)
                continue
                    
        # first convert known nans
        # dataDf.replace(r'^#+$', np.nan, regex=True, inplace=True) # pd.NA. Raises FutureWarning: Downcasting behavior in `replace` is deprecated and will be removed in a future version. To retain the old behavior, explicitly call `result.infer_objects(copy=False)`. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`
        dataDf = dataDf.replace(r'^#+$', np.nan, regex=True).infer_objects(copy=False)
        
        # the datatypes here are a problem. Most can be cast to float64 after the ##### values have been interpreted as NaN but runTime cannot be converted. 
        # it needs seperate conversion from hh:mm:ss.00 to ss.00
        
        for item in ("runTime", "onTime"):
            # runTime & onTime are exported as a string in the format h:mm:ss.f. We will convert it to ss.f.
            if item in dataDf:                
                # first parse the string to a datetime timestamp
                dataDf[item] = pd.to_datetime(dataDf[item], format='%H:%M:%S.%f') # returns datetime64[ns]
                
                # then get seconds with fractions by calculation
                dataDf[item] = (dataDf[item].dt.hour*60 + dataDf[item].dt.minute)*60 + dataDf[item].dt.second + (dataDf[item].dt.microsecond / 1000000)
            
        
        # convert string enumerations back to numeric
        dataDf = self.denumerate(dataDf)
        
        # convert remaining columns to numeric and add absolute timestamp
        dataDf = dataDf.apply(col_to_numeric) # convert all columns of DataFrame
        dataDf["timeStamp"] = self.startTime + ((dataDf['Time']*1000000000).astype("timedelta64[ns]")) 
        
        self.data = dataDf.copy() # helps avoid fragmentation warnings
        
    
    def denumerate(self, df):
        # function uses a json dictionary to map string values of enumerated channels back to numeric
        
        with open(self.dictFile, "r") as f:
            translation = json.load(f)
            
        # df.replace(translation, inplace=True) # check. this does it in one shot and I think aborts the replacement if some values not in the dictionary
        df = df.replace(translation).infer_objects(copy=False)

        for k, v in translation.items():
            if k in df:
                # self.data.replace({k: v}, inplace=True) 
                # self.data[k] = self.data[k].map(v).fillna(self.data[k]) # this should do the mapping and pass the original value if a translation wasn't available but it doesn't seem to work
                # check if all values got converted and raise error if not
                if not is_numeric_dtype(df[k]):
                    raise ValueError(f'Not all values of channel {k} were converted to numeric. Check input data and translation')
        return df
