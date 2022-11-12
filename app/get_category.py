
import os 
from app import env
import datetime
from ftplib import FTP
from urllib.parse import urlparse
from dateutil import parser
import pandas as pd
import numpy as np
from app.connect_FTP import process

def check_FTP_connection(FTP_HOST,FTP_USER,FTP_PASS):
    try:
        lines = []
        file_name= []
        last_modified = []
        parsed = urlparse(FTP_HOST)
        ftp = FTP(parsed.netloc)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(parsed.path)
        print("connection established successfully.")
        parsed = urlparse(FTP_HOST)
        ftp.dir(parsed.path, lines.append)
        for line in lines:
            tokens = line.split(maxsplit = 9)
            name = tokens[8]
            time_str = tokens[5] + " " + tokens[6] + " " + tokens[7]
            time = parser.parse(time_str)
            file_name.append(name)
            last_modified.append(str(time))
        file_table = pd.DataFrame(list(zip(file_name, last_modified)),columns =['File_Name', 'Last_Modified_Date'])
        latest_files = file_table['File_Name'].tolist()
        file_categories = [file.split('.')[0] for file in latest_files if (file.endswith('.zip') or file.endswith('.gz'))]
        uniq_cat = [*set(file_categories)]
        return file_table, uniq_cat
    except:
        print("error while connecting FTP server.")
        return False


def get_latest_File(FTP_HOST,FTP_USER,FTP_PASS):
    try:
        file_table, cat = check_FTP_connection(FTP_HOST,FTP_USER,FTP_PASS)

        file_table['last_run_date'] = pd.to_datetime(env.last_run_date)
        file_table['Last_Modified_Date'] = pd.to_datetime(file_table['Last_Modified_Date'])

        file_table['latest_files'] = np.where((file_table['Last_Modified_Date'] > file_table['last_run_date']),file_table['File_Name'], '')
        
        filtered_files = file_table.loc[file_table['latest_files'] !='']
        output_table = filtered_files[['File_Name','Last_Modified_Date','last_run_date']]
        latest_files = filtered_files['latest_files'].tolist()
        file_categories = [file.split('.')[0] for file in latest_files if (file.endswith('.zip') or file.endswith('.gz'))]
        uniq_cat = [*set(file_categories)]
        return output_table, uniq_cat
    except Exception as e:
        raise e

def latest_run(host,user,password):

    ftp = check_FTP_connection(host,user,password)
    lat_files, present_cat = get_latest_File(host,user,password)
    
    print(present_cat)   
    lat_files['file_cat']  = np.where(lat_files['File_Name'].str.endswith('.zip')|lat_files['File_Name'].str.endswith('.gz'), lat_files['File_Name'].str.split(".",expand=True)[0], '')
    filtered_cat = lat_files.loc[lat_files['file_cat'] !='']
    get_input = input("please enter the expected categories seperated by ',' to process files: ")
    if len(get_input)!=0:
        user_input  = get_input.split(',')
        user_input = [cat.strip() for cat in user_input]
    else:
        user_input = []
    df_list = []
    if not len(user_input) == 0:
        for cat in user_input:
            files_to_upload = filtered_cat.loc[filtered_cat['file_cat'] == cat]
            df_list.append(files_to_upload)
    final_df = pd.concat(df_list, ignore_index=True)
    print(final_df)
    return final_df

    # print(cat)
if __name__ == "__main__":
    pass
