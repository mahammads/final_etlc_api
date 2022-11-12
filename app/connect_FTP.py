import pysftp as sftp
import os 
import boto3
from botocore.exceptions import NoCredentialsError
from app import env
from datetime import date
import zipfile
from ftplib import FTP
from urllib.parse import urlparse
import time
import gzip, shutil
import threading

# FTP_HOST =  env.host
# FTP_USER = env.user
# FTP_PASS = env.password

# function to get the file from ftp server.
def getFile(ftp, filename):
    try:
        local_file_path = env.root_folder + '/' + env.local_file_folder +'/' + filename
        ftp.retrbinary("RETR " + filename ,open(local_file_path, 'wb').write)
        return True
    except Exception as e:
        raise(e)

     

# function for donwloading all the files present in ftp server remote directory.
def download_FTP(ftp,file_name):
    try:
        # files_list = ftp.nlst()
        if env.temp_flag:
            file_name = env.temp_file_list

        if isinstance(file_name, list):
            for file_n in file_name:
                getFile(ftp, file_n)
                print(file_n,'downloaded successfully')
        else:
            getFile(ftp, file_name)
            print(file_name,'downloaded successfully')
        return True
    except Exception as e:
      raise e


# function for unzipping the all zip files present in respective folder.
def unzip(file_name):
    try:
        file_extension = file_name.split('.')[-1]
        file_folder = (os.path.basename(file_name)).rsplit('.',1)[0]

        extracted_file_path = env.root_folder + '/' + env.unzip_folder + '/' + file_folder
        if not os.path.exists(extracted_file_path):
            os.makedirs(extracted_file_path)

        if file_extension == 'zip':# check for ".zip" extension
            with zipfile.ZipFile(file_name,"r") as zip_ref:
                zip_ref.extractall(extracted_file_path)
            os.remove(file_name) # delete zipped file

        if file_extension == 'gz': # check for ".gz" extension
            updated_name = (os.path.basename(file_name)).rsplit('.',1)[0]
            gz_file_name = os.path.join(extracted_file_path,updated_name)
            with gzip.open(file_name,"rb") as f_in, open(gz_file_name,"wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(file_name) # delete zipped file

        print(f"{file_name}file unzip successfully")
        return extracted_file_path
    except Exception as e:
        raise e
        # return False

ACCESS_KEY = env.access_key
SECRET_KEY = env.secret_key
region = env.region

s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, region_name =region)

# function for uploading data to amazon S3 bucket using boto3.
def upload_to_aws(local_file, bucket, s3_file):
    
    try:
        s3.upload_file(local_file, bucket, s3_file)
        # print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

# consolidating all the funcitons.
def process(ftp, down_file_name):
    try:
        download_FTP(ftp, down_file_name)
        t0 = time.time()
        root_dir = env.root_folder
        local_file_folder = os.path.join(root_dir,env.local_file_folder)
        list_zip_files = os.listdir(local_file_folder)
        if len(list_zip_files)!= 0:
            bucket_name = env.s3_bucket_name
            s3_output_folder = env.s3_folder
            today_date = date.today()
            d1 = today_date.strftime("%d-%m-%Y")
            for zip_f in list_zip_files:
                abs_file_path = os.path.join(local_file_folder, zip_f)
                file_folder = (os.path.basename(abs_file_path)).rsplit('.',1)[0]
                s3_folder_name = d1
                extract_file_path = unzip(abs_file_path)

                for file in os.listdir(extract_file_path):
                    local_file_name = os.path.join(extract_file_path, file)
                    s3_file_name =s3_folder_name +'/'+ file
                    try:
                        uploaded = upload_to_aws(local_file_name, bucket_name, s3_file_name)
                    except IsADirectoryError:
                        for sub_file in os.listdir(local_file_name):
                            sub_file_name = os.path.join(local_file_name, sub_file)
                            sub_s3_file_name =s3_file_name +'/'+ sub_file
                            uploaded = upload_to_aws(sub_file_name, bucket_name, sub_s3_file_name)
                shutil.rmtree(extract_file_path) # delete zipped file
                print(f"{extract_file_path} file uploaded successfully")
        print("all file uploaded successfully")
        t1 = time.time()
        total = t1-t0
        print(f"total time taken: {total}")
        return True
    except Exception as e:
        raise e
if __name__ == "__main__":
    pass

