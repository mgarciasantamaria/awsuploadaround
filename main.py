#!/usr/bin/env python
#_*_ codig: utf8 _*_
import os, sys, traceback, datetime, boto3, time, json
from Modules.functions import *
from Modules.Constants import *

if __name__ == '__main__':
    Counter=0
    Replace_list=[]
    aws_session=boto3.Session(profile_name='pythonapps')
    s3=aws_session.client('s3')
    while True:
        try:
            date_log=str(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
#-----------Dato de fecha
            Date=date_log.split(' ')[0]
#-----------Dato de hora
            Hour=date_log.split(' ')[1]
            Counter_Before=Counter
            FILES=os.listdir(source_Path)
            if FILES!=[]:
                for file in FILES:
                    print(f'File {file} Selected')
                    file_Path=f"{source_Path}/{file}"
                    if '.mp4' in file:
                        try:
                            if s3.list_objects_v2(Bucket=bucket, Prefix=file)['KeyCount'] != 0:
                                s3.upload_file(file_Path, bucket, file, Callback=ProgressPercentage(file_Path))
                                os.remove(file_Path)
                                Counter+=1
                                Replace_list.append(file)
                                pass
                            else:
                                s3.upload_file(file_Path, bucket, file, Callback=ProgressPercentage(file_Path))
                                os.remove(file_Path)
                                Counter+=1
                        except:
                            error=sys.exc_info()[2]
                            error_Info=traceback.format_tb(error)[0]
                            print(f'An error occurred while uploading file {file} to bucket ')
                            text_Mail=f"{file} Failed to upload to AWs S3 bucket.\n{str(sys.exc_info()[1])}\n{error_Info}"
                            #Send_Mail(text_Mail, 'Warning S3Upload')
                            continue
                    else:
                        os.remove(file_Path)
            if Counter!=0 and Counter==Counter_Before:
                with open(json_path, "r") as json_file:
                    json_data=json.load(json_file)
                date_json=str(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                json_data[date_json]={
                    "Total videos uploaded": {Counter},
                    "Replace List": {Replace_list}
                }
                with open(json_path, "r") as json_file:
                    json.dump(json_data, json_file)
                text_log=f"Total videos uploaded: {Counter}\nReplace List {Replace_list}"
                print(text_log)
                Counter=0
                Replace_list=[]
                time.sleep(300)
            else:
                text_log=f"Files not found"
                print("File not found\n\t\t\tEnd\n")
                time.sleep(300)
        except:
            error=sys.exc_info()[2]
            error_Info=traceback.format_tb(error)[0]
            text_log=f"Total videos uploaded: {Counter}\nReplace List {Replace_list}"
            print(text_log)
            text_Mail=f"An error occurred while executing the awsupload application on the RUNAPPSPROD server (10.10.130.39)\nTraceback info: {error_Info}\nError_Info:{str(sys.exc_info()[1])}\n\n"+text_log
            print(text_log)
            Send_Mail(text_Mail, "Error awsuploadaround")
            quit()