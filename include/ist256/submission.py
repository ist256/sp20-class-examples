import os 
import socket 
import json
import requests
import ipykernel
import time
from datetime import datetime, timezone
from dateutil import parser, tz
import pandas as pd
import urllib3
from minio import Minio
from minio.error import ResponseError


class Submission:
    
    def __init__(self, assignments_url='metadata/assignments.csv', roster_url='metadata/roster.csv', 
                 minio_server='10.30.24.123:9000', access_key='sesuro5pka32vtt',secret_key='c5977GQW2CHF6wsNG5bK'):

        self.__timezone__ = "America/New_York"
        self.set_timezone()

        self.__mc__ = Minio(minio_server, access_key=access_key, secret_key=secret_key, secure=False)

        self.__netid__ = self.get_netid()
        self.__notebook__ = self.get_notebook_path()
        self.__course__, self.__term__, self.__unit__, self.__assignment__, self.__assignment_type__ = self.parse_notebook_path()
        self.__bucket__ = f"{self.__course__}-{self.__term__}"
  
        self.initialize_bucket()

        self.__roster__ = self.load_dataframe(roster_url)
        self.__assignments__ = self.load_dataframe(assignments_url)
        self.__instructor__ = self.__roster__['instructor'][self.__roster__['netid']==self.__netid__][0]
        self.__submit_date__ = datetime.now()
        self.__due_date__ = parser.parse(self.__assignments__['duedate'][ self.__assignments__['unit'] == self.__unit__][self.__assignments__['name'] == self.__assignment__][0])
        self.__time_until_due__ = self.__due_date__ - self.__submit_date__ 
        self.__on_time__ = self.__submit_date__  <= self.__due_date__

    def set_timezone(self):
        '''
        save on a lot of date math.
        '''
        os.environ['TZ'] = self.__timezone__
        time.tzset()
        
    def initialize_bucket(self):        
        if not self.__mc__.bucket_exists(self.__bucket__):
            self.__mc__.make_bucket(self.__bucket__)
        
    def submit(self):
        '''
        Performa a Submissions
        '''
        print(f"NETID       = {self.__netid__}")
        print(f"PATH        = {self.__notebook__}")
        print(f"COURSE      = {self.__course__}")
        print(f"TERM        = {self.__term__}")
        print(f"INSTRUCTOR  = {self.__instructor__}")        
        print(f"UNIT/LESSON = {self.__unit__}")
        print(f"ASSIGNMENT  = {self.__assignment__}")
        print(f"TYPE        = {self.__assignment_type__}")        
        print(f"SUBMIT DATE = {self.__submit_date__}")
        print(f"DUE DATE    = {self.__due_date__}")
        print(f"DUE IN      = {self.__time_until_due__}")
        print(f"ON TIME     = {self.__on_time__}")
        print(f"SAVED TO ???")
        print(f"STATUS ?? OK?")
        print("===============================")
        print(f"BUCKET       = {self.__bucket__}")
        print(f"TIME ZONE    = {self.__timezone__}")
        
        

    def load_dataframe(self, file_url, offset=0):
        csv_file = True if file_url.lower().endswith(".csv") else False
        if csv_file:
            dataframe = pd.read_csv(self.__mc__.get_object(self.__bucket__, file_url))
        else: # on the syllabus
            dataframe = pd.read_html(file_url)[offset]
        return dataframe
    
    def load_roster(self, roster_url):
        if roster_url.lower().endswith(".csv"):
            roster = pd.read_csv(roster_url)
        #todo: html?
        
        return roster
        
    def get_netid(self):
        netid = os.environ.get("JUPYTERHUB_USER").lower()
        hostname = socket.gethostname().lower()
        callback_url = os.environ.get("JUPYTERHUB_OAUTH_CALLBACK_URL").lower()
        activity_url = os.environ.get("JUPYTERHUB_ACTIVITY_URL").lower()
        if callback_url.find(netid)>=0 and activity_url.find(netid)>=0 and hostname.find(netid)>=0:
                return netid
        else:
            raise ValueError(f"Unable to locate NetID={netid} for hostname {hostname}")

    def get_notebook_path(self):
        connection_file = os.path.basename(ipykernel.get_connection_file())
        kernel_id = connection_file.split('-', 1)[1].split('.')[0]
        token = os.environ.get("JUPYTERHUB_API_TOKEN")
        netid = self.__netid__
        response = requests.get(f'http://127.0.0.1:8888/user/{netid}/api/sessions?token={token}')
        response.raise_for_status()
        sessions = response.json()    
        for sess in sessions:
            if sess['kernel']['id'] == kernel_id:
                return sess['notebook']['path']
                break

    def parse_notebook_path(self):
        items = self.__notebook__.split("/")
        if items[5].startswith("CCL"):
            assign_type="Lab"
        elif items[5].startswith("HW"):
            assign_type="Homework"
        else:
            assign_type = "Unknown"
        return items[1], items[2],items[4], items[5], assign_type

    
 