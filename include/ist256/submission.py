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
import logging



class Submission:
    
    def __init__(self, assignments_url='metadata/assignments.csv', roster_url='metadata/roster.csv', 
                 minio_server='10.30.24.123:9000', access_key='sesuro5pka32vtt',secret_key='c5977GQW2CHF6wsNG5bK', debug=False):

        if debug:
            logging.basicConfig(format='%(asctime)s %(levelname)s ==> %(message)s', level=logging.DEBUG,datefmt='%m/%d/%Y %I:%M:%S %p')
        else:
            logging.basicConfig(format='%(asctime)s %(levelname)s ==> %(message)s', level=logging.INFO,datefmt='%m/%d/%Y %I:%M:%S %p')
        
        self.__timezone__ = "America/New_York"
        self.set_timezone()

        logging.debug("Minio Info", minio_server, access_key=access_key, secret_key=secret_key)
        self.__mc__ = Minio(minio_server, access_key=access_key, secret_key=secret_key, secure=False)

        self.__netid__ = self.get_netid()
        self.__notebook__ = self.get_notebook_path()
        self.__notebook_full_path__ = f"{os.environ.get('HOME')}/{self.__notebook__}"
        self.__course__, self.__term__, self.__unit__, self.__assignment__, self.__assignment_type__ = self.parse_notebook_path()
        self.__bucket__ = f"{self.__course__}-{self.__term__}"
  
        self.initialize_bucket()

        self.__roster__ = self.load_dataframe(roster_url)
        self.__assignments__ = self.load_dataframe(assignments_url)
        self.__instructor__ = self.__roster__['instructor'][self.__roster__['netid']==self.__netid__].values[0]
        self.__submit_date__ = datetime.now()
        self.__due_date__ = parser.parse(self.__assignments__['duedate'][ self.__assignments__['unit'] == self.__unit__][self.__assignments__['name'] == self.__assignment__].values[0])
        self.__time_until_due__ = self.__due_date__ - self.__submit_date__ 
        self.__on_time__ = self.__submit_date__  <= self.__due_date__

        self.__target__ = self.generate_target()

        
    def generate_target(self):
        late = "LATE-" if not self.__on_time__ else ""
        filename = f"{late}{self.__netid__}.ipynb"
        return f"{self.__instructor__}/{self.__unit__}/{self.__assignment__}/{filename}"

    def set_timezone(self):
        '''
        save on a lot of date math.
        '''
        os.environ['TZ'] = self.__timezone__
        time.tzset()
        
    def initialize_bucket(self):        
        if not self.__mc__.bucket_exists(self.__bucket__):
            self.__mc__.make_bucket(self.__bucket__)
        
    def get_file_date(self):
        file = self.generate_target()
        for o in self.__mc__.list_objects(self.__bucket__, file):
            return o.last_modified.astimezone(tz.gettz(self.__timezone__))
        else:
            return None
        
    def format_date(self,date):
        return date.strftime("%Y-%m-%d %I:%M:%S %p")
        
    def debug(self):
        last_mod = self.get_file_date()
        
        logging.debug(f"NETID       = {self.__netid__}")
        logging.debug(f"PATH        = {self.__notebook__}")
        logging.debug(f"FULL PATH   = {self.__notebook_full_path__}")
        logging.debug(f"COURSE      = {self.__course__}")
        logging.debug(f"TERM        = {self.__term__}")
        logging.debug(f"INSTRUCTOR  = {self.__instructor__}")        
        logging.debug(f"UNIT/LESSON = {self.__unit__}")
        logging.debug(f"ASSIGNMENT  = {self.__assignment__}")
        logging.debug(f"TYPE        = {self.__assignment_type__}")        
        logging.debug(f"SUBMIT DATE = {self.format_date(self.__submit_date__)}")
        logging.debug(f"DUE DATE    = {self.format_date(self.__due_date__)}")
        logging.debug(f"DUE IN      = {self.__time_until_due__}")
        logging.debug(f"ON TIME     = {self.__on_time__}")
        logging.debug(f"SAVE TO     = {self.__target__}")
        logging.debug(f"BUCKET      = {self.__bucket__}")
        logging.debug(f"TIME ZONE   = {self.__timezone__}")
        logging.debug(f"ASSIGN. LAST MOD.= {self.format_date(last_mod)}")        
        return            
        
    def submit(self):
        '''
        Perform a Submission
        '''
        print("=== SUMBISSON DETAILS ===")
        print(f"Your Netid......... {self.__netid__}")
        print(f"Your Instructor.... {self.__instructor__}")        
        print(f"Assigment Name .... {self.__assignment__}")
        print(f"Assignment Type ... {self.__assignment_type__}")        
        print(f"Submission Date ... {self.format_date(self.__submit_date__)}")
        print(f"Due Date .......... {self.format_date(self.__due_date__)}")
        
        last_mod = self.get_file_date()
        if not self.__on_time__:            
            print("\n=== WARNING: Your Submission is LATE! ===")
            print(f"Your Submission Date   : {self.format_date(self.__submit_date__)}")
            print(f"Due Date For Assignment: {self.format_date(self.__due_date__)}")
            late_confirm = input("Submit This Assignment Anyways [y/n] ?").lower()
            if late_confirm == 'n':
                print("Aborting Submission.")
                return
        if last_mod != None:
                print("\n=== WARNING: This is a Duplicate Submission ==")
                print(f"You Submitted This Assigment On: {self.format_date(last_mod)}")
                again = input("Overwrite Your Previous Submission [y/n] ? ").lower()
                if again == 'n':
                    print("Aborting.")
                    return
                
        print("\n=== SUBMITTING  ===")
        print(f"Uploading: {self.__assignment__}\nTo: {self.__target__} ...")
        etag = self.upload_file()
        print(f"Done!\nReciept: {etag}")
            
    def upload_file(self):
        with open (self.__notebook_full_path__, 'rb') as f:
            stats = os.stat(self.__notebook_full_path__)
            etag = self.__mc__.put_object(self.__bucket__, self.__target__, f , stats.st_size )
            return etag        
        
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
        elif items[5].startswith("HW") or items[5].startswith("NYC"):
            assign_type="Homework"
        else:
            assign_type = "Unknown"
        return items[1], items[2],items[4], items[5], assign_type
    
    
    