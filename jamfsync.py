#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------
# Author: Ervin Kurbegovic

import pdb
from time import sleep
from datetime import datetime
import requests
import pandas as pd
import json
import re
import psycopg2
from psycopg2 import sql, errors
from sqlalchemy import create_engine, text
from unidecode import unidecode
from random import randint
import string, random
import sys
import os
import csv
import argparse
import traceback
from dotenv import load_dotenv

load_dotenv(override=True)

# Load secure environment variables for username and passwod

JAMFDBUSER = os.getenv('JAMFDBUSER2')
JAMFDBPASSWD = os.getenv('JAMFDBPASSWD2')

# Class and methods for authentication and converting API data to DataFrames.
class Jamfapi:
    """
    Connects to JAMF API v1 and retrieves data as pandas DataFrames.

    Provides a method, `get_jamf_data`, to fetch and store API data in accessible variables.

    API credentials can be found in Jamf settings:
        - API endpoint: Settings > API > ...
        - Username: Devices > Enroll Devices > MDM Server URL ...network=****...
    """

    # Constructor: Initializes the object
    def __init__(self, username, password, api_url=None, endpoint=None):
        self.username = username
        self.password = password
        self.api_url = api_url
        self.red = '\033[0;31m'
        self.yellow = '\033[1;33m' 
        self.reset_color = '\033[0m' # reset color
        self.engine = create_engine('postgresql://postgres@:5432/iserv')
        self.headers = {
                        'User-Agent': 'curl/7.24.0',
                        'X-Server-Protocol-Version':'3',
                        'Content-Type': 'application/json'  # Content type for JSON data
                        }
        # Available JAMF API endpoints - As of August 9, 2022 (Note: JAMF is also working on API v2)
        self.endpoints =  {'users': [self.api_url+'users', 'users'], 
                      'devices': [self.api_url+'devices', 'devices'], 
                      'locations': [self.api_url+'locations', 'locations'],
                      'profiles': [self.api_url+'profiles', 'profiles'], 
                      'apps': [self.api_url+'apps', 'apps'], 
                      'classes': [self.api_url+'classes', 'classes'],
                      'dep': [self.api_url+'dep', 'placeholders'], 
                      'devicegroups': [self.api_url+'devices/groups', 'deviceGroups'],
                      'groups': [self.api_url+'users/groups', 'groups'],
                      'teacher': [self.api_url+'teacher', 'teacher'], 
                      'ibeacons': [self.api_url+'ibeacons', 'beacons']}
        try:
            if endpoint != None:
                self.custom = self.__get_jamf_data(str(endpoint))
        except Exception as error00:
            print('Wrong endpoint', error00)
        try:       
          self.dep = self.__get_jamf_data('dep') 
          self.devices = self.__get_jamf_data('devices')
          self.locations = self.__get_jamf_data('locations')
          self.profiles = self.__get_jamf_data('profiles')
          self.apps = self.__get_jamf_data('apps')
          self.classes = self.__get_jamf_data('classes')
          self.devicesgroups = self.__get_jamf_data('devicegroups')
          self.usergroups = self.__get_jamf_data('groups')
          self.beacons = self.__get_jamf_data('ibeacons')
          self.users = self.__get_jamf_data('users')
        except BaseException as ex:
          sys.exit(ex)

    def connect_users_by_act(self):
        """
        Connects user data from two sources (IServ and Jamf|School) based on a
        shared 'act/email' attribute and stores the merged information in a PostgreSQL database table.

        This method assumes you have a valid SQLAlchemy engine object (`self.engine`) configured
        to connect to your PostgreSQL database.

        Raises:
            errors.UndefinedTable: If the target table ('citeq_jamf_userstatus') doesn't exist.
        """
        with self.engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS citeq_jamf_userstatus"))
        # Pulling user data from local database table: users
        df_isv_users = self.__get_iserv_data(data='iserv_users') # all user data from iserv with no filter
        # Pulling user data from jamf|School over the APIv1
        jamf_users = self.__get_jamf_data('users') 
        # Merging iserc user data and jamf|School user data over the e-mail
        df_merged_users = df_isv_users.merge(jamf_users, left_on='email', right_on='email', how='left', suffixes=('_iserv', '_jamf'))
        df_merged_users = df_merged_users[['id_iserv','act', 'id_jamf','username','firstName', 'lastName', 'email', 'locationId']].drop_duplicates()
        df_merged_users['created_for'] = 'jamf_school_api_v1'
        # Enriching data with source and timestamp information
        df_merged_users['last_sync'] = pd.Timestamp(datetime.now()).tz_localize('UTC').tz_convert('Europe/Berlin')
        df_merged_users = df_merged_users[['id_iserv','act', 'id_jamf','username','firstName','lastName', 'email', 'locationId','created_for','last_sync']].drop_duplicates()
        df_merged_users.columns = ['id_iserv', 'act', 'id_jamf','username_jamf','firstName_jamf','lastName_jamf','email','locationId_jamf', 'created_for','last_sync']
        try:
            df_merged_users.to_sql('citeq_jamf_userstatus', self.engine, index=False, if_exists='append')
        except errors.UndefinedTable as e:
            print(f"Error: {e}")
            print("The table 'citeq_jamf_userstatus' does not exist. Proceeding with creating table...")
            df_merged_users.to_sql('citeq_jamf_userstatus', self.engine, index=False, if_exists='replace')

    def sync_user_by_ids(self, ids=None):
        """
        Synchronization of user data using jamf ids to correct errors and ensure data consistency.
        This method assumes you have provided valid list of jamf user ids.

        Raises:
            errors.UndefinedTable: If the target table ('citeq_jamf_userstatus') doesn't exist.
        """
        session = requests.Session()
        iserv_users = self.__get_iserv_data(data='all')
        jamf_users = self.__get_jamf_data('users').reset_index(drop=True).sort_values(by='id')            
        if ids != None:
            iserv_jamf_users = self.__get_iserv_data(data='jamf_users')
            jamf_users
            print('Jamfusers:',len(jamf_users),'\nIserv Jamfusers:', len(iserv_jamf_users))
            input()
            add_users = []
            for counter, (index, data) in enumerate(iserv_jamf_users.iterrows()):
                # Test User-Update
                user_id = data['id']
                jamf_data = jamf_users.loc[jamf_users['id']==user_id]
                if jamf_data.empty:
                    add_users.append(user_id)
                else:
                    current_jamf_username = jamf_data['username'].values[-1] # Probably not needed as username and email are the same!
                    current_jamf_email = jamf_data['email'].values[-1]
                    current_jamf_firstName = jamf_data['firstName'].values[-1]
                    current_jamf_lastName = jamf_data['lastName'].values[-1]
                    current_jamf_locationId = str(jamf_data['locationId'].values[-1])

                    current_iserv_username = data['username']
                    current_iserv_email = data['email']
                    current_iserv_firstName = data['firstName']
                    current_iserv_lastName = data['lastName']
                    current_iserv_locationId = str(data['locationId'])

                    if current_jamf_email != current_iserv_email or \
                        current_jamf_locationId != current_iserv_locationId or \
                        current_jamf_username != current_iserv_username or \
                        current_jamf_firstName != current_iserv_firstName or \
                        current_jamf_lastName != current_iserv_lastName:
                        print('Incorrect:\n', [current_jamf_email == current_iserv_email, 
                                               current_jamf_locationId == current_iserv_locationId, 
                                               current_jamf_username == current_iserv_username,
                                               current_jamf_firstName == current_iserv_firstName, 
                                               current_jamf_lastName == current_iserv_lastName],
                              '\n\nJamf:\t'+current_jamf_email,len(current_jamf_email), '\nIServ:\t'+current_iserv_email,len(current_iserv_email),
                              '\n\nJamf:\t'+current_jamf_locationId,len(current_jamf_locationId), '\nIServ:\t'+current_iserv_locationId,len(current_iserv_locationId),
                              '\n\nJamf:\t'+current_jamf_username,len(current_jamf_username), '\nIServ:\t'+current_iserv_username,len(current_iserv_username),
                              '\n\nJamf:\t'+current_jamf_firstName,len(current_jamf_firstName), '\nIServ:\t'+current_iserv_firstName,len(current_iserv_firstName), 
                              '\n\nJamf:\t'+current_jamf_lastName,len(current_jamf_lastName),'\nIServ:\t'+current_iserv_lastName,len(current_iserv_lastName),'\n')
                        url = self.endpoints['users'][0]+'/'+str(user_id)
                        data = {
                           "username": current_iserv_username, 
                           "password": "",
                           "email": current_iserv_email,
                           "firstName": current_iserv_firstName,
                           "lastName": current_iserv_lastName,
                           "memberOf": [],
                           "locationId": current_iserv_locationId
                        }
                        response = session.put(url, headers=self.headers, json=data, auth=(self.username, self.password))
                        if response.status_code == 200:
                            print('***'*35)
                            print(f"{self.yellow}{user_id},{data['username']} reviewed and corrected - Progress {counter+1} of {len(iserv_jamf_users)}"+self.reset_color)
                            print('***'*35)
                        else:
                            print(f"Error reviewing user {user_id},{data['username']}: {response.status_code} - {response.text}")
            input(sorted(add_users))
        elif ids != None:
            session = requests.Session()
            iserv_users = self.__get_iserv_data(data='all')
            iserv_jamf_users = self.__get_iserv_data(data='jamf_userstatus')
            iserv_jamf_users = iserv_jamf_users.dropna(subset=['id_jamf'])
            all_ids = iserv_jamf_users['id_iserv'].unique().tolist()
            review_users = [iserv_users.loc[iserv_users['user_id']==id][['user_id','email','firstname','lastname','useract']] for id in all_ids if str(id) not in ids]
            review_users = pd.concat(review_users, ignore_index=True).drop_duplicates().reset_index(drop=True)
            dfs_corrected = []
            for counter, (index, data) in enumerate(review_users.iterrows()):
                # Test User-Update
                user_id = data['user_id']
                jamf_user_data = iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id]
                current_jamf_username = jamf_user_data['username_jamf'].values[-1] # Probably not needed as username and email are the same!
                current_jamf_email = jamf_user_data['email'].values[-1]
                current_jamf_firstName = jamf_user_data['firstName_jamf'].values[-1]
                current_jamf_lastName = jamf_user_data['lastName_jamf'].values[-1]
                current_jamf_locationId = jamf_user_data['locationId_jamf'].values[-1]

                current_iserv_username = data['useract']
                current_iserv_email = data['email']
                current_iserv_firstName = data['firstname']
                current_iserv_lastName = data['lastname']

                if current_jamf_email != current_iserv_email or \
                    current_jamf_firstName != current_iserv_firstName or \
                    current_jamf_lastName != current_iserv_lastName:
                    print('Incorrect:\n', [current_jamf_email == current_iserv_email, current_jamf_firstName == current_iserv_firstName, current_jamf_lastName == current_iserv_lastName],
                          '\n\nJamf:\t'+current_jamf_email,len(current_jamf_email), '\nIServ:\t'+current_iserv_email,len(current_iserv_email),
                          '\n\nJamf:\t'+current_jamf_firstName,len(current_jamf_firstName), '\nIServ:\t'+current_iserv_firstName,len(current_iserv_firstName), 
                          '\n\nJamf:\t'+current_jamf_lastName,len(current_jamf_lastName),'\nIServ:\t'+current_iserv_lastName,len(current_iserv_lastName),'\n')
                    iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id, 'act'] = current_iserv_username
                    iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id, 'email'] = current_iserv_email
                    iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id, 'username_jamf'] = current_iserv_email
                    iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id, 'firstName_jamf'] = current_iserv_firstName
                    iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id, 'lastName_jamf'] = current_iserv_lastName
                    selected_columns = [col for col in iserv_jamf_users.columns if 'last_sync' not in col]
                    iserv_jamf_users = iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id][selected_columns].drop_duplicates()
                    iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id, 'last_sync'] = pd.Timestamp(datetime.now()).tz_localize('UTC').tz_convert('Europe/Berlin')
                    dfs_corrected.append(iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id])
                    print(self.yellow,'Corrected: ', iserv_jamf_users.loc[iserv_jamf_users['id_iserv']==user_id], self.reset_color)
            input('finished loop')
            # Drop the table if it exists
            with self.engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS citeq_jamf_userstatus_old"))
                conn.execute(text("alter table citeq_jamf_userstatus rename to citeq_jamf_userstatus_old"))
                print('citeq_jamf_userstatus_old updated')
                self.connect_users_by_act()
            try:
                iserv_jamf_users = pd.concat(dfs_corrected, ignore_index=True)
                iserv_jamf_users.to_sql('citeq_jamf_userstatus', self.engine, index=False, if_exists='append')
            except ValueError as error:
                print('No changed Data: ',error)
            input('Check_db_data')
            jamf_users = self.__get_jamf_data('users').reset_index(drop=True).sort_values(by='id')
            iserv_jamf_users = iserv_jamf_users.dropna(subset=['id_jamf'])
            all_ids = iserv_jamf_users['id_jamf'].to_list()
            review_users = [iserv_jamf_users.loc[iserv_jamf_users['id_jamf']==id] for id in all_ids if str(id) not in ids]
            for counter, data in enumerate(review_users):
                # Test User-Update
                user_id = data['id_jamf'].values[0]
                jamf_user_data = jamf_users.loc[jamf_users['id']==int(user_id)]
                current_jamf_username = jamf_user_data['username'].values[0]
                current_jamf_email = jamf_user_data['email'].values[0]
                current_jamf_firstName = jamf_user_data['firstName'].values[0]
                current_jamf_lastName = jamf_user_data['lastName'].values[0]
#                current_jamf_memberOf = jamf_user_data['memberOf'].values[0]
                current_jamf_locationId = jamf_user_data['locationId'].values[0]
                current_iserv_username = data['username_jamf'].values[0]
                current_iserv_email = data['email'].values[0]
                current_iserv_firstName = data['firstName_jamf'].values[0]
                current_iserv_lastName = data['lastName_jamf'].values[0]
 #               current_iserv_memberOf = data['memberOf'].values[0]
                current_iserv_locationId = str(data['locationId_jamf'].values[0])
                if current_jamf_username != current_iserv_username or \
                    current_jamf_email != current_iserv_email or \
                    current_jamf_firstName != current_iserv_firstName or \
                    current_jamf_lastName != current_iserv_lastName:

                    url = self.endpoints['users'][0]+'/'+str(user_id)
                    data = {
                       "username": current_iserv_username, #data['username'].values[0],
                       "password": "",
                       "email": current_iserv_email, #data['email'].values[0],
                       "firstName": current_iserv_firstName, #data['firstName'].values[0],
                       "lastName": current_iserv_lastName, #data['lastName'].values[0],
                       "memberOf": [],
                       "locationId": current_iserv_locationId, #str(data['locationId'].values[0])
                    }
                    response = session.put(url, headers=self.headers, json=data, auth=(self.username, self.password))
                    if response.status_code == 200:
                        print('***'*35)
                        print(f"{self.yellow}{user_id},{data['username']} reviewed and corrected - Progress {counter+1} of {len(review_users)}"+self.reset_color)
                        print('***'*35)
                    else:
                        print(f"Error reviewing user {user_id},{data['username']}: {response.status_code} - {response.text}")
                else:
                    print(f"{user_id},{current_jamf_username} skipped - Progress {counter+1} of {len(review_users)}")
            session.close()
            input()

    def review_users(self, location='LABOR Citeq'):
        os.system('clear')
        # Pulling data from the local database
        iserv_jamf_users = self.__get_iserv_data(data='citeq_jamf_users') # initial user data from jamf|School (first synchronization)
        # Pulling data from jamf|School over the APIv1
        jamf_users = self.__get_jamf_data('users').reset_index(drop=True).sort_values(by='id') # current user data from jamf|School
        # Pulling data from the local database table members, groups and users
        iserv_users = self.__get_iserv_data(data='all') # iserv user data with filter on firstname != ""
        # Pulling data from local database
        # Note: This user data can only be synchronized via the username. Subsequent changes are not traceable!
        iserv_users['username'] = iserv_users['email']
        iserv_users = iserv_users[['username', 'firstname', 'lastname', 'email', 'user_id']].drop_duplicates()
        if jamf_users.empty:
            raise ValueError("\033[31mNo users in Jamf!\033[0m") 
        elif iserv_jamf_users.empty:
            raise ValueError("\033[31mNo User data in local database!\033[0m")
            # Calling the initial synch and storing jamf|School data in the local database
        else:
            print('Continue with reviewing user data in 3 seconds...')
            sleep(3)
            os.system('clear')
            jamf_ids = set(jamf_users['id'].to_list())
            db_ids = set(iserv_jamf_users['id'].to_list())
            new_ids_in_jamf = jamf_ids.difference(db_ids)
            db_ids_not_in_jamf = db_ids.difference(jamf_ids)
            excluded_ids = [str(iserv_jamf_users.loc[iserv_jamf_users['id']==i]['id'].values[0]) for i in db_ids_not_in_jamf]
            create_ids = [id for id in excluded_ids]
            delete_ids = [str(jamf_users.loc[jamf_users['id']==i]['id'].values[0]) for i in new_ids_in_jamf]
            excluded_ids = create_ids + delete_ids
            self.sync_user_by_ids(ids=excluded_ids)
            input('Stop 2')
            update_users_dict = {}
            input(sorted(create_ids))
            input(iserv_users)
            if add_users != set():
                iserv_users = iserv_users.loc[iserv_users['username'].isin(add_users)]
                api_data = [{i[0]: [i[1], i[2], i[3]]} for x,i in iserv_users.iterrows()]
                update_users_dict['add']=api_data
            if delete_users != set():
                iserv_users = jamf_users.loc[jamf_users['username'].isin(delete_users)]
                api_data = [[i[0], i[4]] for x,i in iserv_users.iterrows()]
                update_users_dict['delete']=api_data
            if add_users == set() and delete_users == set():
                message = "Users are up to date"
                border = '*' * len(message)
                print(f"{border}\n{message}\n{border}")
            input(['ADD', len(update_users_dict['add']), update_users_dict['add']])
            print('---'*30)
            input(['DEL', len(update_users_dict['delete']), update_users_dict['delete']])
            new_data = set(iserv_users['username'].to_list())
            actual_data = set(jamf_users['username'].to_list())
            add_users = new_data.difference(actual_data)
            delete_users = actual_data.difference(new_data)
            update_users_dict = {}
            if add_users != set():
                iserv_users = iserv_users.loc[iserv_users['username'].isin(add_users)]
                api_data = [{i[0]: [i[1], i[2], i[3]]} for x,i in iserv_users.iterrows()]
                update_users_dict['add']=api_data
            if delete_users != set():
                iserv_users = jamf_users.loc[jamf_users['username'].isin(delete_users)]
                api_data = [[i[0], i[4]] for x,i in iserv_users.iterrows()]
                update_users_dict['delete']=api_data
            if add_users == set() and delete_users == set():
                message = "Users are up to date"
                border = '*' * len(message)
                print(f"{border}\n{message}\n{border}")
            input(['ADD', len(update_users_dict['add']), update_users_dict['add']])
            print('---'*30)
            input(['DEL', len(update_users_dict['delete']), update_users_dict['delete']])
        for index1, user1 in iserv_jamf_users.iterrows():
            if user1['username'] not in jamf_users['username'].to_list():
                print('User just in IServ:', self.red, user1['username'], user1['id'],self.reset_color)
        for index1, user1 in jamf_users.iterrows():
            if user1['username'] not in iserv_jamf_users['username'].to_list():
                print('User just in Jamf: ', self.red, user1['email'], user1['id'], self.reset_color)
        input()

    def update_classes(self, class_template=None, location='LABOR Citeq'):
        if self.classes.empty and class_template == None:
            api_data = self.create_class_template(initial_sync=True)
            self.create_classes(api_data, 'isv', location)
            return
        os.system('clear')
        session = requests.Session()
        df_loc = self.locations
        location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
        if class_template == None:
            print("\033[31mNo class data passed on!\033[0m")
            sleep(3)
            return None
        elif 'delete' in class_template[0].keys() and len(class_template[0]['delete']) > 0:
            class_to_delete = ", ".join([key[0] for key in class_template[0]['delete'][0]])
            print(f"Deleting starts in 5 seconds...")
            print('Quantity:', len(class_template[0]['delete'][0]),'\nClasses noted: '+str(class_template[0]['delete'][0]))
            print('-----'*10)
            sleep(5)
            if len(class_template[0]['delete'][1]) > 0:
                for counter, uuid in enumerate(class_template[0]['delete'][1]):
                    url = self.endpoints['classes'][0] + f"/{uuid}"
                    response = session.delete(url, headers=self.headers, auth=(self.username, self.password))
                    # Check response status code to ensure successful request transmission (expected: 200 code)
                    if response.status_code == 200:
                        print(f"Class {class_template[0]['delete'][0][counter]} with id {uuid} deleted - Progress {counter+1} of {len(class_template[0]['delete'][0])}")
                    else:
                        print(f"Error deleting Class: {class_template[0]['delete'][0][counter]} - {class_template[0]['delete'][1][counter]} - {response.status_code} - {response.text}")
            sleep(3)
        else:
            print('No classes to delete - Continue with creating classes in 3 seconds...')
            sleep(3)
            os.system('clear')
        if 'add' in class_template[0].keys() and len(class_template[0]['add']) > 0:
            os.system('clear')
            self.create_classes([class_template[0]['add'], class_template[1]])
        else:
            print('No classes to create - Ending in 3 seconds...')
            sleep(3)
        session.close()
    def update_users(self, user_template=None, location='LABOR Citeq', fresh_users=pd.DataFrame()):
        session = requests.Session()
        df_loc = self.locations
        location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
        if user_template == None:
            print("\033[31mNo data!\033[0m")
            exit()
        elif 'delete' in user_template.keys():
            users_to_delete = ", ".join([key[0] for key in user_template['delete']])
            print(f"Deleting starts in 5 seconds...")
            print('Quantity:', len(users_to_delete.split(', ')),'\nUsers noted: '+str(users_to_delete))
            sleep(3)
            if len(user_template['delete']) > 0:
                for counter, uuid in enumerate(user_template['delete']):
                    url = self.endpoints['users'][0] + f"/{uuid[1]}"
                    response = session.delete(url, headers=self.headers, auth=(self.username, self.password))
                    # Check response status code to ensure successful request transmission (expected: 200 code)
                    if response.status_code == 200:
                        print(f"User {uuid[0]} with id {uuid[1]} deleted - Progress {counter+1} of {len(user_template['delete'])}")
                    else:
                        print(f"Error deleting User: {user_template[counter][0]} - {user_template[counter][1]} - {response.status_code} - {response.text}")
            jamf_users = self.__get_jamf_data('users')
            if jamf_users.empty == False:
                self.__get_jamf_data('users').to_sql('citeq_jamf_users', self.engine, index=False, if_exists='replace')
            else:
                sleep(2)
        else:
            print('No users to delete - Continue with creating users in 5 seconds...')
            sleep(3)
            os.system('clear')
        if 'add' in user_template.keys() and len(user_template['add']) > 0 and fresh_users.empty:
            api_data = user_template['add']
            url = self.endpoints['users'][0]
            print(f"Creating starts in 3 seconds...")
            print('Quantity:',len(api_data),'\nUsers noted: '+str(api_data))
            sleep(3)
            # FOR-Loop for creating users in jamf based on provided IServ data
            for counter, cl in enumerate(api_data):
                key = list(cl.items())[0][0]
                value = list(cl.items())[0][1]
                data = {
                    "username": value[2], #key,
                    "password": "",
                    "email": value[2], 
                    "firstName": str(value[0]),
                    "lastName": value[1],
                    "memberOf": [],
                    "locationId": location_id,
                    "notes": "automatisch generierte Benutzer auf Basis der IServ-Benuter."
                    }
                response = session.post(url, headers=self.headers, json=data, auth=(self.username, self.password))
                # Check response status code to ensure successful request transmission (expected: 200 code)
                if response.status_code == 200:
                    print(f"{cl} created - Progress {counter+1} of {len(api_data)}")
                else:
                    print(f"Error creating users: {response.status_code} - {response.text}")
            jamf_users = self.__get_jamf_data('users')
            if jamf_users.empty == False:
                self.__get_jamf_data('users').to_sql('citeq_jamf_users', self.engine, index=False, if_exists='replace')
            else:
                sleep(2)
        elif 'add' in user_template.keys() and len(user_template['add']) > 0 and fresh_users.empty == False:
            print(fresh_users.empty, fresh_users, 'has to be False')
            api_data = user_template['add']
            url = self.endpoints['users'][0]
            fresh = fresh_users.groupby(['useract']).agg({'group_isv':list}).reset_index()
            print(f"Creating starts in 2 seconds...")
            print('Quantity:',len(api_data),'\nUsers noted: '+str(api_data))
            sleep(2)
            # FOR-Loop for creating users in jamf based on provided IServ data
            for counter, cl in enumerate(api_data):
                key = list(cl.items())[0][0]
                value = list(cl.items())[0][1]
                membership = fresh.loc[fresh['useract']==key]['group_isv'].values[0]
                if 'lehrkraefte' in membership:
                    membership = []
                data = {
                    "username": value[2],
                    "password": "",
                    "email": value[2], 
                    "firstName": str(value[0]),
                    "lastName": value[1],
                    "memberOf": membership,
                    "locationId": location_id,
                    "notes": "automatisch generierte Benutzer auf Basis der IServ-Benuter."
                    }
                response = session.post(url, headers=self.headers, json=data, auth=(self.username, self.password))
                # Check response status code to ensure successful request transmission (expected: 200 code)
                if response.status_code == 200:
                    print(f"{cl} created - Progress {counter+1} of {len(api_data)}")
                else:
                    print(f"Error creating users: {response.status_code} - {response.text}")
            jamf_users = self.__get_jamf_data('users')
            if jamf_users.empty == False:
                self.__get_jamf_data('users').to_sql('citeq_jamf_users', self.engine, index=False, if_exists='replace')
            for user in fresh['useract']:
                try:
                    self.assign_teachers(fresh_teacher=user)
                except Exception as error:
                    input(error)
        else:
            print('No users to create. Ending in 3 seconds...')
            sleep(3)
            os.system('clear')
        session.close()
    # **WARNING:** This method deletes all users in Jamf. Proceed with extreme caution.
    def delete_users(self, location='all', only_iserv_users=True):
        df_users = self.users
        print(f'Starting the Deletion of Users')
        if location == 'all':
            df_users = df_users.loc[(df_users['locationId'] >= 0)]
        else:
            df_loc = self.locations
            location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
            df_users = df_users.loc[(df_users['locationId'] == int(location_id))]
        if df_users.empty:
            print('No users in Jamf. Deletion of users stopped!')
            return None
        else:
            if only_iserv_users == True:
                user_uuids = df_users[df_users['notes'] == 'automatisch generierte Benutzer auf Basis der IServ-Benuter.']['id'].to_list()
                user_names = df_users[df_users['notes'] == 'automatisch generierte Benutzer auf Basis der IServ-Benuter.']['name'].to_list()
            else:
                user_uuids = df_users['id'].to_list()
                user_names = df_users['name'].to_list()
            # Delete all instances using a for loop
            print(F"Following {len(user_names)} users noted: ", user_names)
            print('-----'*15)
            sleep(3)
            session = requests.Session()
            for counter, uuid in enumerate(user_uuids):
                url = self.endpoints['users'][0] + f"/{uuid}"
                response = session.delete(url, headers=self.headers, auth=(self.username, self.password))
                # Check response status code to ensure successful request transmission (expected: 2xx code).
                if response.status_code == 200:
                    print(f"User with id {uuid} deleted - Progress {counter+1} of {len(user_uuids)}")
                else:
                    print(f"Error deleting User: {user_names[counter]} - {response.status_code} - {response.text}")
            session.close()

    # **WARNING:** This method deletes all classes in Jamf. Proceed with extreme caution.
    def delete_classes(self, location='all', only_iserv_classes=True):
        print('Starting the Deletion of Classes')
        df_classes = self.classes
        if df_classes.empty:
            self.delete_classes()
            api_data = self.create_class_template(initial_sync=True)
            self.create_classes(api_data)
        if location == 'all':
            df_classes = df_classes.loc[(df_classes['locationId'] >= 0)]
        else:
            df_loc = self.locations
            location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
            df_classes = df_classes.loc[(df_classes['locationId'] == int(location_id))]
        if df_classes.empty:
            print('No classes in Jamf. Deletion of classes stopped!')
            return None
        else:
            if only_iserv_classes == True:
                class_uuids = df_classes[df_classes['description'] == 'automatisch generierte Klasse auf Basis der IServ-Gruppen.']['uuid'].to_list()
                class_names = df_classes[df_classes['description'] == 'automatisch generierte Klasse auf Basis der IServ-Gruppen.']['name'].to_list()
            else:
                class_uuids = df_classes['uuid'].to_list()
                class_names = df_classes['name'].to_list()
            # Delete all instances using a for loop
            print(f"Following {len(class_names)} classes noted: ", class_names)
            print('-----'*15)
            sleep(3)
            if 'klassen entfernen!' == 'klassen entfernen!':
                session = requests.Session()
                for counter, uuid in enumerate(class_uuids):
                    url = self.endpoints['classes'][0] + f"/{uuid}"
                    response = session.delete(url, headers=self.headers, auth=(self.username, self.password))
                    # Check response status code to ensure successful request transmission (expected: 2xx code).
                    if response.status_code == 200:
                        print(f"Class with uuid {uuid} deleted - Progress {counter+1} of {len(class_uuids)}")
                    else:
                        print(f"Error deleting class: {cl} - {response.status_code} - {response.text}")
                session.close()
            else:
                print('Deletion canceled. Classes still available.')

    def create_users(self, user_template, location='LABOR Citeq'):
        df_loc = self.locations
        location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
        # Create a session to maintain connections and cookies (optional but recommended)
        session = requests.Session()
        url = self.endpoints['users'][0]
        api_data = user_template
        print('Quantity:',len(api_data),'\nUsers noted: '+str(api_data))
        # FOR-Loop for creating users in jamf based on provided IServ data
        for counter, cl in enumerate(api_data):
            key = list(cl.items())[0][0]
            value = list(cl.items())[0][1]
            data = {
                "username": value[2],
                "password": "", # empty
                "email": value[2], # For email addresses (fol/foe indicators), update user data instead of overwriting it.
                "firstName": str(value[0]),
                "lastName": value[1],
                "memberOf": [],
                "locationId": location_id,
                "notes": "automatisch generierte Benutzer auf Basis der IServ-Benuter." #+'|'+str(value[3])
                }
            response = session.post(url, headers=self.headers, json=data, auth=(self.username, self.password))
            # Check response status code to ensure successful request transmission (expected: 2xx code).
            if response.status_code == 200:
                print(f"{cl} created - Progress {counter+1} of {len(api_data)}")
            else:
                print(f"Error creating users: {response.status_code} - {response.text}")
        session.close()
        jamf_users = self.__get_jamf_data('users')
        if jamf_users.empty == False:
            self.__get_jamf_data('users').to_sql('citeq_jamf_users', self.engine, index=False, if_exists='replace')
            self.connect_users_by_act()

    def assign_teachers(self, fresh_teacher=None):
        teacher_list = self.__get_iserv_data(data='teacher_list')
        session = requests.Session()
        jamf_classes = self.__get_jamf_data('classes')
        jamf_users = self.__get_jamf_data('users')
        all_teacher_ids = jamf_users.loc[jamf_users['username'].isin(teacher_list)]['id'].to_list()
        teacher_id = str(jamf_users[jamf_users['username'] == str(fresh_teacher)]['id'].values[0])
        fresh_teacher_membership = pd.read_sql(f'''select * from members where actuser = '{fresh_teacher}';''', self.engine)
        teacher_classes = jamf_classes.loc[jamf_classes['name'].isin(fresh_teacher_membership['actgrp'].to_list())]
        if jamf_classes.empty or fresh_teacher == None:
            return None
        for counter, cl in enumerate(teacher_classes[['uuid','name']].iterrows()):
            url = self.endpoints['classes'][0]+'/'+str(cl[1]['uuid'])+'/users'
            data = {
                "teachers": [teacher_id]
            }
            response = session.put(url, headers=self.headers, json=data, auth=(self.username, self.password))
            # Check response status code to ensure successful request transmission (expected: 2xx code).
            if response.status_code == 200:
                print(f"{fresh_teacher} assigned to {cl[1]['name']}")
            else:
                print(f"Error assigning: {fresh_teacher} - {response.status_code} - {response.text}")
        jamf_classes = self.__get_jamf_data('classes')
        for counter, cl in enumerate(jamf_classes[['name','uuid', 'description']].iterrows()):
            iserv_classes = 'automatisch generierte Klasse auf Basis der IServ-Gruppen.' 
            if 'klasse' in cl[1]['name'] and cl[1]['description'] == iserv_classes:
                url = self.endpoints['classes'][0]+'/'+str(cl[1]['uuid'])+'/users'
                data = {
                    "teachers": all_teacher_ids
                }
                response = session.put(url, headers=self.headers, json=data, auth=(self.username, self.password))
                # Check response status code to ensure successful request transmission (expected: 2xx code).
                if response.status_code == 200:
                    print(f"{teacher_list}, {all_teacher_ids} assigned to {cl[1]['name']}")
                else:
                    print(f"Error assigning: {teacher_list} - {response.status_code} - {response.text}")
        session.close()

    def create_classes(self, class_template, suffix='', praefix='', location='LABOR Citeq'):
        """
        Creates classes on a remote jamf|school system based on the provided class template.

        Args:
            class_template (dict, list): A dictionary or list containing class data.
                - If a dictionary, the keys represent class names and the values are dictionaries containing student IDs and optionally teacher IDs (if not present, uses teachers from class_template[1]).
                - If a list, each element represents a class name. Teacher IDs are retrieved from class_template[1].
            suffix (str, optional): A string to be appended to the end of each class name. Defaults to ''.
            prefix (str, optional): A string to be prepended to the beginning of each class name. Defaults to ''.
            location (str, optional): The name of the location for the created classes. Defaults to 'LABOR Citeq'.

        Returns:
            None

        Raises:
            ValueError: If class_template is None.
        """

        if class_template is None:
            raise ValueError("class_template cannot be None") #return None

        ready_class_data = class_template[0]
        df_loc = self.locations
        location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
        session = requests.Session()
        noted_classes = ", ".join([i for i in ready_class_data.keys()])
        print('Quantity:',len(ready_class_data.keys()),'\nClasses noted: '+noted_classes)
        print('-----'*10)
        sleep(3)
        for counter, cl in enumerate(ready_class_data):
            url = self.endpoints['classes'][0]
            data = {
                "name": f"{praefix}{cl}{suffix}",
                "description":"automatisch generierte Klasse auf Basis der IServ-Gruppen.",
                "students": ready_class_data[cl]['student_ids'],
                "locationId": location_id
            }

            if 'klasse' in str(cl).lower():
                data["teachers"] = class_template[1]
            else:
                data["teachers"] = ready_class_data[cl]['teacher_ids']
            response = session.post(url, headers=self.headers, json=data, auth=(self.username, self.password))

            if response.status_code == 200:
                print(f"{cl} created - Progress {counter+1} of {len(ready_class_data.keys())}")
            else:
                print(f"Error creating classes: {cl} - {response.status_code} - {response.text}")
        session.close()

    def create_user_template(self, initial_sync=False, location=None, fresh_users = None):
        if location == None:
            print('No location. Ending in 3 seconds...')
            sleep(3)
            exit() 
        if fresh_users != None:
            df = fresh_users
        else:
            df = self.__get_iserv_data(data='all')
            df['useract'] = df['email']
        df = df[['useract', 'firstname', 'lastname', 'email', 'user_id']].drop_duplicates()
        if initial_sync == True:
            api_data = [{i[0]: [i[1], i[2], i[3], i[4]]} for x,i in df.iterrows()]
            return api_data
        elif initial_sync == False:
            df_usr = df.copy()
            # Rebuild: Problem The create_user_template method must be called again because the data is not up to date.
            if self.users.empty:
                print('No user data. Progressing with creation of users in 3 seconds...')
                sleep(3)
                api_data = self.create_user_template(initial_sync=True, location=location)
                self.create_users(api_data, location=location)
                exit()
            jamf_usr = self.users[['username', 'firstName', 'lastName', 'email', 'id']].drop_duplicates()
            new_data = set(df_usr['useract'].to_list())
            actual_data = set(jamf_usr['username'].to_list())
            add_users = new_data.difference(actual_data)
            delete_users = actual_data.difference(new_data)
            update_users_dict = {}
            if add_users != set():
                df_usr = df_usr.loc[df_usr['useract'].isin(add_users)]
                api_data = [{i[0]: [i[1], i[2], i[3]]} for x,i in df_usr.iterrows()]
                update_users_dict['add']=api_data
            if delete_users != set():
                df_usr = jamf_usr.loc[jamf_usr['username'].isin(delete_users)]
                api_data = [[i[0], i[4]] for x,i in df_usr.iterrows()]
                update_users_dict['delete']=api_data
            if add_users == set() and delete_users == set():
                message = "Users are up to date"
                border = '*' * len(message)
                print(f"{border}\n{message}\n{border}")
                sleep(5)
            else:
                return update_users_dict

    def create_class_template(self, teacher_group='lehrkraefte', initial_sync=False):
        '''
        Method to create a class template from the provided data. This dictionary stores information about classes.
        Each class name is a key, and its value is a list containing all members (students and teachers). Additionally, it is noted whether each member is a student or a teacher. 

        Args:
            teacher_group (str, required): A str containing the group name, that identifies teachers.
                - If not provided the default IServ teacher group is used.
            initial_sync (boolean, required): A boolean is used to decied if its going to be an inital sync of classes. Defaults to False.

        Returns:
            [dict, list]
        '''
        if self.users.empty:
            print('No users in Jamf! Classes without users are of no use. Ending in 3 seconds...')
            sleep(3)
            raise ValueError("No users in Jamf! Classes without users are of no use.") #return None
        else:
            df_isv_user_group = self.__get_iserv_data(data='sync')
            df_isv_user_group['useract'] = df_isv_user_group['email']
            jamf_users = self.users
            teacher_list = self.__get_iserv_data(data='teacher_list')
            class_dict = {}
            user_w_id = df_isv_user_group.merge(jamf_users, left_on='useract', right_on='username', how='left')
        if initial_sync == True:
            user_w_id = df_isv_user_group.merge(jamf_users, left_on='useract', right_on='username', how='left')
            user_w_id_dict = {}
            # Loop to construct target data
            for key, value in user_w_id.iterrows():
                if value['group_isv'] in user_w_id_dict:   
                    if value['useract'] in teacher_list:
                        user_w_id_dict[value['group_isv']]['teachers'].append(value['useract'])
                        user_w_id_dict[value['group_isv']]['teacher_ids'].append(value['id'])
                    else: 
                        user_w_id_dict[value['group_isv']]['students'].append(value['useract'])
                        user_w_id_dict[value['group_isv']]['student_ids'].append(value['id'])
                else:
                    if value['useract'] in teacher_list:
                        user_w_id_dict[value['group_isv']] = {'teachers': [value['useract']], 'teacher_ids': [value['id']], 'students': [], 'student_ids': []}
                    else:
                        user_w_id_dict[value['group_isv']] = {'teachers': [], 'teacher_ids': [], 'students': [value['useract']], 'student_ids': [value['id']]}
            all_teacher_ids = jamf_users.loc[jamf_users['username'].isin(teacher_list)]['id'].to_list()
            return [user_w_id_dict, all_teacher_ids]
        elif initial_sync == False:
            if self.classes.empty:
                print('\033[31mNo class data!\033[0m')
                sleep(3)
                return None
            actual_classes = set(self.classes['name'].to_list())
            new_classes = set(df_isv_user_group['group_isv'].unique().tolist())
            add_classes = new_classes.difference(actual_classes)
            delete_classes = actual_classes.difference(new_classes)
            if len(add_classes) > 0:
                user_w_id = user_w_id.loc[user_w_id['group_isv'].isin(add_classes)]
                user_w_id_dict = {}
                # Loop to construct target data
                for key, value in user_w_id.iterrows():
                    if value['group_isv'] in user_w_id_dict:   
                        if value['useract'] in teacher_list:
                            user_w_id_dict[value['group_isv']]['teachers'].append(value['useract'])
                            user_w_id_dict[value['group_isv']]['teacher_ids'].append(value['id'])
                        else: 
                            user_w_id_dict[value['group_isv']]['students'].append(value['useract'])
                            user_w_id_dict[value['group_isv']]['student_ids'].append(value['id'])
                    else:
                        if value['useract'] in teacher_list:
                            user_w_id_dict[value['group_isv']] = {'teachers': [value['useract']], 'teacher_ids': [value['id']], 'students': [], 'student_ids': []}
                        else:
                            user_w_id_dict[value['group_isv']] = {'teachers': [], 'teacher_ids': [], 'students': [value['useract']], 'student_ids': [value['id']]}
                class_dict['add'] = user_w_id_dict
            else:
                class_dict['add'] = []
            if len(delete_classes) > 0:
                df_cl = self.classes 
                class_names = df_cl.loc[df_cl['name'].isin(delete_classes)]['name'].to_list()
                class_uuids = df_cl.loc[df_cl['name'].isin(delete_classes)]['uuid'].to_list()
                class_dict['delete'] = [class_names, class_uuids]
            else:
                class_dict['delete'] = []
        all_teacher_ids = jamf_users.loc[jamf_users['username'].isin(teacher_list)]['id'].to_list()
        return [class_dict, all_teacher_ids]

    # Method to retrieve data from the local database iserv
    def __get_iserv_data(self, data='all', teacher_group='lehrkraefte'):
        try:
            if data == 'teacher_list': 
                return pd.read_sql(f'''select actuser, 
                                           CONCAT(actuser, (select '@' || substring(pg_read_file('/etc/hostname') from 7 for char_length(pg_read_file('/etc/hostname')) - 7))) as email
                                           from members where actgrp = 'lehrkraefte';''', self.engine)['email'].to_list()
            elif data == 'all':
                return pd.read_sql('''with a as 
                        (select *, users.id as user_id, users.act as useract, members.actgrp as group_isv from members 
                        left join groups on members.actgrp = groups.act 
                        left join users on members.actuser = users.act) 
                        select useract, firstname, lastname, 
                        CONCAT(useract, (select '@' || substring(pg_read_file('/etc/hostname') from 7 for char_length(pg_read_file('/etc/hostname')) - 7))) as email,
                        group_isv, user_id
                        from a 
                        where firstname != '' order by group_isv;''', self.engine)
            elif data == 'sync':
                return pd.read_sql(f'''with a as 
                                            (select *, groups.deleted as deleted_grp, users.act as useract, members.actgrp as group_isv, groups.type as isv_type 
                                            from members left join groups on members.actgrp = groups.act 
                                            left join users on members.actuser = users.act) 
                                            select deleted_grp, useract, group_isv, isv_type, 
                                            CONCAT(useract, (select '@' || substring(pg_read_file('/etc/hostname') from 7 for char_length(pg_read_file('/etc/hostname')) - 7))) as email
                                            from a 
                                            where firstname != '' and 
                                            isv_type = 'jamfsync' and 
                                            deleted_grp is null or
                                            group_isv = '{teacher_group}' order by group_isv;''', self.engine) 
            elif data == 'citeq_jamf_users':
                return pd.read_sql(f'''select * from citeq_jamf_users;''', self.engine).reset_index(drop=True).sort_values(by='id')
            elif data == 'citeq_jamf_userstatus':
                return pd.read_sql(f'''select * from citeq_jamf_userstatus;''', self.engine).reset_index(drop=True).sort_values(by='id_iserv')

            elif data == 'iserv_users':
                return pd.read_sql(f'''select *, 
                                       CONCAT(act, (select '@' || substring(pg_read_file('/etc/hostname') from 7 for char_length(pg_read_file('/etc/hostname')) - 7))) as email 
                                       from users;''', self.engine).reset_index(drop=True).sort_values(by='id')
            else:
                return None
        except BaseException as ex:
            sys.exit(ex)

    # Method to retrieve jamf data from the api
    def jamf_api_call(self, endpoint, apicolumn, username, password):
        """Implementiert GET-Request an Endpunkte der JAMF-API"""
        headers = {'X-Server-Protocol-Version':'3'}
        try:
            response = requests.get(endpoint, auth=(username, password), headers=headers)
            pdb.set_trace()
        except BaseException as ex:
            sys.exit(ex)
        try:
            return pd.DataFrame(json.loads(response.text)[apicolumn])
        except BaseException as error01:
            sys.exit(error01)
        
    # Method to pass API endpoint and corresponding column.
    def __get_jamf_data(self, endpoint):
        try:
            return self.jamf_api_call(self.endpoints[endpoint][0], self.endpoints[endpoint][1],  self.username, self.password) 
        except BaseException as ex:
            sys.exit(ex)

    # Method to remove umlauts and special characters from the given string.
    def alphanumeric_output(self, daten):
        return "{client}".format(client=unidecode(''.join([zeichen for zeichen in daten if zeichen.isalnum()])))

    # Method specifically for cleaning data from the Devices endpoint. (Cannot be used for other endpoints without prior adaptation!) 
    # Clean data for quality storage & DHCP compatibility. 
    def clean_jamfdevices(self, type):
        # Convert "networkInformation" column (list) to a separate DataFrame to select the "WiFiMAC" column.
        df_wifimac = pd.DataFrame(self.devices['networkInformation'].to_list()).loc[:,['WiFiMAC']]
        
        # ---START- Data selection from DataFrames ---
        clean_devices = self.devices
        clean_users = self.users
        clean_locations = self.locations
        # Data column selection from DataFrames
        clean_devices = clean_devices.loc[:, ['locationId', 'name', 'serialNumber', 'class', 'depProfile']]
        clean_locations = clean_locations.loc[:, ['id', 'name']]
        # ---END- Data selection from DataFrames ---

        # ---START- DataFrames Merge ---
        clean_devices = pd.merge(clean_devices, df_wifimac, left_index=True, right_index=True)
        clean_devices = pd.merge(clean_devices, clean_locations, left_on='locationId', right_on='id').loc[:,['name_x', 'serialNumber' ,'class', 'depProfile', 'WiFiMAC', 'name_y']]
        # ---END- DataFrames Merge ---
         
        # Renaming columns
        clean_devices.columns = ['name', 'serialnumber', 'type', 'depprofile', 'wifimac', 'schule']
        clean_devices['name'] = clean_devices['name'].apply(lambda x: self.alphanumeric_output(x))
        
        # Identify and handle duplicates in the "name" column
        for i in clean_devices.loc[clean_devices['name'].duplicated()]['name'].index:
            clean_devices['name'].loc[i:i] = clean_devices.loc[clean_devices['name'].duplicated()]['name'].apply(lambda x: x+'Duplikat'+''.join([str(randint(0,9)) for i in range(4)])+''.join([random.choice(string.ascii_letters) for i in range(4)]))[i]
            
        clean_devices.loc[clean_devices['depprofile'].str.contains('leh', case=False), 'depprofile'] = 'lehrer'
        clean_devices.loc[clean_devices['depprofile'].str.contains('schuel|shared', case=False), 'depprofile'] = 'schueler'
        clean_devices = clean_devices.loc[clean_devices['type'].str.contains(type, case=False)]
        clean_devices = clean_devices.loc[(~clean_devices['wifimac'].isna())].sort_values(by='schule').reset_index(drop=True)
        clean_devices['created_on'] = pd.Timestamp.now()
        return clean_devices
    
    # Save selected data to CSV file
    def save_as_csv(self, data, path):
        """Geben Sie zuerst den Pandas-DataFrame und anschließend einen gültigen Pfad an."""
        __df = data
        __date = pd.Timestamp.now().strftime('%Y-%m-%d')
        __filename = __date + '_jamf-Liste.csv'
        __path = path
        try:
            __df.to_csv(__path + __filename ,header=True, index=False)
            print('Daten erfolgreich exportiert nach:\n',__path + __filename)
            return __path + __filename
        except Exception as error02:
            print(error02)
