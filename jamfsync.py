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
import socket
#from dotenv import load_dotenv

#load_dotenv(override=True)

# Class and methods for authentication and converting API data to DataFrames.
class JamfAPI:
    """
    The JamfAPI class connects to the Jamf|School APIv1 and retrieves data in the form of pandas DataFrames. 
    In addition, all IServ users and certain IServ groups can be synced. The IServ groups are turned into classes on jamf|School. 
    However, not all groups are synced as classes, but only those with the group characteristic ****, which can be set on the IServ.

    The primary goal of the class is to transfer the user and group data from the school server IServ to the jamf School cloud, 
    and also to create classes in Jamf School, whereby a distinction is made between students and teachers.
    The distinction is based on a group that uniquely identifies the teachers on the IServ. The default group is "lehrkreafte", which can be modified. 
    If the group is not sufficient, you can also use a role such as "ROLE_TEACHER" to separate teachers and students.
    
    Provides a method, `get_jamf_data`, to fetch and store Jamf|School data in accessible variables.

    API credentials can be found in Jamf settings:
        - API endpoint: Settings > API > ...
        - Username: Devices > Enroll Devices > MDM Server URL ...network=****...
    """

    # Constructor: Initializes the object
    def __init__(self, username: str, password: str, api_url: str, endpoint='all', teacher_group='lehrkraefte', teacher_role='ROLE_TEACHER'):
        if not username:
            raise ValueError("username argument is required")
        if not password:
            raise ValueError("Password argument is required")
        if not api_url:
            raise ValueError("API_URL argument is required")
        self.username = username
        self.password = password
        self.api_url = api_url
        self.teacher_role = teacher_role
        self.teacher_group = teacher_group
        self.hostname = '@'+socket.gethostname()[6:]
        self.red = '\033[0;31m'
        self.yellow = '\033[1;33m' 
        self.reset_color = '\033[0m' # reset color
        self.engine = create_engine('postgresql://postgres@:5432/iserv')
        self.session = requests.Session()
        self.headers = {
                        'User-Agent': 'curl/7.24.0',
                        'X-Server-Protocol-Version':'3',
                        'Content-Type': 'application/json'  # Content type for JSON data
                        }
        # Available Jamf|School APIv1 endpoints - As of August 9, 2022 (Note: Jamf|School is working on API v2)
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
            if endpoint != 'all':
                self.custom = self.__get_jamf_data(str(endpoint))
        except Exception as e:
            print('Wrong endpoint', e)
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

    def sync_jamf_data(self, endpoint: str, location: str):
        if not endpoint:
            raise ValueError("Endpoint argument is required")
        if not location:
            raise ValueError("Location argument is required")
        try:
            location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
            if endpoint == 'users':
                df_loc = self.locations
                if df_loc.empty:
                    raise ValueError("DataFrame is empty")
                location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
                url = self.endpoints['users'][0]
                isv_users = self._get_iserv_data('iserv_users')
                isv_students = self._get_iserv_data('iserv_students')
                isv_teachers = self._get_iserv_data('iserv_teachers')
                student_pretty_act = ", ".join([student for student in isv_students['act']])
                teacher_pretty_act = ", ".join([teacher for teacher in isv_teachers['act']])
                print(student_pretty_act)
                print('-----'*10)
                print('STUDENTS:', len(isv_students))
                input('\nEnter to sync...')
                os.system('clear')
                # FOR-Loop for creating users in jamf based on provided IServ data
                result_students = {'ids':[], 'username':[]}
                for counter, (index, data) in enumerate(isv_students.iterrows()):
                    user = data['act']
                    data = {
                        "username": data['act'],
                        "password": "", # empty
                        "email": data['email'], # intern comment important only for citeq@School: For email addresses (fol/foe indicators), update user data instead of overwriting it.
                        "firstName": data["firstname"],
                        "lastName": data['lastname'],
                        "memberOf": data['actgrp'],
                        "locationId": location_id,
                        "notes": "automatisch generierte Benutzer auf Basis der IServ-Benuter."
                        }
                    response = self.session.post(url, headers=self.headers, json=data, auth=(self.username, self.password))
                    # Check response status code to ensure successful request transmission (expected: 2xx code).
                    if response.status_code == 200:
                        print(f"{user} created - Progress {counter+1} of {len(isv_students)}")
                        result_students['ids'].append(json.loads(response.text)['id'])
                        result_students['username'].append(user)
                    else:
                        print(f"Error creating students: {response.status_code} - {response.text}")
                os.system('clear')
                print(teacher_pretty_act)
                print('-----'*10)
                print('TEACHERS:', len(isv_teachers))
                input('\nEnter to sync...')
                os.system('clear')
                result_teachers = {'ids':[], 'username':[], 'groups':[]}
                for counter, (index, data) in enumerate(isv_teachers.iterrows()):
                    user = data['act']
                    teacher_groups = data['actgrp']
                    data = {   
                        "username": data['act'],
                        "password": "", # empty
                        "email": data['email'], # intern comment important only for citeq@School: For email addresses (fol/foe indicators), update user data instead of over>
                        "firstName": data["firstname"],
                        "lastName": data['lastname'],
                        "memberOf": [],
                        "locationId": location_id,
                        "notes": "automatisch generierte Benutzer auf Basis der IServ-Benuter."
                        }
                    response = self.session.post(url, headers=self.headers, json=data, auth=(self.username, self.password))
                    # Check response status code to ensure successful request transmission (expected: 2xx code).
                    if response.status_code == 200:
                        print(f"{user} created - Progress {counter+1} of {len(isv_teachers)}")
                        result_teachers['ids'].append(json.loads(response.text)['id'])
                        result_teachers['username'].append(user)
                        result_teachers['groups'].append(teacher_groups)
                    else:
                        print(f"Error creating teacher: {response.status_code} - {response.text}")
                self._creat_classes(class_template=result_teachers)
            elif endpoint == 'teachers':
                isv_teachers = self._get_iserv_data('iserv_teachers')
                teacher_pretty_act = ", ".join([teacher for teacher in isv_teachers['act']])
                print(teacher_pretty_act)
                print('-----'*10)
                print('TEACHERS:', len(isv_teachers))
                input('\nEnter to sync...')
                os.system('clear')
                result_teachers = {'ids':[], 'username':[], 'groups':[]}
                for counter, (index, data) in enumerate(isv_teachers.iterrows()):
                    user = data['act']
                    teacher_groups = data['actgrp']
                    data = {   
                        "username": data['act'],
                        "password": "", # empty
                        "email": data['email'], # intern comment important only for citeq@School: For email addresses (fol/foe indicators), update user data instead of over>
                        "firstName": data["firstname"],
                        "lastName": data['lastname'],
                        "memberOf": [],
                        "locationId": location_id,
                        "notes": "automatisch generierte Benutzer auf Basis der IServ-Benuter."
                        }
                    response = self.session.post(url, headers=self.headers, json=data, auth=(self.username, self.password))
                    # Check response status code to ensure successful request transmission (expected: 2xx code).
                    if response.status_code == 200:
                        print(f"{user} created - Progress {counter+1} of {len(isv_teachers)}")
                        result_teachers['ids'].append(json.loads(response.text)['id'])
                        result_teachers['username'].append(user)
                        result_teachers['groups'].append(teacher_groups)
                    else:
                        print(f"Error creating teacher: {response.status_code} - {response.text}")
                self._creat_classes(class_template=result_teachers)
        except Exception as e:
            print(e)

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

    def _create_classes(self, class_template: dict, suffix='', praefix=''):
        df_loc = self.locations
        location_id = str(df_loc[df_loc['name'] == location]['id'].values[0])
        isv_groups_to_sync = self._get_iserv_data('iserv_groups')
        pretty_classes = ", ".join([group for group in isv_groups_to_sync])
        print(pretty_classes)
        print('-----'*10)
        print('Classes:', len(isv_groups_to_sync))
        input('\nEnter to sync...')
        os.system('clear')
        for counter, cl in enumerate(isv_groups_to_sync):
            url = self.endpoints['classes'][0]
            data = {
                "name": f"{praefix}{cl}{suffix}",
                "description":"automatisch generierte Klasse auf Basis der IServ-Gruppen.",
                "teachers": [],
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
    def _get_iserv_data(self, data='all', teacher_group='lehrkraefte'):
        try:
            if data == 'teacher_list': 
                return pd.read_sql(f'''select actuser, 
                                           CONCAT(actuser, (select '@' || substring(pg_read_file('/etc/hostname') from 7 for char_length(pg_read_file('/etc/hostname')) - 7))) as email
                                           from members where actgrp = 'lehrkraefte';''', self.engine)['email'].to_list()
            elif data == 'iserv_teachers':
                isv_users = self._get_iserv_data('iserv_users')
                return isv_users.loc[isv_users['teacher'] == True]
            elif data == 'iserv_students':
                isv_users = self._get_iserv_data('iserv_users')
                return isv_users.loc[isv_users['teacher'] == False]
            elif data == 'iserv_users':
                isv_users = pd.read_sql(f'''select * from users;''', self.engine)
                isv_groups = pd.read_sql(f'''select * from groups;''', self.engine)
                isv_members = pd.read_sql(f'''select * from members;''', self.engine)
                isv_users = isv_users[['act', 'firstname', 'lastname']]
                isv_members = isv_members.groupby('actuser').agg(list).reset_index()
                df_prep_users = isv_users.merge(isv_members, left_on='act', right_on='actuser')[['act', 'firstname', 'lastname', 'actgrp']]
                df_prep_users['email'] = df_prep_users['act'].apply(lambda x: x+self.hostname)
                df_prep_users['teacher'] = df_prep_users['actgrp'].apply(lambda x: True if self.teacher_group in x else False)
                return df_prep_users
            elif data == 'iserv_groups':
                isv_groups = pd.read_sql(f'''select * from groups;''', self.engine)
                return isv_groups[(isv_groups['type'] == 'jamfsync') & (isv_groups['deleted'].isna())][['act']]
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
print('test')
jamf = JamfAPI(username='67441756', password='RG5JXPNSGJIJZE95NK5ILYL5HMRHQBVI', api_url='https://laborciteqms.jamfcloud.com/api/')
print('test2')
print('test3')
