#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Ervin Kurbegovic: notwendige Klassen für die täglich generierten JAMF-Listen mittels API
# Version aktualisiert am 15. Aug. 2022

#  Import der erforderlichen Bibliotheken/Module
import requests
from time import sleep
import pandas as pd
#import logging
import json
import re
import psycopg2
from sqlalchemy import create_engine
from unidecode import unidecode
from random import randint
import string, random
import sys
import os
import traceback
from dotenv import load_dotenv
from jamfsync import Jamfapi
load_dotenv(override=True)

def main():
    # Laden der geschützten Globalenvariablen, um Benutzernamen und Passwörter nicht in Klartext zu verwenden
    apiuser = os.getenv('APIUSERNAME2')
    apipwd = os.getenv('APIPASSWORD2')
    api_url = "https://laborciteqms.jamfcloud.com/api/"
    pruef = True
    while pruef:
        os.system('clear')
        jamf = Jamfapi(username=apiuser, password=apipwd, api_url=api_url, endpoint=None) #If the endpoint is set to None, all endpoints are queried
        engine = create_engine('postgresql://postgres@:5432/iserv')
        red = '\033[31m'
        red_end = '\033[0m'
        pruef = True
        if jamf.users.empty and jamf.classes.empty:
            print(red+'No user and class data in jamf!')
            print('Please run the initial synchronization of the users and classes\n', red_end)
        elif jamf.users.empty:
            print(red+'No user data in jamf!')
            print('Please run the initial synchronization of the users\n', red_end)
        elif jamf.classes.empty:
            print(red+'No class data in jamf!')
            print('Please run the initial synchronization of the classes\n', red_end)
        print('''################## Jamfsync ©citeq@School ##################''')
        print('-----'*12,'\n')
        mylocations = {'locations': [['0', 'LABOR Citeq'], ['4', 'Laborschule WFS'], ['1', 'Schule1 (Labor WFS)'],['2', 'Schule2 (Fortbildung)'], ['3', 'Schule3 (Lager)'], ['5', 'Schule4']]} 
        x_loc = 0
        location_key = mylocations['locations'][x_loc][0] # key
        location_name = mylocations['locations'][x_loc][1] # name
        print('Location:\t', location_name, '\nLocation_ID:\t', location_key)
        print('-----'*12,'\n')
        i = input('Jamfsync Options:\n(d)elete - (c)reate - (v)iew - (u)pdate - (r)eview - (q)uit\n\nIhre Eingabe: ')
        os.system('clear')
        if i == 'd':
            x = input('Deleting Options:\n(u)sers or (c)lasses or (a)ll\n\nIhre Eingabe: ')
            os.system('clear')
            if x == 'c':
                oic = input('(o)nly iserv classes\n\n Ihre Eingabe: ')
                if oic == 'o':
                    jamf.delete_classes()
                else:
                    jamf.delete_classes(only_iserv_classes=False)
            elif x == 'u':
                oiu = input('(o)nly iserv users\n\n Ihre Eingabe: ')
                if oiu == 'o':
                    jamf.delete_users()
                else:
                    jamf.delete_users(only_iserv_users=False)
            elif x == 'a':
                jamf.delete_users()
                jamf.delete_classes()
        elif i == 'c':
            z = input('Creating Options:\n(u)sers or (c)lasses or (a)ll\n\nIhre Eingabe: ')
            os.system('clear')
            if z == 'c':
                if jamf.classes.empty == True or jamf.classes['studentCount'].sum() == 0:
                    jamf.delete_classes()
                    jamf = Jamfapi(apiuser, apipwd)
                    api_data = jamf.create_class_template(initial_sync=True)
                    jamf.create_classes(class_template=api_data, location=location_name)
            elif z == 'u':
                initial_user_sync = jamf.create_user_template(initial_sync=True, location=location_name)
                jamf.create_users(initial_user_sync, location_name)
                if jamf.classes.empty == False and jamf.classes['studentCount'].sum() == 0:
                    jamf.delete_classes()
                    jamf = Jamfapi(apiuser, apipwd)
                    api_data = jamf.create_class_template(initial_sync=True)
                    jamf.create_classes(class_template=api_data, location=location_name)
            elif z == 'a':
                initial_user_sync = jamf.create_user_template(initial_sync=True)
                jamf.create_users(initial_user_sync, location_name)
                api_data = jamf.create_class_template()
                jamf.create_classes(class_template=api_data, location=location_name)
        elif i == 'v':
            os.system('clear')
            z = input('Display Options:\n(u)sers or (c)lasses or (a)ll\n\nIhre Eingabe: ')
            os.system('clear')
            if z == 'u':
                jamf_users = jamf.users
                if jamf_users.empty == False:
                    print('************' *3, 'Jamf Users', '************' *3, '\n')
                    print('Quantity:', len(jamf_users), '\nColumns:', ", ".join([i for i in jamf_users.columns]))
                    print('************' *3, 'DataFrame', '************' *3)
                    print(jamf_users[['id', 'username', 'email']].sample(20))
                    input()
                else:
                    print(red,'No user data!', red_end)
                    sleep(3)
            elif z == 'c':
                jamf_classes = jamf.classes
                if jamf_classes.empty == False:
                    print('************' *3, 'Jamf Classes', '************' *3, '\n')
                    print('Quantity:', len(jamf_classes), '\nColumns:', ", ".join([i for i in jamf_classes.columns]))
                    print('************' *3, 'DataFrame', '************' *3)
                    print(jamf_classes[['uuid', 'name', 'description', 'studentCount', 'teacherCount', 'locationId']])
                    input()
                else:
                    print(red,'No class data!', red_end)
                    sleep(3)
            elif z == 'a':
                jamf_users = jamf.users
                if jamf_users.empty == False:
                    print('Jamf Users')
                    print('Quantity:', len(jamf_users)) #, '\nColumns:', ", ".join([i for i in jamf_users.columns]))
                    print('************' *3, 'DataFrame', '************' *3)
                    print(jamf_users[['id', 'username', 'email']].sample(10))
                else:
                    print(red,'No user data!', red_end)
                    sleep(3)
                jamf_classes = jamf.classes
                if jamf_classes.empty == False:
                    print('\nJamf Classes')
                    print('Quantity:', len(jamf_classes)) #, '\nColumns:', ", ".join([i for i in jamf_classes.columns]))
                    print('************' *3, 'DataFrame', '************' *3)
                    print(jamf_classes[['uuid', 'name', 'description', 'studentCount', 'teacherCount', 'locationId']])
                    input()
                else:
                    print(red,'No class data!', red_end)
                    sleep(3)
        elif i == 'u':
            s = input('Syncing Options:\n(u)sers or (c)lasses\n\nIhre Eingabe: ')
            if s == 'u':
                os.system('clear')
                api_data = jamf.create_user_template(location=location_name)
                if api_data != None:
                    if 'add' in api_data.keys():
                        if len(api_data['add']) > 0:
                            fresh_user_keys = [item.keys() for item in api_data['add']]
                            all_keys = [key for sublist in fresh_user_keys for key in sublist]
                            fresh = jamf.review_users(fresh_users = all_keys)
                            jamf.update_users(user_template=api_data, location=location_name, fresh_users=fresh)
                    elif len(api_data['delete']) > 0:
                        jamf.update_users(user_template=api_data, location=location_name)
            elif s == 'c':
                api_data = jamf.create_class_template(initial_sync=False)
                if api_data != None:
                   jamf.update_classes(class_template=api_data, location=location_name)
                else:
                    jamf.update_classes(location=location_name)
        elif i == 'r':
            jamf.review_users(location_name)
        elif i == 'q':
            pruef = False

if __name__ == "__main__":
    main()
