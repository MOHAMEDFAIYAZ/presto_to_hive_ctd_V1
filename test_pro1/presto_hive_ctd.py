# -*- coding: utf-8 -*-
"""
Created on 4th Oct 2022

@author:Faiyaz/Aravind
"""


import json
import csv
import operator
from datetime import datetime
import sys
import logging
import os
import datetime
from functionmapping import *
import numpy as np
import re
import pandas as pd
import configparser
from pathlib import Path
from collections import OrderedDict


# Function for stats of success views
def errorviews(lenerr, errordata, log_path):
    try:
        err_count = 0
        ph_list = []
        err_list = []

        error_colView = make_dir(log_path, 'ColumnViews/errorview')
        ph_colview = make_dir(log_path, 'ColumnViews/phcview')
        # ##print("Number of error views:{}".format(lenerr))
        pherrordatafilename = datetime.datetime.now().strftime('PHerrorviewfile_%d_%m_%Y_%H_%M_%S_%f.txt')
        errordatafilename = datetime.datetime.now().strftime('errorviewfile_%d_%m_%Y_%H_%M_%S_%f.txt')
        error_fname = os.path.join(error_colView, errordatafilename)
        pherror_fname = os.path.join(ph_colview, pherrordatafilename)
        for err in errordata:
            if "Error during conversion" in err:
                err_list.append(err)
                err_count += 1
            else:
                ph_list.append(err)
        ph_count = lenerr - err_count

        f = open(error_fname, "w+")
        ph = open(pherror_fname, "w+")
        f.write("Total Views having Issue/Error : %d " % err_count + "\n")
        f.write("List of Error Views : \n")
        ph.write("Total Views having Placeholders and manual intervention required : %d " % ph_count + "\n")
        ph.write("List of PH Views : \n")

        for i in err_list:
            f.write(i + "\n\n")
            errror_handling_path = make_dir(ph_path, "error_handling")

            with open(errror_handling_path + "/error_details.csv", 'a', newline='') as csvfile:
                # creating a csv writer object
                # #print('i am inside err wrtitng path')
                csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC, delimiter=',')
                # writing the data rows
                rows = i.split('-')
                # ##print("records===================>",rows)
                # csvwriter.writerow()
                csvwriter.writerow(rows)

        f.close()
        for j in ph_list:
            ph.write(j + "\n\n")
        ph.close()
    except:  # catch *all* exceptions
        e = sys.exc_info()[0]
        logging.exception("<p>Error: %s</p>" % e)


# Function for stats of error views
def successviews(lensuccess, successdata, log_path):
    try:
        success_colView = make_dir(log_path, 'ColumnViews/successviews')
        successdatafilename = datetime.datetime.now().strftime('successviewfile_%d_%m_%Y_%H_%M_%S_%f.txt')
        success_fname = os.path.join(success_colView, successdatafilename)
        # ##print("Number of success views:{}".format(lensuccess))
        fs = open(success_fname, "w+")
        fs.write("Total Success Views Created : %d" % lensuccess + "\n")
        fs.write("List of Success Views \n")
        for i in successdata:
            fs.write(i + "\n\n")
        fs.close()
    except:  # catch *all* exceptions
        e = sys.exc_info()[0]
        logging.exception("<p>Error: %s</p>" % e)




# =======================================================================================================================
# main code for the converstion of repo xml

def mainpresto_to_hive_function():
    querydata = ''
    with open("./venv/lookup/function_mapping.json", encoding='utf-8-sig') as json_file:
        json_data = json.load(json_file)
    with open('/Users/mohamed.faiyaz/zillow/Adjust_presto.sql', encoding='utf-8') as file_path:
        querydata_hana = file_path.read()

    # querydata_hana ="""regexp_extract(n.campaign_name,'(.*)\s\(',1)
    # strpos(REGEXP_LIKE(element_at(key,val),pattern),'search_str')
    # """
#     querydata_hana = """
#          ,case when ELEMENT_AT(dim_str, 'TrafficSourceMedium') = 'cpc'
# and regexp_like(ELEMENT_AT(dim_str, 'TrafficSourceAdContent'),'^[0-9]+\|[0-9]+\||^[0-9]+\%7C[0-9]+\%7C')
# then regexp_extract(split_part(regexp_replace(ELEMENT_AT(dim_str, 'TrafficSourceAdContent'), '\%7C','\|'),'|',3), 'dsa-([0-9]+)', 1) else ''
#  end as dyseaad_id"""
    #strpos(REGEXP_LIKE(element_at(key,val),pattern),'search_str')

    today = datetime.datetime.now()
    querydata,error = def_functionmapping(querydata_hana, json_data)

    querydata =querydata.replace("frwd_slashopen","\(").replace("frwd_slashclose","\)").\
        replace("single_quoteopen","'('").replace("single_quoteclose","')'").replace(r"\|",r"\\\|").replace(r"\(",r"\\\(")\
        .replace(r"\)",r"\\\)")

    # logging.exception("<p>Error: %s</p>" % e)


    return querydata

hive_result = mainpresto_to_hive_function()
print('hive_result==>',hive_result)
file = open('/Users/mohamed.faiyaz/Zillow/converted_output/ctd1.sql','w')
file.write(hive_result)



