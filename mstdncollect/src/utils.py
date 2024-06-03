import json
import pandas as pd
import ipdb
from datetime import datetime 
import time 
import pathlib 
import pytz
from bs4 import BeautifulSoup
import jsonlines
import os
import plotly.graph_objects as go
from plotly import io as pio
import numpy as np
from flashtext import KeywordProcessor
import logging
import requests
import sys
import pickle
import sqlite3
from sqlite3 import Error
from collections import Counter

###################################
# directory organisation
###################################

def parse_directories_updsavestructure4(sourcedir, targetdir, dbconn=None, auth_dict=None, cutoff_date="2023-12-02"):
    # change head directory structure to toots/year/month/toots.jsonl
    monthsint = {"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11, "Dec":12}
    i = 0 
    datalists = [f.name for f in os.scandir("{}/toots/".format(sourcedir)) if f.is_dir()]     
    for datafolder in datalists:
        # ipdb.set_trace()
        # print(datafolder)
        datadir = "{}/toots/{}/".format(sourcedir, datafolder)
        # month = datafolder[:3]
        # monthint = monthsint[month] 
        # year = datafolder[3:]        
        with jsonlines.open("{}/toots.jsonl".format(datadir), "r") as jsonl_read:
            try:
                for data in jsonl_read.iter(type=dict, skip_invalid=True):
                    if data["account"]["bot"]:
                        continue
                    acc = add_unique_account_id(data["account"], data["instance_name"])
                    data["account"] = acc
                    data = add_unique_toot_id(data, data["instance_name"])
                    data["rebloggedbyuser"] = None
                    if "spoiler_clean_text" not in data.keys() and isinstance(data["spoiler_text"], str):
                        if len(data["spoiler_text"]) > 0:
                            tootspoiler = BeautifulSoup(data["spoiler_text"], "html.parser")
                            data["spoiler_clean_text"] = tootspoiler.get_text()
                        else:
                            data["spoiler_clean_text"] = ""
                    if "account_note_text" not in data["account"].keys() and isinstance(data["account"]["note"], str):
                        if len(data["account"]["note"]) > 0:
                            accountnote = BeautifulSoup(data["account"]["note"], "html.parser")
                            data["account"]["account_note_text"] = accountnote.get_text()
                        else:
                            data["account"]["account_note_text"] = ""

                    # year, month determined by edited_at if exists, else by created_at
                    if "edited_at" in data.keys() and data["edited_at"] is not None and data["edited_at"] != "":
                        try:
                            monthyear = pd.Timestamp(np.datetime64(data["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
                        except:
                            monthyear = pd.Timestamp(np.datetime64(data["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                    else:
                        try:
                            monthyear = pd.Timestamp(np.datetime64(data["created_at"])).tz_localize("CET").astimezone(pytz.utc)
                        except:
                            monthyear = pd.Timestamp(np.datetime64(data["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                    if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc):
                        continue
                    if dbconn is not None:
                        newrow = build_db_row(data)        
                        if newrow is not None:            
                            execute_insert_sql(dbconn, "toots", newrow)
                        else:
                            print(newrow)
                        
                        # if data["in_reply_to_id"] is not None:
                        # get parent toot and update id in DB
                        try:
                            ancestors = get_parent_toot(data, data["instance_name"], auth_dict=auth_dict)
                            # get immediate parent
                            if ancestors is not None:
                                parenttoot = add_unique_toot_id(ancestors[-1], data["instance_name"])
                                if "edited_at" in parenttoot.keys() and parenttoot["edited_at"] is not None and parenttoot["edited_at"] != "":
                                    try:
                                        monthyear = pd.Timestamp(np.datetime64(parenttoot["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
                                    except:
                                        monthyear = pd.Timestamp(np.datetime64(parenttoot["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                                else:
                                    try:
                                        monthyear = pd.Timestamp(np.datetime64(parenttoot["created_at"])).tz_localize("CET").astimezone(pytz.utc)
                                    except:
                                        monthyear = pd.Timestamp(np.datetime64(parenttoot["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                                if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc):
                                    # do not collect it
                                    raise AttributeError
                                parenttoot["account"] = add_unique_account_id(parenttoot["account"], data["instance_name"])
                                parenttoot["instance_name"] = data["instance_name"]
                                parenttoottext = BeautifulSoup(parenttoot["content"], "html.parser")
                                parenttoot["toottext"] = parenttoottext.get_text()
                                parenttoot["rebloggedbyuser"] = None
                                if "spoiler_clean_text" not in parenttoot.keys() and isinstance(parenttoot["spoiler_text"], str):
                                    if len(parenttoot["spoiler_text"]) > 0:
                                        tootspoiler = BeautifulSoup(parenttoot["spoiler_text"], "html.parser")
                                        parenttoot["spoiler_clean_text"] = tootspoiler.get_text()
                                    else:
                                        parenttoot["spoiler_clean_text"] = ""
                                if "account_note_text" not in parenttoot["account"].keys() and isinstance(parenttoot["account"]["note"], str):
                                    if len(parenttoot["account"]["note"]) > 0:
                                        accountnote = BeautifulSoup(parenttoot["account"]["note"], "html.parser")
                                        parenttoot["account"]["account_note_text"] = accountnote.get_text()
                                    else:
                                        parenttoot["account"]["account_note_text"] = ""
                                
                                parentglobalID = parenttoot["globalID"]
                                parentaccountID = parenttoot["account"]["globalID"]
                                parentrow = build_db_row(parenttoot)        
                                if parentrow is not None:            
                                    execute_insert_sql(dbconn, "toots", parentrow)
                                else:
                                    print(parentrow)

                                data["in_reply_to_id"] = parentglobalID
                                data["in_reply_to_account_id"] = parentaccountID
                                execute_update_replies_sql(dbconn, "toots", data["globalID"], parentglobalID, parentaccountID)
                                if i > 0 and i % 5 == 0:
                                    time.sleep(15)
                        except:
                            # if error retrieving head, remove - if part of relevant conversation, will probably be collected later
                            data["in_reply_to_id"] = ""
                            data["in_reply_to_account_id"] = ""
                            execute_update_replies_sql(dbconn, "toots", data["globalID"], "", "")

                    targetfolder = "{}/toots/{}/{}/".format(targetdir, monthyear.year, monthyear.month)
                    if not pathlib.Path(targetfolder).exists():
                        pathlib.Path(targetfolder).mkdir(parents=True, exist_ok=True)
                    with jsonlines.open("{}/toots.jsonl".format(targetfolder), "a") as jsonl_write:
                        jsonl_write.write(data)

                    i += 1
            except:
                print(datadir)

def parse_directories_updsavestructure3(sourcedir, targetdir):
    # append data to jsonline files: from sourcedir to targetdir
    i = 0 
    j = 0
    k = 0
    datalists = [f.name for f in os.scandir("{}/toots/".format(sourcedir)) if f.is_dir()]     
    for datafolder in datalists:
        print(datafolder)
        datadir = "{}/toots/{}/".format(sourcedir, datafolder)
        targetfolder = "{}/toots/{}/".format(targetdir, datafolder)
        if not pathlib.Path(targetfolder).exists():
            pathlib.Path(targetfolder).mkdir(parents=True, exist_ok=True)        
        with jsonlines.open("{}/toots.jsonl".format(datadir), "r") as jsonl_read:
            with jsonlines.open("{}/toots.jsonl".format(targetfolder), "a") as jsonl_write:
                try:
                    for data in jsonl_read.iter(type=dict, skip_invalid=True):
                        jsonl_write.write(data)
                        i += 1
                except:
                    print(datadir)
                    # ipdb.set_trace()
    # tags
    # datadir = "{}/tags/".format(sourcedir)
    # targetfolder = "{}/tags/".format(targetdir)   
    # if not pathlib.Path(targetfolder).exists():
    #     pathlib.Path(targetfolder).mkdir(parents=True, exist_ok=True)          
    # with jsonlines.open("{}/tags.jsonl".format(datadir), "r") as jsonl_read:
    #     with jsonlines.open("{}/tags.jsonl".format(targetfolder), mode='a') as jsonl_write:
    #         try:
    #             for data in jsonl_read.iter(type=dict, skip_invalid=True):                                                                         
    #                 jsonl_write.write(data)    
    #                 j += 1
    #         except:
    #             print(datadir,"tags")
    # users        
    datadir = "{}/users/".format(sourcedir)
    targetfolder = "{}/users/".format(targetdir)   
    if not pathlib.Path(targetfolder).exists():
        pathlib.Path(targetfolder).mkdir(parents=True, exist_ok=True)          
    with jsonlines.open("{}/users.jsonl".format(datadir), "r") as jsonl_read:
        with jsonlines.open("{}/users.jsonl".format(targetfolder), mode='a') as jsonl_write:
            try:
                for data in jsonl_read.iter(type=dict, skip_invalid=True):                                                                         
                    jsonl_write.write(data)    
                    k += 1
            except:
                print(datadir,"users")
    print(i, j, k)

def parse_directories_updsavestructure2(topdir, servers, language="en", dirout="/tmp/"):
    # for directories of second backlog run
    pathlib.Path("{}".format(dirout)).mkdir(parents=True, exist_ok=True)
    #tootids = KeywordProcessor()
    #userids = KeywordProcessor()
    #tagids = KeywordProcessor()
    toall = 0
    uall = 0
    taall = 0
    for server in servers:
        print(server)
        datadir = "{}/toots/{}/".format(topdir, server)
        if not pathlib.Path(datadir).exists():
            print("not exists: {}".format(server))
            continue
        datelists = [f.name for f in os.scandir(datadir) if f.is_dir()] 
        for date in datelists:
            datedir = "{}/{}/".format(datadir, date)            
            with jsonlines.open("{}/toots.jsonl".format(datedir), "r") as jsonl_f:
               try:
                 for data in jsonl_f:    
                    toall += 1                     
                    if data["language"] != language:
                        continue
                    #if data["id"] in tootids:
                    #    continue
                    #else:
                    #    tootids.add_keywords_from_list([data["id"]])
                    tootdate = pd.Timestamp(data["created_at"])
                    data["instance_name"] = server
                    try:
                        try:
                            monthyear = pd.Timestamp(tootdate).tz_localize("CET").astimezone(pytz.utc).strftime("%b%Y")
                        except:
                            monthyear = pd.Timestamp(tootdate).tz_localize("Europe/Paris").astimezone(pytz.utc).strftime("%b%Y")
                    except:
                        try:
                            monthyear = pd.Timestamp(tootdate).tz_convert(tz=None).strftime("%b%Y")
                        except:
                            monthyear = pd.Timestamp(tootdate).tz_convert("Europe/Paris").strftime("%b%Y")
                    pathout = "{}/toots/{}/".format(dirout, monthyear)
                    pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
                    with jsonlines.open("{}/toots.jsonl".format(pathout), mode='a') as writer:
                        writer.write(data)
               except:
                  print(server,datedir)
                  ipdb.set_trace()
        # tags
        datadir = "{}/tags/{}/".format(topdir, server)        
        with jsonlines.open("{}/tags.jsonl".format(datadir), "r") as jsonl_f:
           try:  
            for data in jsonl_f:                                                     
                taall += 1
                #if data["name"] in tagids:
                #    continue
                #else:
                #    tagids.add_keywords_from_list([data["name"]])                            
                pathout = "{}/tags/".format(dirout)
                pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
                with jsonlines.open("{}/tags.jsonl".format(pathout), mode='a') as writer:
                    writer.write(data)    
           except:
               print(datadir,"tags")
        # users
        datadir = "{}/users/{}/".format(topdir, server)                      
        with jsonlines.open("{}/users.jsonl".format(datadir), "r") as jsonl_f:
          try:  
            for data in jsonl_f:                                                     
                uall += 1
                #if data["id"] in userids:
                #    continue
                #else:
                #    userids.add_keywords_from_list([data["id"]])                            
                pathout = "{}/users/".format(dirout)
                pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
                with jsonlines.open("{}/users.jsonl".format(pathout), mode='a') as writer:
                    writer.write(data)    
          except:
             print(datadir,"users")    
    print(toall, uall, taall)
    #return tootids, userids, tagids       

def parse_directories_updsavestructure(topdir, servers, language="en", dirout="/tmp/"):
    # for directories of first run
    pathlib.Path("{}/{}".format(topdir, dirout)).mkdir(parents=True, exist_ok=True)
    #tootids = KeywordProcessor()
    #userids = KeywordProcessor()
    #tagids = KeywordProcessor()
    toall = 0
    uall = 0
    taall = 0
    for server in servers:
        print(server)
        datadir = "{}/{}/".format(topdir, server)
        datelists = [f.name for f in os.scandir(datadir) if f.is_dir()] 
        for date in datelists:
            print(date)
            datedir = "{}/{}/".format(datadir, date)
            runs_folders = [f.name for f in os.scandir(datedir) if f.is_dir()]        
            for run in runs_folders:
                tootdir = "{}{}/".format(datedir, run)
                # toots
                tootsfiles = [g.name for g in os.scandir(tootdir) if g.is_file() and "toots_" in g.name]
                for tfile in tootsfiles:
                    with jsonlines.open("{}/{}".format(tootdir, tfile), "r") as jsonl_f:
                        try:
                         for data in jsonl_f:    
                            toall += 1                     
                            if data["language"] != language:
                                continue
                            #if data["id"] in tootids:
                            #    continue
                            #else:
                            #    tootids.add_keywords_from_list([data["id"]])
                            tootdate = pd.Timestamp(data["created_at"])
                            data["instance_name"] = server

                            try:
                                monthyear = pd.Timestamp(tootdate).tz_localize("CET").astimezone(pytz.utc).strftime("%b%Y")
                            except:
                                monthyear = pd.Timestamp(tootdate).tz_convert(tz=None).strftime("%b%Y")
                            #pathout = "{}/{}/toots/{}/".format(topdir, dirout, monthyear)
                            #pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
                            #with jsonlines.open("{}/toots.jsonl".format(pathout), mode='a') as writer:
                            #    writer.write(data)    
                        except:
                            print(tootdir,tfile,run)
                            ipdb.set_trace()
                # tags
                tagsfiles = [g.name for g in os.scandir(tootdir) if g.is_file() and "tags_" in g.name]
                for tfile in tagsfiles:
                    with jsonlines.open("{}/{}".format(tootdir, tfile), "r") as jsonl_f:
                        try:
                         for data in jsonl_f:                                                     
                            taall += 1
                            #if data["name"] in tagids:
                            #    continue
                            #else:
                            #    tagids.add_keywords_from_list([data["name"]])                            
                            
                            #pathout = "{}/{}/tags/".format(topdir, dirout)
                            #pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
                            #with jsonlines.open("{}/tags.jsonl".format(pathout), mode='a') as writer:
                            #    writer.write(data)    
                        except:
                            print(tootdir,tfile)
                            continue
                # users
                userfiles = [g.name for g in os.scandir(tootdir) if g.is_file() and "users_" in g.name]
                for tfile in userfiles:
                    with jsonlines.open("{}/{}".format(tootdir, tfile), "r") as jsonl_f:
                        try:
                         for data in jsonl_f:                                                     
                            uall += 1
                            #if data["id"] in userids:
                            #    continue
                            #else:
                            #    userids.add_keywords_from_list([data["id"]])                            
                            #pathout = "{}/{}/users/".format(topdir, dirout)
                            #pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
                            #with jsonlines.open("{}/users.jsonl".format(pathout), mode='a') as writer:
                            #    writer.write(data)    
                        except:
                            print(tootdir,tfile)
                            continue
    print(toall, uall, taall)
    #return tootids, userids, tagids        

def parse_directories_tsv2gargantext(topdir, servers, language="en", dirout="/tmp/"):

    postids = KeywordProcessor()
    for server in servers:
        datadir = "{}/{}/".format(topdir, server)
        datelists = [f.name for f in os.scandir(datadir) if f.is_dir()] 
        for date in datelists:
            gtext = {"Publication Day": [], "Publication Month": [], "Publication Year": [],
                     "Authors": [], "Title": [], "Abstract": [], "Source": []}
            datedir = "{}/{}/".format(datadir, date)
            runs_folders = [f.name for f in os.scandir(datedir) if f.is_dir()]        
            for run in runs_folders:
                tootdir = "{}{}/".format(datedir, run)
                tootsfiles = [g.name for g in os.scandir(tootdir) if g.is_file() and "toots_" in g.name]
                for tfile in tootsfiles:
                    with jsonlines.open("{}/{}".format(tootdir, tfile), "r") as jsonl_f:
                        for data in jsonl_f:                         
                            if data["language"] != language:
                                continue
                            if data["id"] in postids:
                                continue
                            else:
                                postids.add_keywords_from_list([data["id"]])
                            tootdate = pd.Timestamp(data["created_at"])
                            gtext["Publication Day"].append(tootdate.day)
                            gtext["Publication Month"].append(tootdate.month)
                            gtext["Publication Year"].append(tootdate.year)
                            gtext["Authors"].append(data["account"]["id"])
                            gtext["Title"].append(server)
                            gtext["Abstract"].append(data["toottext"]) 
                            gtext["Source"].append("{}-{}-{}".format(data["id"],
                                                                    data["url"],  
                                                                    BeautifulSoup(data["account"]["note"], "html.parser").get_text())) 
            gtexttsv = pd.DataFrame.from_dict(gtext)
            gtexttsv.to_csv("{}/gtexttest_{}.csv".format(dirout, date), sep="\t", index=False)

###################################
# data collection based on hashtags
###################################

def collect_hashtag_interactions_apidirect(res, instance_name):

        # convert to json        
        fetched_toots = res.json()    
        print("Fetched {} public toots...".format(len(fetched_toots)))
        filtered_toots  = []
        collected_tags  = []
        collected_users = []
        track_user_ids  = []    
        
        for i in fetched_toots:
            if i["account"]["bot"]:
                continue
            if i["visibility"] != "public":
                continue            
            # extract tags
            if len(i["tags"]) > 0:
                collected_tags.extend(i["tags"])
            # get and store account id and its details
            account_id = i["account"]["acct"]
            if account_id not in track_user_ids:
                collected_users.append(i["account"])
                track_user_ids.append(account_id)
            # extract toot text content
            toot = BeautifulSoup(i["content"], "html.parser")
            # add extra entry in toot with Mastodon instance name
            i["instance_name"] = instance_name
            acc = add_unique_account_id(i["account"], i["instance_name"])
            i["account"] = acc
            i = add_unique_toot_id(i, i["instance_name"])
            i["rebloggedbyuser"] = []
            if isinstance(i["spoiler_text"], str) and len(i["spoiler_text"]) > 0:
                tootspoiler = BeautifulSoup(i["spoiler_text"], "html.parser")
                i["spoiler_clean_text"] = tootspoiler.get_text()
            if isinstance(i["account"]["note"], str) and len(i["account"]["note"]) > 0:
                accountnote = BeautifulSoup(i["account"]["note"], "html.parser")
                i["account"]["account_note_text"] = accountnote.get_text()
            # add extra entry in toot dictionary
            toottext = toot.get_text()            
            i["toottext"] = toottext            
            filtered_toots.append(i)                    
            for kk in i.keys():
                if isinstance(i[kk], datetime):
                    i[kk] = i[kk].astimezone(pytz.utc)
                    i[kk] = i[kk].strftime("%Y-%m-%dT%H:%M:%S")
                elif isinstance(i[kk], dict):
                    for kkk in i[kk].keys():
                        if isinstance(i[kk][kkk], datetime):
                            i[kk][kkk] = i[kk][kkk].astimezone(pytz.utc)
                            i[kk][kkk] = i[kk][kkk].strftime("%Y-%m-%dT%H:%M:%S") 
            
            # identify heads of conversations and get their full context and their reblogs
        # collect dominant hashtags

        return filtered_toots, collected_tags, collected_users 

###################################
# data collection for user posts
###################################

def collect_user_postingactivity_apidirect(useracct, instance_name, savedir="/tmp/", auth_dict=None, cutoff_date="2023-12-02"):

    fetched_toots = get_outbox_from_user(useracct, instance_name, auth_dict)
    if fetched_toots is None:
        # user does not allow his outbox to be queried
        return None, None
    print("Fetched {} public toots...".format(len(fetched_toots)))
    filtered_toots  = []
    collected_tags  = []
    for status in fetched_toots:
        if isinstance(status["object"], str):
            status_id = status["object"].split("/")[-1]
            tootinstance = status["object"][8:].split("/")[0]
        elif isinstance(status["object"], dict):
            status_id = status["object"]["id"].split("/")[-1]
            tootinstance = status["object"]["id"][8:].split("/")[0]
        toot = get_toot_from_statusid(status_id, tootinstance, auth_dict=auth_dict)
        i = toot
        if i["account"]["bot"]:
            continue
        if i["visibility"] != "public":
            continue
        if "edited_at" in i.keys() and i["edited_at"] is not None and i["edited_at"] != "":
            try:
                monthyear = pd.Timestamp(np.datetime64(i["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
            except:
                monthyear = pd.Timestamp(np.datetime64(i["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
        else:
            try:
                monthyear = pd.Timestamp(np.datetime64(i["created_at"])).tz_localize("CET").astimezone(pytz.utc)
            except:
                monthyear = pd.Timestamp(np.datetime64(i["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                
                print(monthyear)

        if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc):
            # do not collect it
            continue
        i["instance_name"] = tootinstance
        acc = add_unique_account_id(i["account"], tootinstance)
        i["account"] = acc
        i = add_unique_toot_id(i, tootinstance)
        i["rebloggedbyuser"] = []
        # extract tags
        if len(i["tags"]) > 0:
            for ktag in i["tags"]:
                if ktag not in collected_tags:
                    collected_tags.append(ktag)
        # extract toot text content
        toot = BeautifulSoup(i["content"], "html.parser")
        # add extra entry in toot dictionary
        toottext = toot.get_text()
        i["toottext"] = toottext
        if isinstance(i["spoiler_text"], str) and len(i["spoiler_text"]) > 0:
            tootspoiler = BeautifulSoup(i["spoiler_text"], "html.parser")
            i["spoiler_clean_text"] = tootspoiler.get_text()
        if isinstance(i["account"]["note"], str) and len(i["account"]["note"]) > 0:
            accountnote = BeautifulSoup(i["account"]["note"], "html.parser")
            i["account"]["account_note_text"] = accountnote.get_text()
        filtered_toots.append(i)                    
        for kk in i.keys():
            if isinstance(i[kk], datetime):
                i[kk] = i[kk].astimezone(pytz.utc)
                i[kk] = i[kk].strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(i[kk], dict):
                for kkk in i[kk].keys():
                    if isinstance(i[kk][kkk], datetime):
                        i[kk][kkk] = i[kk][kkk].astimezone(pytz.utc)
                        i[kk][kkk] = i[kk][kkk].strftime("%Y-%m-%dT%H:%M:%S") 
        save2json_apidirect(filtered_toots, collected_tags, [], [], [], [],
                        "{}".format(savedir))

    return filtered_toots, collected_tags

###################################
# data collection for converations/replies
###################################

def get_conversation_from_head(toot, instance, auth_dict):

    apicallurl = "https://{}/api/v1/statuses/{}/context".format(instance, toot["id"])    
    next_url = apicallurl  
    r = None
    while next_url is not None:
        try:            
            # set 5 mins timeout, to maintain the rate
            if instance in auth_dict.keys() and auth_dict[instance] is not None:
                r = requests.get(next_url, timeout=300, 
                                 headers={'Authorization': auth_dict[instance]})
            elif instance in auth_dict.keys() and auth_dict[instance] is None:
                r = requests.get(next_url, timeout=300)
            else:
                try:
                    # newly discovered instance, not in auth_dict provided by user - try simple request
                    r = requests.get(next_url, timeout=300)
                except:
                    raise NotImplementedError("Unknown Mastodon instance.")
        except requests.exceptions.ConnectionError:
            # Network/DNS error, wait 30mins and retry
            time.sleep(30*60)
            continue
        except requests.exceptions.HTTPError as e:
            print("HTTPError issue: {}...exiting".format(e.response.status_code))
            logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
            break
        except requests.exceptions.Timeout:
            print("Timeout...exiting")
            logging.info("Timeout...exiting")
            break
        except requests.exceptions.TooManyRedirects:
            print("Too many redirects...exiting")
            logging.info("Too many redirects...exiting")
            break
        except requests.exceptions.RequestException as e:
            print("Uknown GET issue: {}...exiting".format(e.response.status_code))
            logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
            break

        if r.status_code == 200:
            print("Executed request: {}".format(r.url))
        elif r.status_code == 503:
            time.sleep(10*60)
            continue
        else:
            print("Check request...")            
            logging.error(instance)
            logging.error("Check request...")
            break
        iteration_links = r.links        
        if "next" in iteration_links.keys():
            next_url = iteration_links["next"]["url"]
        else:            
            next_url = None
        print(next_url)
    
    if r is None or r.status_code != 200:        
        return None
    ancestors = r.json()["ancestors"]
    if len(ancestors) > 0:
        return None
    else:
        return r.json()["descendants"]
    

def get_parent_toot(toot, instance, auth_dict):

    apicallurl = "https://{}/api/v1/statuses/{}/context".format(instance, toot["id"])    
    next_url = apicallurl  
    while next_url is not None:
        try:            
            # set 5 mins timeout, to maintain the rate
            if instance in auth_dict.keys() and auth_dict[instance] is not None:
                r = requests.get(next_url, timeout=300, 
                                 headers={'Authorization': auth_dict[instance]})
            elif instance in auth_dict.keys() and auth_dict[instance] is None:
                r = requests.get(next_url, timeout=300)
            else:
                try:
                    # newly discovered instance, not in auth_dict provided by user - try simple request
                    r = requests.get(next_url, timeout=300)
                except:
                    raise NotImplementedError("Unknown Mastodon instance.")
        except requests.exceptions.ConnectionError:
            # Network/DNS error, wait 30mins and retry
            time.sleep(30*60)
            continue
        except requests.exceptions.HTTPError as e:
            print("HTTPError issue: {}...exiting".format(e.response.status_code))
            logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
            break
        except requests.exceptions.Timeout:
            print("Timeout...exiting")
            logging.info("Timeout...exiting")
            break
        except requests.exceptions.TooManyRedirects:
            print("Too many redirects...exiting")
            logging.info("Too many redirects...exiting")
            break
        except requests.exceptions.RequestException as e:
            print("Uknown GET issue: {}...exiting".format(e.response.status_code))
            logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
            break

        if r.status_code == 200:
            print("Executed request: {}".format(r.url))
        elif r.status_code == 503:
            time.sleep(10*60)
            continue
        else:
            print("Check request...")            
            logging.error(instance)
            logging.error("Check request...")
            break
        iteration_links = r.links        
        if "next" in iteration_links.keys():
            next_url = iteration_links["next"]["url"]
        else:            
            next_url = None
        print(next_url)
    
    ancestors = r.json()["ancestors"]
    if len(ancestors) > 0:
        return ancestors
    else:
        return None


def get_outbox_from_user(useracct, instance, auth_dict):

    apicallurl = "https://{}/users/{}/outbox?page=true".format(instance, useracct)  
    next_url = apicallurl  
    posteditems = []
    while next_url is not None:
        try:            
            # set 5 mins timeout, to maintain the rate
            if instance in auth_dict.keys() and auth_dict[instance] is not None:
                r = requests.get(next_url, timeout=300, 
                                 headers={'Authorization': auth_dict[instance]})
            elif instance in auth_dict.keys() and auth_dict[instance] is None:
                r = requests.get(next_url, timeout=300)
            else:
                try:
                    # newly discovered instance, not in auth_dict provided by user - try simple request
                    r = requests.get(next_url, timeout=300)
                except:
                    raise NotImplementedError("Unknown Mastodon instance.")
        except requests.exceptions.ConnectionError:
            # Network/DNS error, wait 30mins and retry
            time.sleep(30*60)
            continue
        except requests.exceptions.HTTPError as e:
            print("HTTPError issue: {}...exiting".format(e.response.status_code))
            logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
            break
        except requests.exceptions.Timeout:
            print("Timeout...exiting")
            logging.info("Timeout...exiting")
            break
        except requests.exceptions.TooManyRedirects:
            print("Too many redirects...exiting")
            logging.info("Too many redirects...exiting")
            break
        except requests.exceptions.RequestException as e:
            print("Uknown GET issue: {}...exiting".format(e.response.status_code))
            logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
            break
        
        if r.status_code != 200:
            return None
        itms = r.json()
        if len(itms["orderedItems"]) > 0:
            posteditems.extend(itms["orderedItems"])
            print(len(posteditems), len(itms["orderedItems"]))
        if r.status_code == 200:
            print("Executed request: {}".format(r.url))
        elif r.status_code == 503:
            time.sleep(10*60)
            continue
        else:
            print("Check request...")            
            logging.error(instance)
            logging.error("Check request...")
            break
               
        if "next" in itms.keys():
            next_url = itms["next"]
        else:            
            next_url = None
        print(next_url)          
        
    return posteditems


def get_toot_from_statusid(tootid, instance_name, auth_dict):

    r = None
    next_url = "https://{}/api/v1/statuses/{}".format(instance_name, tootid)
    while next_url is not None:
        try:
            # set 5 mins timeout, to maintain the rate
            if instance_name in auth_dict.keys() and auth_dict[instance_name] is not None:
                r = requests.get(next_url, timeout=300, 
                                 headers={'Authorization': auth_dict[instance_name]})
            elif instance_name in auth_dict.keys() and auth_dict[instance_name] is None:
                r = requests.get(next_url, timeout=300)
            else:
                try:
                    # newly discovered instance, not in auth_dict provided by user - try simple request
                    r = requests.get(next_url, timeout=300)
                except:
                    raise NotImplementedError("Unknown Mastodon instance.")
        except requests.exceptions.ConnectionError:
            # Network/DNS error, wait 30mins and retry
            time.sleep(30*60)
            continue
        except requests.exceptions.HTTPError as e:
            print("HTTPError issue: {}...exiting".format(e.response.status_code))
            logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
            break
        except requests.exceptions.Timeout:
            print("Timeout...exiting")
            logging.info("Timeout...exiting")
            break
        except requests.exceptions.TooManyRedirects:
            print("Too many redirects...exiting")
            logging.info("Too many redirects...exiting")
            break
        except requests.exceptions.RequestException as e:
            print("Uknown GET issue: {}...exiting".format(e.response.status_code))
            logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
            break

        if r.status_code == 200:
            print("Executed request: {}".format(r.url))
            next_url = None
        elif r.status_code == 503:
            time.sleep(10*60)
            continue
        else:
            print("Check request...")            
            logging.error(instance_name)
            logging.error("Check request...")
            break
    if r is None or r.status_code != 200:
        return None
    
    return r.json()



###################################
# data collection for boosts/reblogs
###################################
    
def get_boosts(toot, instance, auth_dict):

    apicallurl = "https://{}/api/v1/statuses/{}/reblogged_by".format(instance, toot["id"])    
    next_url = apicallurl  
    r = None
    while next_url is not None:
        try:            
            # set 5 mins timeout, to maintain the rate
            if instance in auth_dict.keys() and auth_dict[instance] is not None:
                r = requests.get(next_url, timeout=300, 
                                 headers={'Authorization': auth_dict[instance]})
            elif instance in auth_dict.keys() and auth_dict[instance] is None:
                r = requests.get(next_url, timeout=300)
            else:
                try:
                    # newly discovered instance, not in auth_dict provided by user - try simple request
                    r = requests.get(next_url, timeout=300)
                except:
                    raise NotImplementedError("Unknown Mastodon instance.")
        except requests.exceptions.ConnectionError:
            # Network/DNS error, wait 30mins and retry
            time.sleep(30*60)
            continue
        except requests.exceptions.HTTPError as e:
            print("HTTPError issue: {}...exiting".format(e.response.status_code))
            logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
            break
        except requests.exceptions.Timeout:
            print("Timeout...exiting")
            logging.info("Timeout...exiting")
            break
        except requests.exceptions.TooManyRedirects:
            print("Too many redirects...exiting")
            logging.info("Too many redirects...exiting")
            break
        except requests.exceptions.RequestException as e:
            print("Uknown GET issue: {}...exiting".format(e.response.status_code))
            logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
            break

        if r.status_code == 200:
            print("Executed request: {}".format(r.url))
        elif r.status_code == 503:
            time.sleep(10*60)
            continue
        else:
            print("Check request...")            
            logging.error(instance)
            logging.error("Check request...")
            break
        iteration_links = r.links        
        if "next" in iteration_links.keys():
            next_url = iteration_links["next"]["url"]
        else:            
            next_url = None
        print(next_url)
    
    if r is None:
        boosts = []
    else:
        try:
            boosts = r.json()
        except json.decoder.JSONDecodeError:            
            return None
    if len(boosts) == 0:
        return None
    else:
        return boosts

###################################
# data collection of public timelines
###################################
    
def collect_timeline_apidirect(dbconnection, url=None, local=False, remote=False, only_media=False,
                            max_id=None, since_id=None, min_id=None,limit=40, 
                            keywords=[], textprocessor=None, savedir="/tmp/", 
                            instance_name=None, auth_dict=None, cutoff_date="2023-12-02"):
    """collect_timeline_apidirect : retrieves the public timeline of an instance and stores toots, users and hashtags

    Args:
        dbconnection (_type_): _description_
        url (_type_, optional): _description_. Defaults to None.
        local (bool, optional): _description_. Defaults to False.
        remote (bool, optional): _description_. Defaults to False.
        only_media (bool, optional): _description_. Defaults to False.
        max_id (_type_, optional): _description_. Defaults to None.
        since_id (_type_, optional): _description_. Defaults to None.
        min_id (_type_, optional): _description_. Defaults to None.
        limit (int, optional): _description_. Defaults to 40.
        keywords (list, optional): _description_. Defaults to [].
        textprocessor (_type_, optional): _description_. Defaults to None.
        savedir (str, optional): _description_. Defaults to "/tmp/".
        instance_name (_type_, optional): _description_. Defaults to None.
        auth_dict (_type_, optional): dictionary containing the authorisation token if one is required by the server. Defaults to None.

    Raises:
        NotImplementedError: _description_

    Returns:
        _type_: _description_
    """
    params = {"local": local, "remote": remote, "only_media": only_media,
            "max_id": max_id, "since_id": since_id, "min_id": min_id, 
            "limit": limit}
    paramsin = params
    next_url = url
    iter = 0
    while next_url is not None:
        try:
            if "max_id" in next_url:
                paramsin = {"since_id": since_id}
            else:
                paramsin = params
            # set 5 mins timeout, to maintain the rate
            if instance_name in auth_dict.keys() and auth_dict[instance_name] is not None:
                r = requests.get(next_url, params=paramsin, timeout=300, 
                                 headers={'Authorization': auth_dict[instance_name]})
            elif instance_name in auth_dict.keys() and auth_dict[instance_name] is None:
                r = requests.get(next_url, params=paramsin, timeout=300)
            else:
                try:
                    # newly discovered instance, not in auth_dict provided by user - try simple request
                    r = requests.get(next_url, timeout=300)
                except:
                    raise NotImplementedError("Unknown Mastodon instance.")
        except requests.exceptions.ConnectionError:
            # Network/DNS error, wait 30mins and retry
            time.sleep(30*60)
            continue
        except requests.exceptions.HTTPError as e:
            print("HTTPError issue: {}...exiting".format(e.response.status_code))
            logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
            break
        except requests.exceptions.Timeout:
            print("Timeout...exiting")
            logging.info("Timeout...exiting")
            break
        except requests.exceptions.TooManyRedirects:
            print("Too many redirects...exiting")
            logging.info("Too many redirects...exiting")
            break
        except requests.exceptions.RequestException as e:
            print("Uknown GET issue: {}...exiting".format(e.response.status_code))
            logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
            break

        if r.status_code == 200:
            print("Executed request: {}".format(r.url))
        elif r.status_code == 503:
            time.sleep(10*60)
            continue
        else:
            print("Check request...")            
            logging.error(instance_name)
            logging.error("Check request...")
            break
        filtered_toots, collected_tags, collected_users = \
                    collect_toots_and_tooters_apidirect(dbconnection, r, keywords, textprocessor, 
                                                        instance_name=instance_name, auth_dict=auth_dict, cutoff_date=cutoff_date)
        save2json_apidirect(filtered_toots, collected_tags, collected_users, [], [], [],
                            "{}".format(savedir))
        
        iteration_links = r.links        
        if "next" in iteration_links.keys():
            next_url = iteration_links["next"]["url"]
        else:            
            next_url = None
        print(next_url)
        if iter > 0 and iter % 5 == 0:
            # 30 sec wait to stick to the limit of 300 req/5 mins
            time.sleep(45)
        iter += 1

    return r

def collect_toots_and_tooters_apidirect(dbconn, res, keywords, textprocessor, instance_name, auth_dict, cutoff_date="2023-12-02"):
    """collect_toots_and_tooters_apidirect : 
    
    Collects NON-BOT generated, PUBLIC toots and extends the toot 
    object with extra fields, then stores it in the database.
    If the toot has a parent the parent is inserted into the database.

    Args:
        dbconn (_type_): _description_
        res (_type_): _description_
        keywords (_type_): _description_
        textprocessor (_type_): _description_
        instance_name (_type_): _description_

    Raises:
        AttributeError: Raises AttributeError if a pre-collection period (before Dec 2023) toot is encountered.

    Returns:
        Triple of toots, tags, users
    """

    # convert to json
    fetched_toots = res.json()    
    print("Fetched {} public toots...".format(len(fetched_toots)))
    filtered_toots  = []
    collected_tags  = []
    collected_users = []
    track_user_ids  = []    
    for i in fetched_toots:
        if i["account"]["bot"]:
            continue
        if i["visibility"] != "public":
            continue
        # extract tags
        if len(i["tags"]) > 0:
            for ktag in i["tags"]:
                if ktag not in collected_tags:
                    collected_tags.append(ktag)
        # get and store account id and its details
        account_id = i["account"]["id"]
        if account_id not in track_user_ids:
            collected_users.append(i["account"])
            track_user_ids.append(account_id)
        # extract toot text content
        toot = BeautifulSoup(i["content"], "html.parser")
        # add extra entry in toot with Mastodon instance name
        i["instance_name"] = instance_name
        # add extra entry in toot dictionary
        toottext = toot.get_text()
        # Note: add extra entry with URLs, remove urls from text and do basic cleaning - TODO later?
        i["toottext"] = toottext
        acc = add_unique_account_id(i["account"], i["instance_name"])
        i["account"] = acc
        i = add_unique_toot_id(i, i["instance_name"])
        i["rebloggedbyuser"] = []
        if isinstance(i["spoiler_text"], str) and len(i["spoiler_text"]) > 0:
            tootspoiler = BeautifulSoup(i["spoiler_text"], "html.parser")
            i["spoiler_clean_text"] = tootspoiler.get_text()
        if isinstance(i["account"]["note"], str) and len(i["account"]["note"]) > 0:
            accountnote = BeautifulSoup(i["account"]["note"], "html.parser")
            i["account"]["account_note_text"] = accountnote.get_text()
        toottext = toottext.lower()
        if len(keywords) > 0:
            def contains_kw(x): return x.lower() in toottext
            found_kws_iterator = filter(contains_kw, keywords)
            # Note: consider more finegrained, e.g. separation of output directory depending - TODO later?
            # on which keywords were spotted
            if len(list(found_kws_iterator)) > 0:
                filtered_toots.append(i)
        else:
            filtered_toots.append(i)        
        
        for kk in i.keys():
            if isinstance(i[kk], datetime):
                i[kk] = i[kk].astimezone(pytz.utc)
                i[kk] = i[kk].strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(i[kk], dict):
                for kkk in i[kk].keys():
                    if isinstance(i[kk][kkk], datetime):
                        i[kk][kkk] = i[kk][kkk].astimezone(pytz.utc)
                        i[kk][kkk] = i[kk][kkk].strftime("%Y-%m-%dT%H:%M:%S") 
        # print(i["created_at"], datetime2snowflake(pd.to_datetime(pd.Timestamp(np.datetime64(i["created_at"])))))
        if dbconn is not None:
            newrow = build_db_row(i)        
            if newrow is not None:            
                execute_insert_sql(dbconn, "toots", newrow)
            else:
                print(newrow)
            
            # get parent toot and update id in DB
            try:
                ancestors = get_parent_toot(i, i["instance_name"], auth_dict=auth_dict)
                time.sleep(3)
                # get immediate parent
                if ancestors is not None:
                    parenttoot = add_unique_toot_id(ancestors[-1], i["instance_name"])
                    if "edited_at" in parenttoot.keys() and parenttoot["edited_at"] is not None and parenttoot["edited_at"] != "":
                        try:
                            monthyear = pd.Timestamp(np.datetime64(parenttoot["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
                        except:
                            monthyear = pd.Timestamp(np.datetime64(parenttoot["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                    else:
                        try:
                            monthyear = pd.Timestamp(np.datetime64(parenttoot["created_at"])).tz_localize("CET").astimezone(pytz.utc)
                        except:
                            monthyear = pd.Timestamp(np.datetime64(parenttoot["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                    if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc):
                        # do not collect it
                        raise AttributeError
                    parenttoot["account"] = add_unique_account_id(parenttoot["account"], i["instance_name"])
                    parenttoot["instance_name"] = i["instance_name"]
                    parenttoottext = BeautifulSoup(parenttoot["content"], "html.parser")
                    parenttoot["toottext"] = parenttoottext.get_text()
                    parenttoot["rebloggedbyuser"] = None
                    if "spoiler_clean_text" not in parenttoot.keys() and isinstance(parenttoot["spoiler_text"], str):
                        if len(parenttoot["spoiler_text"]) > 0:
                            tootspoiler = BeautifulSoup(parenttoot["spoiler_text"], "html.parser")
                            parenttoot["spoiler_clean_text"] = tootspoiler.get_text()
                        else:
                            parenttoot["spoiler_clean_text"] = ""
                    if "account_note_text" not in parenttoot["account"].keys() and isinstance(parenttoot["account"]["note"], str):
                        if len(parenttoot["account"]["note"]) > 0:
                            accountnote = BeautifulSoup(parenttoot["account"]["note"], "html.parser")
                            parenttoot["account"]["account_note_text"] = accountnote.get_text()
                        else:
                            parenttoot["account"]["account_note_text"] = ""
                    parentrow = build_db_row(parenttoot)        
                    if parentrow is not None:            
                        execute_insert_sql(dbconn, "toots", parentrow)
                    else:
                        print(parentrow)

                    parentglobalID = parenttoot["globalID"]
                    parentaccountID = parenttoot["account"]["globalID"]
                    execute_update_replies_sql(dbconn, "toots", i["globalID"], parentglobalID, parentaccountID)
            except:
                # if error retrieving head, remove - if part of relevant conversation, will probably be collected later
                i["in_reply_to_id"] = ""
                i["in_reply_to_account_id"] = ""
                execute_update_replies_sql(dbconn, "toots", i["globalID"], "", "")

    return filtered_toots, collected_tags, collected_users 


###################################
# data collection for follower relationships
###################################

def collect_user_followers_apidirect(res, usr_id, keywords, textprocessor, instance_name):

    # convert to json
    fetched_followers = res.json()    
    print("Fetched {} followers of user {}...".format(len(fetched_followers), usr_id))
    
    followers  = []
    for i in fetched_followers:       
        for kk in i.keys():
            if isinstance(i[kk], datetime):
                i[kk] = i[kk].astimezone(pytz.utc)
                i[kk] = i[kk].strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(i[kk], dict):
                for kkk in i[kk].keys():
                    if isinstance(i[kk][kkk], datetime):
                        i[kk][kkk] = i[kk][kkk].astimezone(pytz.utc)
                        i[kk][kkk] = i[kk][kkk].strftime("%Y-%m-%dT%H:%M:%S") 
        # print(i["created_at"], datetime2snowflake(pd.to_datetime(pd.Timestamp(np.datetime64(i["created_at"])))))
        followers.append(i)
    
    return {usr_id: followers} 

def collect_followers_apidirect(url=None, usr_id=None, local=False, remote=False, only_media=False,
                                max_id=None, since_id=None, min_id=None,limit=40, 
                                keywords=[], textprocessor=None, savedir="/tmp/", 
                                instance_name=None, acct=None, auth_dict=None):
    
    params = {"local": local, "remote": remote, "only_media": only_media,
              "max_id": max_id, "since_id": since_id, "min_id": min_id, 
              "limit": limit}
    paramsin = params
    next_url = url
    iter = 0
    while next_url is not None:
        try:
            if "max_id" in next_url:
                paramsin = {"since_id": since_id}
            else:
                paramsin = params
            # set 5 mins timeout, to maintain the rate
            if instance_name in auth_dict.keys() and auth_dict[instance_name] is not None:
                r = requests.get(next_url, params=paramsin, timeout=300, 
                                 headers={'Authorization': auth_dict[instance_name]})
            elif instance_name in auth_dict.keys() and auth_dict[instance_name] is None:
                r = requests.get(next_url, params=paramsin, timeout=300)
            else:
                try:
                    # newly discovered instance, not in auth_dict provided by user - try simple request
                    r = requests.get(next_url, timeout=300)
                except:
                    raise NotImplementedError("Unknown Mastodon instance.")
        except requests.exceptions.ConnectionError:
            # Network/DNS error, wait 30mins and retry
            time.sleep(30*60)
            continue
        except requests.exceptions.HTTPError as e:
            print("HTTPError issue: {}...exiting".format(e.response.status_code))
            logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
            break
        except requests.exceptions.Timeout:
            print("Timeout...exiting")
            logging.info("Timeout...exiting")
            break
        except requests.exceptions.TooManyRedirects:
            print("Too many redirects...exiting")
            logging.info("Too many redirects...exiting")
            break
        except requests.exceptions.RequestException as e:
            print("Uknown GET issue: {}...exiting".format(e.response.status_code))
            logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
            break

        if r.status_code == 200:
            print("Executed request: {}".format(r.url))
        elif r.status_code == 503:
            time.sleep(10*60)
            continue
        else:
            print("Check request...")            
            logging.error(instance_name)
            logging.error("Check request...")
            break
        collected_followers = \
                    collect_user_followers_apidirect(r, usr_id, keywords, textprocessor, instance_name=instance_name)
        collected_followers["instance"] = instance_name
        collected_followers["acct"] = acct
        collected_followers["id"] = usr_id
        save2json_apidirect([], [], [], collected_followers=collected_followers, savedir="{}".format(savedir))
        
        iteration_links = r.links        
        if "next" in iteration_links.keys():
            next_url = iteration_links["next"]["url"]
        else:            
            next_url = None
        print(next_url)
        if iter > 0 and iter % 5 == 0:
            # 30 sec wait to stick to the limit of 300 req/5 mins
            time.sleep(30)
        iter += 1

    return r

def get_followers_from_user_list(user_df, tidy_out_dir="/tmp/", savedir="/tmp/", auth_dict=None):

    for i, row in user_df.iterrows():
        # get snowflake user ID
        usr_id = row["acct"]
        instance_name = row["account_url"][8:].split("/")[0]
        instance_url = "https://{}/".format(instance_name)
        usr_id_upd = get_user_id_from_username(usr_id, instance_url, instance_name, auth_dict)
          
        api_call_url = "{}/api/v1/accounts/{}/followers".format(instance_url, usr_id_upd)
        ret = collect_followers_apidirect(url=api_call_url, usr_id=usr_id_upd, local=False, 
                                        remote=False, only_media=False,
                                        max_id=None, since_id=None, min_id=None, limit=40, 
                                        keywords=[], textprocessor=None, savedir=savedir, 
                                        instance_name=instance_url, acct=usr_id)
                    
    users_social_connex = {"id":[], "acct": [], "instance": [], "followers": []}
    with jsonlines.open("{}/followers/followers.jsonl".format(savedir), "r") as jsonl_read:
        with jsonlines.open("{}/user_followers.jsonl".format(tidy_out_dir), "a") as jsonl_write:
            i = 0
            try:
                for data in jsonl_read.iter(type=dict, skip_invalid=True):
                    userid = data["id"]
                    if userid in users_social_connex["id"]:
                        users_social_connex["followers"].extend(data[userid])
                    else:
                        if len(users_social_connex["acct"]) > 0:
                            jsonl_write.write(users_social_connex)
                            print(users_social_connex["id"])
                            users_social_connex = {"id": [], "acct": [], "instance": [], "followers": []}
                        else:
                            users_social_connex["id"].append(userid)
                            users_social_connex["acct"].append(data["acct"])
                            users_social_connex["instance"].append(data["instance"])
                            # all or keep: id, username, acct, bot, created_at, uri, followers_count, following_count, statuses_count, last_status_at
                            users_social_connex["followers"].extend(data[userid])
                           
                    i += 1
            except:
                print(i)
               

###################################
# helper function to get user id given their username
###################################

def get_user_id_from_username(username, url, instance_name, auth_dict):

    apicall = "{}api/v1/accounts/lookup?acct=@{}".format(url, username)
    try:
        # set 5 mins timeout, to maintain the rate
        if instance_name in auth_dict.keys() and auth_dict[instance_name] is not None:
            r = requests.get(apicall, timeout=300, 
                                headers={'Authorization': auth_dict[instance_name]})
        elif instance_name in auth_dict.keys() and auth_dict[instance_name] is None:
            r = requests.get(apicall, timeout=300)
        else:
            try:
                # newly discovered instance, not in auth_dict provided by user - try simple request
                r = requests.get(apicall, timeout=300)
            except:
                raise NotImplementedError("Unknown Mastodon instance.")
    except requests.exceptions.ConnectionError:
        # Network/DNS error, wait 30mins and retry
        time.sleep(30*60)
        return
    ret = r.json()

    return ret["id"]

###################################
# save outputs to jsonl
###################################

def save2json_apidirect(filtered_toots, collected_tags, collected_users, collected_followers=[], collected_contexts=[], collected_boosts=[], savedir="/tmp/"):

    # write out toots
    for toot in filtered_toots:
        if "edited_at" in toot.keys() and toot["edited_at"] is not None:
            try:
                monthyear = pd.Timestamp(np.datetime64(toot["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
            except:
                monthyear = pd.Timestamp(np.datetime64(toot["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
        else:
            try:
                monthyear = pd.Timestamp(np.datetime64(toot["created_at"])).tz_localize("CET").astimezone(pytz.utc)
            except:
                monthyear = pd.Timestamp(np.datetime64(toot["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
        pathout = "{}/toots/{}/{}/".format(savedir, monthyear.year, monthyear.month)
        pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
        with jsonlines.open("{}/toots.jsonl".format(pathout), mode='a') as writer:
            writer.write(toot)    
    # writeout tags    
    if len(collected_tags) > 0:
        pathout = "{}/tags/".format(savedir)
        pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
        with jsonlines.open("{}/tags.jsonl".format(pathout), mode='a') as writer:
            writer.write_all(collected_tags)
    # writeout users    
    if len(collected_users) > 0:
        pathout = "{}/users/".format(savedir)
        pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
        collected_users = datetime4json(collected_users)
        with jsonlines.open("{}/users.jsonl".format(pathout), mode='a') as writer:
            writer.write_all(collected_users)       
    # writeout followers    
    if len(collected_followers) > 0:
        pathout = "{}/followers/".format(savedir)
        pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
        collected_followers = datetime4json(collected_followers)
        with jsonlines.open("{}/followers.jsonl".format(pathout), mode='a') as writer:
            writer.write(collected_followers)   
    # writeout conversations    
    if len(collected_contexts) > 0:
        pathout = "{}/contexts/".format(savedir)
        pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
        collected_contexts["descendants"] = datetime4json(collected_contexts["descendants"])
        with jsonlines.open("{}/contexts.jsonl".format(pathout), mode='a') as writer:
            writer.write(collected_contexts)   
    # writeout boosts    
    if len(collected_boosts) > 0:
        pathout = "{}/boosts/".format(savedir)
        pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)    
        collected_boosts["boosters"] = datetime4json(collected_boosts["boosters"])
        with jsonlines.open("{}/boosters.jsonl".format(pathout), mode='a') as writer:
            writer.write(collected_boosts)   

###################################
# helper function to convert datetime object to Snowflake ID
###################################
            
def datetime2snowflake(timestamp):
    # timestamp: datetime object
    return (int(timestamp.timestamp()) << 16) * 1000

###################################
# helper function to print datetime to string for json storage
###################################

def datetime4json(inputlist):
    
    outputlist = []
    if isinstance(inputlist, list):    
        for i in inputlist:
            for kk in i.keys():
                if isinstance(i[kk], datetime):
                    i[kk] = i[kk].astimezone(pytz.utc)
                    i[kk] = i[kk].strftime("%Y-%m-%dT%H:%M:%S")
                elif isinstance(i[kk], dict):
                    for kkk in i[kk].keys():
                        if isinstance(i[kk][kkk], datetime):
                            i[kk][kkk] = i[kk][kkk].astimezone(pytz.utc)
                            i[kk][kkk] = i[kk][kkk].strftime("%Y-%m-%dT%H:%M:%S")
                        elif isinstance(i[kk][kkk], dict):
                            for kkkk in i[kk][kkk].keys():
                                if isinstance(i[kk][kkk][kkkk], datetime):
                                    i[kk][kkk][kkkk] = i[kk][kkk][kkkk].astimezone(pytz.utc)
                                    i[kk][kkk][kkkk] = i[kk][kkk][kkkk].strftime("%Y-%m-%dT%H:%M:%S")
            outputlist.append(i)
    else:
        # followers dict
        outputlist = dict()
        k = list(inputlist.keys())[0]
        outputlist[k] = datetime4json(inputlist[k])
        outputlist["instance"] = inputlist["instance"]
        outputlist["acct"] = inputlist["acct"]
        outputlist["id"] = inputlist["id"]
        
    return outputlist

###################################
# helper functions to collect all users/tags in single csv (deprecated, latest collection is in single csv by default)
###################################

def collect_all_users(input_dir):

    users = []
    subfolders = [f.name for f in os.scandir(input_dir) if f.is_dir()]
    for sf in subfolders:
        subfiles = [ff.name for ff in os.scandir("{}/{}".format(input_dir, sf)) if ff.is_file() and "users" in ff.name]
        for sff in subfiles:
            with jsonlines.open("{}/{}/{}".format(input_dir, sf, sff), "r") as jsonl_f:
                for userline in jsonl_f:
                    users.append(userline["id"])
    
    df = pd.DataFrame.from_dict({"users":users})
    df.to_csv("{}/allusers.csv".format(input_dir), index=False)
    return users

def collect_all_tags(input_dir):

    tags = []
    subfolders = [f.name for f in os.scandir(input_dir) if f.is_dir()]
    for sf in subfolders:
        subfiles = [ff.name for ff in os.scandir("{}/{}".format(input_dir, sf)) if ff.is_file() and "tags" in ff.name]
        for sff in subfiles:
            with jsonlines.open("{}/{}/{}".format(input_dir, sf, sff), "r") as jsonl_f:
                for tagline in jsonl_f:
                    tags.append(tagline["name"])
    
    df = pd.DataFrame.from_dict({"tags":tags})
    df.to_csv("{}/alltags.csv".format(input_dir), index=False)
    return tags

def collect_users_activity_stats(input_dir, years=[], months=[]):

    dictout = {"acct": [], "globalID": [], "instance_name": [], "followers": [], "following": [], "statuses": []}
    if len(years) == 0:
        years = [f.name for f in os.scandir("{}/toots/".format(input_dir)) if f.is_dir()]    
    for year in years:
        if len(months) == 0:
            months = [f.name for f in os.scandir("{}/toots/{}/".format(input_dir, year)) if f.is_dir()]
        for month in months:
            try:
                with jsonlines.open("{}/toots/{}/{}/toots.jsonl".format(input_dir, year, month), "r") as jsonl_read:
                    for data in jsonl_read.iter(type=dict, skip_invalid=True):
                        usr_acct = data["account"]["acct"]
                        usr_accid = data["account"]["globalID"]
                        usr_instance = data["instance_name"]
                        usr_followers_cnt = data["account"]["followers_count"]
                        usr_following_cnt = data["account"]["following_count"]
                        usr_statuses_cnt = data["account"]["statuses_count"]
                        dictout["acct"].append(usr_acct)
                        dictout["globalID"].append(usr_accid)
                        dictout["instance_name"].append(usr_instance)
                        dictout["followers"].append(usr_followers_cnt)
                        dictout["following"].append(usr_following_cnt)
                        dictout["statuses"].append(usr_statuses_cnt)
            except:
                continue

    if len(dictout["acct"]) > 0:
        return pd.DataFrame.from_dict(dictout)
    else:
        return None


###################################
# plotting number of toots, users (deprecated uses old directory structure)
###################################

def plot_tootsandusers_stats(input_dir):

    # unique toots and users
    daily_toots = dict()
    daily_active_users = dict()
    subfolders = [f.name for f in os.scandir(input_dir) if f.is_dir()]
    for sf in subfolders:
        subfiles = [ff.name for ff in os.scandir("{}/{}".format(input_dir, sf)) if ff.is_file() and "toots" in ff.name]
        for sff in subfiles:
            try:
                with jsonlines.open("{}/{}/{}".format(input_dir, sf, sff), "r") as jsonl_f:
                    for tootline in jsonl_f:
                        date = pd.Timestamp(tootline["created_at"]).date()
                        if date not in daily_toots.keys():
                            daily_toots[date] = [tootline["id"]]
                        else:
                            if tootline["id"] not in daily_toots[date]:
                                daily_toots[date].append(tootline["id"])
                        account_usrname = tootline["account"]["acct"]
                        if date not in daily_active_users.keys():
                            daily_active_users[date] = [account_usrname]
                        else:
                            if account_usrname not in daily_active_users[date]:
                                daily_active_users[date].append(account_usrname)
            except:
                print("{}/{}/{}".format(input_dir, sf, sff))
                continue

    dates = []
    userno = []
    tootno = []
    for k in daily_toots.keys():
        dates.append(k)
        userno.append(len(daily_active_users[k]))
        tootno.append(len(daily_toots[k]))
    
    stats = pd.DataFrame.from_dict({"dates":dates, "toots": tootno, "users": userno})
    stats = stats.sort_values(by='dates')
    # stats = stats[stats.dates >= pd.Timestamp("2023-11-01").date()]
    stats.to_csv("{}/extractionstats1.csv".format(input_dir), index=False)

    # stats = pd.read_csv("{}/extractionstats1.csv".format(input_dir))

    fig = go.Figure()
    fig.add_trace(go.Bar(x=stats.dates.values, y=stats.toots.values, name="Toots"))
    fig.add_trace(go.Bar(x=stats.dates.values, y=stats.users.values, name="Users"))
    pio.write_html(fig, "{}/extractionstats1.html".format(input_dir), auto_open=False)
    pio.write_image(fig, "{}/extractionstats1.html".format(input_dir).replace(".html", ".pdf"), engine="kaleido")
    pio.write_image(fig, "{}/extractionstats1.html".format(input_dir).replace("html", "png"), width=1540, height=871, scale=1)
    
    return daily_toots, daily_active_users, stats


###################################
# helper functions to add unique key to toot, account, extend to anonymise
###################################


def add_unique_toot_id(toot, instancename):
    # get id and instance from uri, which is unique
    tootid = toot["id"]    
    toot["globalID"] = "{}@@{}".format(tootid, instancename)
    
    return toot


def add_unique_account_id(account, instancename):
    
    accountid = account["acct"]    
    account["globalID"] = "{}@@{}".format(accountid, instancename)

    return account


###################################
# helper functions for daily hashtag and user collection
###################################

def daily_collection_hashtags_users(toot_dir, hashtag_lists_dir, topics, topic_lists_dir):
    """daily_collection_hashtags_users :
    
    Takes as input the directories that contain the saved toots, 
    topic word lists (in .csv or Flashtext KeywordProcessor format) 
    and hashtags of interest.

    Scans the toots and extracts their hashtags if the toot text 
    contains words that are in the topic lists. Most popular hashtags 
    (95th percentile of hashtags distribution) are then added to the hashtags lists.

    NOTE: currently for the topics of Climate, COVID-19 and Immigration. For extra topics, it would need to be extended.
          
    TODO: modify to work on database instead of toots.json directory

    Args:
        toot_dir (_type_): _description_
        hashtag_lists_dir (bool): _description_
        topics (string list): topics of interest, hashtag lists will be processed in same
                              order as topics in this list
        topic_lists_dir (_type_): _description_
    """
    # load topic word lists
    keywordsearchers, _ = load_keywords_topic_lists(topics=topics, topic_lists_dir=topic_lists_dir)

    hashtags_dict = dict()
    for top in topics:
        hashtags_dict[top] = []

    yearlists = [f.name for f in os.scandir("{}/toots/".format(toot_dir)) if f.is_dir()]     
    for yearfolder in yearlists:
        print(yearfolder)
        datadir = "{}/toots/{}/".format(toot_dir, yearfolder)
        monthlists = [f.name for f in os.scandir("{}/toots/{}".format(toot_dir, yearfolder)) if f.is_dir()]
        for monthfolder in monthlists:
            with jsonlines.open("{}/{}/toots.jsonl".format(datadir, monthfolder), "r") as jsonl_read:
                # start reading jsonl for daily collected toots
                for data in jsonl_read.iter(type=dict, skip_invalid=True):
                    tootwords = data["toottext"].split()
                    # extract hashtags from topic relevant toots (based on context) and store in temporary lists
                    tags = [tg["name"] for tg in data["tags"]]
                    if len(tags) > 0:
                        for tw in tootwords:
                            for kwindex in range(len(keywordsearchers)):
                                topkw = topics[kwindex]
                                kwsearch = keywordsearchers[kwindex]
                                if tw in kwsearch:
                                    hashtags_dict[topkw].extend(tags)
                           
    # keep top 95th percentile of hashtags and add them to hashtag list   
    for hashtagkey in topics:
        topichashtags = hashtags_dict[hashtagkey]
        if len(topichashtags) > 0:
            hashtagscnts = Counter(topichashtags)
            hashtagscntsval = [*hashtagscnts.values()]
            tophst = np.percentile(hashtagscntsval, 95, interpolation="higher")
            newtopichashtags = [i for i in hashtagscnts.keys() if hashtagscnts[i] >= tophst]
            print(newtopichashtags)
            if pathlib.Path("{}/{}_hashtags_upd.csv".format(hashtag_lists_dir, hashtagkey)).exists():
                hash_tmp = pd.read_csv("{}/{}_hashtags_upd.csv".format(hashtag_lists_dir, hashtagkey))
                hash_tmp = hash_tmp.tags.tolist()
                hash_tmp.extend(newtopichashtags)
                hash_tmp = np.unique(hash_tmp).tolist()
                pd.DataFrame.from_dict({"tags": hash_tmp}).to_csv("{}/{}_hashtags_upd.csv".format(hashtag_lists_dir, hashtagkey), index=False)
            else:
                pd.DataFrame.from_dict({"tags": newtopichashtags}).to_csv("{}/{}_hashtags_upd.csv".format(hashtag_lists_dir, hashtagkey), index=False)


###################################
# toots db
###################################


def connectTo_weekly_toots_db(dbfile):
    
    conn = None
    try:
        conn = sqlite3.connect(dbfile)
        print(sqlite3.version)
    except Error as e:
        print(e)
    
    return conn


def build_db_row(toot):
    
    required = ["globalID", "id", "account", "created_at", "in_reply_to_id", "in_reply_to_account_id",
                "sensitive", "visibility", "uri", "content", "instance_name", "toottext"]
    for reqkey in required:
        if reqkey not in toot.keys():
            print(reqkey)
            return None
    
    if "spoiler_text" not in toot.keys():
        toot["spoiler_text"] = ""
    if "spoiler_clean_text" not in toot.keys():
        toot["spoiler_clean_text"] = ""
    if "language" not in toot.keys():
        toot["language"] = ""
    if "url" not in toot.keys():
        toot["url"] = ""
    if "replies_count" not in toot.keys():
        toot["replies_count"] = 0
    if "reblogs_count" not in toot.keys():
        toot["reblogs_count"] = 0
    if "favourites_count" not in toot.keys():
        toot["favourites_count"] = 0
    if "edited_at" not in toot.keys():
        toot["edited_at"] = ""
    if "reblog" not in toot.keys():
        toot["reblog"] = False
    if "rebloggedbyuser" not in toot.keys():
        toot["rebloggedbyuser"] = ""
    if "media_attachments" not in toot.keys():
        toot["media_attachments"] = ""
    if "mentions" not in toot.keys():
        toot["mentions"] = ""
    if "tags" not in toot.keys():
        toot["tags"] = ""
    if "emojis" not in toot.keys():
        toot["emojis"] = ""
    if "card" not in toot.keys():
        toot["card"] = ""
    if "poll" not in toot.keys():
        toot["poll"] = ""
    if "muted" not in toot.keys():
        toot["muted"] = False
    if "reblogged" not in toot.keys():
        toot["reblogged"] = False
    if "favourited" not in toot.keys():
        toot["favourited"] = False

    newrow = (toot["globalID"], toot["id"], toot["account"]["globalID"], str(toot["account"]), toot["created_at"], toot["in_reply_to_id"], 
            toot["in_reply_to_account_id"], toot["sensitive"], toot["spoiler_text"], toot["spoiler_clean_text"], toot["visibility"], 
            toot["language"], toot["uri"], toot["url"], toot["replies_count"], toot["reblogs_count"], toot["favourites_count"], 
            toot["edited_at"], toot["content"], toot["reblog"], str(toot["rebloggedbyuser"]), str(toot["media_attachments"]), 
            str(toot["mentions"]), str(toot["tags"]), str(toot["emojis"]), str(toot["card"]), str(toot["poll"]), 
            toot["instance_name"], toot["toottext"], toot["muted"], toot["reblogged"], toot["favourited"])  
    
    return newrow


def db_row_to_json(newrow):
    
    account = eval(newrow[3])
    account["globalID"] = newrow[2]
    toot = {"globalID": newrow[0], "id": newrow[1], "account": account, "created_at": newrow[4], "in_reply_to_id": newrow[5], 
            "in_reply_to_account_id": newrow[6], "sensitive": False if newrow[7]==0 else True, "spoiler_text": newrow[8], "spoiler_clean_text": newrow[9], "visibility": newrow[10], 
            "language": newrow[11], "uri": newrow[12], "url": newrow[13], "replies_count": int(newrow[14]), "reblogs_count": int(newrow[15]), "favourites_count": newrow[16], 
            "edited_at": newrow[17], "content": newrow[18], "reblog": newrow[19], "rebloggedbyuser": eval(newrow[20]), "media_attachments": eval(newrow[21]), 
            "mentions": eval(newrow[22]), "tags": eval(newrow[23]), "emojis": eval(newrow[24]), "card": eval(newrow[25]), "poll": eval(newrow[26]), 
            "instance_name": newrow[27], "toottext": newrow[28], "muted": False if newrow[29]==0 else True, "reblogged": False if newrow[30]==0 else True, 
            "favourited": False if newrow[31]==0 else True}  
    
    return toot



def execute_create_sql(dbconnection, command):
    
    try:
        c = dbconnection.cursor()
        c.execute(command)
    except Error as e:
        print(e)


def execute_insert_sql(dbconnection, table, newrow):

    sql = ''' INSERT OR IGNORE INTO {} (globalID, id, accountglobalID, account, created_at, in_reply_to_id, in_reply_to_account_id, sensitive, spoiler_text, spoiler_clean_text, 
                            visibility, language, uri, url, replies_count, reblogs_count, favourites_count, edited_at, content, reblog, rebloggedbyuser,
                            media_attachments, mentions, tags, emojis, card, poll, instance_name, toottext, muted, reblogged, favourited)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''.format(table)
    cur = dbconnection.cursor()
    cur.execute(sql, newrow)
    dbconnection.commit()
    
    return cur.lastrowid


def execute_update_replies_sql(dbconnection, table, tootglobalID, inreply2id, inreply2accountid):
    
    # in reply to id, in reply to account id
    sql = ''' UPDATE {}
              SET in_reply_to_id = ?,
                  in_reply_to_account_id = ?
              WHERE globalID = ?'''.format(table)
    cur = dbconnection.cursor()
    cur.execute(sql, (inreply2id, inreply2accountid, tootglobalID))
    dbconnection.commit()
    print("Updated toot entry {}-{}-{}".format(inreply2id, inreply2accountid, tootglobalID))

def execute_update_reblogging_sql(dbconnection, table, tootglobalID, boosters):
    # add boosters.acct of boosters.jsonl as a list str
    
    sql = ''' UPDATE {}
              SET rebloggedbyuser = ?
              WHERE globalID = ?'''.format(table)
    cur = dbconnection.cursor()
    cur.execute(sql, (boosters, tootglobalID))
    dbconnection.commit()

def execute_update_reblogging_counts_sql(dbconnection, table, tootglobalID, replies_count, reblogs_count, favourites_count):
        
    sql = ''' UPDATE {}
              SET replies_count = ?, reblogs_count = ?, favourites_count = ?
              WHERE globalID = ?'''.format(table)
    cur = dbconnection.cursor()
    cur.execute(sql, (replies_count, reblogs_count, favourites_count, tootglobalID))
    dbconnection.commit()


def retrieve_toot_from_id_in_toots_list(tootid, tootsinteractionlist):

    for i in tootsinteractionlist:
        if i["id"] == tootid:
            return i
    return None

def execute_update_context_sql(dbconnection, table, headtoot, repliestoot, auth_dict, cutoff_date="2023-12-02"):
    # iterate over replies list, build unique ids from URIs and insert in DB
    updreplies = [headtoot]
    for replyidx in range(len(repliestoot)):
        reply = repliestoot[replyidx]
        # extract toot text content
        toot = BeautifulSoup(reply["content"], "html.parser")
        # add extra entry in toot with Mastodon instance name
        reply["instance_name"] = headtoot["instance_name"]
        # add extra entry in toot dictionary
        toottext = toot.get_text()
        reply["toottext"] = toottext
        acc = add_unique_account_id(reply["account"], reply["instance_name"])
        reply["account"] = acc
        reply = add_unique_toot_id(reply, reply["instance_name"])
        reply["rebloggedbyuser"] = []
        if isinstance(reply["spoiler_text"], str) and len(reply["spoiler_text"]) > 0:
            tootspoiler = BeautifulSoup(reply["spoiler_text"], "html.parser")
            reply["spoiler_clean_text"] = tootspoiler.get_text()
        if isinstance(reply["account"]["note"], str) and len(reply["account"]["note"]) > 0:
            accountnote = BeautifulSoup(reply["account"]["note"], "html.parser")
            reply["account"]["account_note_text"] = accountnote.get_text()
        usertoot = reply
        newrow = build_db_row(usertoot) 
        execute_insert_sql(dbconnection, table, newrow)
        updreplies.append(reply)
    # update entries in the db with reply data
    for replyidx in range(len(updreplies)):
        reply = updreplies[replyidx]
        if reply["in_reply_to_id"] is None:
            continue
        parenttoot = retrieve_toot_from_id_in_toots_list(reply["in_reply_to_id"], updreplies)
        if parenttoot is None:
            parenttoot = get_toot_from_statusid(reply["in_reply_to_id"], headtoot["instance_name"], auth_dict=auth_dict)
            if parenttoot is None:
                continue
            else:
                toot = BeautifulSoup(parenttoot["content"], "html.parser")
                parenttoot["instance_name"] = headtoot["instance_name"]
                toottext = toot.get_text()
                parenttoot["toottext"] = toottext
                acc = add_unique_account_id(parenttoot["account"], parenttoot["instance_name"])
                parenttoot["account"] = acc
                parenttoot = add_unique_toot_id(parenttoot, parenttoot["instance_name"])
                parenttoot["rebloggedbyuser"] = []
                if isinstance(parenttoot["spoiler_text"], str) and len(parenttoot["spoiler_text"]) > 0:
                    tootspoiler = BeautifulSoup(parenttoot["spoiler_text"], "html.parser")
                    parenttoot["spoiler_clean_text"] = tootspoiler.get_text()
                if isinstance(parenttoot["account"]["note"], str) and len(parenttoot["account"]["note"]) > 0:
                    accountnote = BeautifulSoup(parenttoot["account"]["note"], "html.parser")
                    parenttoot["account"]["account_note_text"] = accountnote.get_text()               
                parentrow = build_db_row(parenttoot)        
                if parentrow is not None:            
                    execute_insert_sql(dbconnection, "toots", parentrow)
                else:
                    print(parentrow)
        if "edited_at" in parenttoot.keys() and parenttoot["edited_at"] is not None and parenttoot["edited_at"] != "":
            try:
                monthyear = pd.Timestamp(np.datetime64(parenttoot["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
            except:
                monthyear = pd.Timestamp(np.datetime64(parenttoot["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
        else:
            try:
                monthyear = pd.Timestamp(np.datetime64(parenttoot["created_at"])).tz_localize("CET").astimezone(pytz.utc)
            except:
                monthyear = pd.Timestamp(np.datetime64(parenttoot["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
        if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc):
            # do not collect it, in_reply_to_id field of reply remains unchanged
            continue
        parentglobalID = parenttoot["globalID"]
        parentaccountID = parenttoot["account"]["globalID"]
        execute_update_replies_sql(dbconnection, table, reply["globalID"], parentglobalID, parentaccountID)


def sqlite2jsonl(dbconnection, table, pathout="/tmp/"):
    
    try:
        sql = '''SELECT * FROM {}'''.format(table)
        c = dbconnection.cursor()
        c.execute(sql)
        for row in c:
            toot = db_row_to_json(row)
            if "edited_at" in toot.keys() and toot["edited_at"] is not None and toot["edited_at"] != "":
                try:
                    monthyear = pd.Timestamp(np.datetime64(toot["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
                except:
                    monthyear = pd.Timestamp(np.datetime64(toot["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
            else:
                try:
                    monthyear = pd.Timestamp(np.datetime64(toot["created_at"])).tz_localize("CET").astimezone(pytz.utc)
                except:
                    monthyear = pd.Timestamp(np.datetime64(toot["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
            targetfolder = "{}/toots/{}/{}/".format(pathout, monthyear.year, monthyear.month)
            if not pathlib.Path(targetfolder).exists():
                pathlib.Path(targetfolder).mkdir(parents=True, exist_ok=True)
            with jsonlines.open("{}/toots.jsonl".format(targetfolder), "a") as jsonl_write:
                jsonl_write.write(toot)
    except Error as e:
        print(e)



###################################
# keyword lists
###################################

def load_keywords_topic_lists(topics=["climatechange", "epidemics", "immigration"], topic_lists_dir="./topiclists_iscpif/"):

    keywordsearchers = []
    for topicname in topics:
        try:
            with open("{}/{}_glossary_kw.pickle".format(topic_lists_dir, topicname), "rb") as f:
                topicname_kw = pickle.load(f)
            print("Loaded {} keyword searcher".format(topicname))
        except:
            topicdf = pd.read_csv("{}/{}_glossary.csv".format(topic_lists_dir, topicname)).dropna().drop_duplicates().reset_index(drop=False)
            topiclist = topicdf.words.tolist()
            topicname_kw = KeywordProcessor()
            topicname_kw.add_keywords_from_list(topiclist)
            with open("{}/{}_glossary_kw.pickle".format(topic_lists_dir, topicname), "wb") as f:
                pickle.dump(topicname_kw, f, protocol=4)
        keywordsearchers.append(topicname_kw)

    # load extra keywords to look for
    extra_keywords = pd.read_csv("{}/extra_kw.csv".format(topic_lists_dir), header=None).values.flatten().tolist()

    return keywordsearchers, extra_keywords

if __name__ == "__main__":

    with open("./authorisations/auth_dict.json", "r") as f:
        auth_dict = json.load(f) 

    # datain = pd.read_csv("/tmp/test.csv")
    # ipdb.set_trace()

    #######################################################

    ##### 1 - to run on current structure to parse directories, upd store date and insert into db
    # sourcedir = "/mnt/mastodonalldata_final/"  #"/home/yannis/Dropbox (Heriot-Watt University Team)/mstdncollect/teststruct/"
    # targetdir = "/mnt2/mstdndata/"
    # database = "/mnt2/toots_db.db"
    # sql_create_toots_table = """ CREATE TABLE IF NOT EXISTS toots (
    #                                    globalID text PRIMARY KEY,
    #                                    id text NOT NULL,
    #                                    accountglobalID text SECONDARY KEY,
    #                                    account text NOT NULL,
    #                                    created_at text NOT NULL,
    #                                    in_reply_to_id text,
    #                                    in_reply_to_account_id text,
    #                                    sensitive boolean NOT NULL,
    #                                    spoiler_text text,
    #                                    spoiler_clean_text text,
    #                                    visibility text NOT NULL,
    #                                    language text,
    #                                    uri text NOT NULL,
    #                                    url text NOT NULL,
    #                                    replies_count integer,
    #                                    reblogs_count integer,
    #                                    favourites_count integer,
    #                                    edited_at text,
    #                                    content text,
    #                                    reblog boolean,
    #                                    rebloggedbyuser text,
    #                                    media_attachments text,
    #                                    mentions text,
    #                                    tags text,
    #                                    emojis text,
    #                                    card text,
    #                                    poll text,
    #                                    instance_name text NOT NULL,
    #                                    toottext text,
    #                                    muted boolean,
    #                                    reblogged boolean,
    #                                    favourited boolean,
    #                                    UNIQUE(globalID, accountglobalID)
    #                                ); """
    # dbconn = connectTo_weekly_toots_db(database)
    # execute_create_sql(dbconn, sql_create_toots_table) 
    # parse_directories_updsavestructure4(sourcedir, targetdir, dbconn)
    #####

    pathout = "/mnt2/mstdndatatest/"
    pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)
    databasepath = "/mnt2/"
    databases = [f.name for f in os.scandir(databasepath) if "toots" in f and ".db" in f]        
    print(databases)
    ipdb.set_trace()
    for db in databases:
        dbconn = connectTo_weekly_toots_db("{}/{}".format(databasepath, db))
        sqlite2jsonl(dbconn, "toots", pathout=pathout)
    sys.exit(0)
    
    database = "/mnt2/toots_db_hashtags.db"
    pathout = "/mnt2/mstdndatatest/"
    pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)
    dbconn = connectTo_weekly_toots_db(database)
    sqlite2jsonl(dbconn, "toots", pathout=pathout)

    database = "/mnt2/toots_hashtags_2023-12-31_2024-01-07.db"
    pathout = "/mnt2/mstdndatatest/"
    pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)
    dbconn = connectTo_weekly_toots_db(database)
    sqlite2jsonl(dbconn, "toots", pathout=pathout)
  
    database = "/mnt2/toots_hashtags_2024-01-08_2024-01-15.db"
    pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)
    dbconn = connectTo_weekly_toots_db(database)
    sqlite2jsonl(dbconn, "toots", pathout=pathout)

    database = "/mnt2/toots_hashtags_2024-01-16_2024-01-23.db"
    pathout = "/mnt2/mstdndatatest/"
    pathlib.Path(pathout).mkdir(parents=True, exist_ok=True)
    dbconn = connectTo_weekly_toots_db(database)
    sqlite2jsonl(dbconn, "toots", pathout=pathout)


    ##### 2 - weekly post collection: all toots that contained at least one topic word and were conversation heads: get conversation + boosts
    #         weekly user collection: for critical (very active) users of last week: get outbox for the last week + boosts

    ##### 3 - collect toots for each topic based on hashtags: for head toots, get conversation + boosts
    # hashtags_contexts_collection.py

    #######################################################

    # daily_collection_hashtags_users(toot_dir="/tmp/", hashtag_lists_dir="/home/yannis/Dropbox (Heriot-Watt University Team)/mstdncollect/collection_hashtags/", topic_lists_dir="./")
    # df = collect_users_activity_stats("/tmp/", years=[2024], months=[3])
    # posteditems = get_outbox_from_user("davidchavalarias", "piaille.fr", auth_dict)
    # toot = get_toot_from_statusid("111773812868337511", "mastodon.social", auth_dict)
    # collect_user_postingactivity_apidirect("chavalarias", "mastodon.social", savedir="/tmp/", auth_dict)  

    # user_df = pd.read_csv("/home/yannis/Dropbox (Heriot-Watt University Team)/mstdncollect/active_users_imigration_extended_dictionary.csv", sep="\t")
    # get_followers_from_user_list(user_df, savedir="/tmp/", auth_dict)
    # sys.exit(0)

    # parse_directories_updsavestructure3("/home/ubuntu/mastodonalldata_final/", "/mnt/mastodonalldata_final/")
    sys.exit(0)
    # DIR_out = "/home/yannis/Dropbox (Heriot-Watt University Team)/mstdncollect/"
    DIR_out = "/home/ubuntu/"
    servers = ["counter.social", "toot.io", "cupoftea.social", "mastodon.com.pl", 
                   "qaf.men", "mastodon.social", "mas.to", "vivaldi.social",
                   "mastodon.online", "mastodon.green", "climatejustice.social", 
                   "mastodon.sdf.org", "c.im", "mastodon.uno", "mastodonapp.uk",
                   "mstdn.party", "masto.ai", "nerdculture.de", "planetearth.social", 
                   "fairmove.net", "climatejustice.rocks", "mastodon.world",
                   "mastodon.partipirate.org", "oc.todon.fr", "earthstream.social", 
                   "mastodon.floe.earth", "federated.press", "fediscience.org"]
    # parse_directories_tsv2gargantext(topdir=DIR_out, servers=servers, language="en", dirout=DIR_out)

    # servers = ["mastodon.social_allpublic_apidirect_nofilter"]
    #toots, users, tags = parse_directories_updsavestructure("/mnt/mastodon-data/", servers[10:], language="en", dirout="mastodonalldata_final")
    #print(len(toots))
    #print(len(users))
    #print(len(tags))
    parse_directories_updsavestructure2("/mnt/mastodon_alldata/".format(DIR_out), servers[11:], language="en", dirout="/mnt/mastodon-data/mastodonalldata_final")
    # pathlib.Path(DIR_out).mkdir(parents=True, exist_ok=True)
    # tags = collect_all_tags(DIR_out)
    # print(tags)
    # daily_toots, daily_active_users, stats = plot_tootsandusers_stats(DIR_out)
    
