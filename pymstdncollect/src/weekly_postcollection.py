import ipdb
import os
from datetime import datetime, timezone, timedelta
import time 
import pathlib 
import pytz
from pymstdncollect.src.utils import connectTo_weekly_toots_db, \
    daily_collection_hashtags_users, collect_users_activity_stats, \
        collect_user_postingactivity_apidirect, get_boosts, execute_insert_sql,\
        execute_update_reblogging_sql, execute_update_reblogging_counts_sql, \
        get_conversation_from_head, execute_update_context_sql, build_db_row, \
        load_keywords_topic_lists
        
import logging
import multiprocessing
import sqlite3
from sqlite3 import Error
import jsonlines
import pandas as pd
import numpy as np 
import pickle
from flashtext import KeywordProcessor
import json


def weekly_users_postcollection(sourcedir, mindate, maxdate, dbconn=None, outdir="/tmp/", auth_dict=None):
    """weekly_users_postcollection 
    
    Collects ans stores the most active users (95th percentile of user 
    activity measured in number of toots) based on the toots
    in the "sourcedir" directory. For those users, it extracts their
    (written or edited) toots between (mindate, maxdate) and adds 
    them to the database.

    Args:
        sourcedir (_type_): _description_
        mindate (_type_): _description_
        maxdate (_type_): _description_
        dbconn (_type_, optional): _description_. Defaults to None.
        outdir (str, optional): _description_. Defaults to "/tmp/".
    """
    # change head directory structure to toots/year/month/toots.jsonl

    years = [f.name for f in os.scandir("{}/toots/".format(sourcedir)) if f.is_dir()]     
    months = [f.name for m in years for f in os.scandir("{}/toots/{}/".format(sourcedir, m)) if f.is_dir()]     
    usersactivity = collect_users_activity_stats(sourcedir, years=years, months=months)    
    # 95th percentile of user activity in number of posts
    topactivity = np.percentile(usersactivity.statuses.values, 90, interpolation="higher")
    topusers = usersactivity.loc[usersactivity.statuses >= topactivity]
    topusers = topusers.drop_duplicates(["acct"]).reset_index(drop=True)
    if len(topusers) > 10:
        topactivity = np.percentile(topusers.followers.values, 98, interpolation="higher")
        topusers = topusers.loc[topusers.followers >= topactivity].reset_index(drop=True)
    pathlib.Path("{}/criticalusers/".format(outdir)).mkdir(parents=True, exist_ok=True)
    topusers.to_csv("{}/criticalusers/users_{}_{}.csv".format(outdir, mindate.strftime("%d%m%Y"), maxdate.strftime("%d%m%Y")))
    print(topusers)
    
    doneusers = []   
    for i, row in topusers.iterrows():
        try:            
            # get user outbox for current week  
            usertoots, tags = collect_user_postingactivity_apidirect(row["acct"],  
                                    row["instance_name"], savedir=outdir, auth_dict=auth_dict)     
            if usertoots is None:
                continue        
            for usertoot in usertoots:                
                # update db with toots if their date is in current week
                if "edited_at" in usertoot.keys() and usertoot["edited_at"] is not None and usertoot["edited_at"] != "":
                    try:
                        tootdate = pd.Timestamp(np.datetime64(usertoot["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
                    except:
                        tootdate = pd.Timestamp(np.datetime64(usertoot["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                else:
                    try:
                        tootdate = pd.Timestamp(np.datetime64(usertoot["created_at"])).tz_localize("CET").astimezone(pytz.utc)
                    except:
                        tootdate = pd.Timestamp(np.datetime64(usertoot["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                if tootdate < mindate or tootdate > maxdate:
                    continue
                # get boosts for user statuses of current week                
                reblogs = get_boosts(usertoot, row["instance_name"], auth_dict=auth_dict)
                # insert toot if not already in db and update with reblogs
                boosters = str(reblogs)                                          
                if dbconn is not None:
                    newrow = build_db_row(usertoot)                   
                    execute_insert_sql(dbconn, "toots", newrow)
                    execute_update_reblogging_sql(dbconn, "toots", usertoot["globalID"], boosters)
                    execute_update_reblogging_counts_sql(dbconn, "toots", usertoot["globalID"], usertoot["replies_count"], 
                                                            usertoot["reblogs_count"], usertoot["favourites_count"])

                i += 1
                doneusers.append(row["globalID"])
        except:
            print(row)


def weekly_toots_postcollection(sourcedir, mindate, maxdate, dbconn=None, 
                                topics=["climatechange", "epidemics", "immigration"], 
                                topic_lists_dir="./topiclists_iscpif/", auth_dict=None):
    """weekly_toots_postcollection 

        Scans "sourcedir" for toots that are conversation heads 
        and contain given keywords and collects th full 
        conversation and boosts.

    Args:
        sourcedir (_type_): _description_
        mindate (_type_): _description_
        maxdate (_type_): _description_
        dbconn (_type_, optional): _description_. Defaults to None.
        topics: list of strings containing dictionary names.

                NOTE: the dictionaries should be stored in topic_lists_dir is csv format
                        with the following naming convention: "{topicname}_glossary.csv".


        topic_lists_dir (str, optional): _description_. Defaults to "./topiclists_iscpif/".
    """
    # changed head directory structure to toots/year/month/toots.jsonl

    # load topic word lists
    keywordsearchers, extra_keywords = load_keywords_topic_lists(topics=topics, topic_lists_dir=topic_lists_dir)

    years = [f.name for f in os.scandir("{}/toots/".format(sourcedir)) if f.is_dir()]     
    months = [f.name for m in years for f in os.scandir("{}/toots/{}/".format(sourcedir, m)) if f.is_dir()]     
    
    for year in years:
        for month in months:
            if not pathlib.Path("{}/toots/{}/{}".format(sourcedir, year, month)).exists():
                continue
            with jsonlines.open("{}/toots/{}/{}/toots.jsonl".format(sourcedir, year, month), "r") as jsonl_read:
                try:
                    for data in jsonl_read.iter(type=dict, skip_invalid=True):
                        tootwords = data["toottext"].split()
                        for tw in tootwords:
                            # tw in climate_kw or tw in epidemics_kw or tw in immigration_kw or
                            if any(tw in kw for kw in keywordsearchers) or\
                                  any(ext in data["toottext"] for ext in extra_keywords):                                
                                # extract conversation if toot is head, store all conversation toots in db if not in already, upd toot reply links
                                descendants = get_conversation_from_head(data, data["instance_name"], auth_dict)                               
                                if descendants is not None and (isinstance(descendants, list) and len(descendants) > 0):                
                                    reblogshead = get_boosts(data, data["instance_name"], auth_dict)
                                    boostershead = str(reblogshead)
                                    descendants_upd = []
                                    for idesc in descendants:
                                        if idesc["account"]["bot"]:
                                            continue
                                        # keep public posts                
                                        if idesc["visibility"] != "public":
                                            continue   

                                        if "edited_at" in idesc.keys() and idesc["edited_at"] is not None and idesc["edited_at"] != "":
                                            try:
                                                tootdate = pd.Timestamp(np.datetime64(idesc["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
                                            except:
                                                tootdate = pd.Timestamp(np.datetime64(idesc["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                                        else:
                                            try:
                                                tootdate = pd.Timestamp(np.datetime64(idesc["created_at"])).tz_localize("CET").astimezone(pytz.utc)
                                            except:
                                                tootdate = pd.Timestamp(np.datetime64(idesc["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                                        if tootdate < mindate or tootdate > maxdate:
                                            # do not collect it
                                            continue 
                                        
                                        # get boosts    
                                        reblogs = get_boosts(idesc, data["instance_name"], auth_dict)
                                        if reblogs is None:
                                            idesc["rebloggedbyuser"] = []
                                        else:
                                            boosters = str(reblogs)
                                            idesc["rebloggedbyuser"] = boosters
                                        
                                        descendants_upd.append(idesc)   
                                    # insert replies toot if not already in db, now containing reblogs, and add interaction data                                   
                                    execute_update_context_sql(dbconn, "toots", data, descendants_upd, auth_dict)                                    
                                    # update head in db, will be in there already
                                    execute_update_reblogging_sql(dbconn, "toots", data["globalID"], boostershead)
                                    execute_update_reblogging_counts_sql(dbconn, "toots", data["globalID"], data["replies_count"], 
                                                                         data["reblogs_count"], data["favourites_count"])
                                # as long as one toot word in at least one dictionary topic, we can store it and move to the next
                                break    
                except:
                    print(year, month)


if __name__ == "__main__":

    with open("./authorisations/auth_dict.json", "r") as f:
        auth_dict = json.load(f) 

    database = "/mnt2/toots_db.db"

    dbconn = connectTo_weekly_toots_db(database)
    toot_dir = "/mnt2/mstdndata/"
    hashtag_lists_dir = "/home/ubuntu/mstdncollect/collection_hashtags/"
    topic_lists_dir = "/home/ubuntu/mstdncollect/topiclists_iscpif/"
    
    """ 
    NOTE: the dictionaries should be stored in topic_lists_dir is csv format
        with the following naming convention: "{topicname}_glossary.csv".

        Special keywords can be stored in extra_kw.csv in topic_lists_dir.

    """
    topics = ["climatechange", "epidemics", "immigration"]
    maxdate = datetime.now(timezone.utc)
    mindate = maxdate - timedelta(days=10)
    daily_collection_hashtags_users(toot_dir, hashtag_lists_dir, topics, topic_lists_dir)
    weekly_users_postcollection(toot_dir, mindate, maxdate, dbconn=dbconn, outdir=toot_dir, auth_dict=auth_dict)
    weekly_toots_postcollection(toot_dir, mindate, maxdate, dbconn=dbconn, topics=topics, topic_lists_dir=topic_lists_dir, auth_dict=auth_dict)  