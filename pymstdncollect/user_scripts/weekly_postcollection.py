import os
from datetime import datetime, timezone, timedelta
import pathlib 
import pytz      
from pymstdncollect.src.utils import load_keywords_topic_lists
from pymstdncollect.src.interactions import get_conversation_from_head, get_boosts
from pymstdncollect.src.db import execute_update_context_sql, build_db_row, \
                                    execute_update_reblogging_counts_sql, execute_update_reblogging_sql, \
                                        connectTo_weekly_toots_db, execute_update_reblogging_counts_sql, \
                                            execute_insert_sql, db_row_to_json  
from pymstdncollect.src.toots import collect_user_postingactivity_apidirect, daily_collection_hashtags_users,\
                                        collect_users_activity_stats
import jsonlines
import pandas as pd
import numpy as np 
import json


def weekly_users_postcollection(sourcedir=None, mindate=None, maxdate=None, dbconn=None, outdir="/tmp/", auth_dict=None, dbtablename="toots"):
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
    
    if sourcedir is not None:
        years  = [f.name for f in os.scandir("{}/toots/".format(sourcedir)) if f.is_dir()]     
        months = [f.name for m in years for f in os.scandir("{}/toots/{}/".format(sourcedir, m)) if f.is_dir()]  
    else:
        years  = []
        months = []   
    usersactivity = collect_users_activity_stats(dbconn=dbconn, input_dir=sourcedir, 
                                                years=years, months=months, dbtablename=dbtablename)    
    # 95th percentile of user activity in number of posts
    topactivity = np.percentile(usersactivity.statuses.values, 95, method="higher")
    topusers = usersactivity.loc[usersactivity.statuses >= topactivity]
    topusers = topusers.drop_duplicates(["acct"]).reset_index(drop=True)
    if len(topusers) > 10:
        topactivity = np.percentile(topusers.followers.values, 98, interpolation="higher")
        topusers = topusers.loc[topusers.followers >= topactivity].reset_index(drop=True)
    if outdir is None:
        outdir = "/tmp/"
    pathlib.Path("{}/criticalusers/".format(outdir)).mkdir(parents=True, exist_ok=True)
    topusers.to_csv("{}/criticalusers/users_{}_{}.csv".format(outdir, mindate.strftime("%d%m%Y"), maxdate.strftime("%d%m%Y")))
        
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
                if "edited_at" in usertoot.keys() and usertoot["edited_at"] is not None:     
                    if "Z" in usertoot["edited_at"]:
                        usertoot["edited_at"] = usertoot["edited_at"][:-5]
                if "Z" in usertoot["created_at"]:
                    usertoot["created_at"] = usertoot["created_at"][:-5]        
                if "Z" in usertoot["account"]["created_at"]:
                    usertoot["account"]["created_at"] = usertoot["account"]["created_at"][:-5]    
        
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
                if tootdate < mindate or tootdate > maxdate or tootdate > pd.Timestamp(datetime.today().strftime('%Y-%m-%d')).tz_localize("Europe/Paris").astimezone(pytz.utc):
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

def update_relevant_toots(dbconn, data, keywordsearchers, extra_keywords, auth_dict, mindate, maxdate):

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
                    
                    if "edited_at" in idesc.keys() and idesc["edited_at"] is not None:     
                        if "Z" in idesc["edited_at"]:
                            idesc["edited_at"] = idesc["edited_at"][:-5]
                    if "Z" in idesc["created_at"]:
                        idesc["created_at"] = idesc["created_at"][:-5]        
                    if "Z" in idesc["account"]["created_at"]:
                        idesc["account"]["created_at"] = idesc["account"]["created_at"][:-5]    

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
                    if tootdate < mindate or tootdate > maxdate or tootdate > pd.Timestamp(datetime.today().strftime('%Y-%m-%d')).tz_localize("Europe/Paris").astimezone(pytz.utc):
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

def weekly_toots_postcollection(sourcedir=None, mindate=None, maxdate=None, dbconn=None, 
                                topics=["climatechange", "epidemics", "immigration"], 
                                topic_lists_dir="./topiclists_iscpif/", auth_dict=None, dbtablename="toots"):
    """weekly_toots_postcollection 

        Scans "sourcedir" for toots that are conversation heads 
        and contain given keywords and collects the full 
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

    if dbconn is None and sourcedir is not None:
        years = [f.name for f in os.scandir("{}/toots/".format(sourcedir)) if f.is_dir()]     
        months = [f.name for m in years for f in os.scandir("{}/toots/{}/".format(sourcedir, m)) if f.is_dir()]      
        for year in years:
            for month in months:
                if not pathlib.Path("{}/toots/{}/{}".format(sourcedir, year, month)).exists():
                    continue
                with jsonlines.open("{}/toots/{}/{}/toots.jsonl".format(sourcedir, year, month), "r") as jsonl_read:
                    try:
                        for data in jsonl_read.iter(type=dict, skip_invalid=True):
                            update_relevant_toots(dbconn, data, keywordsearchers, extra_keywords, auth_dict, mindate, maxdate)
                    except:
                        print(year, month)

    elif dbconn is not None and sourcedir is None:
        sql = '''SELECT * FROM {}'''.format(dbtablename)
        c = dbconn.cursor()
        c.execute(sql)
        for row in c:
            toot = db_row_to_json(row)
            update_relevant_toots(dbconn, toot, keywordsearchers, extra_keywords, auth_dict, mindate, maxdate)


if __name__ == "__main__":

    authdict_fullpath = "./authorisations/auth_dict.json"
    with open(authdict_fullpath, "r") as f:
        auth_dict = json.load(f)   
    # Provide full path for the output database
    database = "/tmp/toots_weekly_db_{}.db".format(datetime.now().astimezone(pytz.utc).strftime("%Y-%m-%d"))
    
    dbconn = connectTo_weekly_toots_db(database)
    # Provide path for toot output in JSON format, else set to None to utilise the database
    toot_dir = None
    # Provide paths of directories that contain the hashtags lists that will be used, as well as the topic specific dictionaries
    hashtag_lists_dir = "/home/ubuntu/PyMstdnCollect/collection_hashtags/"
    topic_lists_dir = "/home/ubuntu/PyMstdnCollect/topiclists/"
    
    """ 
    NOTE: the dictionaries should be stored in topic_lists_dir is csv format
        with the following naming convention: "{topicname}_glossary.csv".

        Special keywords can be stored in extra_kw.csv in topic_lists_dir.

    """
    topics = ["epidemics"]
    maxdate = datetime.now(timezone.utc)
    mindate = maxdate - timedelta(days=7)
    daily_collection_hashtags_users(dbconn=dbconn, toot_dir=toot_dir, hashtag_lists_dir=hashtag_lists_dir, topics=topics, 
                                    topic_lists_dir=topic_lists_dir, dbtablename="toots")
    weekly_users_postcollection(sourcedir=None, mindate=mindate, maxdate=maxdate, dbconn=dbconn, 
                                outdir=toot_dir, auth_dict=auth_dict, dbtablename="toots")
    weekly_toots_postcollection(sourcedir=None, mindate=mindate, maxdate=maxdate, dbconn=dbconn, topics=topics, 
                                topic_lists_dir=topic_lists_dir, auth_dict=auth_dict, dbtablename="toots")  
    