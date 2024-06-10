import json
import requests
import pandas as pd
import ipdb
import time 
import pathlib 
import pytz
from pymstdncollect.src.utils import datetime2snowflake, save2json_apidirect
from pymstdncollect.src.interactions import get_conversation_from_head, get_boosts 
from pymstdncollect.src.toots import collect_hashtag_interactions_apidirect
from pymstdncollect.src.db import execute_update_context_sql, \
                                    execute_update_reblogging_counts_sql, execute_update_reblogging_sql, \
                                        connectTo_weekly_toots_db, execute_create_sql
import logging
from datetime import timedelta
from collections import Counter
import numpy as np
from treelib import Tree

def collect_timeline_hashtag_apidirect(hashtag=None, url=None, local=False, remote=False, only_media=False,
                            max_id=None, since_id=None, min_id=None,limit=40, 
                            keywords=[], textprocessor=None, savedir="/tmp/", 
                            instance_name=None, allcollectedhashtags=[], print_tree=False, dbconn=None, auth_dict=None, cutoff_date="2023-12-02"):
    """collect_timeline_hashtag_apidirect 

    Collects timelines and conversation data based on 
    given hashtags and stores them to the database.

    Args:
        hashtag (_type_, optional): _description_. Defaults to None.
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
        allcollectedhashtags (list, optional): _description_. Defaults to [].
        print_tree (bool, optional): _description_. Defaults to False.
        dbconn (_type_, optional): _description_. Defaults to None.
        auth_dict (_type_, optional): _description_. Defaults to None.

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
        else:
            print("Check request...")            
            logging.error(instance_name)
            logging.error("Check request...")
            break

        filtered_toots, collected_tags, collected_users = \
                    collect_hashtag_interactions_apidirect(r, instance_name=instance_name)
        save2json_apidirect(filtered_toots, collected_tags, collected_users, [], [], [],
                            "{}".format(savedir))
        
        collected_tags = [j["name"] for j in collected_tags]
        allcollectedhashtags.extend(collected_tags)

        for ftoot in filtered_toots:
            # get context, i.e. conversations
            descendants = get_conversation_from_head(ftoot, instance_name, auth_dict)
            if descendants is not None and (isinstance(descendants, list) and len(descendants) > 0):  
                reblogshead = get_boosts(ftoot, ftoot["instance_name"], auth_dict=auth_dict)
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
                            monthyear = pd.Timestamp(np.datetime64(idesc["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
                        except:
                            monthyear = pd.Timestamp(np.datetime64(idesc["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                    else:
                        try:
                            monthyear = pd.Timestamp(np.datetime64(idesc["created_at"])).tz_localize("CET").astimezone(pytz.utc)
                        except:
                            monthyear = pd.Timestamp(np.datetime64(idesc["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
                    if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc):
                        # do not collect it
                        continue 

                    # get boosts    
                    reblogs = get_boosts(idesc, ftoot["instance_name"], auth_dict=auth_dict)
                    boosters = str(reblogs)
                    idesc["rebloggedbyuser"] = boosters
                    
                    # toot = BeautifulSoup(idesc["content"], "html.parser")                                                
                    # toottext = toot.get_text()                
                    # idesc["toottext"] = toottext
                    descendants_upd.append(idesc)  
                # insert replies toot if not already in db, now containing reblogs, and add interaction data
                execute_update_context_sql(dbconn, "toots", ftoot, descendants_upd, auth_dict)                                    
                # update head in db, will be in there already
                execute_update_reblogging_sql(dbconn, "toots", ftoot["globalID"], boostershead)
                execute_update_reblogging_counts_sql(dbconn, "toots", ftoot["globalID"], ftoot["replies_count"], 
                                                        ftoot["reblogs_count"], ftoot["favourites_count"])  

                # context = {"instance": instance_name, "head": ftoot, "descendants": descendants_upd}                
                if print_tree:
                    tree = Tree()
                    tree.create_node("###### Init post: " + str(ftoot["created_at"])[:16] + " " + ftoot["account"]["username"] + ": " + ftoot["toottext"] + " " + ftoot["uri"], ftoot["id"])                    
                    for status in descendants_upd:                        
                        try:
                            tree.create_node("###### Reply: " + str(status["created_at"])[:16] + " " + status["account"]["username"] + ": " + status["toottext"] + " " + status["uri"], status["id"], parent=status["in_reply_to_id"])
                        except:                            
                            print("Problem adding node to the tree")
                    tree.show(line_type="ascii-em")
                    pathlib.Path("{}/trees/".format(savedir)).mkdir(parents=True, exist_ok=True)
                    tree.save2file("{}/trees/{}_{}.txt".format(savedir, ftoot["id"], instance_name), line_type="ascii-em")
                    
                # get boosts
                # boosters = get_boosts(ftoot, instance_name, auth_dict)                
                # if boosters is not None:
                #     boostdict = {"status": ftoot, "boosters": boosters}
                #     save2json_apidirect([], [], [], [], context, boostdict, "{}".format(savedir))
                # else:
                #     save2json_apidirect([], [], [], [], context, [], "{}".format(savedir))
                
                # add hashtag conversations in the db
                execute_update_context_sql(dbconn, "toots", ftoot, descendants_upd, auth_dict) 
        
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

    return allcollectedhashtags




if __name__ == "__main__":

    # DEVISE STOPPING RULE FOR HASHTAGS, INFEASIBLE TO CHECK ALL OF THEM

    parallel = False
    with open("./authorisations/auth_dict.json", "r") as f:
        auth_dict = json.load(f)    
    topics = ["climatechange", "epidemics", "immigration"]

    upperend = pd.Timestamp("2024-02-14") # datetime.now(timezone.utc) 
    # upperend = upperend - timedelta(days=15) # as per David: collect past 72h-48h intervals so that we have "favorited post" information 
    max_id_snowflake = datetime2snowflake(upperend)
    timestamp = upperend - timedelta(days=7)
    min_id_snowflake = datetime2snowflake(timestamp)    
    print(max_id_snowflake, min_id_snowflake)
    database = "/mnt2/toots_hashtags_{}_{}.db".format(timestamp.strftime("%Y-%m-%d"), upperend.strftime("%Y-%m-%d"))
    sql_create_toots_table = """ CREATE TABLE IF NOT EXISTS toots (
                                       globalID text PRIMARY KEY,
                                       id text NOT NULL,
                                       accountglobalID text SECONDARY KEY,
                                       account text NOT NULL,
                                       created_at text NOT NULL,
                                       in_reply_to_id text,
                                       in_reply_to_account_id text,
                                       sensitive boolean NOT NULL,
                                       spoiler_text text,
                                       spoiler_clean_text text,
                                       visibility text NOT NULL,
                                       language text,
                                       uri text NOT NULL,
                                       url text NOT NULL,
                                       replies_count integer,
                                       reblogs_count integer,
                                       favourites_count integer,
                                       edited_at text,
                                       content text,
                                       reblog boolean,
                                       rebloggedbyuser text,
                                       media_attachments text,
                                       mentions text,
                                       tags text,
                                       emojis text,
                                       card text,
                                       poll text,
                                       instance_name text NOT NULL,
                                       toottext text,
                                       muted boolean,
                                       reblogged boolean,
                                       favourited boolean,
                                       UNIQUE(globalID, accountglobalID)
                                   ); """
    dbconn = connectTo_weekly_toots_db(database)
    execute_create_sql(dbconn, sql_create_toots_table) 
    ##################################################
    

    # database = "/mnt2/toots_hashtags_{}_{}.db".format(timestamp.strftime("%Y-%m-%d"), upperend.strftime("%Y-%m-%d"))
    # dbconn = connectTo_weekly_toots_db(database)
    hashtag_lists_dir = "/home/ubuntu/mstdncollect/collection_hashtags/"
    tree = False    
    climate_hashtags = pd.read_csv("{}/climate_hashtags_upd.csv".format(hashtag_lists_dir), header=None)  # for subsequent runs change to _upd
    covid_hashtags = pd.read_csv("{}/epidemics_hashtags_upd.csv".format(hashtag_lists_dir), header=None)
    immigration_hashtags = pd.read_csv("{}/immigration_hashtags_upd.csv".format(hashtag_lists_dir), header=None)
    climate_hashtags_list = climate_hashtags.values.flatten().tolist()
    covid_hashtags_list = covid_hashtags.values.flatten().tolist()
    immigration_hashtags_list = immigration_hashtags.values.flatten().tolist()

    hashtag_list_all = [climate_hashtags_list, covid_hashtags_list, immigration_hashtags_list]
    hashtag_list_names = topics
    allcollectedhashtags = []
    for hashtaglistidx in range(3):
        hashtaglist = hashtag_list_all[hashtaglistidx]
        name = hashtag_list_names[hashtaglistidx]
        for hashtag in hashtaglist:
            for server in auth_dict.keys():
                apibaseurl = "https://{}/api/v1/timelines/tag/{}".format(server, hashtag)
                allcollectedhashtags = collect_timeline_hashtag_apidirect(hashtag=hashtag, url=apibaseurl, local=False, remote=False, only_media=False,
                                max_id=max_id_snowflake, since_id=min_id_snowflake, min_id=None, limit=40, 
                                keywords=[], textprocessor=None, savedir="/tmp/", 
                                instance_name=server, allcollectedhashtags=allcollectedhashtags, print_tree=tree, dbconn=dbconn, auth_dict=auth_dict)
                
        # get hashtags of 95th percentile of hashtag count distribution
        hashdict = Counter(allcollectedhashtags)
        ninthdec = np.percentile([*hashdict.values()], 95, method="lower")  
        for i in hashdict.keys():
            if hashdict[i] >= ninthdec and i not in hashtaglist:
                hashtaglist.append(i)
        print(hashtaglist)
        pd.DataFrame.from_dict({"hashtags": list(set(hashtaglist))}).to_csv("{}/{}_hashtags_upd.csv".format(hashtag_lists_dir, name), index=False, header=False)