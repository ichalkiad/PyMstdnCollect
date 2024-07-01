import pandas as pd
import ipdb
from datetime import datetime 
import time 
import pathlib 
import pytz
from bs4 import BeautifulSoup
import jsonlines
import os
import numpy as np
import logging
import requests
from collections import Counter
from pymstdncollect.src.utils import add_unique_account_id, add_unique_toot_id, \
                        load_keywords_topic_lists, get_toot_from_statusid, \
                            save2json_apidirect
from pymstdncollect.src.interactions import get_outbox_from_user, get_parent_toot
from pymstdncollect.src.db import build_db_row, execute_insert_sql, execute_update_replies_sql

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
            
            if "edited_at" in i.keys() and i["edited_at"] is not None:     
                if "Z" in i["edited_at"]:
                    i["edited_at"] = i["edited_at"][:-5]
            if "Z" in i["created_at"]:
                i["created_at"] = i["created_at"][:-5]        
            if "Z" in i["account"]["created_at"]:
                i["account"]["created_at"] = i["account"]["created_at"][:-5]    
        
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

        return filtered_toots, collected_tags, collected_users 

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
    (95th percentile of hashtag distribution) are then added to the hashtags lists.

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
            tophst = np.percentile(hashtagscntsval, 95, method="higher")
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
        if "edited_at" in i.keys() and i["edited_at"] is not None:     
            if "Z" in i["edited_at"]:
                i["edited_at"] = i["edited_at"][:-5]
        if "Z" in i["created_at"]:
            i["created_at"] = i["created_at"][:-5]               
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

        if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc) or monthyear > pd.Timestamp(datetime.today().strftime('%Y-%m-%d')).tz_localize("Europe/Paris").astimezone(pytz.utc):
            # do not collect it
            continue
        i["instance_name"] = tootinstance
        acc = add_unique_account_id(i["account"], tootinstance)
        i["account"] = acc
        if "Z" in i["account"]["created_at"]:
            i["account"]["created_at"] = i["account"]["created_at"][:-5] 
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
        if "edited_at" in i.keys() and i["edited_at"] is not None:     
            if "Z" in i["edited_at"]:
                i["edited_at"] = i["edited_at"][:-5]
        if "Z" in i["created_at"]:
            i["created_at"] = i["created_at"][:-5]        
        if "Z" in i["account"]["created_at"]:
            i["account"]["created_at"] = i["account"]["created_at"][:-5]     
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
        if "Z" in i["account"]["created_at"]:
            i["account"]["created_at"] = i["account"]["created_at"][:-5]    
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
                    
                    if "edited_at" in parenttoot.keys() and parenttoot["edited_at"] is not None:     
                        if "Z" in parenttoot["edited_at"]:
                            parenttoot["edited_at"] = parenttoot["edited_at"][:-5]
                    if "Z" in parenttoot["created_at"]:
                        parenttoot["created_at"] = parenttoot["created_at"][:-5]        
                    if "Z" in parenttoot["account"]["created_at"]:
                        parenttoot["account"]["created_at"] = parenttoot["account"]["created_at"][:-5]     

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
                    if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc) or monthyear > pd.Timestamp(datetime.today().strftime('%Y-%m-%d')).tz_localize("Europe/Paris").astimezone(pytz.utc):
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

