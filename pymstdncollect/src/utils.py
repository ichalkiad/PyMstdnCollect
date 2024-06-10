import json
import pandas as pd
import ipdb
from datetime import datetime 
import time 
import pathlib 
import pytz
import jsonlines
import os
import numpy as np
from flashtext import KeywordProcessor
import logging
import requests
import sys
import pickle

###################################
# save outputs to jsonl
###################################

def save2json_apidirect(filtered_toots, collected_tags, collected_users, 
                            collected_followers=[], collected_contexts=[], 
                            collected_boosts=[], savedir="/tmp/"):

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
# helper functions to add unique key to toot, account
###################################

def add_unique_toot_id(toot, instancename):    
    # get id and instance from uri, which is unique
    tootid = toot["id"]    
    toot["globalID"] = "{}@@{}".format(tootid, instancename)
    
    return toot

def add_unique_account_id(account, instancename): 
    accountid = account["acct"]   
    if account["username"] == accountid:
        account["globalID"] = "{}@{}@@{}".format(accountid, instancename, instancename)
    else:
        account["globalID"] = "{}@@{}".format(accountid, instancename)

    return account

###################################
# helper functions to retrieve toot/user id 
###################################

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
    
