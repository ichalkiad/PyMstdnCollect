import pandas as pd
from datetime import datetime 
import time 
import pathlib 
import pytz
import jsonlines
import numpy as np
from flashtext import KeywordProcessor
import logging
import requests
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

def load_keywords_topic_lists(topics=["epidemics"], topic_lists_dir="./topiclists/"):

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

