import json
import ipdb
from datetime import datetime 
import time 
import pytz
import jsonlines
import logging
import requests
from pymstdncollect.src.utils import save2json_apidirect, get_user_id_from_username


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
# data collection for follower relationships
###################################

def collect_user_followers_apidirect(res, usr_id, keywords=None, textprocessor=None, instance_name=None):

    # convert to json
    fetched_followers = res.json()    
    print("Fetched {} followers of user {}...".format(len(fetched_followers), usr_id))
    
    followers  = []
    for i in fetched_followers:  

        if "Z" in i["created_at"]:
            i["created_at"] = i["created_at"][:-5]             
    
        for kk in i.keys():
            if isinstance(i[kk], datetime):
                i[kk] = i[kk].astimezone(pytz.utc)
                i[kk] = i[kk].strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(i[kk], dict):
                for kkk in i[kk].keys():
                    if isinstance(i[kk][kkk], datetime):
                        i[kk][kkk] = i[kk][kkk].astimezone(pytz.utc)
                        i[kk][kkk] = i[kk][kkk].strftime("%Y-%m-%dT%H:%M:%S") 
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
               

