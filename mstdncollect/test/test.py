import ipdb
import os
from datetime import datetime, timezone, timedelta
import time 
import pathlib 
import pytz
from mstdncollect.src.utils import connectTo_weekly_toots_db, \
    daily_collection_hashtags_users, collect_users_activity_stats, \
        collect_user_postingactivity_apidirect, get_boosts, execute_insert_sql,\
        execute_update_reblogging_sql, execute_update_reblogging_counts_sql, \
        get_conversation_from_head, execute_update_context_sql, build_db_row, \
        load_keywords_topic_lists, datetime2snowflake, execute_create_sql, collect_hashtag_interactions_apidirect,\
        save2json_apidirect, get_outbox_from_user, get_toot_from_statusid, get_parent_toot, collect_toots_and_tooters_apidirect,\
        collect_user_followers_apidirect, get_user_id_from_username
from mstdncollect.src.weekly_postcollection import connectTo_weekly_toots_db, weekly_toots_postcollection, weekly_users_postcollection
from mstdncollect.src.hashtags_contexts_collection import collect_timeline_hashtag_apidirect 
import logging
import pandas as pd
import numpy as np 
import json
import requests
from treelib import Node, Tree

if __name__ == "__main__":


    savedir = "/tmp/"
    with open("../../authorisations/auth_dict.json", "r") as f:
        auth_dict = json.load(f)    
    
    topics = ["climatechange", "epidemics", "immigration"]
    hashtag_lists_dir = "./collection_hashtags/"
    topic_lists_dir = "./topiclists_iscpif/"
    
    
    upperend = datetime.now(timezone.utc) 
    max_id_snowflake = datetime2snowflake(upperend)
    timestamp = upperend - timedelta(days=7)
    min_id_snowflake = datetime2snowflake(timestamp)    
    print(max_id_snowflake, min_id_snowflake)
    database = "/{}/test_{}_{}.db".format(savedir, timestamp.strftime("%Y-%m-%d"), upperend.strftime("%Y-%m-%d"))
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
    ##################################################


    ##################################################
    ##################################################
    hashtag = "euelections"
    instance_name = "mastodon.social"
    apibaseurl = "https://{}/api/v1/timelines/tag/{}".format(instance_name, hashtag)
    try:
        r = requests.get(apibaseurl, timeout=300)
    except requests.exceptions.ConnectionError:
        # Network/DNS error, wait 30mins and retry
        time.sleep(60)
    except requests.exceptions.HTTPError as e:
        print("HTTPError issue: {}...exiting".format(e.response.status_code))
        logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
    except requests.exceptions.Timeout:
        print("Timeout...exiting")
        logging.info("Timeout...exiting")
    except requests.exceptions.TooManyRedirects:
        print("Too many redirects...exiting")
        logging.info("Too many redirects...exiting")
    except requests.exceptions.RequestException as e:
        print("Uknown GET issue: {}...exiting".format(e.response.status_code))
        logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
    print("Testing 'collect_hashtag_interactions_apidirect'...")
    filtered_toots, collected_tags, collected_users = \
                    collect_hashtag_interactions_apidirect(r, instance_name=instance_name)
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'save2json_apidirect' - output should be in {}...".format(savedir))
    save2json_apidirect(filtered_toots, collected_tags, collected_users, [], [], [],
                            "{}".format(savedir))
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'get_outbox_from_user'...")
    useracct = "davidchavalarias"
    instance_name = "piaille.fr"
    fetched_toots = get_outbox_from_user(useracct, instance_name, auth_dict=auth_dict)
    print(fetched_toots)
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'get_toot_from_statusid'...")
    tootid = "111773812868337511"
    instance_name = "mastodon.social"
    toot = get_toot_from_statusid(tootid, instance_name, auth_dict)
    print(toot)
    ##################################################
    ##################################################
    
    
    ##################################################
    ##################################################
    print("Testing 'collect_user_postingactivity_apidirect'...")
    useracct = "chavalarias"
    instance_name = "mastodon.social"
    filtered_toots, collected_tags = collect_user_postingactivity_apidirect(useracct, instance_name, savedir=savedir, auth_dict=auth_dict)  
    print("Collected {} toots and {} tags...".format(len(filtered_toots), len(collected_tags)))
    ##################################################
    ##################################################
    

    ##################################################
    ##################################################
    print("Testing 'get_conversation_from_head'...")
    instance_name = "piaille.fr"
    tootid = "112149279526485295"
    toot = get_toot_from_statusid(tootid, instance_name, auth_dict)
    descendants = get_conversation_from_head(toot, instance_name, auth_dict)
    tree = Tree()
    tree.create_node("###### Init post: " + str(toot["created_at"])[:16] + " " + toot["account"]["username"] + ": " + toot["content"] + " " + toot["uri"], toot["id"])                    
    for status in descendants:                        
        try:
            tree.create_node("###### Reply: " + str(status["created_at"])[:16] + " " + status["account"]["username"] + ": " + status["content"] + " " + status["uri"], status["id"], parent=status["in_reply_to_id"])
        except:                            
            print("Problem adding node to the tree")
    tree.show(line_type="ascii-em")
    pathlib.Path("{}/trees/".format(savedir)).mkdir(parents=True, exist_ok=True)
    tree.save2file("{}/trees/{}_{}.txt".format(savedir, toot["id"], instance_name), line_type="ascii-em")
    print("Collected {} replies...".format(len(descendants)))
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'get_parent_toot'...")
    toot = {"id": "112153235645211454"}
    ancestors = get_parent_toot(toot, instance_name, auth_dict)
    print("Collected {} ancestor messages...".format(len(ancestors)))
    ##################################################
    ##################################################
    
    
    ##################################################
    ##################################################
    print("Testing 'get_boosts'...")
    instance_name = "piaille.fr"
    toot = {"id": "112082346954829534"}
    boosts = get_boosts(toot, instance_name, auth_dict)
    print("Collected {} boosts...".format(len(boosts)))
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'collect_toots_and_tooters_apidirect'...")
    keywords = []
    textprocessor = None
    instance_name = "piaille.fr"
    apibaseurl = "https://{}/api/v1/timelines/public".format(instance_name)        
    try:
        r = requests.get(apibaseurl, timeout=300)
    except requests.exceptions.ConnectionError:
        # Network/DNS error, wait 30mins and retry
        time.sleep(60)
    except requests.exceptions.HTTPError as e:
        print("HTTPError issue: {}...exiting".format(e.response.status_code))
        logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
    except requests.exceptions.Timeout:
        print("Timeout...exiting")
        logging.info("Timeout...exiting")
    except requests.exceptions.TooManyRedirects:
        print("Too many redirects...exiting")
        logging.info("Too many redirects...exiting")
    except requests.exceptions.RequestException as e:
        print("Uknown GET issue: {}...exiting".format(e.response.status_code))
        logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
    filtered_toots, collected_tags, collected_users = \
                    collect_toots_and_tooters_apidirect(dbconn, r, keywords, 
                                                        textprocessor, 
                                                        instance_name=instance_name, 
                                                        auth_dict=auth_dict)
    print("Collected {} toots, {} tags and {} users...".format(len(filtered_toots), 
                                                               len(collected_tags), 
                                                               len(collected_users)))
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'get_user_id_from_username'...")
    username = "davidchavalarias"
    instance_name = "piaille.fr"
    instance_url = "https://{}/".format(instance_name)
    usr_id = get_user_id_from_username(username, instance_url, instance_name, auth_dict)
    print(username, instance_name, usr_id)
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'collect_user_followers_apidirect'...")
    keywords = []
    textprocessor = None
    apibaseurl = "{}/api/v1/accounts/{}/followers".format(instance_url, usr_id)
    try:
        r = requests.get(apibaseurl, timeout=300)
    except requests.exceptions.ConnectionError:
        # Network/DNS error, wait 30mins and retry
        time.sleep(60)
    except requests.exceptions.HTTPError as e:
        print("HTTPError issue: {}...exiting".format(e.response.status_code))
        logging.info("HTTPError issue: {}...exiting".format(e.response.status_code))
    except requests.exceptions.Timeout:
        print("Timeout...exiting")
        logging.info("Timeout...exiting")
    except requests.exceptions.TooManyRedirects:
        print("Too many redirects...exiting")
        logging.info("Too many redirects...exiting")
    except requests.exceptions.RequestException as e:
        print("Uknown GET issue: {}...exiting".format(e.response.status_code))
        logging.info("Uknown GET issue: {}...exiting".format(e.response.status_code))
    collected_followers = \
            collect_user_followers_apidirect(r, usr_id, keywords, textprocessor, instance_name=instance_name)
    print("Collected {} user followers (subset of total followers, no pagination)...".format(len(collected_followers[usr_id])))
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'daily_collection_hashtags_users'...")
    daily_collection_hashtags_users(savedir, hashtag_lists_dir, topics, topic_lists_dir)
    print("Updated hashtag list in {}.".format(hashtag_lists_dir))
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    upperend = datetime.now(timezone.utc) 
    timestamp = upperend - timedelta(minutes=30)   
    print("Testing 'weekly_users_postcollection'...")
    weekly_users_postcollection(savedir, timestamp, upperend, dbconn=dbconn, outdir=savedir, auth_dict=auth_dict)
    print("Saved list of critical users in {}/criticalusers/.".format(savedir))
    ##################################################
    ##################################################


    ##################################################
    ##################################################
    print("Testing 'weekly_toots_postcollection'...")
    weekly_toots_postcollection(savedir, timestamp, upperend, dbconn=dbconn, topics=topics[:1], topic_lists_dir=topic_lists_dir, auth_dict=auth_dict)  
    print("Collected conversations of toots that are conversation starters.")
    ##################################################
    ##################################################



    ##################################################
    ##################################################
    hashtag = "euelections"
    instance_name = "mastodon.social"
    apibaseurl = "https://{}/api/v1/timelines/tag/{}".format(instance_name, hashtag)
    allcollectedhashtags = []
    tree = False
    upperend = datetime.now(timezone.utc) 
    max_id_snowflake = datetime2snowflake(upperend)
    timestamp = upperend - timedelta(days=15)
    min_id_snowflake = datetime2snowflake(timestamp)    
    print(max_id_snowflake, min_id_snowflake)
    allcollectedhashtags = collect_timeline_hashtag_apidirect(hashtag=hashtag, url=apibaseurl, local=False, remote=False, only_media=False,
                                max_id=max_id_snowflake, since_id=min_id_snowflake, min_id=None, limit=40, 
                                keywords=[], textprocessor=None, savedir="/tmp/", 
                                instance_name=instance_name, allcollectedhashtags=allcollectedhashtags, print_tree=tree, dbconn=dbconn, auth_dict=auth_dict)
    print("Collected {} toots based on hashtag {} from {}.".format(len(allcollectedhashtags), hashtag, instance_name))
    ##################################################
    ##################################################
    
    