import ipdb
from datetime import datetime, timezone, timedelta
import time 
import pathlib 
from pymstdncollect.user_scripts.weekly_postcollection import connectTo_weekly_toots_db, weekly_toots_postcollection, \
                                                                weekly_users_postcollection
from pymstdncollect.user_scripts.hashtags_contexts_collection import collect_timeline_hashtag_apidirect 
from pymstdncollect.src.utils import datetime2snowflake, save2json_apidirect, get_toot_from_statusid, \
                                        get_user_id_from_username
from pymstdncollect.src.db import connectTo_weekly_toots_db, execute_create_sql 
from pymstdncollect.src.interactions import get_conversation_from_head, get_boosts, get_outbox_from_user, \
                                                get_parent_toot, collect_user_followers_apidirect
from pymstdncollect.src.toots import collect_hashtag_interactions_apidirect, \
                                        collect_user_postingactivity_apidirect, \
                                            daily_collection_hashtags_users, collect_toots_and_tooters_apidirect
import logging
import json
import requests
from treelib import Tree

# if __name__ == "__main__":
def test_12():


    savedir = "/tmp/"
    with open("./authorisations/auth_dict.json", "r") as f:
        auth_dict = json.load(f)    
    
    topics = ["epidemics"]
    hashtag_lists_dir = "./pymstdncollect/tests/collection_hashtags/"
    topic_lists_dir = "./pymstdncollect/tests/topiclists/"
    
    
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
    upperend = datetime.now(timezone.utc) 
    timestamp = upperend - timedelta(minutes=30)   
    print("Testing 'weekly_users_postcollection'...")
    weekly_users_postcollection(sourcedir=None, mindate=timestamp, maxdate=upperend, dbconn=dbconn, 
                                outdir=savedir, auth_dict=auth_dict, dbtablename="toots")
    print("Saved list of critical users in {}/criticalusers/.".format(savedir))
    ##################################################
    ##################################################
