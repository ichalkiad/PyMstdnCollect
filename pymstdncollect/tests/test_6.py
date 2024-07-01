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
def test_6():

    with open("./authorisations/auth_dict.json", "r") as f:
        auth_dict = json.load(f)    
    

    ##################################################
    ##################################################
    print("Testing 'get_parent_toot'...")
    instance_name = "piaille.fr"
    toot = {"id": "112153235645211454"}
    ancestors = get_parent_toot(toot, instance_name, auth_dict)
    print("Collected {} ancestor messages...".format(len(ancestors)))
    ##################################################
    ##################################################