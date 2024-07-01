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
def test_5():

    savedir = "/tmp/"
    with open("./authorisations/auth_dict.json", "r") as f:
        auth_dict = json.load(f)    
        
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