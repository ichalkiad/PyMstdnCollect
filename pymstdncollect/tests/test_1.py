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


# pytest --pyargs pymstdncollect/tests

# if __name__ == "__main__":
def test_1():

    savedir = "/tmp/"
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