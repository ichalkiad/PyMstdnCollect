import ipdb
from datetime import datetime, timezone, timedelta
import time 
import pathlib 
import pytz
from pymstdncollect.src.utils import datetime2snowflake 
from pymstdncollect.src.toots import collect_timeline_apidirect
from pymstdncollect.src.db import connectTo_weekly_toots_db, execute_create_sql 
import logging
import multiprocessing
import json


if __name__ == "__main__":

    parallel = False
    authdict_fullpath = "./authorisations/auth_dict.json"
    with open(authdict_fullpath, "r") as f:
        auth_dict = json.load(f)  
   
    upperend = datetime.now(timezone.utc) 
    max_id_snowflake = datetime2snowflake(upperend)
    timestamp = upperend - timedelta(days=1)
    min_id_snowflake = datetime2snowflake(timestamp)    
    print(max_id_snowflake, min_id_snowflake)
    
    # Provide full path to output directory
    DIR_out = "/tmp/"   
    pathlib.Path(DIR_out).mkdir(parents=True, exist_ok=True)
    pathlib.Path("{}/logging/".format(DIR_out)).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename="{}/logging/logging_{}.txt".format(DIR_out, 
                    datetime.now().astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S")),
                    filemode='w',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
    logging.info("Until: {}".format(timestamp.strftime("%Y-%m-%dT%H:%M:%S")))
    
    database = "{}/toots_db_{}_{}.db".format(DIR_out, timestamp.strftime("%Y-%m-%d"), upperend.strftime("%Y-%m-%d"))

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
    dbconnection = connectTo_weekly_toots_db(database)
    execute_create_sql(dbconnection, sql_create_toots_table) 

    processes = []
    for server in auth_dict.keys():
        apibaseurl = "https://{}/api/v1/timelines/public".format(server)        
        print(apibaseurl)
        if parallel:
            process = multiprocessing.Process(target=collect_timeline_apidirect, args=(dbconnection, apibaseurl, 
                                                False, False, False, max_id_snowflake, min_id_snowflake, None, 40, 
                                                [], None, DIR_out, server, auth_dict, timestamp.strftime("%Y-%m-%d")))
            processes.append(process)        
            process.start()              
        else:
            res = collect_timeline_apidirect(dbconnection=dbconnection, url=apibaseurl, max_id=max_id_snowflake, since_id=min_id_snowflake, 
                                             savedir=DIR_out, instance_name=server, auth_dict=auth_dict, cutoff_date=timestamp.strftime("%Y-%m-%d"))                
        time.sleep(300)
    if parallel:
        for process in processes:
            process.join()  
    logging.info("Finished.")
    logging.FileHandler("{}/logging/logging_{}.txt".format(DIR_out, datetime.now().astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S"))).close()    
