import ipdb
from datetime import datetime, timezone, timedelta
from pymstdncollect.src.toots import daily_collection_hashtags_users
from pymstdncollect.src.db import connectTo_weekly_toots_db, execute_create_sql 


if __name__ == "__main__":


    mindate = datetime.now(timezone.utc) 
    maxdate = mindate - timedelta(days=7)    
    database = "/mnt2/dailycollects_pymstdn/toots_db_{}_{}.db".format(mindate.strftime("%Y-%m-%d"), maxdate.strftime("%Y-%m-%d"))

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
                                        muted boolean,
                                        instance_name text NOT NULL,
                                        toottext text,
                                        UNIQUE(globalID, accountglobalID)
                                    ); """
    

    dbconn = connectTo_weekly_toots_db(database)
    execute_create_sql(dbconn, sql_create_toots_table)
    toot_dir = "/home/ubuntu/mstdncollect/"
    hashtag_lists_dir = "/home/ubuntu/mstdncollect/collection_hashtags/"
    topic_lists_dir = "/home/ubuntu/mstdncollect/"
    daily_collection_hashtags_users(dbconn=dbconn, toot_dir=None, hashtag_lists_dir=hashtag_lists_dir, 
                                    topic_lists_dir=topic_lists_dir, dbtablename="toots")