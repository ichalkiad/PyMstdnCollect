from datetime import datetime, timezone, timedelta
from pymstdncollect.src.toots import daily_collection_hashtags_users
from pymstdncollect.src.db import connectTo_weekly_toots_db, execute_create_sql 


if __name__ == "__main__":

    # Data collection period
    mindate = datetime.now(timezone.utc) 
    maxdate = mindate - timedelta(days=1)    
    # Provide full file path of the SQLite database
    database = "/tmp/toots_db_{}_{}.db".format(mindate.strftime("%Y-%m-%d"), maxdate.strftime("%Y-%m-%d"))
    # Provide path for toot output in JSON format, else set to None to utilise the database
    toot_dir = None
    # Provide paths of directories that contain the hashtags lists that will be used, as well as the topic specific dictionaries
    hashtag_lists_dir = "/home/ubuntu/PyMstdnCollect/collection_hashtags/"
    topic_lists_dir = "/home/ubuntu/PyMstdnCollect/topiclists/"

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
    daily_collection_hashtags_users(dbconn=dbconn, toot_dir=toot_dir, hashtag_lists_dir=hashtag_lists_dir, 
                                    topic_lists_dir=topic_lists_dir, dbtablename="toots")