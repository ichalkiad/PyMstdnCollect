import pandas as pd
import ipdb
import pathlib 
import pytz
from bs4 import BeautifulSoup
import jsonlines
import numpy as np
import sqlite3
from sqlite3 import Error
from pymstdncollect.src.utils import add_unique_account_id, \
                        add_unique_toot_id, get_toot_from_statusid
from datetime import datetime

###################################
# toots db
###################################

def get_table_columns(cursor, table_name):
    """
    Get column names and types for a given table.
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    
    return {row[1]: row[2] for row in cursor.fetchall()}

def get_row_count(cursor, table_name):
    """
    Get the total number of rows in a table.
    """
    
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    
    return cursor.fetchone()[0]

def insert_rows_with_error_handling(cursor, table_name, rows, columns):
    """
    Insert rows with handling for UNIQUE constraint violations.
    
    Args:
        cursor: SQLite cursor
        table_name: Name of the target table
        rows: List of rows to insert
        columns: Dictionary of column names and types
    
    Returns:
        tuple: (successful_inserts, failed_inserts)
    """
    placeholders = ','.join(['?' for _ in columns])
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
    
    successful_inserts = 0
    failed_inserts = 0
    
    try:
        # First try batch insert
        cursor.executemany(insert_sql, rows)
        successful_inserts = len(rows)
    except sqlite3.IntegrityError:
        # If batch fails, try one by one
        print("Batch insert failed. Switching to individual row processing...")
        for row in rows:
            try:
                cursor.execute(insert_sql, row)
                successful_inserts += 1
            except sqlite3.IntegrityError:
                failed_inserts += 1
                if failed_inserts <= 5:  # Show only first 5 failures
                    print(f"UNIQUE constraint failed for row: {row}")
                elif failed_inserts == 6:
                    print("Additional failures exist but won't be shown...")
    
    return successful_inserts, failed_inserts


def merge_sqlite_databases(source_dbs, target_db, table_name, missing_columns, default_values, batch_size=5000):
    """
    Merge tables from multiple SQLite databases into a single database,
    adding specified missing columns with default values.
    
    Args:        
        target_db: Path to the target database file
        table_name: Name of the table to merge
        missing_columns: Dictionary of column names and their SQLite types to add
        default_values: Dictionary of column names and their default values
    """
    start_time = datetime.now()
    print(f"Starting merge operation at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nWill add these missing columns:")
    for col, col_type in missing_columns.items():
        default = default_values.get(col, "NULL")
        print(f"- {col} ({col_type}), default value: {default}")
    
    # Create target database
    target_conn = sqlite3.connect(target_db)
    target_cursor = target_conn.cursor()
    
    # Get list of source databases    
    total_dbs = len(source_dbs)
    print(f"\nFound {total_dbs} source databases to process")
    
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
    target_cursor.execute(sql_create_toots_table)
    all_columns = get_table_columns(target_cursor, table_name)
    # Process each database
    total_successful = 0
    total_failed = 0    
    print("\nStarting data transfer...")
    
    for db_idx, source_db in enumerate(source_dbs, 1):
        try:
            source_conn = sqlite3.connect(source_db)
            source_cursor = source_conn.cursor()
            existing_columns = get_table_columns(source_cursor, table_name)
            db_rows = get_row_count(source_cursor, table_name)
            print(f"\nProcessing database {db_idx}/{total_dbs}: {source_db}")
            print(f"Contains {db_rows:,} rows")
            if db_rows < 1:
                continue
            
            # Create SELECT statement with default values for missing columns
            select_cols = []
            for col in all_columns.keys():
                if col in existing_columns:
                    select_cols.append(col)
                else:
                    default_val = default_values.get(col, "NULL")
                    if isinstance(default_val, str) and default_val.upper() != "NULL":
                        default_val = f"'{default_val}'"
                    select_cols.append(f"{default_val} as {col}")
            
            select_sql = f"SELECT {', '.join(select_cols)} FROM {table_name}"
            source_cursor.execute(select_sql)
            
            # Process in batches
            batch_count = 0
            while True:
                rows = source_cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                successful, failed = insert_rows_with_error_handling(
                    target_cursor, table_name, rows, all_columns
                )
                
                total_successful += successful
                total_failed += failed
                
                batch_count += 1
                progress = (batch_count * batch_size / db_rows) * 100
                print(f"Batch {batch_count}: {successful} inserted, {failed} failed - "
                      f"Progress: {min(progress, 100):.1f}%")
                
                target_conn.commit()

            source_conn.close()        
            
        except sqlite3.Error as e:
            print(f"Error processing {source_db}: {e}")
            continue
    
    # Final statistics
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\nMerge operation completed!")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Ended: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"Successfully inserted rows: {total_successful:,}")
    print(f"Failed inserts: {total_failed:,}")
    print(f"Final database: {target_db}")
    
    # Verify final row count
    final_count = get_row_count(target_cursor, table_name)
    print(f"Verification: {final_count:,} rows in target database")
    
    target_conn.close()


def connectTo_weekly_toots_db(dbfile):
    
    conn = None
    try:
        conn = sqlite3.connect(dbfile)
        print(sqlite3.version)
    except Error as e:
        print(e)
    
    return conn


def build_db_row(toot):
    
    required = ["globalID", "id", "account", "created_at", "in_reply_to_id", "in_reply_to_account_id",
                "sensitive", "visibility", "uri", "content", "instance_name", "toottext"]
    for reqkey in required:
        if reqkey not in toot.keys():
            print(reqkey)
            return None
    
    if "spoiler_text" not in toot.keys():
        toot["spoiler_text"] = ""
    if "spoiler_clean_text" not in toot.keys():
        toot["spoiler_clean_text"] = ""
    if "language" not in toot.keys():
        toot["language"] = ""
    if "url" not in toot.keys():
        toot["url"] = ""
    if "replies_count" not in toot.keys():
        toot["replies_count"] = 0
    if "reblogs_count" not in toot.keys():
        toot["reblogs_count"] = 0
    if "favourites_count" not in toot.keys():
        toot["favourites_count"] = 0
    if "edited_at" not in toot.keys():
        toot["edited_at"] = ""
    if "reblog" not in toot.keys():
        toot["reblog"] = False
    if "rebloggedbyuser" not in toot.keys():
        toot["rebloggedbyuser"] = ""
    if "media_attachments" not in toot.keys():
        toot["media_attachments"] = ""
    if "mentions" not in toot.keys():
        toot["mentions"] = ""
    if "tags" not in toot.keys():
        toot["tags"] = ""
    if "emojis" not in toot.keys():
        toot["emojis"] = ""
    if "card" not in toot.keys():
        toot["card"] = ""
    if "poll" not in toot.keys():
        toot["poll"] = ""
    if "muted" not in toot.keys():
        toot["muted"] = False
    if "reblogged" not in toot.keys():
        toot["reblogged"] = False
    if "favourited" not in toot.keys():
        toot["favourited"] = False

    newrow = (toot["globalID"], toot["id"], toot["account"]["globalID"], str(toot["account"]), toot["created_at"], toot["in_reply_to_id"], 
            toot["in_reply_to_account_id"], toot["sensitive"], toot["spoiler_text"], toot["spoiler_clean_text"], toot["visibility"], 
            toot["language"], toot["uri"], toot["url"], toot["replies_count"], toot["reblogs_count"], toot["favourites_count"], 
            toot["edited_at"], toot["content"], toot["reblog"], str(toot["rebloggedbyuser"]), str(toot["media_attachments"]), 
            str(toot["mentions"]), str(toot["tags"]), str(toot["emojis"]), str(toot["card"]), str(toot["poll"]), 
            toot["instance_name"], toot["toottext"], toot["muted"], toot["reblogged"], toot["favourited"])  
    
    return newrow


def db_row_to_json(newrow):
    
    account = eval(newrow[3])
    account["globalID"] = newrow[2]
    toot = {"globalID": newrow[0], "id": newrow[1], "account": account, "created_at": newrow[4], "in_reply_to_id": newrow[5], 
            "in_reply_to_account_id": newrow[6], "sensitive": False if newrow[7]==0 else True, "spoiler_text": newrow[8], "spoiler_clean_text": newrow[9], "visibility": newrow[10], 
            "language": newrow[11], "uri": newrow[12], "url": newrow[13], "replies_count": int(newrow[14]), "reblogs_count": int(newrow[15]), "favourites_count": newrow[16], 
            "edited_at": newrow[17], "content": newrow[18], "reblog": newrow[19], "rebloggedbyuser": eval(newrow[20]), "media_attachments": eval(newrow[21]), 
            "mentions": eval(newrow[22]), "tags": eval(newrow[23]), "emojis": eval(newrow[24]), "card": eval(newrow[25]), "poll": eval(newrow[26]), 
            "instance_name": newrow[27], "toottext": newrow[28], "muted": False if newrow[29]==0 else True, "reblogged": False if newrow[30]==0 else True, 
            "favourited": False if newrow[31]==0 else True}  
    
    return toot



def execute_create_sql(dbconnection, command):
    
    try:
        c = dbconnection.cursor()
        c.execute(command)
    except Error as e:
        print(e)


def execute_insert_sql(dbconnection, table, newrow):

    sql = ''' INSERT OR IGNORE INTO {} (globalID, id, accountglobalID, account, created_at, in_reply_to_id, in_reply_to_account_id, sensitive, spoiler_text, spoiler_clean_text, 
                            visibility, language, uri, url, replies_count, reblogs_count, favourites_count, edited_at, content, reblog, rebloggedbyuser,
                            media_attachments, mentions, tags, emojis, card, poll, instance_name, toottext, muted, reblogged, favourited)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''.format(table)
    cur = dbconnection.cursor()
    cur.execute(sql, newrow)
    dbconnection.commit()
    
    return cur.lastrowid


def execute_update_replies_sql(dbconnection, table, tootglobalID, inreply2id, inreply2accountid):
    
    # in reply to id, in reply to account id
    sql = ''' UPDATE {}
              SET in_reply_to_id = ?,
                  in_reply_to_account_id = ?
              WHERE globalID = ?'''.format(table)
    cur = dbconnection.cursor()
    cur.execute(sql, (inreply2id, inreply2accountid, tootglobalID))
    dbconnection.commit()
    print("Updated toot entry {}-{}-{}".format(inreply2id, inreply2accountid, tootglobalID))

def execute_update_reblogging_sql(dbconnection, table, tootglobalID, boosters):
    # add boosters.acct of boosters.jsonl as a list str
    
    sql = ''' UPDATE {}
              SET rebloggedbyuser = ?
              WHERE globalID = ?'''.format(table)
    cur = dbconnection.cursor()
    cur.execute(sql, (boosters, tootglobalID))
    dbconnection.commit()

def execute_update_reblogging_counts_sql(dbconnection, table, tootglobalID, replies_count, reblogs_count, favourites_count):
        
    sql = ''' UPDATE {}
              SET replies_count = ?, reblogs_count = ?, favourites_count = ?
              WHERE globalID = ?'''.format(table)
    cur = dbconnection.cursor()
    cur.execute(sql, (replies_count, reblogs_count, favourites_count, tootglobalID))
    dbconnection.commit()


def retrieve_toot_from_id_in_toots_list(tootid, tootsinteractionlist):

    for i in tootsinteractionlist:
        if i["id"] == tootid:
            return i
    return None

def execute_update_context_sql(dbconnection, table, headtoot, repliestoot, auth_dict, cutoff_date="2023-12-02"):
    # iterate over replies list, build unique ids from URIs and insert in DB
    updreplies = [headtoot]
    for replyidx in range(len(repliestoot)):
        reply = repliestoot[replyidx]  
        if "edited_at" in reply.keys() and reply["edited_at"] is not None:     
            if "Z" in reply["edited_at"]:
                reply["edited_at"] = reply["edited_at"][:-5]
        if "Z" in reply["created_at"]:
            reply["created_at"] = reply["created_at"][:-5]           
        # extract toot text content
        toot = BeautifulSoup(reply["content"], "html.parser")
        # add extra entry in toot with Mastodon instance name
        reply["instance_name"] = headtoot["instance_name"]
        # add extra entry in toot dictionary
        toottext = toot.get_text()
        reply["toottext"] = toottext
        acc = add_unique_account_id(reply["account"], reply["instance_name"])
        reply["account"] = acc
        if "Z" in reply["account"]["created_at"]:
            reply["account"]["created_at"] = reply["account"]["created_at"][:-5]     
        reply = add_unique_toot_id(reply, reply["instance_name"])
        reply["rebloggedbyuser"] = []
        if isinstance(reply["spoiler_text"], str) and len(reply["spoiler_text"]) > 0:
            tootspoiler = BeautifulSoup(reply["spoiler_text"], "html.parser")
            reply["spoiler_clean_text"] = tootspoiler.get_text()
        if isinstance(reply["account"]["note"], str) and len(reply["account"]["note"]) > 0:
            accountnote = BeautifulSoup(reply["account"]["note"], "html.parser")
            reply["account"]["account_note_text"] = accountnote.get_text()
        usertoot = reply
        newrow = build_db_row(usertoot) 
        execute_insert_sql(dbconnection, table, newrow)
        updreplies.append(reply)
    # update entries in the db with reply data
    for replyidx in range(len(updreplies)):
        reply = updreplies[replyidx]
        if reply["in_reply_to_id"] is None:
            continue
        parenttoot = retrieve_toot_from_id_in_toots_list(reply["in_reply_to_id"], updreplies)
        if parenttoot is None:
            parenttoot = get_toot_from_statusid(reply["in_reply_to_id"], headtoot["instance_name"], auth_dict=auth_dict)
            if parenttoot is None or (isinstance(parenttoot, list) and len(parenttoot) == 0):
                continue
            else:                        
                toot = BeautifulSoup(parenttoot["content"], "html.parser")
                parenttoot["instance_name"] = headtoot["instance_name"]
                toottext = toot.get_text()
                parenttoot["toottext"] = toottext
                acc = add_unique_account_id(parenttoot["account"], parenttoot["instance_name"])
                parenttoot["account"] = acc
                parenttoot = add_unique_toot_id(parenttoot, parenttoot["instance_name"])
                parenttoot["rebloggedbyuser"] = []
                if isinstance(parenttoot["spoiler_text"], str) and len(parenttoot["spoiler_text"]) > 0:
                    tootspoiler = BeautifulSoup(parenttoot["spoiler_text"], "html.parser")
                    parenttoot["spoiler_clean_text"] = tootspoiler.get_text()
                if isinstance(parenttoot["account"]["note"], str) and len(parenttoot["account"]["note"]) > 0:
                    accountnote = BeautifulSoup(parenttoot["account"]["note"], "html.parser")
                    parenttoot["account"]["account_note_text"] = accountnote.get_text()               
                parentrow = build_db_row(parenttoot)        
                if parentrow is not None:            
                    execute_insert_sql(dbconnection, "toots", parentrow)
                else:
                    print(parentrow)
        if "edited_at" in parenttoot.keys() and parenttoot["edited_at"] is not None:     
            if "Z" in parenttoot["edited_at"]:
                parenttoot["edited_at"] = parenttoot["edited_at"][:-5]
        if "Z" in parenttoot["created_at"]:
            parenttoot["created_at"] = parenttoot["created_at"][:-5]   
        if "Z" in parenttoot["account"]["created_at"]:
            parenttoot["account"]["created_at"] = parenttoot["account"]["created_at"][:-5]   
        if "edited_at" in parenttoot.keys() and parenttoot["edited_at"] is not None and parenttoot["edited_at"] != "":
            try:
                monthyear = pd.Timestamp(np.datetime64(parenttoot["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
            except:
                monthyear = pd.Timestamp(np.datetime64(parenttoot["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
        else:
            try:
                monthyear = pd.Timestamp(np.datetime64(parenttoot["created_at"])).tz_localize("CET").astimezone(pytz.utc)
            except:
                monthyear = pd.Timestamp(np.datetime64(parenttoot["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
        if monthyear < pd.Timestamp(cutoff_date).tz_localize("Europe/Paris").astimezone(pytz.utc) or monthyear > pd.Timestamp(datetime.today().strftime('%Y-%m-%d')).tz_localize("Europe/Paris").astimezone(pytz.utc):
            # do not collect it, in_reply_to_id field of reply remains unchanged
            continue
        parentglobalID = parenttoot["globalID"]
        parentaccountID = parenttoot["account"]["globalID"]
        execute_update_replies_sql(dbconnection, table, reply["globalID"], parentglobalID, parentaccountID)


def sqlite2jsonl(dbconnection, table, pathout="/tmp/"):
    
    try:
        sql = '''SELECT * FROM {}'''.format(table)
        c = dbconnection.cursor()
        c.execute(sql)
        for row in c:
            toot = db_row_to_json(row)
            if "edited_at" in toot.keys() and toot["edited_at"] is not None and toot["edited_at"] != "":
                try:
                    monthyear = pd.Timestamp(np.datetime64(toot["edited_at"])).tz_localize("CET").astimezone(pytz.utc)
                except:
                    monthyear = pd.Timestamp(np.datetime64(toot["edited_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
            else:
                try:
                    monthyear = pd.Timestamp(np.datetime64(toot["created_at"])).tz_localize("CET").astimezone(pytz.utc)
                except:
                    monthyear = pd.Timestamp(np.datetime64(toot["created_at"])).tz_localize("Europe/Paris").astimezone(pytz.utc)
            targetfolder = "{}/toots/{}/{}/".format(pathout, monthyear.year, monthyear.month)
            if not pathlib.Path(targetfolder).exists():
                pathlib.Path(targetfolder).mkdir(parents=True, exist_ok=True)
            with jsonlines.open("{}/toots.jsonl".format(targetfolder), "a") as jsonl_write:
                jsonl_write.write(toot)
    except Error as e:
        print(e)


