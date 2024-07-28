# PyMstdnCollect
A Python package for social science research data collection with Mastodon's public API

[![DOI](https://zenodo.org/badge/349102514.svg)](10.5281/zenodo.13119144)
[![Python 3.8+](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)
![GitHub Actions Workflow Status](https://github.com/ichalkiad/PyMstdnCollect/actions/workflows/tests.yml/badge.svg)


<tt>PyMstdnCollect</tt> is a Python wrapper for Mastodon's public API. Mastodon is a novel online social medium which has been gaining in popularity over alternatives (e.g. X, formerly known as Twitter) both for users and for social and political science researchers. Mastodon has attracted the attention of the latter, particularly after its more well-known alternatives do not offer open access APIs anymore. Contrary to existing low-level Python wrappers of Mastodon's API, <tt>PyMstdnCollect</tt> is built as a higher-level software that goes beyond being an API wrapper on top of which further code needs to be developed, but rather it is a readily usable tool for the social science researcher who needs network data (interactions and text) but does not have the time or expertise to develop the data collection process from scratch. The software package aims to facilitate research with the Mastodon community and help towards safeguarding and preserving healthier interaction between user communities on the platform.


**Disclaimer**: Any data collected with <tt>PyMstdnCollect</tt> must be used according to the rules of each Mastodon instance they originate from and the ethics of the broader Mastodon and Fediverse communities. It remains the sole responsibility of the user of the present software to ensure that their research and use of data is fully compliant with Mastodon instance rules and user privacy choices. We refer the library users to recent research that provides recommendations on how to best approach research with Mastodon [Roscam Abbing & Gehl 2024](https://doi.org/10.1016/j.patter.2023.100914) in a way that is beneficial to both the researchers and the Mastodon user community.

The package provides code for collecting network data using the platform's public API (currently v1), storing them in a lightweight database and exporting them to JSON format.

## How to install

From inside the repository folder run

`pip install -e .`

in the command line. 

## Test the installation

The requirements for installing and running the package and testing suite are included in auxiliary files <tt>pyproject.toml</tt>, <tt>requirements.txt</tt>, <tt>setup.cfg</tt> and <tt>tox.ini</tt>. The tests run each of the main functions separately and they can be performed by running the command:

`pytest`

from the root directory (full tests last ~10 minutes). Optionally, the flag `-s` may be used to print test outputs (quite long) to the standard output. The output of the command will be a coverage report in the <tt>htmlcov</tt> directory. To view the report the user may execute the following command:

``cd htmlcov python -m http.server``

and visit the localhost link in a browser. For testing in a clean environment for several python versions (3.8-3.10), the user may employ Tox by running 
`tox`
in the root directory. Note that this takes significantly longer to run, so it is best to perform it as a last check. Tox is a more general testing tool that allows the user (and the developer) to check that the package builds and installs correctly under different environments (e.g. Python implementations, versions or installation dependencies), to run tests in each of the environments with the test tool of choice, and also to act as a frontend to continuous integration servers. Whenever the code is pushed to the remote repository, the test suite is automatically run using GitHub Actions. The output of the automated tox testing report may be found in the GitHub Actions tab.

## Example use cases

### Collect data from the past 24h

```
repo_dir = "/home/ubuntu/mstdncollect/"
with open("{}/authorisations/auth_dict.json".format(repo_dir), "r") as f:
    auth_dict = json.load(f)    

upperend = datetime.now(timezone.utc) 
max_id_snowflake = datetime2snowflake(upperend)
timestamp = upperend - timedelta(days=1)
min_id_snowflake = datetime2snowflake(timestamp)    

DIR_out = "/tmp/output/"   
pathlib.Path(DIR_out).mkdir(parents=True, exist_ok=True)
pathlib.Path("{}/logging/".format(DIR_out)).mkdir(parents=True, exist_ok=True)    
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

apibaseurl = "https://mastodon.social/api/v1/timelines/public".format(server)        
res = collect_timeline_apidirect(dbconnection=dbconnection, url=apibaseurl, max_id=max_id_snowflake, since_id=min_id_snowflake, 
                                         savedir=DIR_out, instance_name=server, auth_dict=auth_dict, cutoff_date=timestamp.strftime("%Y-%m-%d"))                
logging.info("Finished.")
logging.FileHandler("{}/logging/logging_{}.txt".format(DIR_out, datetime.now().astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S"))).close()    
```

### Collect toots from the last two weeks that contain a given set of hashtags

```
topics = ["climatechange"]
upperend = datetime.now(timezone.utc) 
max_id_snowflake = datetime2snowflake(upperend)
timestamp = upperend - timedelta(days=14)
min_id_snowflake = datetime2snowflake(timestamp)    

dbconn = connectTo_weekly_toots_db(database)
hashtag_lists_dir = "{}/collection_hashtags/".format(repo_dir)
tree = False    
climate_hashtags = pd.read_csv("{}/climate_hashtags.csv".format(hashtag_lists_dir), header=None)
climate_hashtags_list = climate_hashtags.values.flatten().tolist()
hashtag_list_all = [climate_hashtags_list]
hashtag_list_names = topics
allcollectedhashtags = []
for hashtaglistidx in range(1):
    hashtaglist = hashtag_list_all[hashtaglistidx]
    name = hashtag_list_names[hashtaglistidx]
    for hashtag in hashtaglist:
        apibaseurl = "https://mastodon.social/api/v1/timelines/tag/{}".format(server, hashtag)
        allcollectedhashtags = collect_timeline_hashtag_apidirect(hashtag=hashtag, url=apibaseurl, local=False, remote=False, only_media=False,
                            max_id=max_id_snowflake, since_id=min_id_snowflake, min_id=None, limit=40, 
                            keywords=[], textprocessor=None, savedir="/tmp/", 
                            instance_name=server, allcollectedhashtags=allcollectedhashtags, print_tree=tree, dbconn=dbconn, auth_dict=auth_dict)                
```


