import requests
import pandas as pd
from urllib.request import Request, urlopen
import json
from sqlalchemy import create_engine
import psycopg2

database_url="XXXXXXXXXXXXXXXXXXXX",
database_port= XXXX,
database_name= "XXXXXXXXXXXXXXXXXXXX",
database_user= "XXXXXXXXXXXXXXXXXXXX",
database_password= "XXXXXXXXXXXXXXXXXXXX"

#Create db connection            
sink_db_engine = create_engine('postgresql+psycopg2://database_user:database_password@database_url:database_port/database_name
#Get existing posts and comments ids to be used to filter dfs returned from API
with sink_db_engine.begin() as conn: 
    existing_post_ids = conn.exec_driver_sql("SELECT id FROM posts order by id").all()
    existing_post_ids= list(existing_post_ids)
    existing_post_ids=[i[0] for i in existing_post_ids]

    existing_comments_ids = conn.exec_driver_sql("SELECT id FROM post_comments order by id").all()
    existing_comments_ids= list(existing_comments_ids)
    existing_comments_ids=[i[0] for i in existing_comments_ids]


#Get posts data and convert it to a dataframe
posts_request=Request("https://jsonplaceholder.typicode.com/posts")
posts_response = urlopen(posts_request)
posts_elevations = posts_response.read()
posts_data = json.loads(posts_elevations)
posts_df = pd.json_normalize(posts_data)

#check columns names and order to match db table
print(posts_df.info())

#Change user_id column name to be mapped correctly on db side
posts_df.rename(columns={"userId": "user_id"},inplace=True)

#Get comments data and convert it to a dataframe
comments_request=Request("https://jsonplaceholder.typicode.com/comments")
comments_response = urlopen(comments_request)
comments_elevations = comments_response.read()
comments_data = json.loads(comments_elevations)
comments_df = pd.json_normalize(comments_data)

#check columns names and order to match db table
print(comments_df.info())

#Change post_id column name to be mapped correctly on db side
comments_df.rename(columns={"postId": "post_id"},inplace=True)


#Filter out posts and comments that already exists in the sink db out of the API returned data
new_posts_df = posts_df[~posts_df.id.isin(existing_post_ids)]
new_comments_df = comments_df[~comments_df.id.isin(existing_comments_ids)]


#write posts and comments df into the sink db
new_posts_df.to_sql('posts', con=sink_db_engine, if_exists='append', index=False)
new_comments_df.to_sql('post_comments', con=sink_db_engine, if_exists='append', index=False)
