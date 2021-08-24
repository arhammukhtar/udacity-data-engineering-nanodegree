## Sparkify Data Modeling Project 

The database created in this project will help the startup, Sparkify, derive various interesting analysis from the data. It can help them improve their services as well as their customer experience.

### Schema of the database

We used a star schema while creating our database because it is simple and easy to implement when you have just a single fact table and multiple dimension tables.
Our fact table is called songplays. It contains 1 primary key: songplay_id as well as 3 foreign keys: user_id, song_id, artist_id.
While we have 4 dimension tables users, songs, artists, and time. They all contain 1 primary key each.


### List and purpose of the submitted files.
- sql_queries.py: This file contains all the sql queries that will be used throughout the project at one place.
- create_tables.py: This script allows us to drop existing tables and create new ones in order to build or refresh our database.
- etl.py: This file contains our major extract-transform-load process. The main function is to read all the log/song json files given in a mentioned directory and later take out all required data from them to add to the database.
- readme.md: Documentation for the project.



### How to run the files:

1. First run create_tables.py to build the database.
2. Run etl.py in order to populate the database.
3. Run test.ipynb in order to query results from the database.

### Example Queries:

- At what time of the day, maximum number of songplays occur. This will allow Sparkify to manage their load by firing up more nodes during peak times.

- Which artists are more popular with which audiences. This can improve the recommendation system.