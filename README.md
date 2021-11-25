# TwitchIRC_tracker


Requires python >= 3.7 and PostgreSQL >= 11 (may work for previous version but not tested)

## Usage

You have to create a Twitch client ID and client secret [here](https://dev.twitch.tv/dashboard/apps/create).
 
 * Install pip requirements:
```
pip3 install -r requirements.txt
```
 
* Create a database, no need to create tables and columns (done by the app).
```
psql -U postgres -c "CREATE DATABASE twitchirc_tracker"
```

* Then, you can run the app with your config file completed:
```
python3 src/main.py -c config1.cfg
```
