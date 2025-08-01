# PODSCRAPE
Download and transcribe podcasts

* podcasts save to sftp server
* metadata saves to psql db
* db can be connected to using ssh tunnel if USE_SSH_TUNNEL=1 in env

## Important Files
* podscrape.py - cli app for most common usage
* test_connections.py - test db & sftp connections
* db_client.py has a setup func for a postgres db (u have to create the db first)
* download_from_db.py has some searching/saving features, but will need to eventually be expanded
* transcribe.py - transcribe episodes. should eventually just be added into podscrape.py
to include searching transcripts (expand db_client as well to support) as well as more analysis
on the stuff the filtered data

## Transcription Model Info
estimates for transcribing 39k podcasts:
* openai_whisper_base          : 8471.9 s for 20 eps →  est. 4612.4 h for all
* faster_whisper_base          : 2458.0 s for 20 eps →  est. 1338.2 h for all
* faster_whisper_tiny          : 1928.3 s for 20 eps →  est. 1049.8 h for all

