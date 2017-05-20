Firehose Player
================

CLI app to record and replay firehose topics/streams. Uses msgpack and gzip to achieve lower disk usage compared to raw JSON.

## Arguments:
```
--mode              record|play
        
[Record]
    --topic             Topic to be recorded
    --zookeeper-host    IP address of the zookeeper host
    --file-dir          The directory to which the stream must be saved

[Play]
    --file              The EStream file to playback
    --records           No.of records to play (optional). Defaults to all records till EOF.
    --output            Optional. When specified, it must be an URL. Defaults to stdout.
```

## Usage example:

`node main.js --mode record --topic prod-firehose-userinfohighvalue --zookeeper-host 127.0.0.1 --file-dir ./data`