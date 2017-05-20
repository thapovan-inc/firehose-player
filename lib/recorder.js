const Firehose = require('@webng/firehose');
const check_opts = require('./opts');
const fs = require('fs');
const zlib = require('zlib');
const msgpack_lite = require('msgpack-lite');

const required_opts_for_constructor = ['topic', 'zookeeper', 'filepath'];

class Recorder {

    constructor(options) {
        let args = check_opts(options, required_opts_for_constructor);
        this.topic = args.topic;
        this.broker = args.broker;
        this.filepath = args.filepath;
        this.firehose = new Firehose({
            config: {
                host: `${args.zookeeper}:2181`,
                zk: undefined,   // put client zk settings if you need them (see Client) 
                batch: undefined, // put client batch settings if you need them (see Client) 
                ssl: false, // optional (defaults to false) or tls options hash 
                groupId: 'Firehose-Recorder',
                sessionTimeout: 15000,
                // An array of partition assignment protocols ordered by preference. 
                // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol) 
                protocol: ['roundrobin'],

                // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved) 
                // equivalent to Java client's auto.offset.reset 
                fromOffset: 'latest', // default 

                // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset 
                outOfRangeOffset: 'latest', // default 
                migrateHLC: false,    // for details please see Migration section below 
                migrateRolling: true
            },
            topics: [this.topic],
            decodeJSON: true
        });

        this._transform = (msg) => {
            delete msg.login_key;
            return msg
        };

        this._appendToFile = (msg) => {
            this.outputStream.write(this._transform(msg));
        };
    }

    async start() {
        try {
            let fileStream = fs.createWriteStream(this.filepath);
            this.outputStream = msgpack_lite.createEncodeStream();
            this.gzip = zlib.createGzip();
            this.outputStream.pipe(this.gzip).pipe(fileStream);
            this.firehose.addListener('message', this._appendToFile);
            await this.firehose.connect();
            console.info(`Recording from topic ${this.topic} into file ${this.filepath}`);
        } catch (e) {
            throw e;
        }
    }

    async stop() {
        await this.firehose.disconnect();
        this.firehose.removeListener('message',this._appendToFile);
        this.gzip.flush();
        this.outputStream.end();
    }
}

module.exports = Recorder;