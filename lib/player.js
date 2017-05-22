const check_opts = require('./opts');
const fs = require('fs');
const zlib = require('zlib');
const msgpack_lite = require('msgpack-lite');
const request = require('request');
const Transform = require('stream').Transform;
const PassThrough = require('stream').PassThrough;

const required_opts_for_constructor = ['file'];

class StreamRateLimiter extends Transform {
    
    constructor(opts) {
        super({objectMode: true});
        this.buffer = [];

        if (opts.interval && (opts.interval - 0 ) && opts.interval>0) {
            this.interval = opts.interval
        } else {
            this.interval = 1000;
        }

        this.bufferSize = (opts.bufferSize && (opts.bufferSize - 0 ) && opts.bufferSize > 0) ? opts.bufferSize : 10;

        this.currentTimer = -1;
    }

    _transform (data, encoding, callback) {
        this.buffer.push(data);
        if(this.buffer.length == this.bufferSize) {
            this.currentTimer = setTimeout(()=>{
                this.push(this.buffer);
                this.currentTimer = -1;
                this.buffer = [];
                callback();
            },this.interval);
        } else {
            callback();
        }
        return;
    }

    _flush (callback) {
        if(this.currentTimer != -1) {
            clearTimeout(this.currentTimer);
        }
        this.push(buffer);
        this.buffer = [];
        callback();
    }
}

class Player {

    constructor(opts) {
        opts = check_opts(opts, required_opts_for_constructor);
        this.file = opts.file;
        this.records = opts.records || -1;
        if (opts.output) {
            this.out_mode = 'url';
            this.out_url = opts.output;
            this._dataHandler = (data) => {
                process.nextTick(() => request.post(this.out_url,{json: data},(err,resp)=>{
                    if(err) {
                        console.log(err);
                    }
                }));
            }
        } else {
            this.out_mode = 'stdout';
            this._dataHandler = async (data) => {
                console.log(data);
            }
        }

        this.limit = (opts.limit && opts.limit.interval)? opts.limit : null;
    }

    async start() {
        this.fileStream = fs.createReadStream(this.file);
        this.msgpack_decode_stream = msgpack_lite.createDecodeStream();
        if (this.records > 0) {
            this.msgpack_decode_stream.prependListener('data', async () => {
                this.records--;
                if (this.records == 0) {
                    this.msgpack_decode_stream.once('data', () => {
                        this.msgpack_decode_stream.removeAllListeners();
                        this.stop();
                    })
                }
            });
        }
        this.gunzip = zlib.createGunzip();
        this.rateLimiter = null;
        if(this.limit != null) {
            this.rateLimiter = new StreamRateLimiter(this.limit);
            this.rateLimiter.on("data", this._dataHandler);
        } else {
            this.msgpack_decode_stream.on("data", this._dataHandler);
        }
        let tubing = this.fileStream.pipe(this.gunzip).pipe(this.msgpack_decode_stream);
        if(this.rateLimiter != null) {
            tubing.pipe(this.rateLimiter);
        }
    }

    async stop() {
        this.fileStream.close();
    }
}

module.exports = Player;