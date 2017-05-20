const check_opts = require('./opts');
const fs = require('fs');
const zlib = require('zlib');
const msgpack_lite = require('msgpack-lite');

const required_opts_for_constructor = ['file'];

class Player {

    constructor(opts) {
        opts = check_opts(opts, required_opts_for_constructor);
        this.file = opts.file;
        this.records = opts.records || -1;
        if (opts.output) {
            this.out_mode = 'url';
            this.out_url = opts.output;
            this._dataHandler = () => {
                console.log('URL posting not yet implemented');
            }
        } else {
            this.out_mode = 'stdout';
            this._dataHandler = async (data) => {
                console.log(data);
            }
        }
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
        this.msgpack_decode_stream.on("data", this._dataHandler);
        this.gunzip = zlib.createGunzip();
        this.fileStream.pipe(this.gunzip).pipe(this.msgpack_decode_stream);
    }

    async stop() {
        this.fileStream.close();
    }
}

module.exports = Player;