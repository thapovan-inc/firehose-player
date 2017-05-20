const Recorder = require('./lib/recorder');
const Player = require('./lib/player');
const verifyOpts = require('./lib/opts');
const path = require('path');
const cli_args = require('minimist');

const usage_string = `
Arguments:
    --mode              record|play
            
    [Record]
        --topic             Topic to be recorded
        --zookeeper-host    IP address of the zookeeper host
        --file-dir          The directory to which the stream must be saved
    
    [Play]
        --file              The EStream file to playback
        --records           No.of records to play (optional). Defaults to all records till EOF.
        --output            Optional. When specified, it must be an URL. Defaults to stdout.
`;

const options = {
    string: ['mode', 'topic', 'zookeeper-host', 'file-dir'],
    default: {
        mode: 'record'
    }
};

const args = cli_args(process.argv.slice(2), options);

if (args.mode === 'record') {

    try{
    verifyOpts(args,['topic','zookeeper-host','file-dir']);
    
    const recorder = new Recorder({
        topic: args.topic,
        zookeeper: args['zookeeper-host'],
        filepath: path.join(args['file-dir'], `${args.topic}-${Date.now()}.estream`)
    });

    recorder.start();
    console.log("Started recorder");
    process.on('SIGINT', async ()=> {
        await recorder.stop();
        console.info('Stopped recorder');
        process.exit(0);
    });

    } catch(e) {
        if(e.message.indexOf('ARG_MISSING') === 0) {
            console.error(e.message);
            console.info(usage_string);
        } else {
            console.error(e);
        }
    }

} else if(args.mode === 'play') {
    verifyOpts(args,['file']);
    const player = new Player(args);
    
    player.start();
    process.on('SIGINT', async ()=> {
        await player.stop();
        process.exit(0);
    });
} else {
    console.error(`Unknown mode ${args.mode}`)
    console.info(usage_string)
}