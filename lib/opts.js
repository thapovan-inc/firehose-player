function verifyOpts(args, required_args) {
    if(required_args) {
        if(required_args instanceof Array) {
            for (var index = required_args.length-1; index > -1; index--){
                if(!(required_args[index] in args)) {
                    throw new Error(`ARG_MISSING: ${required_args[index]} is required`);
                }
            }
        }
    }
    return args;
}

module.exports = verifyOpts;