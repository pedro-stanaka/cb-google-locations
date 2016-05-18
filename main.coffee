

fs = require('fs')
JSONStream = require('JSONStream')
es = require('event-stream')
couchbase = require('couchbase')

argv = require('yargs')
    .demand 'f'
    .alias 'f', 'file'
    .check (argv, options)-> 
        try 
            fs.accessSync(argv.f, fs.F_OK)
            true
        catch e
            false
        
    .nargs 'f', 1
    .help 'h'
    .alias 'h', 'help'
    .argv


configurationFile = 'config.json';
configs = JSON.parse fs.readFileSync(configurationFile)
configs.debug = configs.debug || false



processLocation = (location, verbose, bucket) ->
    bucket.upsert('pedro-gl-'+location.timestampMs, location, (err, result) ->
        if err 
            console.log "There was a problem while inserting the document... ", err
            
        if verbose then console.log result.value
    )

fs.access(argv.f, fs.F_OK, (err)->
    if !err
        cluster = new couchbase.Cluster('couchbase://' + configs.host)
        bucket = cluster.openBucket(configs.bucket)
        fileStream = fs.createReadStream(argv.f, {encoding: 'utf8'})
        fileStream.pipe(JSONStream.parse('locations.*')).pipe(es.through((data)->
            console.log 'location of file::'
            console.log data
            processLocation data, configs.debug, bucket # use same connection to insert all the nodes
        , ->
            console.log 'stream reading ended'
            this.emit 'end'
        ))
        bucket.disconnect()
)




