

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

count = 0
configurationFile = 'config.json';
configs = JSON.parse fs.readFileSync(configurationFile)
configs.debug = configs.debug || false

console.log configs

processLocation = (location, verbose, bucket, count = 0) ->
    bucket.upsert('pedro-gl-'+location.timestampMs, location, (err, result) ->
        if err 
            console.log "There was a problem while inserting the document (#docs : #{count}..."
            throw err
            
        if verbose then console.log result.value
    )

fs.access(argv.f, fs.F_OK, (err)->
    if !err
        cluster = new couchbase.Cluster('couchbase://' + configs.host)
        bucket = cluster.openBucket(configs.bucket)
        fileStream = fs.createReadStream(argv.f, {encoding: 'utf8'})
        console.log "Began processing data..."
        fileStream.pipe(JSONStream.parse('locations.*')).pipe(es.through((data)->
            console.log 'location of file::' if configs.debug
            console.log data if configs.debug
            processLocation data, configs.debug, bucket, count # use same connection to insert all the nodes
            count++
        , ->
            console.log 'stream reading ended'
            this.emit 'end'
        ))
        fileStream.on('end', (err) ->
            if err
                throw err
            bucket.disconnect()
            console.log "Ending streaming of file..."
        )
)




