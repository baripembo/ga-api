/*  Google analytics API acees with concurrency limiting and retry + caching built in.  */
require('dotenv').load();
var googleapis = require('googleapis'),
    crypto = require('crypto'),
    OAuth2 = googleapis.auth.OAuth2,
    _ = require('lodash'),
    fs = require('fs'),
    compactObject = function(o) {
        var clone = _.clone(o);
        _.each(clone, function(v, k) {
            if(!v) {
                delete clone[k];
            }
        });
        return clone;
    },

    //  For caching - generates a file name based on parameters
    getCacheFileName = function(args){
        //  Remove auth info
        var fnArgs = _.clone(args),
            shasum = crypto.createHash('sha1'),
            fileName;
        delete fnArgs.auth;
        fileName = JSON.stringify(fnArgs);
        shasum.update(fileName);
        return cacheDir + shasum.digest('hex');
    },

    //  Executes a ga query, or returns cache if available
    gaExecuteQuery = function(args, callback, cache, retryCount){
        retryCount = retryCount || 0;
        concurrentUp();

        //console.log(args);

        var req = {
            reportRequests: [{
                viewId: args.viewId,
                dateRanges: [{
                    startDate: args.startDate,
                    endDate: args.endDate,
                }],
                metrics: [{
                    expression: args.metrics,
                }],
                orderBys:[
                  {
                    "fieldName": args.metrics, "sortOrder": "DESCENDING"
                  }],
                pageSize: args.pageSize,
                samplingLevel: "LARGE",
                dimensions: [{"name": args.dimensions}],
                dimensionFilterClauses: args.filters
            }]
        };

        googleapis.analyticsreporting('v4').reports.batchGet({
            quotaUser: args.quotaID,
            headers: {
                "Content-Type": "application/json"
            },
            auth: args.auth,
            resource: req 
        }, function(err, result){
            concurrentDown();
            if(err) {
                //  403 error: wait 1 sec...
                if(err.code === 403) {
                    setTimeout(function(){
                        if(retryCount < concurrentMaxRetry) {
                            retryCount += 1;
                            gaExecuteQuery.apply(this, [args, callback, cache, retryCount]);
                        } else {
                            //  Give up
                            return callback(err);
                        }
                    }, concurrentDelay);
                    return;
                } else {
                    return callback(err);
                }
            }

            //use only params to generate hash filename
            var gaHash = compactObject({
                'startDate': args.startDate,
                'endDate': args.endDate,
                'createdDate': args.createdDate,
                'metrics': args.metrics,
                'filters': args.filters,
                'dimensions': args.dimensions,
                'pageSize': args.pageSize
            });

            //  Cache the response
            var fileName = getCacheFileName(gaHash);
            fs.writeFileSync(fileName, JSON.stringify(result), {encoding: "utf8"});

            callback(null, result);
        });
    },
    //  Concurrent limiting, GA default is 10 concurrent connections
    concurrentLimit = 10,
    concurrentDelay = 1000,
    concurrentMaxRetry = 3,
    concurrentQueries = 0,
    concurrentUp = function(){
        concurrentQueries += 1;
    },
    concurrentDown = function(){
        concurrentQueries -= 1;
        //  Execute any queries
        if(queryQueue.length > 0) {
            gaExecuteQuery.apply(this, queryQueue.shift());
        }
    },
    queryQueue = [],
    gaQuery = function() {
        if(concurrentQueries < concurrentLimit) {
            gaExecuteQuery.apply(this, arguments);
        } else {
            var args = Array.prototype.slice.call(arguments);
            queryQueue.push(args);
        }
    },

    cache = 0;
    cacheDir = process.env.CACHEDIR || '/cached/';
    tokenDir = cacheDir + process.env.TOKENDIR || '/tokens/';

module.exports = function(args, callback, settings){
    if(settings) {
        cache = typeof settings.cache !== 'undefined'? settings.cache: cache;
        cacheDir = typeof settings.cacheDir !== 'undefined'? settings.cacheDir: cacheDir;
        concurrentLimit = typeof settings.concurrentLimit !== 'undefined'? settings.concurrentLimit: concurrentLimit;
        concurrentDelay = typeof settings.concurrentDelay !== 'undefined'? settings.concurrentDelay: concurrentDelay;
        concurrentMaxRetry = typeof settings.concurrentMaxRetry !== 'undefined'? settings.concurrentMaxRetry: concurrentMaxRetry;
    }

    var jwt = new googleapis.auth.JWT(
            args.email,
            args.key,
            null,
            ['https://www.googleapis.com/auth/analytics.readonly']
        ),
        oauth2Client = new OAuth2(
            args.clientId,
            null,
            'postmessage'
        ),
        sessionFile = tokenDir + "ga-runner-" + args.quotaID,
        authorize = function(authCallback) {
            fs.readFile(sessionFile, {encoding: "utf8"}, function(err, result) {
                //  If the file was read successfully
                if(!err) {
                    //  If we cannot parse the file
                    try {
                        var json = JSON.parse(result);
                        //  If session is still valid
                        if(new Date(json.expiry_date) > Date.now()) {
                            return authCallback(null, json);
                        }
                    } catch(e) {}
                }
                concurrentUp();
                jwt.authorize(function(err, result){
                    concurrentDown();
                    fs.writeFile(sessionFile, JSON.stringify(result));
                    authCallback.apply(this, arguments);
                });
            });
        };

    //  Check if we have required values
    _.each(['startDate', 'endDate', 'metrics', 'dimensions', 'filters', 'pageSize'], function(value, key){
        if(!args[value]) {
            callback("Missing argument for " + value);
            return false;
        }
    })

    //  Make sure we are authorized, then make request
    authorize(function(err, result) {
        if(err) {
            callback(err);
        } else {
            oauth2Client.setCredentials({
                access_token: result.access_token,
                refresh_token: result.refresh_token
            });

            var gaArgs = compactObject({
                'quotaID': args.quotaID,
                'viewId': args.viewId,
                'startDate': args.startDate,
                'endDate': args.endDate,
                'createdDate': args.createdDate,
                'metrics': args.metrics,
                'filters': args.filters,
                'dimensions': args.dimensions,
                'pageSize': args.pageSize,
                'auth': oauth2Client
            });

            //  Use only params to generate hash filename
            var gaHash = compactObject({
                'startDate': args.startDate,
                'endDate': args.endDate,
                'createdDate': args.createdDate,
                'metrics': args.metrics,
                'filters': args.filters,
                'dimensions': args.dimensions,
                'pageSize': args.pageSize
            });

            //  Get cached response or create one
            var fileName = getCacheFileName(gaHash),
                stats;

            fs.readFile(fileName, "utf8", function(err, data){
                if(!err) {
                    stats = fs.statSync(fileName);
                    var now = new Date();
                    var currMth = now.getMonth();
                    var created = new Date(args.createdDate+'-02');
                    var visited = new Date(args.endDate);
                    var modified = new Date(stats.mtime);
                    var isCurrent = false;

                    //  Determine currency of cached file
                    if (modified.getMonth()==currMth && modified.getDate()==now.getDate()){
                        isCurrent = true;
                    }
                    else{
                        isCurrent = (visited.getMonth()==currMth || created.getMonth()==currMth) ? false : true;
                    }

                    //  Return cached file if current, otherwise update it
                    if(stats.isFile() && isCurrent) {
                        console.log('get cached file');
                        return callback(null, JSON.parse(data));
                    }
                }
                console.log('create new cached file');
                gaQuery(gaArgs, callback, cache);
            });
        }
    });
};
