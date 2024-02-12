/**
 * Module Dependencies
 */
var Q = require('q')
  , _ = require('lodash')
  , jsforce = require('jsforce')
  , moment = require('moment')
  , captains = require('captains-log')
  , log = captains()
  , Errors = require('waterline-errors').adapter


module.exports = (function () {

  // Keep track of all the connections used by the app
  var connections = {}

  var adapter = {
    // to track schema internally
    syncable: false,
    defaults: {
      maxConnectionAge: {unit: 'minutes', val: 10}
    },
    adapterApiVersion: 1,

    // expose connections/datastores as it will used by the waterline orm version ^0.15.0 to validate datastore connection.
    datastores: connections,

    registerDatastore: function(connection, collections, cb) {
      if(!connection.identity) return cb(Errors.IdentityMissing)
      if(connections[connection.identity]) return cb(Errors.IdentityDuplicate)

      // Initialize the connection details
      connections[connection.identity] = {
        config: connection,
        collections: collections,
        connection: {},
        // Set a connection state of expired so that a fresh connection is
        // spawned during connection registration.
        expiresOn: 0
      }

      // Spawn a new Salesforce connection before completing registration. The
      // connection is cached during generation so we can just discard the
      // one that is returned by the promise.
      if (connection.disableSFDCConnectionHook) {
        cb()
      } else {
        spawnConnection(connection.identity)
          .then(function() {cb()})
          .fail(cb)
          .done();
      }
    },

    count: function(connectionName, options, cb) {
      log.verbose('counting ' + options.using)
      var whereClause = options.criteria.where
      
      whereClause = parseClause(whereClause)

      spawnConnection(connectionName)
        .then(function(connection) {
          connection.sobject(options.using)
            .count(function(){
              cb(arguments[0], arguments[1])
            }
          ).where(whereClause)
        })
        .fail(cb)
        .done();
    },

    find: function(connectionName, options, cb) {
      log.verbose('finding ' + options.using)

      var whereClause = options.criteria.where

      // Shim in required query params and parse any logical operators.
      whereClause = parseClause(whereClause);

      spawnConnection(connectionName)
        .then(function(connection) {
          var results = []
          connection.sobject(options.using)
            .select(options.criteria.select)
            .where(whereClause)
            .sort(parseSortClause(options.criteria.sort))
            .limit(options.criteria.limit === Number.MAX_SAFE_INTEGER ? undefined : options.criteria.limit)
            .skip(options.criteria.skip)
            .on('record', function(record) {
              results.push(record)
            })
            .on('end', function(query) {
              sails.log.silly('Total in database: ' + (query && query.totalSize))
              sails.log.silly('Total fetched: ' + (query && query.totalFetched))
              cb(null, results)
            })
            .on('error', function(err) {
              log.error(err.toString())
              cb(err)
            })
            // TODO: Move these to a default config that can be overridden by
            // the connection config within the app.
            .execute({autoFetch: true, maxFetch: 20000})
        })
        .fail(cb)
        .done();
    },

    create: function(connectionName, options, cb) {
      log.verbose('creating ' + options.using)

      spawnConnection(connectionName)
        // Execute logic
        .then(function (connection) {

          connection.sobject(options.using).create(options.newRecord, function(err, result) {
            if (err) {return cb(err)}
            // Salesforce seems to embed some errors within the result. I'm not
            // sure if jsforce processes this and returns it via the `err` arg
            // within the callback or not. Until I know for sure, I'll also
            // verify these fields separately.
            if (result.success !== true) return cb(err)
            if (result.errors.length !== 0) return cb(err)

            // Errors should be accurately represented by the app's response
            // code so drop these error related fields before returning.
            return cb(null, _.omit(result, 'success', 'errors'))
          })
        })
        .fail(cb)
        .done();
    },

    // In sails v1 adapter create() does not take array as parameter,
    // It is replaced with createEach instead
    createEach: function(connectionName, options, cb) {
      log.verbose('creating each ' + options.using)

      spawnConnection(connectionName)
        // Execute logic
        .then(function (connection) {

          connection.sobject(options.using).create(options.newRecords, function(err, results) {
            if (err) {return cb(err)}
            
            var processedResult = [];
            results.forEach(function (result) {
              delete result.success
              delete result.errors
              processedResult.push(result);
            })

            return cb(null, processedResult)
          })
        })
        .fail(cb)
        .done();
    },

    update: function(connectionName, options, cb) {
      log.verbose('updating ' + options.using)

      spawnConnection(connectionName)
        // Execute logic
        .then(function (connection) {
          connection.sobject(options.using)
            .update(_.extend(options.criteria.where, options.valuesToSet), function (err, ret) {
              if (err) {return cb(err)}
              
              if(options.meta.fetch) {
                var result = _.isArray(ret) ? ret : [ret]
                return cb(err, result);
              }
              return cb(err);
            })
        })
        .fail(cb)
        .done();
    },

    native: function(connectionName, collectionName, cb) {
      log.verbose('Spawning native connection for ' + collectionName)

      spawnConnection(connectionName)
        .then(function(connection) {
          cb(null, connection.sobject(collectionName))
        })
        .fail(cb)
        .done()
    },

    // Executes SOQL Query
    query: function(connectionName, collectionName, query, cb) {
      spawnConnection(connectionName)
        .then(function(connection) {
          connection.query(query, cb);
        })
        .fail(cb)
        .done();
    },

    // Return an Apex REST connection.
    apex: function(connectionName, cb) {
      spawnConnection(connectionName)
        .then(function(connection) {
          cb(null, connection.apex);
        })
        .fail(cb)
        .done();
    },

    // TODO: Implement teardown process.
    teardown: function(connectionName, cb) { cb() },
    // TODO: Implement `Model.define()` functionality.
    define: function(connectionName, collectionName, definition, cb) { cb() },
    // TODO: Implement `Model.describe()` functionality.
    describe: function(connectionName, collectionName, cb) { cb() },
    // TODO: Implement `Model.drop` functionality.
    drop: function(connectionName, collectionName, relations, cb) { cb() },
    // TODO: Implement `Model.drop` functionality.
    // The destroy method accepts ID and deletes a single record.
    destroy: function(connectionName, options, cb) {
      log.verbose('deleting ' + options.using)
      spawnConnection(connectionName)
        .then(function (connection) {
          // When destroy recieves an array of Id's
          if(_.isObject(options.criteria.where.Id)) {
            options.criteria.where.Id = options.criteria.where.Id.in || ''
          }
          connection.sobject(options.using).destroy(options.criteria.where.Id, function(err, ret) {
            if(err) {
              return cb(err)
            }
            if(options.meta.fetch) {
              var result = _.isArray(ret) ? ret : [ret]
              return cb(err, result);
            }
            return cb(err);
          });
        })
        .fail(cb)
        .done();
    },
    ///////////////////////////////////////////////////////////////////////////
    // Optional Overrides :: Methods defined here can override built in dynamic
    //                       finders such as `Model.findOrCreate`.

    ///////////////////////////////////////////////////////////////////////////
    // Custom Methods :: Methods defined here will be available on all models
    //                   which are hooked up to this adapter.

  }

  /////////////////////////////////////////////////////////////////////////////
  /// Private Methods

  // Wrap a function in the logic necessary to provision a connection
  // (grab from the pool or create a client)
  function spawnConnection(connectionName) {
    return Q.promise(function (resolve, reject) {
      var connectionObject = connections[connectionName]
        , expiresOn = connectionObject.expiresOn
        , config = connectionObject.config
        , connection = connectionObject.connection

      // Ensure that a connection object exists for this connection.
      if(!connectionObject) return reject(Errors.InvalidConnection)

      // Check if there is an existing connection in the cache that has not yet
      // expired. If there is, then return this active session instead of
      // spawning a new one.
      if(connection && moment().isBefore(expiresOn)) {
        log.silly('Using cached connection...')
        return resolve(connectionObject.connection)
      }

      // Otherwise, spawn a fresh connection.
      connection = new jsforce.Connection(config.connectionParams)

      // If the server instance using this adapter wants to run locally then
      // just return the connection without connecting to Salesforce. This
      // setting can be useful to avoid a dependency on an outbound internet
      // connection.
      if (config.runLocal) {
        return resolve(connection);
      }

      connection
        .login(config.username, config.password)
        .then(function (user) {
          log.verbose('SFDC connection spawned: ' + JSON.stringify(user))

          // Set the connection expiration and cache the connection in memory
          // so it can be reused by future requests.
          connections[connectionName].expiresOn = moment()
            .add(config.maxConnectionAge.val, config.maxConnectionAge.unit)
          connections[connectionName].connection = connection

          // Return the active connection
          resolve(connection)
        })
        .fail(function (err) {
          /*
           * Considering SFDC could have downtime (planned / unplanned) and
           * that it might not be the only primary dependency for an app,
           * failSafe config is to ensure the sails adapter still returns a
           * connection object to enable successful lift of sails app with
           * the understanding that SFDC API calls would fail during downtime
           * and app is handling fallback / error.
           *
           * This is considered false by default to keep it consistent with
           * other adapters and each app could explicitly set it to true
           * knowing the expected functionality / impact.
           */
          var failSafe = config.failSafe

          failSafe = (
            (failSafe === true) ||
            (failSafe === 'true')
          )

          log.error('SFDC connection failed: ', err)

          if (failSafe) {
            return resolve(connection)
          }

          reject(err)
        })
    })
  }

  // Expose adapter definition
  return adapter

  // This will convert sort by clause into jsforce format.
  // Since the new format of sails v1 is not compatible with jsforce
  function parseSortClause(sortOptions) {
    var convertedSortClause = {}
      , sortKeyMap = {
        'ASC': 1,
        'DESC': -1
      }
    if (!sortOptions.length) {
      return {};
    }
    sortOptions.forEach(function (item) {
      _.extend(convertedSortClause, item)
    })
    return _.mapValues(convertedSortClause, function(sortOrder) {
      return sortKeyMap[sortOrder]
    })
  }

  // This will convert and,or,like,in in where clause to $and,$or,$like,$in.
  // Since the new format of sails v1 is not compatible with jsforce
  function parseClause(original) {
    var replace = ['and', 'or', 'like', 'in']
    const mapperFn = (value, key) => replace.indexOf(key) != -1 ? `$${key}`: key
    const goDeep = (obj, mapper) => {
      if(_.isPlainObject(obj)) {
        const newObj = _.mapKeys(obj, mapper)
        return _.mapValues(newObj, (item) => {
          return goDeep(item, mapper)
        })
      }
      if(_.isArray(obj)) {
          return obj.map((arrItem) => goDeep(arrItem, mapper))
      }
      return obj
    }
    return goDeep(original, mapperFn)
  }

})()

