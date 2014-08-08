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

    registerConnection: function(connection, collections, cb) {
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
      spawnConnection(connection.identity)
        .then(function() {cb()})
        .fail(cb)
    },

    find: function(connectionName, collectionName, options, cb) {
      log.verbose('finding ' + collectionName)
      var collection = connections[connectionName].collections[collectionName]

      // Shim in required query params.
      options.where = queryShim(options.where, collectionName)

      spawnConnection(connectionName)
        .then(function(connection) {
          var results = []
          connection.sobject(collectionName)
            .select(_.keys(collection.definition))
            .where(options.where)
            .limit(options.limit)
            .skip(options.skip)
            .on('record', function(record) {
              results.push(record)
            })
            .on('end', function(query) {
              sails.log.silly('Total in database: ' + query.totalSize)
              sails.log.silly('Total fetched: ' + query.totalFetched)
              cb(null, results)
            })
            .on('error', function(err) {
              log.error(err.toString())
              cb(err.toString())
            })
            // TODO: Move these to a default config that can be overridden by
            // the connection config within the app.
            .execute({autoFetch: true, maxFetch: 4000})
        })
    },

    create: function(connectionName, collectionName, data, cb) {
      log.verbose('creating ' + collectionName)

      spawnConnection(connectionName)
        // Execute logic
        .then(function (connection) {

          connection.sobject(collectionName).create(data, function(err, result) {
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
    },

    update: function(connectionName, collectionName, options, values, cb) {
      log.verbose('updating ' + collectionName)

      spawnConnection(connectionName)
        // Execute logic
        .then(function (connection) {
          connection.sobject(collectionName)
            .update(_.extend(options.where, values), function (err, result) {
              if (err || !result.success) {return cb(err)}

              cb(null, result)
            })
        })
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

    // TODO: Implement teardown process.
    teardown: function(connectionName, cb) { cb() },
    // TODO: Implement `Model.define()` functionality.
    define: function(connectionName, collectionName, definition, cb) { cb() },
    // TODO: Implement `Model.describe()` functionality.
    describe: function(connectionName, collectionName, cb) { cb() },
    // TODO: Implement `Model.drop` functionality.
    drop: function(connectionName, collectionName, relations, cb) { cb() },
    // TODO: Implement `Model.drop` functionality.
    destroy: function(connectionName, collectionName, options, cb) { cb() },

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
      connection.login(config.username, config.password)
        .then(function (user) {
          log.verbose('SFDC connection spawned: ' + JSON.stringify(user))

          // Set the connection expiration and cache the connection in memory
          // so it can be reused by future requests.
          connections[connectionName].expiresOn = moment()
            .add(config.maxConnectionAge.unit, config.maxConnectionAge.val)
          connections[connectionName].connection = connection

          // Return the active connection
          resolve(connection)
        })
        .fail(reject)
    })
  }

  // Expose adapter definition
  return adapter

  /**
   * Method to shim query defaults for certain objects
   */
  function queryShim(options, collectionName) {
    // Mapping for Salesforce objects which require certain query params in
    // order to complete a `.find()` operation.
    shimMap = {
      'Knowledge_Base__kav': {
        language: 'en_US',
        publishStatus: 'online'
      }
    }
    if (options === null) options = {}
    if (!_.contains(Object.keys(shimMap), collectionName)) { return options }

    return _.defaults(options, shimMap[collectionName])
  }

})()

