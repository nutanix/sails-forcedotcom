/**
 * Module Dependencies
 */
var Q = require('q')
  , _ = require('underscore')
  , jsforce = require('jsforce')
  , captains = require('captains-log')
  , log = captains()
  , Errors = require('waterline-errors').adapter


module.exports = (function () {

  // Keep track of all the connections used by the app
  var connections = {}

  var adapter = {
    syncable: false,
    defaults: {},

    registerConnection: function(connection, collections, cb) {
      if(!connection.identity) return cb(Errors.IdentityMissing)
      if(connections[connection.identity]) return cb(Errors.IdentityDuplicate)

      // Store the connection.
      connections[connection.identity] = {
        config: connection,
        collections: collections,
        connection: {}
      }

      // Create a new active connection.
      spawnConnection(connection)
        .fail(cb)
        .then(function(db) {
          connections[connection.identity].connection = db
        })
        .done(cb)
    },

    find: function(connectionName, collectionName, options, cb) {
      var connection = connections[connectionName].connection
        , collection = connections[connectionName].collections[collectionName]

      // TODO(jsims): Include a check to ensure the connection is still
      // alive. If the connection has died we will need to spawn a new one.
      connection.sobject(collectionName)
        .select(Object.keys(collection.definition))
        .where(options.where)
        .limit(options.limit)
        .offset(options.skip)
        .execute(function(err, results) {
          if (err) cb(err)
          else cb(null, results)
        })
    },

    create: function(connectionName, collectionName, data, cb) {
      var connection = connections[connectionName].connection
        , collection = connections[connectionName].collections[collectionName]

      connection.sobject(collectionName).create(data, function(err, result) {
        if (err) cb(err)
        cb(null, result)
      })
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
    update: function(connectionName, collectionName, options, values, cb) { cb() },
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

  function spawnConnection(config) {
    var deferred = Q.defer()
      , connection = new jsforce.Connection(config.connectionParams)

    // TODO(jsims): Migrate this over to OAuth so refresh token is supported.
    // TODO(jsims): Implement some connection pooling here.
    // TODO(jsims): Need better error handling here. The .fail() doesn't seem
    //  to be triggered properly upon a connection failure.
    connection.login(config.username, config.password)
      .fail(deferred.reject)
      .done(function(user) {
        log.verbose('SFDC connection spawned as ' + user.id)
        deferred.resolve(connection)
      })

    // Return our own promise containing the successful connection.
    return deferred.promise
  }

  // Expose adapter definition
  return adapter

})()

