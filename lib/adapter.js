/**
 * Module Dependencies
 */
var Q = require('q')
  , _ = require('underscore')
  , jsforce = require('jsforce')

// Errors
var todoError = new Error('TODO: Implement this functionality.')

module.exports = (function () {

  // Registered collection cache
  var _modelReferences = {}

  var adapter = {
    syncable: false,
    defaults: {},

    registerCollection: function(collection, cb) {
      // Keep a reference to this collection
      _modelReferences[collection.identity] = collection

      cb()
    },

    find: function(collectionName, options, cb) {
      var collection = _modelReferences[collectionName]
      spawnConnection(collection.config)
        .then(execLogic)
        .fail(cb)
        .done(function(results) {
          cb(null, results)
        })

      function execLogic(connection) {
        var deferred = Q.defer()
        connection.sobject(collectionName)
          .select(Object.keys(collection.definition))
          .where(options.where)
          .limit(options.limit)
          .offset(options.skip)
          .execute(function(err, results) {
            if (err) deferred.reject(err)
            else deferred.resolve(results)
          })

        return deferred.promise
      }
    },

    create: function(collectionName, data, cb) {
      var collection = _modelReferences[collectionName]
      spawnConnection(collection.config)
        .then(execLogic)
        .fail(cb)
        .done(function(result) {
          cb(null, result)
        })

      // Main logic for operation.
      function execLogic(connection) {
        var deferred = Q.defer()
        connection.sobject(collectionName).create(data)
          .done(deferred.resolve, deferred.reject)

        return deferred.promise
      }
    },

    // TODO: Implement teardown process.
    teardown: function(cb) { cb() },
    // TODO: Implement `Model.define()` functionality.
    define: function(collectionName, definition, cb) { cb(todoError) },
    // TODO: Implement `Model.describe()` functionality.
    describe: function(collectionName, cb) { cb(todoError) },
    // TODO: Implement `Model.drop` functionality.
    drop: function(collectionName, relations, cb) { cb(todoError) },
    // TODO: Implement `Model.drop` functionality.
    update: function(collectionName, options, values, cb) { cb(todoError) },
    // TODO: Implement `Model.drop` functionality.
    destroy: function(collectionName, options, cb) { cb(todoError) },

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

    // TODO(jsims): Implement some connection pooling here.
    // TODO(jsims): Need better error handling here. The .fail() doesn't seem
    //  to be triggered properly upon a connection failure.
    connection.login(config.username, config.password)
      .fail(deferred.reject)
      .done(function(user) {
        sails.log.verbose('logged in as :' + user.id)
        deferred.resolve(connection)
      })

    // Return our own promise containing the successful connection.
    return deferred.promise
  }

  // Expose adapter definition
  return adapter

})()

