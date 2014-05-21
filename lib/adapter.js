/**
 * Module Dependencies
 */
var Q = require('q')
  , _ = require('lodash')
  , jsforce = require('jsforce')
  , captains = require('captains-log')
  , log = captains()
  , Errors = require('waterline-errors').adapter


module.exports = (function () {

  // Keep track of all the connections used by the app
  var connections = {}

  var adapter = {
    // to track schema internally
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
      log.verbose('finding ' + collectionName)
      var connection = connections[connectionName].connection
        , collection = connections[connectionName].collections[collectionName]

      // Shim in required query params.
      options.where = queryShim(options.where, collectionName)

      // TODO(jsims): Include a check to ensure the connection is still
      // alive. If the connection has died we will need to spawn a new one.
      connection.sobject(collectionName)
        .select(Object.keys(collection.definition))
        .where(options.where)
        // TODO(jsims): Remove this. Artificially limiting results until I can
        //  speed up the result response time.
        .limit(options.limit)
        .offset(options.skip)
        .execute(function(err, results) {
          if (err) return cb(err.toString())
          cb(null, results)
        })
    },

    create: function(connectionName, collectionName, data, cb) {
      log.verbose('creating ' + collectionName)
      var connection = connections[connectionName].connection
        , collection = connections[connectionName].collections[collectionName]

      connection.sobject(collectionName).create(data, function(err, result) {
        if (err) return cb(err)
        // Salesforce seems to embed some errors within the result. I'm not
        //  if jsforce processes this and returns it via through the callback
        //  or not. Until I know for sure, I'll also verify these fields
        //  separately.
        if (result.success !== true) return cb(err)
        if (result.errors.length !== 0) return cb(err)
        // Errors should be accurately represented by the app's response code
        //  so I'll drop these error related fields before returning.
        delete result.success
        delete result.errors

        return cb(null, result)
      })
    },

    update: function(connectionName, collectionName, options, values, cb) {
      log.verbose('updating ' + collectionName)

       var conn = connections[connectionName].connection
        , collection = connections[connectionName].collections[collectionName]
        , updateObj = _.extend(options.where, values)

      // TODO(jsims): Include a check to ensure the connection is still
      // alive. If the connection has died we will need to spawn a new one.
      conn.sobject(collectionName).update(updateObj, function (err, ret) {
        if (err || !ret.success) {return cb(err.toString())}

        cb(null, ret)
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
    destroy: function(connectionName, collectionName, options, cb) { cb() },

    // TODO(jsims): Implement join funcitonality. This currently doesn't work
    // for the KnowledgeArticleVersion object in Salesforce.
    //
//    join: function(connectionName, collectionName, options, cb) {
//      var connection = connections[connectionName].connection
//        , collection = connections[connectionName].collections[collectionName]
//        , baseQuery = connection.sobject(collectionName)
//                        .select(Object.keys(collection.definition))
//                        .where(options.where)
//      // Add in children
//      options.joins.forEach(function(child, index, array) {
//        var childName = child.child
//        console.dir(child)
//        log.debug('joining child: ' + childName)
//        // TODO(jsims): Maybe add this functionality into jsforce.
//        // Salesforce expects child relationships to be supplied in their
//        //  plural form.
//        baseQuery.include(childName)
//          .select(child.select.join(','))
//          .end()
//      })
//      // Execute query
//      baseQuery.execute(function(err, results) {
//        if (err) cb(err)
//        else cb(null, results)
//      })
//    },

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
    //  to be triggered properly upon a connection failure. This section will
    //  simply fail with `user.id` is undefined.
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

