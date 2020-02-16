/*---------------------------------------------------------------
  :: sails-mongo
  -> adapter
---------------------------------------------------------------*/

const util = require('util');
const async = require('async');
const _ = require('lodash');
const ObjectId = require('mongodb').ObjectID;
const Errors = require('waterline-errors').adapter;
const _runJoins = require('waterline-cursor');
const Connection = require('./connection');
const Collection = require('./collection');
const utils = require('./utils');


module.exports = (function () {

    // Keep track of all the connections used by the app
    var connections = {};

    const adapter = {

        // Which type of primary key is used by default
        pkFormat: 'string',

        // to track schema internally
        syncable: true,

        // Expose all the connection options with default settings
        defaults: {


            // Connection Configuration
            host: 'localhost',
            database: 'sails',
            port: 27017,
            readOnly: false,
            user: null,
            password: null,
            schema: false,


            // Allow a URL Config String
            url: null,


            // DB Options
            w: 1,
            wtimeout: 0,
            fsync: false,
            journal: false,
            readPreference: null,
            nativeParser: false,
            forceServerObjectId: false,
            recordQueryStats: false,
            retryMiliSeconds: 5000,
            numberOfRetries: 5,

            // Server Options
            ssl: false,
            poolSize: 50,

            auto_reconnect: true,

            reconnectTries: 30, // defaults to mongodb recommended settings
            reconnectInterval: 1000, // defaults to mongodb recommended settings


            // Waterline NEXT
            // These are flags that can be toggled today and expose future features. If any of the following are turned
            // on the adapter tests will probably not pass. If you toggle these know what you are getting into.
            wlNext: {

                // Case sensitive - false
                // In the next version of WL queries will be case sensitive by default.
                // Set this to true to experiment with that feature today.
                caseSensitive: false

            }

        },

        /**
         * Register A Connection
         *
         * Will open up a new connection using the configuration provided and store the DB
         * object to run commands off of. This creates a new pool for each connection config.
         *
         * @param {Object} connection
         * @param {Object} collections
         * @param {Function} cb
         */

        registerConnection: function (connection, collections, cb) {
            const startTime = new Date().getTime();
            logger.log('Beginning to register a connection to mongodb');
            if (!connection.identity) return cb(Errors.IdentityMissing);
            if (connections[connection.identity]) return cb(Errors.IdentityDuplicate);

            // Merging default options
            connection = _.defaults(connection, this.defaults);

            // Store the connection
            connections[connection.identity] = {
                config: connection,
                collections: {}
            };

            // Create a new active connection
            logger.log(`Attempting to create a new connection for ${JSON.stringify(connection)}`)
            new Connection(connection, function (_err, db) {
                logger.log(`Successfully received db object in callback, took ${new Date().getTime() - startTime}`);
                if (_err) {
                    return cb((function _createError() {
                        var msg = util.format('Failed to connect to MongoDB.  Are you sure your configured Mongo instance is running?\n Error details:\n%s', util.inspect(_err, false, null));
                        var err = new Error(msg);
                        err.originalError = _err;
                        return err;
                    })());
                }
                connections[connection.identity].connection = db;

                // Build up a registry of collections
                Object.keys(collections).forEach(function (key) {
                    connections[connection.identity].collections[key] = new Collection(collections[key], db);
                });

                cb();
            });

        },

        /**
         * Teardown
         *
         * Closes the connection pool and removes the connection object from the registry.
         *
         * @param {String} conn
         * @param {Function} cb
         */

        teardown: function (conn, cb) {
            if (typeof conn == 'function') {
                cb = conn;
                conn = null;
            }

            if (conn === null) {
                var _connections = _.map(_.values(connections), 'connection');
                if (!_connections.length) {
                    return cb();
                }

                var dbs = _.map(_connections, 'db');
                if (!dbs.length) {
                    return cb();
                }

                connections = {};
                return async.each(dbs, function (db, onClosed) {
                    if (db === undefined) {
                        return onClosed();
                    }
                    db.close(onClosed);
                }, cb);
            }

            if (!connections[conn]) return cb();

            var dbConnection = connections[conn].connection.client;
            dbConnection.close(function () {
                delete connections[conn];
                cb();
            });
        },

        /**
         * Describe
         *
         * Return the Schema of a collection after first creating the collection
         * and indexes if they don't exist.
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Function} cb
         */

        describe: function (connectionName, collectionName, cb) {

            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];
            var schema = collection.schema;
            var names = connectionObject.connection.client.db().listCollections(collectionName);
            if (names.length > 0) return cb(null, schema);
            cb();

        },

        /**
         * Define
         *
         * Create a new Mongo Collection and set Index Values
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Object} definition
         * @param {Function} cb
         */

        define: function (connectionName, collectionName, definition, cb) {

            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Create the collection and indexes

            if (connectionObject.connection.config.readOnly) {
                //No creating indexes and collections for readOnly collections
                cb()
            } else {
                connectionObject.connection.createCollection(collectionName, collection, cb);
            }

        },

        /**
         * Drop
         *
         * Drop a Collection
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Array} relations
         * @param {Function} cb
         */

        drop: function (connectionName, collectionName, relations, cb) {

            if (typeof relations === 'function') {
                cb = relations;
                relations = [];
            }

            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Drop the collection and indexes
            if (connectionObject.connection.config.readOnly) {
                //No creating indexes and collections for readOnly collections
                cb()
            } else {
                connectionObject.connection.dropCollection(collectionName, function (err) {

                    // Don't error if droping a collection which doesn't exist
                    if (err && err.errmsg === 'ns not found') return cb();
                    if (err) return cb(err);
                    cb();
                });
            }

        },

        /**
         * Native
         *
         * Give access to a native mongo collection object for running custom
         * queries.
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Function} cb
         */

        native: function (connectionName, collectionName, cb) {

            const connectionObject = connections[connectionName];
            cb(null, connectionObject.connection.client.db().collection(collectionName));

        },

        /**
         * Mongo object with mongoDB native methods
         */

        mongo: {

            /**
             * ObjectId
             *
             * Return a Mongo ObjectID from a string
             *
             * @param {String} id
             */
            objectId: function (id) {
                if (!id) return null;
                try {
                    return new ObjectId(id);
                } catch (err) {
                    return null;
                }
            }

        },

        /**
         * Create
         *
         * Insert a single document into a collection.
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Object} data
         * @param {Function} cb
         */

        create: function (connectionName, collectionName, data, cb) {

            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Insert a new document into the collection
            if (connectionObject.connection.config.readOnly) {
                //No creating indexes and collections for readOnly collections
                cb()
            } else {
                collection.insert(data, function (err, results) {
                    if (err) return cb(utils.clarifyError(err));
                    cb(null, results[0]);
                });
            }

        },

        /**
         * Create Each
         *
         * Insert an array of documents into a collection.
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Object} data
         * @param {Function} cb
         */

        createEach: function (connectionName, collectionName, data, cb) {

            if (data.length === 0) {
                return cb(null, []);
            }

            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Insert a new document into the collection
            collection.insert(data, function (err, results) {
                if (err) return cb(utils.clarifyError(err));
                cb(null, results);
            });
        },

        /**
         * Find
         *
         * Find all matching documents in a collection.
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Object} options
         * @param {Function} cb
         */

        find: function (connectionName, collectionName, options, cb) {
            options = options || {};
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Find all matching documents
            collection.find(options, function (err, results) {
                if (err) return cb(err);
                cb(null, results);
            });
        },

        /**
         * Update
         *
         * Update all documents matching a criteria object in a collection.
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Object} options
         * @param {Object} values
         * @param {Function} cb
         */

        update: function (connectionName, collectionName, options, values, cb) {
            options = options || {};
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Update matching documents
            if (connectionObject.connection.config.readOnly) {
                //No creating indexes and collections for readOnly collections
                cb()
            } else {
                collection.update(options, values, function (err, results) {
                    if (err) return cb(utils.clarifyError(err));
                    cb(null, results);
                });
            }

        },

        /**
         * Destroy
         *
         * Destroy all documents matching a criteria object in a collection.
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Object} options
         * @param {Function} cb
         */

        destroy: function (connectionName, collectionName, options, cb) {
            options = options || {};
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Find matching documents
            if (connectionObject.connection.config.readOnly) {
                //No creating indexes and collections for readOnly collections
                cb()
            } else {
                collection.find(options, function (err, results) {
                    if (err) return cb(err);

                    // Destroy matching documents
                    collection.destroy(options, function (err) {
                        if (err) return cb(err);
                        cb(null, results);
                    });
                });
            }
        },

        /**
         * Count
         *
         * Return a count of the number of records matching a criteria.
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Object} options
         * @param {Function} cb
         */

        count: function (connectionName, collectionName, options, cb) {
            options = options || {};
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Find matching documents and return the count
            collection.count(options, function (err, results) {
                if (err) return cb(err);
                cb(null, results);
            });
        },


        /**
         * Join
         *
         * Peforms a join between 2-3 mongo collections when Waterline core
         * needs to satisfy a `.populate()`.
         *
         * @param  {[type]}   connectionName [description]
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   criteria       [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        join: function (connectionName, collectionName, criteria, cb) {

            // Ignore `select` from waterline core
            if (typeof criteria === 'object') {
                delete criteria.select;
            }

            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            // Populate associated records for each parent result
            // (or do them all at once as an optimization, if possible)
            _runJoins({

                instructions: criteria,
                parentCollection: collectionName,

                /**
                 * Find some records directly (using only this adapter)
                 * from the specified collection.
                 *
                 * @param  {String}   collectionIdentity
                 * @param  {Object}   criteria
                 * @param  {Function} cb
                 */
                $find: function (collectionIdentity, criteria, cb) {
                    var connectionObject = connections[connectionName];
                    var collection = connectionObject.collections[collectionIdentity];
                    return collection.find(criteria, cb);
                },

                /**
                 * Look up the name of the primary key field
                 * for the collection with the specified identity.
                 *
                 * @param  {String}   collectionIdentity
                 * @return {String}
                 */
                $getPK: function (collectionIdentity) {
                    if (!collectionIdentity) return;
                    var connectionObject = connections[connectionName];
                    if (!connectionObject) {
                        throw new Error('Consistency violation in sails-mongo: Unrecognized datastore (i.e. connection): `' + connectionName + '`.');
                    }
                    var collection = connectionObject.collections[collectionIdentity];
                    if (!collection) {
                        throw new Error('Consistency violation in sails-mongo: Unrecognized collection: `' + collectionIdentity + '` in datastore (i.e. connection): `' + connectionName + '`.');
                    }
                    return collection._getPK();
                }
            }, cb);

        },

        /**
         * Stream
         *
         * Stream one or more documents from the collection
         * using where, limit, skip, and order
         * In where: handle `or`, `and`, and `like` queries
         *
         * @param {String} connectionName
         * @param {String} collectionName
         * @param {Object} options
         * @param {Object} stream
         */
        stream: function (connectionName, collectionName, options, stream) {
            options = options || {};
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];

            collection.stream(options, stream);
        },

        identity: 'sails-mongo'
    };

    return adapter;
})();
