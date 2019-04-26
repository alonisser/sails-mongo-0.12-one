/**
 * Module dependencies
 */

const async = require('async');

const MongoClient = require('mongodb').MongoClient;

/**
 * Manage a connection to a Mongo Server
 *
 * @param {Object} config
 * @param {function} cb
 * @return {Object}
 * @api private
 */

var Connection = module.exports = function Connection(config, cb) {
    var self = this;

    // Hold the config object
    this.config = config || {};

    // Build Database connection
    this._buildConnection(function (err, client) {
        if (err) return cb(err);
        if (!client) return cb(new Error('no db object'));

        // Store the DB object
        self.client = client;

        // Return the connection
        cb(null, self);
    });
};


/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Create A Collection
 *
 * @param {String} name
 * @param {Object} collection
 * @param {Function} cb
 * @api public
 */

Connection.prototype.createCollection = function createCollection(name, collection, cb) {
    var self = this;

    // Create the Collection
    this.client.db().createCollection(name, function (err, result) {
        if (err) return cb(err);

        // Create Indexes
        self._ensureIndexes(result, collection.indexes, cb);
    });
};

/**
 * Drop A Collection
 *
 * @param {String} name
 * @param {Function} cb
 * @api public
 */

Connection.prototype.dropCollection = function dropCollection(name, cb) {
    this.client.db().dropCollection(name, cb);
};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Build Server and Database Connection Objects
 *
 * @param {Function} cb
 * @api private
 */

Connection.prototype._buildConnection = function _buildConnection(cb) {

    // Set the configured options
    var connectionOptions = {};
    connectionOptions.readPreference = this.config.readPreference;

    connectionOptions.ssl = this.config.ssl;
    connectionOptions.sslValidate = this.config.sslValidate;
    connectionOptions.sslCA = this.config.sslCA;
    connectionOptions.sslCert = this.config.sslCert;
    connectionOptions.sslKey = this.config.sslKey;
    connectionOptions.poolSize = this.config.poolSize;

    connectionOptions.autoReconnect = this.config.auto_reconnect;

    connectionOptions.reconnectInterval = this.config.reconnectInterval;
    connectionOptions.reconnectTries = this.config.reconnectTries;
    connectionOptions.useNewUrlParser = true;

    // Build A Mongo Connection String
    let connectionString = 'mongodb://';

    // If auth is used, append it to the connection string
    if (this.config.user && this.config.password) {

        // Ensure a database was set if auth in enabled
        if (!this.config.database) {
            throw new Error('The MongoDB Adapter requires a database config option if authentication is used.');
        }

        connectionString += this.config.user + ':' + this.config.password + '@';
    }

    // Append the host and port
    connectionString += this.config.host + ':' + this.config.port + '/';

    if (this.config.database) {
        connectionString += this.config.database;
    }

    // Use config connection string if available
    if (this.config.url) connectionString = this.config.url;

    // Open a Connection
    MongoClient.connect(connectionString, connectionOptions, cb);
};

/**
 * Ensure Indexes
 *
 * @param {String} collection
 * @param {Array} indexes
 * @param {Function} cb
 * @api private
 */

Connection.prototype._ensureIndexes = function _ensureIndexes(collection, indexes, cb) {

    function createIndex(item, next) {
        collection.createIndex(item.index, item.options, next);
    }

    async.each(indexes, createIndex, cb);
};
