/**
 * Module dependencies
 */

const async = require('async');
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;
const querystring = require('querystring');
const logger = require('./logger');
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
        logger.log('Successfully connected to the db');
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

function extractQueryFromUri(uri){
 if(!uri){
     return
 }

 const brokenUri = uri.split("?");
 if (brokenUri.length > 1){
     let query = querystring.parse(brokenUri[1]);
     return query
 } else {
     return {}
 }

}

function buildConnectionString(params){
    let connectionString = 'mongodb://';

    // If auth is used, append it to the connection string
    if (params.user && params.password) {

        // Ensure a database was set if auth in enabled
        if (!params.database) {
            throw new Error('The MongoDB Adapter requires a database config option if authentication is used.');
        }

        connectionString += params.user + ':' + params.password + '@';
    }

    // Append the host and port
    connectionString += params.host + ':' + params.port + '/';

    if (params.database) {
        connectionString += params.database;
    }
    return connectionString
}
/**
 * Build Server and Database Connection Objects
 *
 * @param {Function} cb
 * @api private
 */

Connection.prototype._buildConnection = function _buildConnection(cb) {
    const startTime = new Date().getTime();
    logger.log('Started building a connection string to the database');
    let queryParams = {};
    if(this.config.url){
        queryParams = extractQueryFromUri(this.config.uri)
    }
    const availableConfiguration = _.defaults({},queryParams, this.config, {useNewUrlParser:true});
    // Set the configured options
    const connectionOptions = {};
    if (!availableConfiguration.useUnifiedTopology){
        connectionOptions.readPreference = availableConfiguration.readPreference;

        connectionOptions.autoReconnect = availableConfiguration.auto_reconnect;

        connectionOptions.reconnectInterval = availableConfiguration.reconnectInterval;
        connectionOptions.reconnectTries = availableConfiguration.reconnectTries;
    } else {
        connectionOptions.useUnifiedTopology = true
    }
    let connectionString;
    if (availableConfiguration.url){
        logger.log(`Using url as connection string, took: ${new Date().getTime() - startTime}`);
        connectionString = availableConfiguration.url;
        delete availableConfiguration.url;
    }
    else {
        logger.log(`Using other configuration as connection string, took: ${new Date().getTime() - startTime}`);
        connectionString = buildConnectionString(availableConfiguration)
        _.forEach(['user', 'password', 'database', 'host', 'port'],(prop)=>{
            if (availableConfiguration.hasOwnProperty(prop)){
                delete availableConfiguration[prop]
            }
        })
    }
    if(availableConfiguration.hasOwnProperty('authSource')){
        connectionOptions.authSource =availableConfiguration.authSource
    }





    // Build A Mongo Connection String


    // Use config connection string if available

    // Open a Connection
    logger.log(`Attempting to connect to mongo, time from start: ${new Date().getTime() - startTime}`);
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
