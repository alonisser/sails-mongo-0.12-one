module.exports = {
    log(...params) {
        if(process.env.LOG_SAILS_MONGO_ORM) {
            console.log(...params)
        }
    }
}