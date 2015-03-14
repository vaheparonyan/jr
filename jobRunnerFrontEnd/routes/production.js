/**
 * Created by vparonyan on 01/10/2014.
 */
/**
 * index of routes/controllers
 *
 * @param app
 */
module.exports = function(app, express, server, config, db){
    require('./local/schedule')(app, express, config, db);
    require('./local/executor')(app, express, config, db);
    require('./local/monitor')(app, express, server, config, db);
};