var mysql  = require('mysql'),
    my_env = 'staging',  //which section of env.json to get env parameters from
    env = require('../env.json'),
    config = env[my_env]; //the 'config' object now contains the parameters for the required environment
//Config = require('../config/services.config').Config;
/**
 *
 *
 * @constructor
 */
function MySQLDBConnector(env){
    this.env = env || 'staging';
    this.connection = null;
};

/**
 *
 *
 */
MySQLDBConnector.prototype.connect = function(){

    var self = this;

    if(self.connection == null){

        // use the enviorment
        //var sqlConf = JSON.parse(Config.env[this.env].mysql);
        self.connection = mysql.createConnection({
                            host        : config.db_path.host,
                            user        : config.db_path.user,
                            database    : config.db_path.database,
                            port        : config.db_path.port
                        });

        self.connection.    connect(function(err) {
            if (err) {
                console.log(err);
                throw (err);
            }
            console.log('MySQL connected on ' + self.env);
        });
    }

    return this;
};

MySQLDBConnector.prototype.disconnect = function(){
    console.log('MySQL disconnected on ' + this.env);
    self.connection.destroy();
    return this;
};

exports.MySQLDBConnector = MySQLDBConnector;