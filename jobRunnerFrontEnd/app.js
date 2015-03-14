var my_env = 'staging',  //which section of env.json to get env parameters from
    env = require('./env.json'),
    config = env[my_env], //the 'config' object now contains the parameters for the required environment
    express = require('express'),
    path = require('path'),
    fs  = require("fs"),
    //create express app, use public folder for static files
    app = express(),
    bodyParser = require('body-parser'),
    session = require('express-session'),
    mysqldb = require('./datab/mysql_connector').MySQLDBConnector,
    db = new mysqldb().connect();

app.use(express.static(path.join(__dirname)));

//is necessary for parsing POST request
//app.use(express.bodyParser());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}));

app.use(session({   secret: '1234567890QWERTY',
                    saveUninitialized: true,
                    resave: true}));


var server = app.listen(8008);

//schedule, execute jobs part
require('./routes/' + my_env)(app, express, server, config, db.connection);

app.get('/', function(req, res){
    console.log("app.get('/index' Starting  .... ");

    fs.readFile('views/index.html', 'utf8', function(err, text){
        res.send(text);
    });
    console.log("app.get('/index' Ends  .... ");

});


process.on('uncaughtException', function (err) {
    console.error("********** ERROR : "  + (new Date).toUTCString() + ' uncaughtException:', err.message);
    console.error(err.stack);
    process.exit(1)
});