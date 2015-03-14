var my_env = 'production';  //which section of env.json to get env parameters from
var env = require('./env.json');
var config = env[my_env]; //the 'config' object now contains the parameters for the required environment

var express = require('express');
var path = require('path');
//noinspection CodeAssistanceForRequiredModule
var fs = require('fs');

//create express app, use public folder for static files
var app = express();
app.use(express.static(path.join(__dirname)));

//is necessary for parsing POST request
app.use(express.bodyParser());

// initialise cookies for storing session data
app.use(express.cookieParser());
app.use(express.session({secret: '1234567890QWERTY'}));

var server = app.listen(8008);

var java = require("java");
java.classpath.push("java/JsonFromJobXML.jar");
java.classpath.push("java/json-simple-1.1.1.jar");
var url = require('url');

// monitor part
var io = require('socket.io').listen(server);
io.set('log level', 1); // reduce logging

createTableHTML = require('./createTableHTML');

var cachedRows = null;
var hashRows = createTableHTML.create();

//connect to the MySQL
var mysql      = require('mysql');
var db = mysql.createConnection({
    host        : config.host,
    user        : config.user,
    database    : config.database,
    port        : config.port
});
var POLLING_INTERVAL = 4000,
    pollingTimer;
var connectionsArray = [];
var cacheRowArray = [];

var rows = {};

// Long pooling
var pollingLoop = function () {
    //console.log("pollingLoop Starting  .... ");

    console.log("pollingLoop count = " + connectionsArray.length);

    var quertStr = 'SELECT parent, status, env, region, start_date, end_date, file_name, machine_name, ' +
        'user_name, start_time, end_time, job_name, error_msg, id FROM jobs_log ' +
        'order by start_time';

    var query = db.query(quertStr);

    rows = {}; // this hash will contain the result of our db query

    function addToHash(row) {
        if(row.parent == row.id) {
            // then we got parent job, add it to the start of list
            if (!rows[row.id]) {
                rows[row.id] = [];
            }
            rows[row.id].unshift(row);
        } else {
            // we got child, add this to the end of list
            if (!rows[row.parent]) {
                rows[row.parent] = [];
            }
            rows[row.parent].push(row);
        }
    }
    // set up the query listeners
    query
        .on('error', function (err) {
            // Handle error, and 'end' event will be emitted after this as well
            console.log(err);
            console.log(err.stack);
            updateSockets(err);
        })
        .on('result', function (row) {
            addToHash(row);
        })
        .on('end', function () {
            // loop on itself only if there are sockets still connected
            //console.log("   pollingLoop end function .... connectionsArray.length =" + connectionsArray.length + ", (rows).length = " + Object.keys(rows).length);

            hashRows.set(rows);
            if (connectionsArray.length > 0) {
                updateSockets(rows);
                pollingTimer = setTimeout(function(){
                    pollingLoop()
                },
                POLLING_INTERVAL);
            }


        });
    //console.log("pollingLoop Ends  .... ");
};

// create a new web socket connection to keep the content updated without any AJAX request
io.sockets.on( 'connection', function ( socket ) {
    console.log("io.sockets.on.connection Starting  .... connectionsArray.length = " + connectionsArray.length);

    // start the polling loop only if at least there is one user connected
    if (!connectionsArray.length) {
        pollingLoop();
    }

    socket.on('error', function (err) {
        console.log("io.sockets.on.error :" + err);
    });

    socket.on('disconnect', function () {
        var socketIndex = connectionsArray.indexOf( socket );

        if (socketIndex >= 0) {
            connectionsArray.splice( socketIndex, 1 );
            cacheRowArray.splice( socketIndex, 1 );
        }

        console.log("io.sockets.on.disconnect :" + socketIndex + ", count = " + connectionsArray.length);
    });

    console.log("pushing socket :" + connectionsArray.length);

    connectionsArray.push( socket );
    cacheRowArray.push( null );

    console.log("io.sockets.on.connection Ends  .... connectionsArray.length = " + connectionsArray.length);
});

app.get('/monitor', function(req, res){
    console.log("app.get('/monitor' Starting  .... ");

    /*if (cachedRows) {
        console.log("    in app.get('/monitor'  .... cachedRows.length = " + Object.keys(cachedRows).length);
    } else {
        console.log("    in app.get('/monitor'  .... cachedRows.length =  0");
    }
    */
    fs.readFile('monitor.html', 'utf8', function(err, text){
        res.send(text);

        //pollingLoop();
    });
    console.log("app.get('/monitor' Ends  .... ");

});

app.post('/delete_data', function(req, res){

    //console.log("app.post('/delete_data' Starting  .... ");
    var data = req.body;

    var queryStr = "DELETE from jobs_log " + data["whereClause"];

    //console.log("****** in app.post('/delete_data': " + queryStr);

    function send_response(data){
        res.setHeader("Content-Type","text/xml");
        res.send(data);
    }

    if (data["whereClause"] != "") {
        var query = db.query(queryStr);
        query
            .on('error', function (err) {
                console.log("****** ERROR in app.post('/delete_data': error : " + err);
                // Handle error, and 'end' event will be emitted after this as well
                send_response(err);

            })
            .on('end', function () {
                //console.log("****** in app.post('/delete_data': end : ");
                send_response("");
            });
    }
    console.log("app.post('/delete_data' Ends  .... ");

});

var updateSockets = function (rows) {
    //console.log("updateSockets Starting  .... connectionsArray.length = " + connectionsArray.length);

    for (var socketIndex = 0; socketIndex < connectionsArray.length; socketIndex++) {

        var tmpSocket = connectionsArray[socketIndex];

        //console.log("      ********** updateSockets in loop : socketIndex = " + socketIndex + ", cacheRowArray[" + socketIndex + "] = " + cacheRowArray[socketIndex] );
        var data = hashRows.create(cacheRowArray[socketIndex]);

        cacheRowArray[socketIndex] = rows;

        //console.log("       ****** :" + socketIndex + " : " + cacheRowArray[socketIndex]);
        if (data["load"] && data["load"].length != 0) {

            console.log("      ********* sending load to every socket :" + data["load"].length);
            tmpSocket.volatile.emit( 'load' , data["load"] );

        } else if (data["update"] && data["update"].length != 0) {

            console.log("      ******** sending update to every socket" + data["update"].length);
            tmpSocket.volatile.emit( 'update' , data["update"] );

        } else if (data["delete"] && data["delete"].length != 0) {

            console.log("      ********** sending delete to every socket" + data["delete"].length);
            tmpSocket.volatile.emit( 'delete' , data["delete"] );

        } else {

            //console.log("      ********** sending bulk to every socket");
            tmpSocket.volatile.emit( 'bulk' );

        }
    }
    //console.log("updateSockets Ends  .... connectionsArray.length = " + connectionsArray.length);
};

var excludeXmlList = ["build.xml", "nbbuild.xml"];

// executor part
app.get('/executor', function(req, res){
    console.log("app.get('/executor' Starting  .... ");

    // Read the ?job_group parameter, if it's supplied then populate the session variable (or set to 'default')
    var job_group = req.param('job_group', null);
    if (job_group !== null) {
        req.session.job_group_name = job_group;
    }
    else if (job_group === null && req.session.job_group_name == undefined) {
        req.session.job_group_name = 'default';
    }

    console.log("The execution path is: " + config.file_path[req.session.job_group_name] + 'xml/');

    // Whatever ?job_group parameter was passed in the url, get the file_path from env.json file (config)
    fs.readFile('executor.html', 'utf8', function(err, text){
        var xmlFiles = fs.readdirSync(config.file_path[req.session.job_group_name] + 'xml/');
        //empty option on top of list
        var options = "<option value=\"\"></option>";
        for (var i = 0; i < xmlFiles.length; i++) {
            var parts = xmlFiles[i].split(".");
            if (parts[parts.length-1] == "xml" && excludeXmlList.indexOf(xmlFiles[i]) < 0) {
                options += '<option value="' + xmlFiles[i] + '">' + xmlFiles[i] + '</option>';
            }
        }
        text = text.replace("%xmlOptions%", options);
        res.send(text);
    });
    console.log("app.get('/executor' Ends  .... ");
});

app.get('/getXmlFileList', function(req, res){
    console.log("app.get('/getXmlFileList' Starting  .... ");

    // Read the ?job_group parameter, if it's supplied then populate the session variable (or set to 'default')
    var job_group = req.param('job_group', null);
    if (job_group !== null) {
        req.session.job_group_name = job_group;
    }
    else if (job_group === null && req.session.job_group_name == undefined) {
        req.session.job_group_name = 'default';
    }

    console.log(job_group);
    console.log(req.session.job_group_name);
    console.log("The execution path is: " + config.file_path[req.session.job_group_name] + 'xml/');

    // Whatever ?job_group parameter was passed in the url, get the file_path from env.json file (config)

    var xmlFiles = fs.readdirSync(config.file_path[req.session.job_group_name] + 'xml/');
    //empty option on top of list
    var options = "<option value=\"\"></option>";
    for (var i = 0; i < xmlFiles.length; i++) {
        var parts = xmlFiles[i].split(".");
        if (parts[parts.length-1] == "xml" && excludeXmlList.indexOf(xmlFiles[i]) < 0) {
            options += '<option value="' + xmlFiles[i] + '">' + xmlFiles[i] + '</option>';
        }
    }

    res.send(options);

    console.log("app.get('/getXmlFileList' Ends  .... ");
});

app.get('/getXmlContent', function(req, res){
    console.log("app.get('/getXmlContent' Starting  .... " + req.query.xmlFile);

    fs.readFile(config.file_path[req.session.job_group_name] + "xml/" + req.query.xmlFile, 'utf8', function(err, text){
        res.send(text);
    });
    console.log("app.get('/getXmlContent' Ends  .... ");
});


app.get('/xmlgraphview', function(req, res){
    console.log("app.get('/xmlgraphview' Starting  .... ");

    var url_parts = url.parse(req.url, true);
    var query = url_parts.query;

    var JobXMLtoJSON = java.callStaticMethodSync("visualjobxml.JobXMLtoJSON", "getJsonFor",
            config.file_path[req.session.job_group_name] + "xml/" +query["file"]);

    console.log(JobXMLtoJSON);

    res.contentType('application/json');
    res.send(JSON.parse(JobXMLtoJSON));

    console.log("app.get('/xmlgraphview' Ends  .... ");
});


app.post('/execute', function(req, res){

    console.log("app.post('/execute' Starting  .... ");
    var data = req.body;

    var cmdArgs = [];
    if (data["commandline"] != "") {
        cmdArgs = data["commandline"].split(" ");
        cmdArgs.splice(0,1);
    }

    console.log("app.post('/execute' Executing in : " + config.file_path[req.session.job_group_name] + ", the command : " + data["commandline"]);

    function send_response(data){
        res.setHeader("Content-Type","text/xml");
        res.send(data);
    }

    var spawn = require('child_process').spawn;

    var child = spawn(
        'java',
        cmdArgs,
        {
            detached: true,
            cwd: config.file_path[req.session.job_group_name]
        });

    child.unref();

    send_response("");
    console.log("app.post('/execute' Ends  .... ");

});

process.on('uncaughtException', function (err) {
    console.error("********** ERROR : "  + (new Date).toUTCString() + ' uncaughtException:', err.message);
    console.error(err.stack);
    process.exit(1)
});
