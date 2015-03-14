module.exports = function(app, express, server, config, db){

    var t = express.Router(),
        fs  = require("fs"),
        io = require('socket.io').listen(server),
        connectionsArrayMap = {},
        POLLING_INTERVAL = 4000,
        pollingTimer;

    t.get('/monitor', function(req, res){
        console.log("app.get('/monitor' Starting  .... ");

        fs.readFile('views/monitor.html', 'utf8', function(err, text){
            res.send(text);

        });
        console.log("app.get('/monitor' Ends  .... ");

    });

    var queryDB = function ( queryStr, callback) {

        var query = db.query(queryStr);
        var rows = [];

        // set up the query listeners
        query
            .on('error', function (err) {
                // Handle error, and 'end' event will be emitted after this as well
                console.log(err);
                console.log(err.stack);
                throw (err);
            })
            .on('result', function (row) {
                rows.push(row);
            })
            .on('end', function () {
                callback(rows);
            });
    };

    function restructureRows(rows) {
        var result = {};
        for (var i = 0; i < rows.length; i++) {
            var r = rows[i];
            if (r.parent == r.id) {
                // then we got parent job, add it to the start of list
                if (!result[r.id]) {
                    result[r.id] = [];
                }
                result[r.id].unshift(r);
            } else {
                // we got child, add this to the end of list
                if (!result[r.parent]) {
                    result[r.parent] = [];
                }
                result[r.parent].push(r);
            }
        }
        return result;
    }

    t.post('/delete_data', function(req, res){

        //console.log("app.post('/delete_data' Starting  .... ");
        var data = req.body;

        if (data.whereClause) {
            var queryStr = "DELETE from jobs_log " + data["whereClause"];

            var job_group = data["groupname"];

            queryDB(queryStr, function (rows) {
                updateSockets(req.body["selectedRows"], job_group, 'delete');
                console.log("app.post('/delete_data' Ends  .... ");
                res.send("");
            });
        }
    });

    // Long pooling
    var pollingLoop = function (job_group) {
        //console.log("pollingLoop Starting  .... ");

        if (connectionsArrayMap[job_group].length == 0) {
            return;
        }


        console.log("pollingLoop count = " + connectionsArrayMap[job_group].length);

        var queryStr = 'SELECT parent, status, env, region, start_date, end_date, file_name, machine_name, ' +
            'user_name, start_time, end_time, job_name, error_msg, id FROM jobs_log ' +
            'where job_group=\'' + job_group + '\' order by start_time;';


        queryDB(queryStr, function(rows){

            var new_rows = restructureRows(rows);

            updateSockets(new_rows, job_group, 'update');

            pollingTimer = setTimeout(function(){
                                pollingLoop(job_group)
                            },
                POLLING_INTERVAL);
        });
    };

    // create a new web socket connection to keep the content updated without any AJAX request
    io.sockets.on( 'connection', function ( socket ) {
        console.log("io.sockets.on.connection Starting  .... ");

        var job_group = socket.handshake["query"]["groupname"];
        console.log("io.sockets.on.connection Starting  groupName = " + job_group );

        //todo handle the error where the group name is not passed

        socket.on('error', function (err) {
            console.log("io.sockets.on.error :" + err);
        });

        socket.on('disconnect', function () {
            var socketIndex = connectionsArrayMap[job_group].indexOf( socket );

            if (socketIndex >= 0) {
                connectionsArrayMap[job_group].splice( socketIndex, 1 );
            }

            console.log("io.sockets.on.disconnect :" + socketIndex + ", count = " + connectionsArrayMap[job_group].length);
        });

        console.log("pushing socket :" + job_group);

        if (job_group in connectionsArrayMap) {
            connectionsArrayMap[job_group].push(socket);
        } else {
            connectionsArrayMap[job_group] = [];
            connectionsArrayMap[job_group].push(socket);
        }

        // start the polling loop only if at least there is one user connected
        if (job_group in connectionsArrayMap && connectionsArrayMap[job_group].length == 1) {
            pollingLoop(job_group);
        };

        console.log("io.sockets.on.connection Ends  .... groupName = " + job_group + ", connectionsArrayMap[groupName].length = " + connectionsArrayMap[job_group].length);
    });

    var updateSockets = function (data, job_group, action) {
        for (var socketIndex = 0; socketIndex < connectionsArrayMap[job_group].length; socketIndex++) {

            var tmpSocket = connectionsArrayMap[job_group][socketIndex];

            tmpSocket.volatile.emit(action, data);
        }
    };

    app.use('/', t);
};