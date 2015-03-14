module.exports = function(app, express, config, db){

    var t = express.Router(),
        fs  = require("fs"),
        url = require('url'),
        excludeXmlList = ["build.xml", "nbbuild.xml"],
        java = require("java");

    java.classpath.push("java/JsonFromJobXML.jar");
    java.classpath.push("java/json-simple-1.1.1.jar");


    // executor part
    t.get('/executor', function(req, res){
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
        fs.readFile('views/executor.html', 'utf8', function(err, text){
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

    t.get('/getXmlContent', function(req, res){
        console.log("app.get('/getXmlContent' Starting  .... " + req.query.xmlFile);

        fs.readFile(config.file_path[req.session.job_group_name] + "xml/" + req.query.xmlFile, 'utf8', function(err, text){
            res.send(text);
        });
        console.log("app.get('/getXmlContent' Ends  .... ");
    });


    t.get('/xmlgraphview', function(req, res){
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


    t.post('/execute', function(req, res){

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

    app.use('/', t);
};