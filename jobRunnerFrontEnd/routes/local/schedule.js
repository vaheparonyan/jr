module.exports = function(app, express, config, db){

    var t = express.Router(),
        fs  = require("fs"),
        schedule = require('node-schedule'),
        uuid = require('node-uuid'),
        numberOfRepeatedEvents = 10,
        scheduledJobs = {},
        allEventData = [];

    require('date-utils');

    t.get('/schedule', function(req, res){
        console.log("app.get('/schedule' Starting  .... ");

        fs.readFile('./views/schedule.html', 'utf8', function(err, xml){
            res.send(xml);
        });

        console.log("app.get('/schedule' Ends  .... ");
    });

    var eventData = [
        {
            index: "1",
            parent: "1",
            title: 'brightEdge.xml',
            start: '2014-10-02 01:25',
            job_flow: 'brightEdge.xml',
            range: 'true',
            env: 'production',
            region: 'na',
            start_day: '-7',
            end_day: '-1',
            repeat:
            {
                frequency: 'day',
                occurance: { day: '3' },
                end: { type: 'no_end_date' }
            }
        }
    ];

    var constructData = function() {
        for (var i = 0; i < eventData.length; i++) {
            if (eventData[i]["repeat"]) {
                var repeatData = createRepeatEvents(eventData[i]);
                allEventData = allEventData.concat(repeatData);
            } else {
                allEventData.push(eventData[i]);
            }
            //scheduleJob(eventData[i].index, "update");
        }
    };

    var scheduleJob = function(index, action) {

        if (index in scheduledJobs) {
            scheduledJobs[index].cancel();
        }

        var event = null;
        for (var i = 0; i < eventData.length; i++) {
            if (eventData[i].index == index) {
                event = eventData[i];
                break;
            }
        }

        if (action === "update") {
            if (!event) {
                throw "Could not find event with index= " + index;
            }

            if (!event.repeat) {
                //run it once
                var CronJob = require('cron').CronJob;
                var job = new CronJob(new Date(event.start), function(){
                        console.log("runs once at the specified date.");
                    }, function () {
                        console.log("This function is executed when the job stops");
                    },
                    false
                );
            } else {
                var executionTime = '00 * * * * 1-5';
                /*
                 1 - Seconds: 0-59
                 2 - Minutes: 0-59
                 3 - Hours: 0-23
                 4 - Day of Month: 1-31
                 5 - Months: 0-11
                 6 - Day of Week: 0-6
                 */
                var CronJob = require('cron').CronJob;
                var job = new CronJob({

                    cronTime: '00 * * * * 1-5',
                    onTick: function() {
                        var stDate = new Date(event.start),
                            endDate = new Date(event.start);

                        stDate.setDate(event.start_day);
                        endDate.setDate(event.end_day);

                        console.log("java -env " + event.env +
                                        " -range " + event.range +
                                        " -region " + event.region +
                                        " -start_day " + stDate.toFormat("YYYY-MM-DD") +
                                        " -end_day " + endDate.toFormat("YYYY-MM-DD") +
                                        " -jobs + " + event.job_flow +
                                        " -date_range false");

                        // Runs every weekday (Monday through Friday)
                        // at 11:30:00 AM. It does not run on Saturday
                        // or Sunday.
                    },
                    start: false
                });
                scheduledJobs.index = job;
                job.start();
            }
        }
    };

    var findEventIndexAndClean = function(event) {
        var index = null;
        for (var i = 0; i < eventData.length; i++ ) {
            if(event.parent === eventData[i].index) {
                index = eventData[i].index;
                eventData.splice(i, 1);
                break;
            }
        }
        return index;
    };

    var findEventAndClean = function(index) {
        var data = {};
        data.delete = [];
        for (var i = allEventData.length-1; i >= 0; i-- ) {
            if(allEventData[i].index === index) {
                data.delete.push(allEventData[i].index);
                data.event = JSON.parse(JSON.stringify(allEventData[i]));
                allEventData.splice(i, 1);
            } else if (allEventData[i].parent === index) {
                data.delete.push(allEventData[i].index);
                allEventData.splice(i, 1);
            }
        }
        return data;

    };

    var getNextIndex = function() {
        var index = 1;
        for (var i = 0; i < eventData.length; i++ ) {
            if (parseInt(eventData[i].index) === index) {
                index++;
            }
        }
        return index.toString();
    };

    var applyDelta = function (event, delta) {
        var dt = new Date(event.start);

        var negative = false;
        if (parseInt(delta.years) < 0 ||
            parseInt(delta.months) < 0 ||
            parseInt(delta.days) < 0 ||
            parseInt(delta.hours) < 0 ||
            parseInt(delta.min) < 0) {

            negative = true;
        }

        if (negative) {
            dt.addMinutes(parseInt(delta.min)); // add minutes to existing time
            dt.addHours(parseInt(delta.hours)); // add hours to existing time
            dt.addDays(parseInt(delta.days)); // add days to existing time
            dt.addMonths(parseInt(delta.months)); // add months to existing time
            dt.addYears(parseInt(delta.years)); // add years to existing time
        } else {
            dt.addYears(parseInt(delta.years)); // add years to existing time
            dt.addMonths(parseInt(delta.months)); // add months to existing time
            dt.addDays(parseInt(delta.days)); // add days to existing time
            dt.addHours(parseInt(delta.hours)); // add hours to existing time
            dt.addMinutes(parseInt(delta.min)); // add minutes to existing time
        }

        event.start = dt.toFormat("YYYY-MM-DD HH:MI");
    };

    t.get('/scheduled_data', function(req, res) {
        res.send(allEventData);
    });

    t.get('/scheduled_job_flow_files', function(req, res) {

        var job_group = req.param('job_group', null);
        if (job_group !== null) {
            req.session.job_group_name = job_group;
        }
        else if (job_group === null && req.session.job_group_name == undefined) {
            req.session.job_group_name = 'default';
        }

        var xmlFiles = fs.readdirSync(config.file_path[req.session.job_group_name] + 'xml/');

        var jobs = {};
        //empty option on top of list
        jobs[""] = "";
        for (var i = 0; i < xmlFiles.length; i++) {
            var parts = xmlFiles[i].split(".");
            if (parts[parts.length - 1] == "xml") {
                jobs[xmlFiles[i]] = xmlFiles[i];
            }
        }
        res.send(jobs);
    });

    t.post('/save_scheduled_data', function(req, res) {

        console.log("/save_data Starting  ....");

        var index = findEventIndexAndClean(req.body);

        var data = {};
        if (index) {
            // this is existing event, and we are modifying it
            data = findEventAndClean(index);
            if (!data.event) {
                throw ("Something went wrong, did not get event for index = " + index.toString());
            }
        } else {
            //this is new event
            index = getNextIndex();
        }

        if (!data.event) {
            data.event = {};
        }

        data.event.end_day = req.body.end_day;
        data.event.env = req.body.env;
        //data.event.index = req.body.index;
        data.event.job_flow = req.body.job_flow;
        data.event.parent = req.body.parent;
        data.event.range = req.body.range;
        data.event.region = req.body.region;
        if (req.body.repeat) {
            data.event.repeat = JSON.parse(JSON.stringify(req.body.repeat));
        }

        var dtNew = new Date(req.body.start);

        data.event.start = dtNew.toFormat("YYYY-MM-DD HH:MI");

        data.event.start_day = req.body.start_day;
        data.event.title = req.body.title;

        data.event.index = index;
        var allData = [];
        if ( data.event["repeat"]) {
            var repeatData = createRepeatEvents(data.event);
            allEventData = allEventData.concat(repeatData);
            allData = allData.concat(repeatData);
        } else {
            data.event.index = index;
            data.event.parent = index;

            allEventData.push(data.event);
            allData.push(data.event);
        }

        eventData.push(data.event);

        var retData = {};
        retData.oldData = data;
        retData.newData = allData;

        console.log("/save_data Ends  ....");
        res.send(retData);

        scheduleJob(index, "update");
    });

    t.post('/drop_scheduled_data', function(req, res) {

        console.log("/drop_data Starting  ....");

        for (var i = 0; i < eventData.length; i++ ) {
            if(req.body.parent === eventData[i].index) {
                applyDelta(eventData[i], req.body);
            }
        }
        for (var i = 0; i < allEventData.length; i++) {
            if(allEventData[i].parent === req.body.parent &&
                allEventData[i].parent !== allEventData[i].index) {

                applyDelta(allEventData[i], req.body);
            }
        }

        console.log("/drop_data Ends  ....");
        res.send("");
        //scheduleJob(req.body.parent, "update");
    });

    t.post('/delete_scheduled_data', function(req, res) {

        console.log("/delete_data Starting  ....");

        var index = findEventIndexAndClean(req.body);

        var data = {};
        if (index) {
            // this is existing event, and we are modifying it
            data = findEventAndClean(index);
            if (!data.event) {
                throw ("Something went wrong, did not get event for index = " + index.toString());
            }
        } else {
            //this is new event
            index = getNextIndex();
        }

        data.event.end_day = req.body.end_day;
        data.event.env = req.body.env;
        //data.event.index = req.body.index;
        data.event.job_flow = req.body.job_flow;
        data.event.parent = req.body.parent;
        data.event.range = req.body.range;
        data.event.region = req.body.region;
        if (req.body.repeat) {
            data.event.repeat = JSON.parse(JSON.stringify(req.body.repeat));
        }

        var dtNew = new Date(req.body.start);

        data.event.start = dtNew.toFormat("YYYY-MM-DD HH:MI");

        data.event.start_day = req.body.start_day;
        data.event.title = req.body.title;

        eventData.push(data.event);

        var retData = {};
        retData.oldData = data;
        retData.newData = [];

        console.log("/delete_data Ends  ....");
        res.send(retData);

        scheduleJob(req.body.parent, "delete");
    });

    // the main event will have an index equal to parent, and the repeated events,
    // i.e. the children will have index equal to <parent index>_<number>
    var createRepeatEvents = function(event) {
        var repeatEvents = [];
        repeatEvents.push(event);


        var repeat = event.repeat;
        if (repeat.frequency === 'day') {
            var step = parseInt(repeat.occurance.day);
            if (repeat.end.type === 'no_end_date') {
                for (var i = 1; i < numberOfRepeatedEvents; i++ ) {
                    var newData = JSON.parse(JSON.stringify(event));
                    var dt = new Date(newData.start);

                    dt.addDays(step*i); // add days to existing time

                    newData.start = dt.toFormat("YYYY-MM-DD HH:MI");
                    newData.index += "_" + i.toString();

                    repeatEvents.push(newData);
                }
            }
        }
        return repeatEvents;
    };

    constructData();

    app.use('/', t);
};