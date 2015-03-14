
var createTableHTML = function(hashRows) {
    this.hashRows = {};
    this.hashRows = hashRows;
};

/*
 To compare to hashes we need to do the following:
 1. check if keys in hashRows exist in cachedHashRows(the one we have in class)
 2. check whether the length of each value in hashRows exist in cachedHashRows
 3. for each value check whether the data is same, we are interested to check following columns:
 status
 end_time
 id
 */
var getUpdates = function (cachedRows, hashRows) {
    //console.log("createTableHTML.getUpdates Starting ..... ");

    var ret = {};
    ret["update"] = [];
    ret["delete"] = [];
    ret["load"] = [];

    for (var hashObj in  hashRows) {
        if (!cachedRows || !cachedRows[hashObj]) {
            ret["update"].push(hashRows[hashObj]);
        } else {
            for (var i = 0; i < hashRows[hashObj].length; i++) {
                if (JSON.stringify(cachedRows[hashObj][i]) != JSON.stringify(hashRows[hashObj][i])) {

                    ret["update"].push(hashRows[hashObj]);
                    //break;
                }
            }
        }
    }
    for (hashObj in  cachedRows) {
        if (!hashRows || !hashRows[hashObj]) {
            ret["delete"].push(cachedRows[hashObj]);
        }
    }

    //console.log("createTableHTML.getUpdates Ends ..... ");
    return ret;
};

var rowStyle;
var mainRowStyle;

function getRowStyle(curr_row) {

    if (rowStyle == "odd_light") {
        rowStyle = "ev_light";
    } else {
        rowStyle = "odd_light";
    }

    return rowStyle + "_" + curr_row.status;
}

function getMainRowStyle(curr_row) {

    if (mainRowStyle == "odd_light") {
        mainRowStyle = "ev_light";
    } else {
        mainRowStyle = "odd_light";
    }

    return mainRowStyle + "_" + curr_row.status;
}


function createHTMLRow(curr_row) {

    var tblRow  = "<tr ";

    var checkbox_id = "ck_" + curr_row.id.replace(" ", "_");

    var checked_or_not = "unchecked";

    tblRow = tblRow + "id =" + curr_row.id.replace(" ", "_") + " ";

    var expandControls = "";
    if (curr_row.parent == curr_row.id) {
        var mainRowStyle = getMainRowStyle(curr_row);

        tblRow = tblRow + 'class =' + mainRowStyle + " ";

        expandControls = "<input id=" + checkbox_id + " type=\"checkbox\" name=checkbox_id onclick=\"doCheck(this)\"" + checked_or_not +">" +
            "<img align=\"absmiddle\" src=\"imgs/closed_folder.png\" title=\"Check the group\" onclick=\"doExpand(this, false)\">" + "</td>";
    } else {
        var rowStyle = getRowStyle(curr_row);

        tblRow = tblRow + 'class =' + rowStyle + " ";

        tblRow = tblRow + "style=\"display: none\"; ";
        expandControls = "<input class=invisible id=" + checkbox_id + " type=\"checkbox\" name=checkbox_id onclick=\"doCheck(this)\"" + checked_or_not +">" +
            "<img class=invisible align=\"absmiddle\" src=\"imgs/closed_folder.png\" title=\"Expand\" onclick=\"doExpand(this, false)\">" + "</td>";
    }
    tblRow += ">";

    tblRow +=
        "<td style=\"text-align:center;\">" + expandControls +
            "<td class=invisible title=\"" + curr_row.parent + "\" style=\"width: 3%\">" + curr_row.parent + "</td>" +
            "<td title=\"" + curr_row.status + "\" style=\"width: 4%\">" + curr_row.status + "</td>" +
            "<td title=\"" + curr_row.env + "\"; style=\"width: 5%\">" + curr_row.env + "</td>" +
            "<td title=\"" + curr_row.region + "\" style=\"width: 5%\">" + curr_row.region + "</td>" +
            "<td title=\"" + curr_row.start_date + "\" style=\"width: 10%\">" + curr_row.start_date + "</td>" +
            "<td title=\"" + curr_row.end_date + "\" style=\"width: 10%\">" + curr_row.end_date + "</td>" +
            "<td title=\"" + curr_row.file_name + "\" style=\"width: 10%\">" + curr_row.file_name + "</td>" +
            "<td title=\"" + curr_row.machine_name + "\" style=\"width: 10%\">" + curr_row.machine_name + "</td>" +
            "<td title=\"" + curr_row.user_name + "\" style=\"width: 5%\">" + curr_row.user_name + "</td>" +
            "<td title=\"" + curr_row.start_time + "\" style=\"width: 10%\">" + curr_row.start_time + "</td>" +
            "<td title=\"" + curr_row.end_time + "\" style=\"width: 9%\">" + curr_row.end_time + "</td>" +
            "<td title=\"" + curr_row.job_name + "\" style=\"width: 5%\">" + curr_row.job_name + "</td>" +
            "<td title=\"" + curr_row.error_msg + "\" style=\"width: 9%\">" + curr_row.error_msg + "</td>" +
            "<td class=invisible title=\"" + curr_row.id + "\" style=\"width: 2%\">" + curr_row.id + "</td>";

    tblRow += "</tr>";

    return tblRow;
}

createTableHTML.prototype.create = function(cachedRows){
    /*if (cachedRows) {
        console.log("createTableHTML.prototype.create Starting  .... cachedRows.length = " + Object.keys(cachedRows).length);
    } else {
        console.log("createTableHTML.prototype.create Starting  .... cachedRows.length = 0");
    }
    */
    var tblHtml = "<table id=\"monitor_main_table\" class=\"obj row20px\" cellspacing=\"0\" cellpadding=\"0\" style=\"width: 100%;\">" +
                    "<tbody>" +
                    "<tr style=\"height: auto;\">" +
                    "<th style=\"width: 3%;\"></th>" +
                    "<th style=\"width: 3%;\"></th>" +
                    "<th style=\"width: 4%;\"></th>" +
                    "<th style=\"width: 5%;\"></th>" +
                    "<th style=\"width: 5%;\"></th>" +
                    "<th style=\"width: 10%;\"></th>" +
                    "<th style=\"width: 10%;\"></th>" +
                    "<th style=\"width: 10%;\"></th>" +
                    "<th style=\"width: 10%;\"></th>" +
                    "<th style=\"width: 5%;\"></th>" +
                    "<th style=\"width: 10%;\"></th>" +
                    "<th style=\"width: 9%;\"></th>" +
                    "<th style=\"width: 5%;\"></th>" +
                    "<th style=\"width: 9%;\"></th>" +
                    "<th style=\"width: 2%;\"></th>" +
                    "</tr>" +
                    "<tr style=\"height: auto;\">" +
                    "<td class=head style=\"width: 3%;\">" +
                    "<input id=\"delete_rows\" type=\"button\" class=\"del_btn\" value=\"Delete\" onclick=\"delete_rows()\"> </td>" +
                    "<td class=\"head invisible\" style=\"width: 3%;\">Parent</td>" +
                    "<td class=head style=\"width: 4%;\">Status</td>" +
                    "<td class=head style=\"width: 5%;\">Env</td>" +
                    "<td class=head style=\"width: 5%;\">Region</td>" +
                    "<td class=head style=\"width: 10%;\">Start Date</td>" +
                    "<td class=head style=\"width: 10%;\">End Date</td>" +
                    "<td class=head style=\"width: 10%;\">File Name</td>" +
                    "<td class=head style=\"width: 10%;\">Machine Name</td>" +
                    "<td class=head style=\"width: 5%;\">User Name</td>" +
                    "<td class=head style=\"width: 10%;\">Start Time</td>" +
                    "<td class=head style=\"width: 9%;\">End Time</td>" +
                    "<td class=head style=\"width: 5%;\">Job Name</td>" +
                    "<td class=head style=\"width: 9%;\">Error Msg</td>" +
                    "<td class=\"head invisible\" style=\"width: 2%;\">Id</td>" +
                    "</tr>";


    //console.log("      ------ Starting to compare hashRows .... ");
    var upd = getUpdates(cachedRows, this.hashRows);

    if (upd["update"] && this.hashRows && Object.keys(upd["update"]).length == Object.keys(this.hashRows).length) {
        for (var hashObj in  upd["update"]) {
            for (var i = 0; i < upd["update"][hashObj].length; i++) {
                var curr_row = upd["update"][hashObj][i];

                tblHtml += createHTMLRow(curr_row);
            }
        }
        tblHtml += "</tbody></table>";
    } else {
        tblHtml = "";
    }

    if (tblHtml != "") {
        upd["load"] = [];
        upd["load"].push(tblHtml);
    }

    /*console.log("createTableHTML.prototype.create Ends  .... " +
                    "upd[update].length = " + upd["update"].length +
                    ", upd[load].length = " + upd["load"].length +
                    ", upd[delete].length = " + upd["delete"].length);
    */
    return upd;
};

createTableHTML.prototype.set = function(hashRows){
    this.hashRows = hashRows;
};

module.exports.create = function() {

    return new createTableHTML();
};

module.exports._class = createTableHTML;
