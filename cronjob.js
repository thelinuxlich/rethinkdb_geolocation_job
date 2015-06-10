"use strict";

const CronJob = require('cron').CronJob,
    r = require('rethinkdbdash')(), // enter your RethinkDB config here
    geo_job = require("./jobs/pending_geo")(r);

// It will run every 30 seconds
new CronJob("*/30 * * * * *", geo_job, null, true);