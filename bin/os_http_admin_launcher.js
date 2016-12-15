#!/usr/local/bin/node

/**
 * Executable to create the http api client
 * If no config found a default config will be used, this confing
 * will use a redis localhost running at default port
 */

try {
  require("newrelic")
} catch(e) {}
var adm_api = require("../ext/openstack/bq_os_admin_http_api.js");
var log = require("../lib/bq_logger.js");
var cluster = require('cluster');
var http = require('http');
var numCPUs = require('os').cpus().length;
var externalConfig = process.argv[2]

var numCPUs = require('os').cpus().length - 1;
console.log("Starting")

var admConfig = {
    "logLevel":"error",
    "adminRoleId":"",
    "mysqlConf":{
        host     : '127.0.0.1',
        port     : 3306,
        user     : 'root',
        password : 'root',
        database : 'bigqueue'
    }
}

var httpConfig = {
    "admConfig":admConfig,
    "port":8080,
    "logLevel":"error",
    "maxTtl":4*24*60*60,
    "maxTtl_description":"4 days"
}

//Check for external config
var config
console.log(externalConfig)
console.log("Loading ["+externalConfig+"]");
config = require(externalConfig).httpApiConfig;
log.setLevel(config.logLevel || "error");

//Run config
console.log("Using config: "+JSON.stringify(config))

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  cluster.on('exit',function(worker, code, signal) {
    console.log("worker "+worker.process.pid+" died --> Creating new fork");
    cluster.fork();
  });
} else {
  adm_api.startup(config)
}

console.log("Admin api started")
