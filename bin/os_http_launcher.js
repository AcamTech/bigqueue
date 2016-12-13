#!/usr/local/bin/node

/**
 * Executable to create the http api client
 * If no config found a default config will be used, this confing
 * will use a redis localhost running at default port
 */
try {
  require("newrelic")
} catch(e) {
  console.log(e)
}
var bq = require('../lib/bq_client.js'),
    bqc = require('../lib/bq_cluster_client.js'),
    http_api = require("../ext/openstack/bq_os_http_api.js"),
    log = require("../lib/bq_logger.js");

var cluster = require('cluster');
var http = require('http');
var numCPUs = require('os').cpus().length - 1;
var externalConfig = process.argv[2]


//Default redis conf
var redisLocalhost = {
    host:"127.0.0.1",
    port:6379,
    "options":{"command_queue_high_water":5000,"command_queue_low_water":1000}
}

//Default api conf
var httpApiConfig = {
    "port": 8081,
    "bqConfig": redisLocalhost,
    "bqClientCreateFunction": bq.createClient,
    "logLevel":"info"
}

//Check for external config
var config
if(externalConfig){
    config = require(externalConfig).httpApiConfig
}else{
    config = httpApiConfig
}

log.setLevel(config.logLevel || "error");
//Run config
console.log("Using config: "+JSON.stringify(config))

  // Worker processes have a http server.
if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', function(worker) {
    console.log('worker ' + worker.pid + ' died, process restarted');
    setTimeout(function(){
        console.log("restarting fork")
        cluster.fork()
    },1000)
  });
} else {
  // Worker processes have a http server.
    http_api.startup(config)
}
