var request = require('request');
var http = require('http');
var resolve = require('./dns_resolver');
var log = require('./bq_logger.js');

http.globalAgent.maxSockets = Infinity;

var PULSAR_URLS = {
  notifications: "internal-mqapi-not-ElasticL-D5MT1NTKB2PM-1938523477.us-east-1.elb.amazonaws.com"
};

var ALLOWED_CLUSTERS = ['notifications'];

Object.keys(PULSAR_URLS).forEach(function (key) {
  resolve(PULSAR_URLS[key], function() {});
});

function publish(cluster, msg, callback) {
  if (ALLOWED_CLUSTERS.indexOf(cluster) != -1) {
    var url = PULSAR_URLS[cluster];
    resolve(url, function(err, addr) {
      if (err) {
        log.log("error", "Error resolving dns [%s]", err);
        return callback(err);
      }
      if (!addr) {
        log.log("error", "Error resolving dns [%s]", addr);
        return callback("error resolving dns");
      }

      request.post({
        url: "http://"+addr+"/messages",
        timeout: 100,
        headers: {
          "Content-Type": "application/json"
        },
        body: msg,
        json: true
      }, function (error, resp, body) {
        if (error) {
          log.log("error", "Error publishing to pulsar addr:", addr, ", error:", error);
          return callback(error);
        }
        if (resp.statusCode > 300) {
          log.log("error", "Error publishing to pulsar addr:", addr, ", status:", resp.statusCode);
          return callback("error publishing to pulsar");
        }
        return callback(null);
      });
    });
  } else {
    callback(null);
  }
}

module.exports = {
  publish: publish
}
