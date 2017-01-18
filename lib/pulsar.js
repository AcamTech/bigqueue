var request = require('request');
var http = require('http');
var resolve = require('./dns_resolver');
var log = require('./bq_logger.js');

http.globalAgent.maxSockets = Infinity;

var PULSAR_URLS = {
  notifications: "internal-mqapi-not-ElasticL-D5MT1NTKB2PM-1938523477.us-east-1.elb.amazonaws.com"
};

var ADMIN_URL = "internal-mqadmin-p-ElasticL-ECAW0CC7QU2O-1527504892.us-east-1.elb.amazonaws.com"

var ALLOWED_CLUSTERS = ['notifications'];

Object.keys(PULSAR_URLS).forEach(function (key) {
  resolve(PULSAR_URLS[key], function() {});
});
resolve(ADMIN_URL, function() {});

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

function getMessage(cluster, topic, consumer, callback) {
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

      request.get({
        url: "http://"+addr+"/topics/"+topic+"/consumers/"+consumer+"/messages",
        timeout: 100,
        headers: {
          "Content-Type": "application/json"
        },
        json: true
      }, function (error, resp, body) {
        if (error) {
          log.log("error", "Error consuming from pulsar addr:", addr, ", error:", error);
          return callback(error);
        }
        return callback(null, resp.statusCode, body);
      });
    });
  } else {
    callback("cluster not enabled");
  }
}

function ackMessage(cluster, topic, consumer, id, callback) {
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

      request.delete({
        url: "http://"+addr+"/topics/"+topic+"/consumers/"+consumer+"/messages/"+id,
        timeout: 100,
        headers: {
          "Content-Type": "application/json"
        }
      }, function (error, resp, body) {
        if (error) {
          log.log("error", "Error acknowledging to pulsar addr:", addr, ", error:", error);
          return callback(error);
        }
        return callback(null, resp.statusCode, body);
      });
    });
  } else {
    callback("cluster not enabled");
  }
}

function getConsumer(topic, consumer, callback) {
  resolve(ADMIN_URL, function(err, addr) {
    if (err) {
      log.log("error", "Error resolving dns [%s]", err);
      return callback(err);
    }
    if (!addr) {
      log.log("error", "Error resolving dns [%s]", addr);
      return callback("error resolving dns");
    }

    request.get({
      url: "http://"+addr+"/topics/"+topic+"/consumers/"+consumer,
      timeout: 100,
      headers: {
        "Content-Type": "application/json"
      },
      json: true
    }, function (error, resp, body) {
      if (error) {
        log.log("error", "Error getting consumer from pulsar addr:", addr, ", error:", error);
        return callback(error);
      }
      return callback(null, resp.statusCode, body);
    });
  });
}

function resetConsumer(topic, consumer, callback) {
  resolve(ADMIN_URL, function(err, addr) {
    if (err) {
      log.log("error", "Error resolving dns [%s]", err);
      return callback(err);
    }
    if (!addr) {
      log.log("error", "Error resolving dns [%s]", addr);
      return callback("error resolving dns");
    }

    request.put({
      url: "http://"+addr+"/topics/"+topic+"/consumers/"+consumer,
      timeout: 100,
      headers: {
        "Content-Type": "application/json"
      },
      body: {},
      json: true
    }, function (error, resp, body) {
      if (error) {
        log.log("error", "Error resetting pulsar consumer addr:", addr, ", error:", error);
        return callback(error);
      }
      return callback(null, resp.statusCode, body);
    });
  });
}

module.exports = {
  publish: publish,
  getMessage: getMessage,
  ackMessage: ackMessage,
  getConsumer: getConsumer,
  resetConsumer: resetConsumer
}
