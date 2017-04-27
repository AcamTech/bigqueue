var request = require('request');
var async = require('async');
var http = require('http');
var resolve = require('./dns_resolver');
var log = require('./bq_logger.js');

http.globalAgent.maxSockets = Infinity;

var PULSAR_URLS = {
  notifications: "internal-mqapi-not-ElasticL-D5MT1NTKB2PM-1938523477.us-east-1.elb.amazonaws.com",
  default: "internal-mqapi-def-ElasticL-GVYUVZW490E0-547389323.us-east-1.elb.amazonaws.com",
  default2: "internal-default2-mqapi-46b3d5-1413713165.us-east-1.elb.amazonaws.com",
  apicore: "internal-apicore-mqapi-9a3c98-2099306125.us-east-1.elb.amazonaws.com"
};

var ADMIN_URL = "internal-mqadmin-p-ElasticL-ECAW0CC7QU2O-1527504892.us-east-1.elb.amazonaws.com"

var ALLOWED_CLUSTERS = ['notifications', 'default', 'default2', 'apicore'];

var FULLY_MIGRATED_CLUSTERS = ["notifications", "default", "default2", "apicore"];

var CONSUMER_TYPES = {
  notifications: "pulsar",
  default: "pulsar",
  default2: "pulsar",
  apicore: "pulsar"
};

if (process.env.SERV == "admin_api") {
  async.retry({times: 30, interval: 10}, function(cb, res) {
    resolve(ADMIN_URL, function(err) {
      if (err) {
        log.log("error", "Error on initial resolve for", ADMIN_URL, err);
      }
      cb(err);
    });
  }, function(err, res) {
    if (err) {
      log.log("error", "Failed initial resolve for", ADMIN_URL, err);
    } else {
      log.log("error", "Successful initial resolve for", ADMIN_URL, err);
    }
  });
}

function publish(cluster, msg, callback) {
  if (ALLOWED_CLUSTERS.indexOf(cluster) != -1) {
    var url = PULSAR_URLS[cluster];
    resolve(url, function(err, addr) {
      if (err) {
        log.log("error", "Error resolving dns for url " + url, err);
        return callback(err);
      }
      if (!addr) {
        log.log("error", "Error resolving dns no address returned", addr);
        return callback("error resolving dns");
      }

      request.post({
        url: "http://"+addr+"/messages",
        timeout: 1200,
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
        timeout: 1200,
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

      request.del({
        url: "http://"+addr+"/topics/"+topic+"/consumers/"+consumer+"/messages/"+id,
        timeout: 1200,
        headers: {
          "Content-Type": "application/json"
        }
      }, function (error, resp, body) {
        if (error) {
          log.log("error", "Error acknowledging to pulsar addr:", addr, ", error:", error);
          return callback(error);
        }
        if (resp.statusCode >= 300) {
          log.log("error", "Error acknowledging to pulsar addr:", addr, ", error:", resp.statusCode);
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
      log.log("error", "Error resolving dns " + ADMIN_URL, err);
      return callback(err);
    }
    if (!addr) {
      log.log("error", "Error resolving dns [%s]", addr);
      return callback("error resolving dns");
    }

    request.get({
      url: "http://"+addr+"/topics/"+topic+"/consumers/"+consumer,
      timeout: 1200,
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

function createTopic(topic, cluster, callback) {
  if (ALLOWED_CLUSTERS.indexOf(cluster) != -1 && process.env.SERV == "admin_api") {
    resolve(ADMIN_URL, function(err, addr) {
      if (err) {
        log.log("error", "Error resolving dns " + ADMIN_URL, err);
        return callback(err);
      }
      if (!addr) {
        log.log("error", "Error resolving dns [%s]", addr);
        return callback("error resolving dns");
      }

      request.post({
        url: "http://"+addr+"/topics",
        timeout: 10000,
        headers: {
          "Content-Type": "application/json"
        },
        body: {
          "name": topic,
          "cluster": cluster
        },
        json: true
      }, function (error, resp, body) {
        if (error) {
          log.log("error", "Error creating topic from pulsar addr:", addr, ", error:", error);
          return callback(error);
        }
        return callback(null, resp.statusCode, body);
      });
    });
  } else {
    callback();
  }
}

function getTopic(topic, callback){
  resolve(ADMIN_URL, function(err, addr) {
      if (err) {
        log.log("error", "Error resolving dns " + ADMIN_URL, err);
        return callback(err);
      }
      if (!addr) {
        log.log("error", "Error resolving dns [%s]", addr);
        return callback("error resolving dns");
      }

      request.get({
        url: "http://"+addr+"/topics/"+topic,
        timeout: 5000,
        headers: {
          "Content-Type": "application/json"
        },
        json: true
      }, function (error, resp, body) {
        if (error) {
          log.log("error", "Error getting topic from pulsar addr:", addr, ", error:", error);
          return callback(error);
        }
        return callback(null, resp.statusCode, body);
      });
    });
}

function createConsumer(topic, consumer, cluster, type, callback) {
  if (ALLOWED_CLUSTERS.indexOf(cluster) != -1 && process.env.SERV == "admin_api") {
    var enabled = false;
    if (consumerTypeForCluster(cluster) == "pulsar") {
      enabled = true;
    }
    resolve(ADMIN_URL, function(err, addr) {
      if (err) {
        log.log("error", "Error resolving dns " + ADMIN_URL, err);
        return callback(err);
      }
      if (!addr) {
        log.log("error", "Error resolving dns [%s]", addr);
        return callback("error resolving dns");
      }

      request.post({
        url: "http://"+addr+"/topics/"+topic+"/consumers",
        timeoutmigq: 5000,
        headers: {
          "Content-Type": "application/json"
        },
        body: {
          "name": consumer,
          "enabled": enabled,
          "type": type
        },
        json: true
      }, function (error, resp, body) {
        if (error) {
          log.log("error", "Error creating consumer from pulsar addr:", addr, ", error:", error);
          return callback(error);
        }
        return callback(null, resp.statusCode, body);
      });
    });
  } else {
    callback();
  }
}

function destroyConsumer(topic, consumer, callback) {
  if (process.env.SERV == "admin_api") {
    resolve(ADMIN_URL, function(err, addr) {
      if (err) {
        log.log("error", "Error resolving dns " + ADMIN_URL, err);
        return callback(err);
      }
      if (!addr) {
        log.log("error", "Error resolving dns [%s]", addr);
        return callback("error resolving dns");
      }

      request.del({
        url: "http://"+addr+"/topics/"+topic+"/consumers/"+consumer,
        timeout: 5000,
        headers: {
          "Content-Type": "application/json"
        },
        json: true
      }, function (error, resp, body) {
        if (error) {
          log.log("error", "Error destroying consumer from pulsar addr:", addr, ", error:", error);
          return callback(error);
        }
        return callback(null, resp.statusCode, body);
      });
    });
  } else {
    callback();
  }
}

function resetConsumer(topic, consumer, callback) {
  resolve(ADMIN_URL, function(err, addr) {
    if (err) {
      log.log("error", "Error resolving dns " + ADMIN_URL, err);
      return callback(err);
    }
    if (!addr) {
      log.log("error", "Error resolving dns [%s]", addr);
      return callback("error resolving dns");
    }

    request.put({
      url: "http://"+addr+"/topics/"+topic+"/consumers/"+consumer,
      timeout: 5000,
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

function consumerTypeForCluster(cluster) {
  return CONSUMER_TYPES[cluster] || "native";
}

module.exports = {
  publish: publish,
  getMessage: getMessage,
  ackMessage: ackMessage,
  getConsumer: getConsumer,
  resetConsumer: resetConsumer,
  createConsumer: createConsumer,
  destroyConsumer: destroyConsumer,
  createTopic: createTopic,
  getTopic: getTopic,
  FULLY_MIGRATED_CLUSTERS: FULLY_MIGRATED_CLUSTERS,
  ALLOWED_CLUSTERS: ALLOWED_CLUSTERS,
  consumerTypeForCluster: consumerTypeForCluster
}
