var events = require("events"),
    utils = require("../lib/bq_client_utils.js"),
    log = require("../lib/bq_logger.js"),
    mysql = require('mysql'),
    async = require("async"),
    os = require("os"),
    jsdog = require("jsdog-meli").configure(),
    METRICS_INTERVAL=100;
function BigQueueClustersAdmin(defaultCluster, mysqlConf, defaultTtl){
    this.defaultCluster = defaultCluster
    this.shutdowned = false
    this.asyncStatsQueue = [];
    this.mysqlPool = mysql.createPool(mysqlConf.default || mysqlConf);
    this.mysqlPool.on('connection', function(connection) {
        connection.query('SET autocommit=1');
    });

    if(mysqlConf.critic) {
      this.mysqlCriticPool = mysql.createPool(mysqlConf.critic || mysqlConf);
      this.mysqlCriticPool.on('connection', function(connection) {
          connection.query('SET autocommit=1');
      });
    } else {
      this.mysqlCriticPool = this.mysqlPool;
    }

    if(mysqlConf.tasks) {
      this.mysqlTasksPool = mysql.createPool(mysqlConf.tasks || mysqlConf);
      this.mysqlTasksPool.on('connection', function(connection) {
          connection.query('SET autocommit=1');
      });
    } else {
      this.mysqlTasksPool = this.mysqlPool
    }

    this.local_tasks_counter = 0;
    this.defaultTtl = defaultTtl || 259200; //3 days
    this.init()
    this.updateNodeMetricsAsync();
}
function getCriteriaQuery(criteria, prefix) {
  var query = "";
  var elems = [];
  if(criteria && Object.keys(criteria).length > 0 ) {
    query +="WHERE ";
    Object.keys(criteria).forEach(function(e) {
      var name = e;
      //Nprmalize names
      if(prefix) {
        if(name.indexOf(".") == -1) {
          name = prefix+name;
        }
      }
      if(criteria[e] instanceof Array) {
        if(criteria[e].length > 0) {
          query += "?? in (";
          elems.push(name);
          var in_q = "";
          criteria[e].forEach(function(val) {
            in_q +=",?"
            elems.push(val);
          });
          in_q = in_q.length > 0 ? in_q.substr(1) : "";
          query +=in_q + ") AND"
        }
      } else {
        query+="?? = ? AND ";
        elems.push(name, criteria[e]);
      }
    });
    query = query.substring(0, query.lastIndexOf(" AND"));
  }
  return {query: query, args: elems};
}
BigQueueClustersAdmin.prototype = Object.create(require('events').EventEmitter.prototype);


BigQueueClustersAdmin.prototype.shutdown = function(){
    this.shutdowned = true
    this.mysqlPool.end();
}

BigQueueClustersAdmin.prototype.init = function(){
  var self = this;
  process.nextTick(function() {
    self.emit("ready")
  });

}

BigQueueClustersAdmin.prototype.createBigQueueCluster = function(clusterData,callback){
  var self = this
  if(!clusterData.name){
      return callback({"msg":"No cluster name exists","code":404})
  }

  if(!clusterData.nodes) {
    clusterData.nodes = [];
  }

  if(!clusterData.journals) {
    clusterData.journals = [];
  }

  if(!clusterData.endpoints) {
    clusterData.endpoints = [];
  }
  var err = clusterData.nodes.concat(clusterData.journals).some(function(e) {
    return !self.validateNodeJson(e);
  });

  if(err) {
    return callback({msg: "Some journal or node has incorrect parameters"});
  }
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      log.log("error", "Error getting connection from mysql on createBigQueueCluster", err);
      return callback(err);
    }
    mysqlConn.query("INSERT INTO clusters (name) VALUES (?)",[clusterData.name], function(err) {
      mysqlConn.release();
      if(err) {
        return callback(err);
      }
      async.series([
        function(endSection) {
          async.each(clusterData.journals, function(e, cb) {
            self.addJournalToCluster(clusterData.name,e, cb);
          }, endSection);
        },
        function(endSection) {
          async.each(clusterData.nodes, function(e, cb) {
            self.addNodeToCluster(clusterData.name,e, cb);
          }, endSection);
        },
        function(endSection) {
          async.each(clusterData.endpoints, function(e, cb) {
            self.addEndpointToCluster(clusterData.name,e, cb);
          }, endSection);
        }

      ],function(err) {
        callback(err);
      });
    });
  });
}

BigQueueClustersAdmin.prototype.createNode = function(clusterName,nodesData,nodeType,callback){
  nodesData.options = nodesData.options ? nodesData.options : {};
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      log.log("error", "Error getting conection from mysql on createNode",err);
      return callback(err);
    }
    async.series([
      function(d) {
        mysqlConn.beginTransaction(d);
      },
      function(d) {
        mysqlConn.query("INSERT INTO data_nodes (id, host, port, status, type, options, cluster) VALUES(?,?,?,?,?,?,?)",
                      [nodesData.id, nodesData.host, nodesData.port, nodesData.status, nodeType, JSON.stringify(nodesData.options), clusterName],d);
      },
      function(d) {
        if(nodesData.journals && nodesData.journals.length > 0) {
          async.each(nodesData.journals, function(e, cb) {
            mysqlConn.query("INSERT INTO node_journals VALUES (?,?)",[nodesData.id,e],cb);
          },d);
        } else {
          d();
        }
      },
      function(d) {
        mysqlConn.commit(d);
      }
    ], function(err) {
      if(err) {
        mysqlConn.rollback(function() {
          mysqlConn.release();
          callback(err);
        })
      } else {
        mysqlConn.release();
        callback(err);
      }
    });
  });
}

BigQueueClustersAdmin.prototype.validateNodeJson = function(node,callback){
    if(node.id == undefined ||
            node.host == undefined ||
            node.host == "" ||
            node.port == undefined ||
            node.port == "" ){
        if(callback)
          callback({"msg":"Node should have a name and a config","code":406})
        return false
    }
    return true
}

BigQueueClustersAdmin.prototype.addNodeToCluster = function(cluster,node,callback){
  var self = this;
  if(this.validateNodeJson(node)){
      async.series([
        function(cb) {
          self.createNode(cluster,node,"node",function(err){
              if(err)
                  err={"msg":err}
              cb(err)
          })
        }, function(cb) {
          self.getClusterData(cluster, true,function(err, data) {
            if(err) {
              return cb(err);
            }
            var tasks = [];
            data.topics.forEach(function(topic) {
              tasks.push({data_node_id: node.id, task_type:"CREATE_TOPIC", task_data: { topic_id:topic.topic_id, ttl: topic.ttl}});
              topic.consumers.forEach(function(consumer) {
                tasks.push({data_node_id: node.id, task_type:"CREATE_CONSUMER", task_data: { topic_id:topic.topic_id, consumer_id: consumer.consumer_id}});
              });
            });
            self.createTasks(tasks, cb);
          });
        }
      ], callback);
    } else {
      callback({msg: "Data is not valid for create node"})
    }
}

BigQueueClustersAdmin.prototype.addJournalToCluster = function(cluster,node,callback){
    if(this.validateNodeJson(node)){
        this.createNode(cluster,node,"journal",function(err){
            if(err)
                err={"msg":err}

            callback(err)
        })
    } else {
      callback({msg: "Data is not valid for create journal"})
    }
}

BigQueueClustersAdmin.prototype.addEndpointToCluster = function(cluster,node,callback){
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      log.log("error", "Error getting connection from mysql on addEnpointToCluster", err);
      return callback(err);
    }
    mysqlConn.query("INSERT INTO endpoints (host, port, description, cluster) VALUES (?,?,?,?)",[node.host,node.port,node.description,cluster], function(err) {
      mysqlConn.release();
      callback(err && {msg: JSON.stringify(err)});
    });
  });
}

BigQueueClustersAdmin.prototype.updateJournalData = function(clusterName,node,callback){
  this.updateGenericNodeData(clusterName, node, "journals", callback);
}
BigQueueClustersAdmin.prototype.updateNodeData = function(clusterName,node,callback){
  this.updateGenericNodeData(clusterName, node, "nodes", callback);
}
BigQueueClustersAdmin.prototype.updateGenericNodeData = function(clusterName,node,type,callback){
    var self = this
    var query = "UPDATE data_nodes SET";
    if(node.cluster || node.type || node.host || node.port) {
      return callback({msg:"You can't modify primary properties, in that case create a new node and delete this"});
    }
    if(!node.id) {
      return callback({msg: "id is required"});
    }
    var queryValues = [];
    Object.keys(node).forEach(function(e) {
      if(e != "journals" && e != "id") {
        query+=" ?? = ?, ";
        queryValues.push(e, node[e]);
      }
    });
    query = query.substring(0,query.lastIndexOf(", "));
    query+=" WHERE ?? = ?";
    queryValues.push("id", node.id);
    this.mysqlPool.getConnection(function(err, mysqlConn) {
      if(err) {
        if(mysqlConn) {
          mysqlConn.release();
        }
        log.log("error", "Error getting connection from mysql on updateGenericNodeData", err);
        return callback(err);
      }
      async.series([
        function(d) {
          mysqlConn.beginTransaction(d);
        }, function(d) {
          //If you only change journals will not update this
          if(queryValues.length > 2) {
            mysqlConn.query(query, queryValues, d);
          } else {
            d();
          }
        }, function(d) {
          if(node.journals) {
            mysqlConn.query("DELETE FROM node_journals WHERE node_id = ?",node.id, function(err) {
              if(err) {
                d(err);
              }
              async.each(node.journals, function(e, cb) {
                mysqlConn.query("INSERT INTO node_journals VALUES (?,?)",[node.id,e],cb);
              },d);
            });
          } else {
            d();
          }
        }, function(d) {
          mysqlConn.commit(d);
        }
      ],function(err) {
        if(err) {
          mysqlConn.rollback(function() {
            mysqlConn.release();
            callback(err);
          });
        } else {
          mysqlConn.release();
          callback(err);
        }
      });
    });
}

BigQueueClustersAdmin.prototype.getDefaultCluster = function(callback) {
 this.mysqlPool.getConnection(function(err, mysqlConn) {
  if(err) {
    if(mysqlConn) {
      mysqlConn.release();
    }
    log.log("error", "Error getting connection from mysql on getDefaultCluster",err);
    return callback(err);
  }
  mysqlConn.query("SELECT name FROM clusters WHERE `default` = ? limit 1",["Y"], function(err, data) {
    mysqlConn.release();
    if(err) {
      callback(err);
    } else {
      callback(undefined, data && data[0] && data[0].name);
    }
  });
 });
}

BigQueueClustersAdmin.prototype.createTopic = function(topic,callback){
 var self = this;
 if(!topic.name || !topic.tenant_id || ! topic.tenant_name) {
  return callback({msg: "name, tenant_id and tenant_name are required" });
 }
 topic.ttl = topic.ttl || self.defaultTtl;
 this.mysqlPool.getConnection(function(err, mysqlConn) {
  if(err) {
    log.log("error", "Error getting connection from mysql on createTopic",err);
    return callback(err);
  }
  var id = topic.tenant_id+"-"+topic.tenant_name+"-"+topic.name;
  var cluster;
  async.series([
   function(cb) {
    mysqlConn.beginTransaction(cb);
   },
   function(cb) {
    if(topic.cluster) {
      mysqlConn.query("SELECT name FROM clusters where name = ?",[topic.cluster], function(err, data) {
       if(err) {
        cb(err);
       }else if(data.length == 0) {
        cb({"err":"Cluster "+topic.cluster+" not found"});
       } else {
         cluster = topic.cluster;
         cb();
       }
      })
    } else {
      self.getDefaultCluster(function(err, c) {
        cluster = c;
        cb(err);
      });
    }
  },
  function(cb) {
    mysqlConn.query("INSERT INTO topics (topic_id, tenant_id, tenant_name, topic_name, cluster, ttl, create_time) VALUES (?,?,?,?,?,?, now())",
                   [id, topic.tenant_id, topic.tenant_name, topic.name, cluster, topic.ttl], cb);
  },
  function(cb) {
    self.createTasksForAllNodes(cluster,{task_type:"CREATE_TOPIC", task_data:{topic_id: id, ttl: topic.ttl}},cb);
  }
  ], function(err) {
   function ret(err) {
     mysqlConn.release();
     if(err) {
        callback(err);
      } else {
        self.getTopicData(id,callback);
      }
    }
    if(err) {
      mysqlConn.rollback(function(e) {
        ret(e || err);
      });
    } else {
      mysqlConn.commit(ret);
    }
  });
 });
}

BigQueueClustersAdmin.prototype.deleteTopic = function(topic,callback){
  var self = this
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      log.log("error", "Error getting conectin from mysql in deleteTopic", err);
      return callback(err);
    }
    var cluster;
    async.series([
     function(cb) {
        mysqlConn.beginTransaction(cb);
      },
     function(cb) {
        self.getTopicData(topic, function(err, data) {
          if(data && data.consumers && data.consumers.length > 0) {
            return cb({msg: "Topic ["+topic+"] has consumers, please delete all consumers before delete this topic"});
          }
          cluster = data.cluster;
          cb(err);
        });
      },
      function(cb) {
        mysqlConn.query("DELETE FROM topics WHERE topic_id = ?", [topic], cb);
      },
      function(cb) {
        self.createTasksForAllNodes(cluster, {task_type:"DELETE_TOPIC", task_data:{topic_id: topic}}, cb);
      }

    ], function(err) {
      function ret(err) {
        mysqlConn.release();
        callback(err);
      }
      if(err) {
        mysqlConn.rollback(function(e) {
          ret(e || err);
        })
      } else {
        mysqlConn.commit(ret);
      }
    });
  });
}

BigQueueClustersAdmin.prototype.createConsumerGroup = function(consumer,callback){
  var self = this
  if(!consumer.name || !consumer.topic_id || !consumer.tenant_id || !consumer.tenant_name) {
    return callback("name, topic_id, tenant_id, tenant_name are required");
  }
  var id = consumer.tenant_id+"-"+consumer.tenant_name+"-"+consumer.name;
  var cluster;
  self.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      log.log("error", "Error getting connection from mysql on createConsumerGroup", err);
      return callback(err);
    }
    async.series([
      function(cb) {
       //If topic doesn't exist it will get an error and the consumer will not be created
       self.getTopicData(consumer.topic_id,function(err, data) {
         cluster = data && data.cluster;
         cb(err, data);
       });
      },
      function(cb) {
        mysqlConn.beginTransaction(cb);
      },
      function(cb) {
        if(err) {
          cb(err);
        }
        mysqlConn.query("INSERT INTO consumers (consumer_id, tenant_id, tenant_name, consumer_name, cluster, topic_id, create_time) VALUES (?,?,?,?,?,?,now())",
                         [id, consumer.tenant_id, consumer.tenant_name, consumer.name, cluster, consumer.topic_id], cb);
      },
      function(cb) {
        self.createTasksForAllNodes(cluster, {task_type:"CREATE_CONSUMER", task_data:{consumer_id: id, topic_id:consumer.topic_id}}, cb);
      }
    ],function(err) {
      function ret(err) {
        mysqlConn.release();
        if(err) {
          callback({msg: JSON.stringify(err)});
        } else {
          self.getConsumerData(consumer.topic_id,id,function(err, data) {
            callback(err, data);
          });
        }
      }
      if(err) {
        mysqlConn.rollback(function(mysqlError) {
         ret(mysqlError || err);
        });
      } else {
        mysqlConn.commit(ret);
      }
    });
  });
}

BigQueueClustersAdmin.prototype.deleteConsumerGroup = function(topic,consumer,callback){
 var self = this
 self.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      log.log("error", "Error getting connection from mysql on deleteConsumerGroup", err);
      return callback(err);
    }
    var cluster;
    async.series([
      function(cb) {
        self.getTopicData(topic, function(err, data) {
          cluster = data && data.cluster;
          cb(err);
        });
      },
      function(cb) {
        mysqlConn.beginTransaction(cb);
      },
      function(cb) {
        mysqlConn.query("DELETE FROM consumers WHERE consumer_id = ? and topic_id = ? and cluster = ?",[consumer, topic, cluster], function(err, data) {
          if(!err) {
            if(data.affectedRows != 1) {
              return cb({msg: "Consumer can't be deleted [NOT_FOUND]"})
            }
          }
          cb(err);
        })
      },
      function(cb) {
        self.createTasksForAllNodes(cluster, {task_type:"DELETE_CONSUMER", task_data:{consumer_id: consumer, topic_id: topic}}, cb);
      }
    ],function(err) {
     function ret(err) {
        mysqlConn.release();
        callback(err);
      }
      if(err) {
        mysqlConn.rollback(function(e) {
          ret(e || err);
        })
      } else {
        mysqlConn.commit(ret);
      }
    });
  });
}


BigQueueClustersAdmin.prototype.getTopicStats = function(topic,cluster,callback){
    this.mysqlPool.query("SELECT consumer, sum(lag) as lag, sum(fails) as fails, sum(processing) as processing "+
                    "FROM stats "+
                    "WHERE cluster = ? AND topic = ? "+
                    "GROUP BY cluster, topic, consumer", [cluster, topic], function(err, data) {
        var ret;
        if(data) {
          ret = [];
          data.forEach(function(val) {
            ret.push({consumer_id: val.consumer, consumer_stats: {lag: val.lag, fails: val.fails, processing: val.processing}});
          });
        }

        return callback(err, ret);

  });
}

/**
 * Get all endpoints for an specific cluster
 */
BigQueueClustersAdmin.prototype.getEndpoinsForCluster = function(cluster, callback) {
  this.mysqlCriticPool.query("SELECT host, port, description FROM endpoints WHERE cluster = ?", [cluster],function(err,data) {
    callback(err, data);
  });

}

BigQueueClustersAdmin.prototype.getTopicDataByCriteria = function(criteria, callback) {
  var self = this;
  var topics = [];
  //We use an idx to modify elements without look into the array
  var topics_idx={};
  var query = "SELECT topic_id, ttl, cluster FROM topics t "
  var query_criteria = getCriteriaQuery(criteria);
  query += query_criteria.query;
  this.mysqlCriticPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      log.log("error", "Error getting connection from mysql on getTopicDataByCriteria", err);
      return callback(err);
    }
    async.waterfall([
      //Base topic data
      function(cb) {
        mysqlConn.query(query, query_criteria.args, function(err, data) {
          mysqlConn.release();
          cb(err, data);
        });
      },
      function(topics_db, cb) {
        //Consumer data
        var topics_arr= [];
        topics_db.forEach(function(topic) {
          var data = {topic_id: topic.topic_id, cluster: topic.cluster, ttl: topic.ttl, consumers: []};
          topics.push(data);
          topics_idx[topic.topic_id] = data;
        });
        self.getConsumerByCriteria(criteria, function(err, data) {
          if(err) {
            return cb(err);
          }
          data.forEach(function(consumer) {
            var topic_data = topics_idx[consumer.topic_id]
            if(topic_data) {
              topic_data.consumers.push({consumer_id: consumer.consumer_id, consumer_stats: consumer.consumer_stats});
            } else {
              log.log("error", "Inconsistency found on consumer ["+consumer.consumer_id+"]");
            }
          });
          cb(err);
        });
      }
      ],
      function(err, res) {
        callback(err, topics);
      });
  });
}

BigQueueClustersAdmin.prototype.listTopicsByCriteria = function(criteria, callback) {
  var self = this;
  //We use an idx to modify elements without look into the array
  var query = "SELECT topic_id FROM topics t "
  if(criteria) {
    var query_criteria = getCriteriaQuery(criteria,"t.");
    query += query_criteria.query;
  }
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      log.log("error", "Error getting connection from mysql on listTopicsByCriteria",err);
      return callback(err);
    }
    mysqlConn.query(query, query_criteria? query_criteria.args : [], function(err, data) {
      mysqlConn.release();
      callback(err, data);
    });
  });
}

/**
 * Get full topic data (topic, consumers and cluster data)
 */
BigQueueClustersAdmin.prototype.getTopicData = function(topic,callback){
    var self = this
    var topicData = {};

    async.series([
      //Base topic data
      function(cb) {
        self.getTopicDataByCriteria({topic_id: topic}, function(err, data) {
          if(data && data.length != 1) {
            return cb({msg: "Topic ["+topic+"] not found"});
          }
          topicData = (data && data[0]) || {};
          cb(err);
        });
      },
      function(cb) {
       //endpoints data
       self.getEndpoinsForCluster(topicData.cluster, function(err, data) {
        topicData["endpoints"] = data;
        cb(err);
       });
      }
    ], function(err) {
      callback(err, topicData);
    });
}



BigQueueClustersAdmin.prototype.getConsumerByCriteria = function(criteria, callback) {
  var self = this
  var consumers = [];
  var criteria_elems = [];
  var query = "SELECT c.consumer_id consumer_id, c.topic_id topic_id, "+
   "c.cluster cluster,sum(lag) as lag, "+
   "sum(fails) as fails, sum(processing) as processing "+
   "FROM consumers c " +
   "LEFT OUTER JOIN stats s "+
   "ON(s.consumer = c.consumer_id AND s.topic = c.topic_id) ";
  var query_criteria = getCriteriaQuery(criteria,"c.");
  query+=query_criteria.query;
  query+=" GROUP BY c.consumer_id , c.topic_id, c.cluster";
  this.mysqlCriticPool.getConnection(function(err, mysqlConn) {
    if(err) {
      if(mysqlConn) {
        mysqlConn.release();
      }
      log.log("error", "Error getting connection from mysql on getConsumerByCriteria", err);
      return callback(err);
    }
    mysqlConn.query(query, query_criteria.args, function(err, data) {
      mysqlConn.release();
      if(err) {
        return callback(err);
      }
      data.forEach( function(e){
        if(e.consumer_id)
          consumers.push({topic_id: e.topic_id, consumer_id: e.consumer_id, ttl: e.ttl,
                         cluster: e.cluster,
                         consumer_stats: {lag: e.lag, fails: e.fails, processing: e.processing}});
      });
      callback(err, consumers);
    });
   });
}

BigQueueClustersAdmin.prototype.getGroupTopics = function(group,callback){
  this.getConsumerByCriteria({"topic_id": group}, function(err, data) {
      callback(err, data);
  });
}

BigQueueClustersAdmin.prototype.getConsumerData = function(topic,consumer,callback){
  var self = this;
  this.getConsumerByCriteria({"topic_id": topic, "consumer_id": consumer}, function(err, data) {
    var consumer_data = {};
    async.series([
      function(cb) {
        if((data && data.length != 1) || !data) {
          cb({msg: "Consumer ["+consumer+"] for topic ["+topic+"] not found"});
        } else {
          consumer_data = data[0];
          cb(err);
        }
      },
      function(cb) {
       self.getEndpoinsForCluster(consumer_data.cluster, function(err, data) {
         consumer_data["endpoints"] = data;
          cb(err);
       });
      }], function(err) {
        consumer_data["topic_id"] = topic;
        if(err) {
          callback({msg: JSON.stringify(err)});
        } else {
          callback(err, consumer_data);
        }
      });
  });
}

BigQueueClustersAdmin.prototype.listClusters = function(callback){
    this.mysqlCriticPool.query("SELECT name FROM clusters", function(err, data) {
     var clusters = [];
      if(err) {
        callback(err);
      } else {
        data.forEach(function(e) {
          clusters.push(e.name);
        });
        callback(err, clusters);
      }
  });
}

BigQueueClustersAdmin.prototype.getNodesByCluster = function(cluster, callback) {

    this.mysqlCriticPool.query("SELECT dn.id id, dn.host host, dn.port port, dn.status status, dn.type type, dn.options options, nj.journal_id journal "+
                        "FROM data_nodes dn " +
                        "LEFT OUTER JOIN node_journals nj " +
                        "ON (dn.id = nj.node_id) " +
                        "WHERE dn.cluster = ? " +
                        "order by dn.id", [cluster], function(err, data) {
      var nodes_data = {nodes:[], journals:[]};
      var actual = {};
      if(err) {
          return callback(err);
      } else {
        data.forEach(function(elem) {
          //Elements are in order
          if(elem.id != actual.id) {
            //If it's a new element
            actual = elem;
            if(actual.type == "node") {
              actual.journals = [];
              if(actual.journal) {
                actual.journals.push(actual.journal);
              }
            }
            delete actual["journal"]
            //We use the plural type name (nodeS, journalS)
            nodes_data[actual.type+"s"].push(actual);
          } else {
            if(elem.journal && actual && actual.journal) {
              actual.journal.push(elem.journal);
            }
          }
        });
        callback(err, nodes_data);
      }
  });
}

BigQueueClustersAdmin.prototype.getClusterData = function(cluster, full,callback){
  var self = this

  var cluster_data = {cluster: cluster};
    async.series([
      function(cb) {
        self.listClusters(function(err, data) {
          if(err) {
            cb(err);
          } else {
            if(data && data.indexOf(cluster) == -1) {
              return cb({msg:"Cluster not found"});
            }
            cb();
          }
        });
      },
      function(cb) {
        self.getNodesByCluster(cluster, function(err, data) {
          if(err) {
            cb(err);
          } else {
            Object.keys(data).forEach(function(e) {
              cluster_data[e] = data[e];
            });
            cb();
          }
        });
      },
      function(cb) {
        //GET ENDPOINTS
       self.getEndpoinsForCluster(cluster, function(err, data) {
         cluster_data["endpoints"] = data;
         cb(err);
       });
      },
      function(cb) {
        //GET TOPICS
        if(full) {
          self.getTopicDataByCriteria({cluster: cluster}, function(err, data) {
            cluster_data["topics"] = data;
            cb(err, data);
          });
        } else {
          cb();
        }
      }
    ], function(err) {
      if(err) {
        callback(err);
      } else {
       callback(undefined, cluster_data);
      }
    });
}

BigQueueClustersAdmin.prototype.getNodeDataByType = function(cluster,node,type,callback){
  var self = this
  var node_data;

  async.series([
    function(cb) {
      self.mysqlCriticPool.query("SELECT id, host, port, status, options FROM data_nodes WHERE type = ? and cluster = ? and id = ?", [type, cluster, node], function(err,data) {
         if(err) {
           return cb(err);
         }
         if(data.length != 1) {
           cb({msg:"Node ["+node+"] of type ["+type+"] not found for cluster ["+cluster+"]"});
         } else {
           node_data = data[0]
           cb();
         }
       });
      },
      function(cb) {
        if(type == "node") {
          self.mysqlCriticPool.query("SELECT journal_id FROM node_journals WHERE node_id = ?", [node], function(err, data) {
            node_data["journals"] = data;
            cb(err);
          });
        } else {
         cb();
        }
      }
    ], function(err) {
      callback(err, node_data);
    });

}

BigQueueClustersAdmin.prototype.getNodeData = function(cluster,node,callback){
    this.getNodeDataByType(cluster,node,"node",callback)
}

BigQueueClustersAdmin.prototype.getJournalData = function(cluster,journal,callback){
    this.getNodeDataByType(cluster,journal,"journal",callback)
}

BigQueueClustersAdmin.prototype.updateNodeMetricsAsync = function() {
  var self = this;

  function doNodeMetrics() {
    var q = self.asyncStatsQueue;
    self.asyncStatsQueue = [];
    if(q.length > 0) {
      self.mysqlPool.getConnection(function(err, connection) {
          if(err) {
            if(connection) connection.release();
            log.log("error","Error recording node metric %s ["+err+"]");
            self.asyncStatsQueue = self.asyncStatsQueue.concat(q);
            setTimeout(doNodeMetrics, METRICS_INTERVAL)
          } else {
            async.eachSeries(q, function(stat, cb) {
              var node = stat.node;
              var cluster = stat.cluster;
              var topic_stats = stat.topic_stats;
              var sample_date = stat.sample_date;
              async.each(topic_stats, function(topic_stats, tCb) {
                var topic_id = topic_stats.topic_id;

                if(!topic_id || !topic_stats.consumers || !topic_stats.consumers.length == undefined) {
                  return tCb({msg: "Missing values"});
                }
                async.each(topic_stats.consumers, function(consumer,cCb) {
                  var stats = consumer.consumer_stats;
                  if(!consumer.consumer_id ||
                     stats.lag == undefined ||
                       stats.fails == undefined ||
                         stats.processing == undefined) {
                    return cCb({msg:"Property missing on consumer stats"})
                  }

                  connection.query("UPDATE stats SET lag = ?, fails = ?, processing = ?, last_update = ? WHERE cluster = ? and node = ? and topic = ? and consumer = ?",
                    [stats.lag, stats.fails, stats.processing, sample_date,
                      cluster, node, topic_stats.topic_id, consumer.consumer_id], function(err, res) {
                      if(err) {
                        return cCb(err);
                      }
                      if(res.affectedRows > 0) {
                        cCb();
                      } else {
                        connection.query("INSERT INTO stats (cluster, node, topic, consumer, lag, fails, processing, last_update) VALUES (?, ?, ? ,? ,?, ? ,? ,?) ",
                                        [cluster, node, topic_stats.topic_id, consumer.consumer_id,
                                          stats.lag, stats.fails, stats.processing, sample_date],function(err) {
                                            cCb(err)
                                          });
                      }
                  });
                },function(err) {
                  tCb(err)
                });
              },function(err) {
                cb(err)
              })
            },function(err) {
              connection.release();
              setTimeout(doNodeMetrics, METRICS_INTERVAL);
            });
          }
      });
    } else {
      setTimeout(doNodeMetrics, METRICS_INTERVAL);
    }
  }
  doNodeMetrics();
}

BigQueueClustersAdmin.prototype.updateNodeMetrics = function(cluster,node,stats, callback) {
  var self = this;
  if(!stats || !stats.topics_stats) {
    return callback({msg: "Invalid data for stats"});
  }
  var node_tags = ["cluster:"+cluster];
  try {
    if(stats.node_stats) {
      var cluster_node_tags = node_tags.concat(["cluster_node:"+node]);
      var n_stats = stats.node_stats;
      jsdog.recordPlainGauge("application.bigqueue.cluster_node.user_memory_b", n_stats.used_memory && parseInt(n_stats.used_memory), cluster_node_tags);
      jsdog.recordPlainGauge("application.bigqueue.cluster_node.used_cpu_sys", n_stats.used_cpu_sys && parseFloat(n_stats.used_cpu_sys), cluster_node_tags);
      jsdog.recordPlainGauge("application.bigqueue.cluster_node.used_cpu_user", n_stats.used_cpu_user && parseFloat(n_stats.used_cpu_user), cluster_node_tags);
      jsdog.recordPlainGauge("application.bigqueue.cluster_node.ops_per_sec", n_stats.instantaneous_ops_per_sec && parseInt(n_stats.instantaneous_ops_per_sec), cluster_node_tags);
    }
  }catch(e) {
    //Fail metric should not fail process
    log.log("error","Error recording node metric %s ["+e+"]", node_tags);
  }
  //Add stats to queue to be pushed to the DB
  self.asyncStatsQueue.push({"cluster":cluster,
                          "node":node,
                          "topic_stats":stats.topics_stats,
                          "sample_date": new Date(stats.sample_date)});

  var lastCheck = stats.sample_time;
  //Update consumer metrics
  //Use a single connection becasuse is a high traffic method
  async.each(stats.topics_stats, function(topic_stats, tCb) {
    var topic_id = topic_stats.topic_id;
    var topic_tags = node_tags.concat(["topic:"+topic_id]);

    if(!topic_id || !topic_stats.consumers || !topic_stats.consumers.length == undefined) {
      return tCb({msg: "Missing values"});
    }
    async.each(topic_stats.consumers, function(consumer,cCb) {
      var stats = consumer.consumer_stats;
      if(!consumer.consumer_id ||
         stats.lag == undefined ||
           stats.fails == undefined ||
             stats.processing == undefined) {
        return cCb({msg:"Property missing on consumer stats"})
      }
      //Record metric
      var consumer_tags = topic_tags.concat(["consumer:"+consumer.consumer_id]);
      var consumer_parts = consumer.consumer_id.split("-")
      if(consumer_parts.length >= 2) {
        consumer_tags.push("department:"+consumer_parts[1]);
      }
      var cluster_node_tags = topic_tags.concat(["cluster_node:"+node]);
      //We can calculate an avg
      try {
        jsdog.recordPlainGauge("application.bigqueue.consumers.stats.lag",stats.lag, consumer_tags);
        jsdog.recordPlainGauge("application.bigqueue.consumers.stats.fails",stats.fails, consumer_tags);
        jsdog.recordPlainGauge("application.bigqueue.consumers.stats.processing",stats.processing, consumer_tags);
        jsdog.recordPlainGauge("application.bigqueue.cluster_node.stats.lag",stats.lag, consumer_tags);
        jsdog.recordPlainGauge("application.bigqueue.cluster_node.stats.fails",stats.fails, consumer_tags);
        jsdog.recordPlainGauge("application.bigqueue.cluster_node.stats.processing",stats.processing, consumer_tags);
      } catch(e) {
        //Fail metrics should not fail process
        log.log("error","Error recording consumer metric %s %j", consumer_tags,e);
      }
      cCb();
    }, tCb);
  }, function(err) {
    callback(err);
  });
}

BigQueueClustersAdmin.prototype.createTasksForAllNodes = function(cluster, task, callback) {
  var self = this;
  var tasks = [];
  this.getClusterData(cluster, false, function(err, data) {
    if(err) {
      return callback(err);
    }
    data.nodes.forEach(function(node) {
      tasks.push({data_node_id: node.id, task_type: task.task_type, task_data: task.task_data});
    });
    self.createTasks(tasks, callback);
  });
}

BigQueueClustersAdmin.prototype.resetConsumer = function(topic, consumer, callback) {
  var self = this;
  this.getConsumerData(topic,consumer,function(err,data) {
    if(err) {
      return callback(err);
    }
    self.createTasksForAllNodes(data.cluster, {task_type:"RESET_CONSUMER",task_data:{topic_id: topic, consumer_id: consumer}}, callback);
  });
}

BigQueueClustersAdmin.prototype.createTasks = function(tasks, callback) {
  var self = this;
  this.mysqlPool.getConnection(function(err, mysqlConn) {
    if(err) {
      log.log("error", "Error getting connection from mysql on createTasks", err);
      if(mysqlConn) {
        mysqlConn.release();
      }
      return callback(err);
    }
    self.local_tasks_counter++;
    var group_id = ""+new Date().getTime()+"-"+os.hostname()+"-"+self.local_tasks_counter;
    async.series([
      function(cb) {
        mysqlConn.beginTransaction(cb);
      },
      function(cb) {
        async.each(tasks, function(task, sqlCb) {
          task.task_data = task.task_data || {};
          mysqlConn.query("INSERT INTO tasks (data_node_id,task_group,task_type, task_data, create_time, last_update_time) VALUES (?,?,?,?, now(), now())", [task.data_node_id,group_id, task.task_type, JSON.stringify(task.task_data)],sqlCb);
        },function(err) {
          cb(err);
        });
      }
    ], function(err) {
      function ret() {
        mysqlConn.release();
        callback(err);
      }
        if(err) {
          mysqlConn.rollback(ret);
        } else {
          mysqlConn.commit(ret);
        }
      });
  });
}

function build_criteria_query(criteria, callback) {
  var criteria_arr = [];
  var query = "";
  var criteria_keys = criteria && Object.keys(criteria);
  if(criteria_keys) {
    query+=" WHERE ";
    criteria_keys.forEach(function(c) {
      query+="?? = ? AND "
      criteria_arr.push(c, criteria[c]);
    });
    query = query.substring(0, query.lastIndexOf(" AND "));
  }
  callback(query, criteria_arr);
}

BigQueueClustersAdmin.prototype.getTasksByCriteria = function(criteria, callback) {
  var self = this;
  var criteria_arr = [];
  var query = "SELECT task_id, data_node_id, task_group, task_type, task_data, task_status, create_time, last_update_time FROM tasks";
  build_criteria_query(criteria, function(query_filter ,criteria_arr) {
    self.mysqlTasksPool.query(query+query_filter, criteria_arr, function(err, data) {
      var full_data = [];
      if(data) {
        data.forEach(function(d) {
          d.task_data = JSON.parse(d.task_data);
          full_data.push(d);
        });
      }
      callback(err, full_data);
    });
   });
}

BigQueueClustersAdmin.prototype.updateTaskStatus = function(task_id, task_status, callback) {

  this.mysqlTasksPool.query("UPDATE tasks SET task_status = ?, last_update_time = ? WHERE task_id = ?", [task_status, new Date(), task_id], function(err, data) {
    if(err) {
      return callback(err);
    }
    if(data.affectedRows != 1) {
      return callback({msg: "Task ["+task+"] not found", code: 404});
    }
    callback();
  });
}

exports.createClustersAdminClient = function(conf){
   var bqadm = new BigQueueClustersAdmin(conf.defaultCluster,conf.mysqlConf);
   return bqadm
}
