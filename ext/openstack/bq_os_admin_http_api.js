var express = require('express'),
    log = require('../../lib/bq_logger.js'),
    bqAdm = require('../../lib/bq_clusters_adm.js'),
    pulsar = require('../../lib/pulsar.js'),
    keystoneMiddlware = require("../../ext/openstack/keystone_middleware.js"),
    bodyParser = require("body-parser"),
    NodeCache = require("node-cache"),
    morgan = require("morgan"),
    async = require("async"),
    YAML = require('json2yaml'),
    jsdog = require("jsdog-meli").configure({
                                            "statsd_server": process.env['DATADOG_PORT_8125_UDP_ADDR'],
                                            "statsd_port": 8125,
                                             "fury_dumper":true
                                           });
var cache = {};

var loadApp = function(app){
    var authorizeTenant = function(userData,tenantId){
    var authorized = false
        try{
            var tenant = userData.access.token.tenant
            if(tenant && tenant.id == tenantId){
                authorized = true
            }
        }catch(e){
            //Property doesn't exist
        }
        return authorized
    }

    var isAdmin = function(userData){
        var idToFind = app.settings.adminRoleId
        var found = false
        var roles = userData.access.user.roles
        if(roles){
            roles.forEach(function(val){
                if(val.id == idToFind){
                    found = true
                    return
                }
            })
        }
        return found
    }

    function getTenantId(req) {
      if(req.keystone &&
         req.keystone.userData &&
         req.keystone.userData.access &&
         req.keystone.userData.access.token &&
         req.keystone.userData.access.token.tenant) {
        return req.keystone.userData.access.token.tenant.id
    }
      return req.body && req.body.tenant_id;
    }

    function getTenantName(req) {

      if(req.keystone &&
         req.keystone.userData &&
         req.keystone.userData.access &&
         req.keystone.userData.access.token &&
         req.keystone.userData.access.token.tenant) {
        return req.keystone.userData.access.token.tenant.name
      }
      return req.body && req.body.tenant_name;

    }

    function getFilterCriteria(req) {
      var criteria = {};

      Object.keys(req.params).forEach(function(e) {
        criteria[e] = req.params[e];
      });
      var tenant_id = getTenantId(req);
      if(tenant_id) {
        criteria["tenant_id"] = tenant_id;
      }
      return criteria;
    }
    app.get(app.settings.basePath+"/clusters",function(req,res){
        app.settings.bqAdm.listClusters(function(err,clusters){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty(clusters,200)
        })
    })

    app.post(app.settings.basePath+"/clusters",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }
        app.settings.bqAdm.createBigQueueCluster(req.body,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                if(!err.code || err.code < 400) {
                  err.code=500
                }
                console.log(err)
                return res.writePretty({"err":errMsg}, err.code )
            }
            return res.writePretty({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/nodes/:node/stats",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }
        app.settings.bqAdm.updateNodeMetrics(req.params.cluster, req.params.node,req.body, function(err) {
          if(err) {
            return res.writePretty({node: req.params.node, cluster: req.params.node.cluster},500);
          }
          return res.writePretty({node: req.params.node, cluster: req.params.node.cluster},200);
        });
    });

    app.post(app.settings.basePath+"/clusters/:cluster/nodes",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }
        app.settings.bqAdm.addNodeToCluster(req.params.cluster,req.body,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/journals",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }
        app.settings.bqAdm.addJournalToCluster(req.params.cluster,req.body,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},201)
        })
    })

    app.post(app.settings.basePath+"/clusters/:cluster/endpoints",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }
        app.settings.bqAdm.addEndpointToCluster(req.params.cluster,req.body,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},201)
        })
    })


    app.put(app.settings.basePath+"/clusters/:cluster/nodes/:node",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }
        var node = req.body
        node["id"] = req.params.node
        app.settings.bqAdm.updateNodeData(req.params.cluster,node,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},200)
        })
    })

    app.put(app.settings.basePath+"/clusters/:cluster/journals/:node",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }
        var node = req.body
        node["id"] = req.params.node
        app.settings.bqAdm.updateJournalData(req.params.cluster,node,function(err){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty({"cluster":req.body.name},200)
        })
    })

    app.get(app.settings.basePath+"/clusters/:cluster/nodes/:node",function(req,res){
        app.settings.bqAdm.getNodeData(req.params.cluster,req.params.node,function(err,data){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty(data,200)
        })
    })

    app.get(app.settings.basePath+"/clusters/:cluster/journals/:journal",function(req,res){
        app.settings.bqAdm.getJournalData(req.params.cluster,req.params.journal,function(err,data){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }
            return res.writePretty(data,200)
        })
    })

    app.get(app.settings.basePath+"/clusters/:cluster/topics",function(req,res){
      if(!cache["clusterStruct"]){
        cache["clusterStruct"] = {}
      }
      if(cache["clusterStruct"][req.params.cluster]) {
        return res.writePretty(cache["clusterStruct"][req.params.cluster],200)
      }
      app.settings.bqAdm.getClusterStruct(req.params.cluster,function(err, data) {
        if(err){
            var errMsg = err.msg || ""+err
            return res.writePretty({"err":errMsg}, 500)
        }
        cache["clusterStruct"][req.params.cluster] = {"topics":data};
        return res.writePretty({"topics":data},200)
      })
    });

    app.get(app.settings.basePath+"/clusters/:cluster",function(req,res){
      var cacheKey = "cluster_data-"+req.params.cluster;
      var cache = app.settings.cache;
      if(cache) {
        var cached = cache.get(cacheKey);
        if(cached && Object.keys(cached).length > 0) {
          return res.writePretty(cached[cacheKey],200);
        }
      }
      var full = req.query && req.query.format && req.query.format == "complete" ? true : false;
      app.settings.bqAdm.getClusterData(req.params.cluster, full,function(err,data){
          if(err){
              var errMsg = err.msg || ""+err
              return res.writePretty({"err":errMsg},err.code || 500)
          }
          if(cache)
            cache.set(cacheKey, data);
          return res.writePretty(data,200)
      })
    })

    app.get(app.settings.basePath+"/topics",function(req,res){
        if(!req.query || Object.keys(req.query).length == 0) {
          req.query = undefined;
        }
        app.settings.bqAdm.listTopicsByCriteria(req.query,function(err,data){
           if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg}, 500)
           }
           var topics = [];
           data.forEach(function(e) {
            topics.push(e.topic_id);
           });
           return res.writePretty(topics,200)
        })
    })

    app.post(app.settings.basePath+"/topics",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }

        if(!req.body.name || req.body.name.indexOf(":") != -1){
            return res.writePretty({"err":"The property [name] must be set and can not contains ':'"},400)
        }
        if(req.keystone && (!req.keystone.authorized && !isAdmin(req.keystone.userData)) ){
          return res.writePretty({"err":"Invalid token for tenant "},401)
        }
        if(!req.body.name){
            return res.writePretty({err:"Topics should contains a name"},400)
        }
        var topic_data = req.body;
        var ttl = req.body.ttl || app.settings.maxTtl;
        if(ttl && ttl > app.settings.maxTtl){
            return res.writePretty({"err":"Max ttl exceeded, max ttl possible: "+app.settings.maxTtl},406)
        }
        topic_data["ttl"] = ttl;
        topic_data["tenant_id"] = getTenantId(req);
        topic_data["tenant_name"] = getTenantName(req);

        var functions = [];
        var errors = [];
        var result;

        functions.push(function (cb) {
            app.settings.bqAdm.createTopic(topic_data, function(err, topicData){
                if(err){
                    var errMsg = err.msg || ""+err
                    errors.push(errMsg);
                    //return res.writePretty({"err":errMsg}, 500)
                }
                result = topicData;
                cb();
                //return res.writePretty(topicData,201)
            });
        });

        functions.push(function(cb) {
            pulsar.createTopic(topic_data["tenant_id"] + "-" + topic_data["tenant_name"] + "-" + req.body.name, req.body.cluster || "default2", function(err, status, data){
                if(status >= 300){
                    errors.push(data);
                }
                if (err) {
                    errors.push(err);
                }
                cb();
            });
        });

        async.parallel(functions, function(err) {
            if(errors.length>0){
                return res.writePretty({"err": errors}, 500);
            } else {
                return res.writePretty(result, 201);
            }
        });
    });

    app.delete(app.settings.basePath+"/topics/:topic_id",function(req,res){
      var criteria = getFilterCriteria(req);
      app.settings.bqAdm.getTopicDataByCriteria(criteria,function(err,data){
            if(err){
               var errMsg = err.msg || ""+err
               return res.writePretty({"err":errMsg},err.code || 500)
            }
        if(data.length != 1) {
           return res.writePretty({"err":"Topic not found or you are not authorized to delete with this token"}, 404)
            }
        app.settings.bqAdm.deleteTopic(req.params.topic_id,function(err,data){
               if(err){
                  var errMsg = err.msg || ""+err
                  return res.writePretty({"err":errMsg},err.code || 500)
                }
                return res.writePretty(undefined,204)
            })
        })
    })

    app.get(app.settings.basePath+"/topics/:topicId",function(req,res){
        var topic = req.params.topicId;
          app.settings.bqAdm.getTopicData(topic,function(err,data){
              if(pulsar.FULLY_MIGRATED_CLUSTERS.indexOf(data.cluster) > -1){
                  pulsar.getTopic(topic, function(err, status, data){
                        if(err){
                            return res.writePretty({"err": err}, status)
                        }
                        return res.writePretty(data, status);
                  });
                  return;
              }
              if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
              }
            //Add by backward compatibility
            data.consumers.forEach(function(e) {
              e["stats"] = e.consumer_stats;
            });
              return res.writePretty(data,200)
          })
    })

    app.get(app.settings.basePath+"/topics/:topicId/consumers",function(req,res){
        var topic = req.params.topicId;
          app.settings.bqAdm.getTopicData(topic,function(err,data){
             if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
              }
              return res.writePretty(data.consumers,200)
          });
    })
    app.post(app.settings.basePath+"/topics/:topicId/consumers",function(req,res){
        if(!req.is("json")){
            return res.writePretty({err:"Error parsing json"},400)
        }

        if(!req.body.name || req.body.name.indexOf(":") != -1){
            return res.writePretty({"err":"The property [name] must be set and can not contains ':'"},400)
        }

        if(req.keystone && (!req.keystone.authorized && !isAdmin(req.keystone.userData)) ){
            return res.writePretty({"err":"Invalid token for tenant "},401)
        }

        if(!req.body.name){
            return res.writePretty({err:"Consumer should contains a name"},400)
        }
        var consumer_data = req.body;
        consumer_data["tenant_id"] = getTenantId(req);
        consumer_data["tenant_name"] = getTenantName(req);
        consumer_data["topic_id"] = req.params.topicId;


        var functions = [];
        var code = 500;

        functions.push(function (cb) {
            app.settings.bqAdm.createConsumerGroup(consumer_data, function(err, consumerData){
                if(err){
                    var errMsg = err.msg || ""+err
                    code = err.code || 500;
                    cb(errMsg);
                    //return res.writePretty({"err":errMsg}, err.code || 500)
                }/*else{
                    return res.writePretty(consumerData,201)
                }*/
                cb(null, consumerData);
            });

        });

        functions.push(function(consumerData, cb) {
            pulsar.createConsumer(consumerData.topic_id, consumerData.consumer_id, consumerData.cluster, function(err, status, body){
                if (err || status >= 300) {
                    cb(err || body);
                }
                cb(null, consumerData);
            });
        });

        async.waterfall(functions, function(err, result) {
            if(err && err != ""){
                return res.writePretty({"err": err}, code);
            } else {
                return res.writePretty(result, 201);
            }
        });

    })

    app.delete(app.settings.basePath+"/topics/:topic_id/consumers/:consumer_id",function(req,res){
        var criteria = getFilterCriteria(req);
        app.settings.bqAdm.getConsumerByCriteria(criteria,function(err,data){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }

            if(data.length != 1) {
               return res.writePretty({"err":"Consumer not found or you are not authorized to delete with this token"}, 404)
            }

            app.settings.bqAdm.deleteConsumerGroup(req.params.topic_id,req.params.consumer_id,function(err,data){
                if(err){
                  var errMsg = err.msg || ""+err
                  return res.writePretty({"err":errMsg},err.code || 500)
                }
                return res.writePretty(undefined,204)
            })
        })
    })

    //Reset on put
    app.put(app.settings.basePath+"/topics/:topic_id/consumers/:consumer_id",function(req,res){
        var criteria = getFilterCriteria(req);
        app.settings.bqAdm.getConsumerByCriteria(criteria,function(err,data){
            if(err){
                var errMsg = err.msg || ""+err
                return res.writePretty({"err":errMsg},err.code || 500)
            }

            if(data.length != 1) {
               return res.writePretty({"err":"Consumer not found or you are not authorized to delete with this token"}, 404)
            }

            if (data[0].consumer_type == 'pulsar') {
              return pulsar.resetConsumer(req.params.topic_id, req.params.consumer_id, function(err, status, body) {
                if(err){
                  var errMsg = err.msg || ""+err
                  return res.writePretty({"err":errMsg}, 500)
                }
                if (status == 204) {
                  return res.writePretty(undefined,204)
                }
                return res.writePretty(body,status)
              });
            }

            app.settings.bqAdm.resetConsumer(req.params.topic_id,req.params.consumer_id,function(err,data){
                if(err){
                  var errMsg = err.msg || ""+err
                  return res.writePretty({"err":errMsg},err.code || 500)
                }
                return res.writePretty(undefined,204)
        })
    })

    });
    app.get(app.settings.basePath+"/topics/:topicId/consumers/:consumerId",function(req,res){
        var topic = req.params.topicId;
        var consumer = req.params.consumerId;
        app.settings.bqAdm.getConsumerData(topic,consumer,function(err,data){
            if(err){
              var errMsg = err.msg || ""+err
              var code = err.code || 500;
              if (err.not_found) {
                code = 404;
              }
              return res.writePretty({"err":errMsg},code)
            }
            if (data.consumer_type == 'pulsar') {
              return pulsar.getConsumer(topic, consumer, function(err, status, body) {
                if (err) {
                  var errMsg = err.msg || err.toString();
                  return res.writePretty({"err":errMsg},code);
                }
                return res.writePretty(body, status);
              })
            }

            return res.writePretty(data,200)
        })
    })

    //TASKS

    app.get(app.settings.basePath+"/tasks", function(req, res) {
      app.settings.bqAdm.getTasksByCriteria(req.query,function(err,data){
        if(err) {
          res.writePretty({"err":JSON.stringify(err)});
        } else {
          res.writePretty(data);
        }
    });
    });
    app.get(app.settings.basePath+"/tasks/:id", function(req, res) {
      app.settings.bqAdm.getTasksByCriteria({task_id: req.params.id},function(err,data){
        if(err) {
          res.writePretty({"err":JSON.stringify(err)}, err.code || 500);
        } else {
          if(data.length == 1) {
            res.writePretty(data[0]);
      } else {
            res.writePretty({"err":"Task ["+req.params.id+"] not found"},404);
      }
    }
      });

    });

    app.put(app.settings.basePath+"/tasks/:id", function(req, res) {
      if(!req.body || !req.body.task_status) {
        return res.writePretty({err: "task_status should be defined"},400);
      }
      app.settings.bqAdm.updateTaskStatus(req.params.id,req.body.task_status,function(err,data){
        if(err) {
          res.writePretty({"err":JSON.stringify(err)}, err.code || 500);
    } else {
          res.writePretty({},204);
  }
      });
    });
    //PING

    app.get("/ping", function(req, res) {
      res.status(200).send("pong");
    });
}

var authFilter = function(config){

    return function(req,res,next){
      //All post should be authenticated
        if((req.method.toUpperCase() === "POST" ||
            req.method.toUpperCase() === "PUT" ||
              req.method.toUpperCase() === "DELETE") && !req.keystone.authorized){
            var excluded = false;
            if(config.authExclusions) {
              config.authExclusions.forEach(function(e) {
                if(req.url.match(e)) {
                  excluded = true;
                }
              });
            }
            if(excluded) {
              next();
            } else {
              res.writePretty({"err":"All post to admin api should be authenticated using a valid X-Auth-Token header"},401)
            }
        }else{
            next()
        }
    }
}
var writeFilter = function(){
    return function(req,res,next){
        res.writePretty = function(obj,statusCode){
          statusCode = statusCode || 200;
            if(req.accepts("json")){
                res.status(statusCode).json(obj)
            }else if(req.accepts("text/plain")){
                res.send(statusCode, YAML.stringify(obj))
            }else{
                //Default
                res.status(statusCode).json(obj);
            }
        }
        next()
    }
}

exports.startup = function(config){
    //Default 5 days
    var authFilterConfig = {authExclusions : [/.*\/clusters\/\w+\/nodes($|\/.+$)/,/.*\/clusters\/\w+\/journals($|\/.+$)/,/\/tasks.*/]}
    var maxTtl = config.maxTtl || 3*24*60*60
    var useCache = config.useCache != undefined ? config.useCache : true;
    var cacheTtl = config.cacheTtl != undefined ? config.cacheTtl: 5;
    var app = express()
    if(config.loggerConf){
        log.log("info", "Using express logger")
        app.use(morgan(config.loggerConf));
    }
    if(app.config && app.config.jsdog != undefined) {
      if(app.config.jsdog.enabled != undefined) {
        jsdog.setEnable(app.config.jsdog.enabled);
      }
    }
    app.use(writeFilter())
    app.enable("jsonp callback")

    app.use(bodyParser.json({"limit":"100mb"}));

    if(config.keystoneConfig){
        app.use(keystoneMiddlware.auth(config.keystoneConfig))
        app.use(authFilter(authFilterConfig))
        app.set("adminRoleId",config.admConfig.adminRoleId || -1)
    }

    app.set("basePath",config.basePath || "")
    app.set("maxTtl",maxTtl)
    app.set("bqAdm",bqAdm.createClustersAdminClient(config.admConfig))
    if(useCache) {
      app.set("cache", new NodeCache({ stdTTL: cacheTtl, checkperiod: 1 }));
    } else {
      app.set("cache", false);
    }
    loadApp(app)
    app.running = true;
    this.socket = app.listen(config.port)
    this.app = app
    this.clearCacheInterval = setInterval(function() {
      cache = {}
    }, config.cacheTime || 1);
    return this
}

exports.shutdown = function(){
    if(this.app.settings.bqAdm)
        this.app.settings.bqAdm.shutdown()
    //this.app.close()
    this.app.running = false;
    this.socket.close();
}
