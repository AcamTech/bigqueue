var express = require('express'),
    log = require('../../lib/bq_logger.js'),
    pulsar = require('../../lib/pulsar.js'),
    bodyParser = require("body-parser"),
    morgan = require("morgan"),
    methodOverride = require("method-override"),
    StatsD = require('hot-shots'),
    async = require("async");
    statsDClient = new StatsD({
                                "host": process.env['DATADOG_PORT_8125_UDP_ADDR'],
                                globalTags:[
                                  "app:"+process.env["APP"],
                                  "scope:"+process.env["SCOPE"],
                                  "region:"+process.env["REGION"]
                                ]
                              });

var maxBody = "128kb"
var bqClient

var valid_element_regex=/^(\w|[0-9]){2,50}$/

var loadApp = function(app){
    app.get("/ping",function(req,res){
      app.settings.bqClient.healthcheck(function(err) {
        if(err) {
          res.status(500).json( err);
        }else{
          res.status(200).send("pong");
        }
      });
    })

    app.post(app.settings.basePath+"/messages",function(req,res){
        if(!req.is("json")){
            return res.status(400).json( {err:"Message should be json"})
        }
        var msgCopy = JSON.parse(JSON.stringify(req.body));
        var topics = req.body.topics
        if(!(topics instanceof Array)){
            return res.status(400).json( {err:"should be declared the 'topics' property"})
        }
        delete req.body["topics"]
        var message
        try{
            message = req.body
            Object.keys(message).forEach(function(val){
                if(message[val] instanceof Object){
                    message[val] = JSON.stringify(message[val])
                }
            })
        }catch(e){
            log.log("error","[ERROR-POSTING] Error parsing json", e)
            return res.status(400).json( {err:"Error parsing json ["+e+"]"})
        }

        var errors = []
        var executed = 0
        var limitRate = false;
        var topicDisabled = false;
        var functions = [];
        topics.forEach(function(topic) {
          functions.push(function (cb) {
            app.settings.bqClient.postMessage(topic,message,function(err,data){
               if(err){
                    limitRate = limitRate || err.limit_rate;
                    topicDisabled = topicDisabled || err.topic_disabled;
                    errors.push(err.msg);
                }
                cb(null, data);
              });
          });
        });
        functions.push(function (cb) {
          var start = new Date()
          pulsar.publish(app.settings.cluster, msgCopy, function(err) {
            if (err) {
              errors.push(err.toString());
            }
            cb();
          });
        });
        async.parallel(functions, function(err, results) {
          if(errors.length>0){
              log.log("error","[ERROR-POSTING] Error posx ting "+ JSON.stringify(errors))
              var code = 500;
              if(topicDisabled) {
                code = 412;
              }
              if(limitRate) {
                code = 429;
              }
              return res.status(code).json( {err:"An error ocurrs posting the messages","errors":errors})
          } else {
              return res.status(201).json(results.filter(function(e) { return e; }))
          }
        });
    })

    app.get(app.settings.basePath+"/topics/:topic/consumers/:consumer/messages",function(req,res){
        var timer = log.startTimer()
        var topic = req.params.topic
        var consumer = req.params.consumer
        var nodeId = req.headers["x-nodeid"]
        var nodeCallCount = 0;
        if(nodeId) {
          var splitedNode = nodeId.split("@");
          if(splitedNode.length != 2) {
            return res.status(400).json( {"err": "Invalid X-NodeId header"});
          }
          nodeId = splitedNode[0];
          try {
            nodeCallCount = parseInt(splitedNode[1]);
          }catch(e) {
            return res.status(400).json( {"err": "Invalid X-NodeId header"});
          }
        }

        function onMessage(err,data){
            if(err){
                var code = 400;
                if(err.consumer_disabled) {
                  code = 412;
                }
                if(err.limit_rate) {
                  code = 429;
                }
                if(typeof(err) == "string") {
                    res.status(code).json( {"err":""+err})
                } else {
                    res.status(code).json(err)
                }
            }else{
                if(data && data.id){
                    Object.keys(data).forEach(function(val){
                        if(typeof(data[val]) === "string"
                           && (data[val].match(/\{.*\}/) || data[val].match(/\[.*\]/))){
                            var orig = data[val]
                            try{
                                data[val] = JSON.parse(data[val])
                            }catch(e){
                                //On error do nothing
                            }
                        }
                    })
                    var remaining = data.remaining;
                    var nodeId = data.nodeId;
                    delete data["remaining"];
                    delete data["nodeId"];
                    if(remaining > 0 && nodeCallCount < app.settings.singleNodeMaxReCall) {
                      nodeCallCount++;
                      res.setHeader("X-Remaining",remaining);
                      res.setHeader("X-NodeId",nodeId+"@"+nodeCallCount);
                    }
                    res.status(200).json( data)
                }else{
                    res.status(204).json( {})
                }
            }
        }

        try{
          if(nodeId) {
            app.settings.bqClient.getMessageFromNode(nodeId,topic,consumer,req.query.visibilityWindow,onMessage)
          } else {
            app.settings.bqClient.getMessage(topic,consumer,req.query.visibilityWindow,onMessage)
          }
        }catch(e){
            log.log("error", "Error getting message [%s]",e)
            res.status(500).json( {err:"Error processing request ["+e+"]"})
        }
    })

    app.delete(app.settings.basePath+"/topics/:topic/consumers/:consumerName/messages/:recipientCallback",function(req,res){
        try{
            var topic = req.params.topic
            var consumer = req.params.consumerName
            app.settings.bqClient.ackMessage(topic,consumer,req.params.recipientCallback,function(err){
                if(err){
                    var errMsg = err.msg || ""+err
                    res.status(200).json( {err:errMsg})
                }else{
                    res.status(204).json( {})
                }
            })
        }catch(e){
            log.log("error", "Error deleting message [%s]",e)
            res.status(500).json( {err:"Error processing request ["+e+"]"})
        }

    })
}

exports.startup = function(config){
    var app = express()
        if(config.loggerConf){
        log.log("info", "Using express logger")
        app.use(morgan(config.loggerConf));
    }

    //It's to lead with the media-typer library Error in Pull request https://github.com/jshttp/media-typer/compare/master...geisbruch:master
    app.use(function(req, res, next) {
      if(req.headers["content-type"] && req.headers["content-type"].indexOf("application/json") != -1) {
        req.headers["content-type"]='application/json'
      }
      next();
    })
    app.use(bodyParser.json({limit: maxBody}))
    app.use(methodOverride());

    app.set("cluster", config.bqConfig.cluster);
    app.set("bqClient",config.bqClientCreateFunction(config.bqConfig));
    app.set("basePath",config.basePath || "");
    app.set("singleNodeMaxReCall", config.singleNodeMaxReCall || 100);


    loadApp(app)

    this.socket = app.listen(config.port)
    console.log("http api running on ["+config.port+"]")
    this.app = app
}

exports.shutdown = function(){
    this.socket.close()
}
