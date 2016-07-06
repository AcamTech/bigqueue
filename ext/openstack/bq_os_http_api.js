var express = require('express'),
    log = require('../../lib/bq_logger.js'),
    bodyParser = require("body-parser"),
    morgan = require("morgan"),
    methodOverride = require("method-override"),
    jsdog = require("jsdog-meli").configure();
var maxBody = "64kb"
var bqClient

var valid_element_regex=/^(\w|[0-9]){2,50}$/

var loadApp = function(app){
    app.get("/ping",function(req,res){
      app.settings.bqClient.healthcheck(function(err) {
        if(err) {
          res.json(500, err);
        }else{
          res.end("pong");
        }
      });
    })

    app.post(app.settings.basePath+"/messages",function(req,res){
        if(!req.is("json")){
            return res.json(400, {err:"Message should be json"})
        }
        var topics = req.body.topics
        if(!(topics instanceof Array)){
            return res.json(400, {err:"should be declared the 'topics' property"})
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
            return res.json(400, {err:"Error parsing json ["+e+"]"})
        }

        var errors = []
        var datas = []
        var executed = 0
        for(var i in topics){
            app.settings.bqClient.postMessage(topics[i],message,function(err,data){
               if(err){
                    errors.push(err.msg)
                }
                if(data){
                    datas.push(data)
                }
                executed++
                if(executed == topics.length){
                    if(errors.length>0){
                        return res.json(500, {err:"An error ocurrs posting the messages","errors":errors})
                    }else{
                        return res.json(201, datas)
                    }
                }
            })
        }
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
            return res.json(400, {"err": "Invalid X-NodeId header"});
          }
          nodeId = splitedNode[0];
          try {
            nodeCallCount = parseInt(splitedNode[1]);
          }catch(e) {
            return res.json(400, {"err": "Invalid X-NodeId header"});
          }
        }

        function onMessage(err,data){
            if(err){
                if(typeof(err) == "string")
                    res.json(400, {"err":""+err})
                else
                    res.json(400, err)
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
                    res.json(200, data)
                }else{
                    res.json(204, {})
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
            res.json(500, {err:"Error processing request ["+e+"]"})
        }
    })

    app.delete(app.settings.basePath+"/topics/:topic/consumers/:consumerName/messages/:recipientCallback",function(req,res){
        try{
            var topic = req.params.topic
            var consumer = req.params.consumerName
            app.settings.bqClient.ackMessage(topic,consumer,req.params.recipientCallback,function(err){
                if(err){
                    var errMsg = err.msg || ""+err
                    res.json(200, {err:errMsg})
                }else{
                    res.json(204, {})
                }
            })
        }catch(e){
            log.log("error", "Error deleting message [%s]",e)
            res.json(500, {err:"Error processing request ["+e+"]"})
        }

    })
}

exports.startup = function(config){
    var app = express()
        if(config.loggerConf){
        log.log("info", "Using express logger")
        app.use(morgan(config.loggerConf));
    }
    app.use(bodyParser.json({limit: maxBody}))
    app.use(methodOverride());

    app.set("bqClient",config.bqClientCreateFunction(config.bqConfig));
    app.set("basePath",config.basePath || "");
    app.set("singleNodeMaxReCall", config.singleNodeMaxReCall || 100);
    if(config && config.jsdog != undefined) {
      if(config.jsdog.enable != undefined) {
        jsdog.setEnable(config.jsdog.enable);
      }
    }


    loadApp(app)

    this.socket = app.listen(config.port)
    console.log("http api running on ["+config.port+"]")
    this.app = app
}

exports.shutdown = function(){
    this.socket.close()
}
