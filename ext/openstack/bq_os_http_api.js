var express = require('express'),
    log = require('../../lib/bq_logger.js'),
    bodyParser = require("body-parser"),
    morgan = require("morgan"),
    methodOverride = require("method-override"),
    jsdog = require("jsdog-meli").configure({
                                            "statsd_server": process.env['DATADOG_PORT_8125_UDP_ADDR'],
                                            "statsd_port": 8125,
                                             "fury_dumper":true
                                           });



var maxBody = "64kb"
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
            return res.status(400).json( {err:"Error parsing json ["+e+"]"})
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
                        return res.status(500).json( {err:"An error ocurrs posting the messages","errors":errors})
                    }else{
                        return res.status(201).json( datas)
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
                if(typeof(err) == "string")
                    res.status(400).json( {"err":""+err})
                else
                    res.status(400).json( err)
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
