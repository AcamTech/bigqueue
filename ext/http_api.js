var express = require('express'),
    log = require('../lib/bq_logger.js'),
    bodyParser = require("body-parser"),
    morgan = require("morgan"),
    methodOverride = require("method-override");
var maxBody = "64kb"
var bqClient

var valid_element_regex=/^(\w|[0-9]){2,50}$/

var loadApp = function(app){
    app.get("/ping",function(req,res){
      res.status(200).send("pong")
    })

    app.get("/topics",function(req,res){
        try{
            bqClient.listTopics(function(data){
                res.status(200).json(data)
            })
        }catch(e){
            log.log("error", "Error getting topics ["+e+"]")
            res.status(500).json({err:"Error processing request ["+e+"]"})
        }

    })

    app.post("/topics",function(req,res){
        if(!req.is("json")){
            return res.status(400).json({err:"Error parsing json"})
        }
        var topic = req.body
        try{
            if(!topic.name.match(valid_element_regex)){
                return res.status(400).json({err:"Topic should be an string without special chars between 2 and 50 chars"})
            }
            bqClient.createTopic(topic.name,function(err){
                if(err){
                    log.error("Error creating topic [%j]", err)
                    var err = err.msg || ""+err
                    return res.status(err.code || 409).json({err:err});
                }else{
                    return res.status(201).json({name:topic.name});
                }
            })
        }catch(e){
            log.log("error", "Error creating topic ["+e+"]")
            return res.status(500).json({err:"Error processing request ["+e+"]"});
        }
    })

    app.get("/topics/:topic/consumers",function(req,res){
        try{
            bqClient.getConsumerGroups(req.params.topic,function(err,data){
                if(err){
                    log.log("error", "Error creating consumer group [%j] [%s]", req.params, err)
                    var err = err.msg || ""+err
                    res.status(err.code || 400).json({err:""+err})
                }else{
                    res.status(200).json(data)
                }
            })
        }catch(e){
            log.log("error", "Error creating consumer group ["+e+"]")
            res.status(500).json({err:"Error processing request ["+e+"]"})
        }

    })

    app.post("/topics/:topic/consumers",function(req,res){
            if(!req.is("json")){
                return res.status(400).json({err:"Content should be json"})
            }
            var consumer = req.body
            var topic = req.params.topic
            if(!consumer.name.match(valid_element_regex)){
              return res.status(400).json({err:"Consumer group should be an string without special chars between 2 and 50 chars"})
            }

            try{
                bqClient.createConsumerGroup(topic,consumer.name,function(err){
                    if(err){
                        var err = err.msg || ""+err
                        return res.status(err.code || 409 ).json({err:""+err})
                    }
                    return res.status(201).json({name:consumer.name})
                })
            }catch(e){
                return res.status(500).json({err:"Error processing request ["+e+"]"})
            }


    })

    app.post("/topics/:topic/messages",function(req,res){
        if(!req.is("json")){
            return res.status(400).json({err:"Message should be json"})
        }
        var message
        try{
            message = req.body
            Object.keys(message).forEach(function(val){
                if(message[val] instanceof Object){
                    message[val] = JSON.stringify(message[val])
                }
            })
        }catch(e){
            return res.status(400).json({err:"Error parsing json ["+e+"]"})
        }
        try{
            bqClient.postMessage(req.params.topic,message,function(err,data){
                if(err){
                   var err = err.msg || ""+err
                   return res.status(err.code || 400).json({"err":err})
                }
                return res.status(201).json(data)
            })
        }catch(e){
            log.log("error", "Error posting message ["+e+"]")
            return res.status(500).json({err:"Error processing request ["+e+"]"})
        }
    })

    app.post("/messages",function(req,res){
        if(!req.is("json")){
            return res.status(400).json({err:"Message should be json"})
        }
        var topics = req.body.topics
        if(!(topics instanceof Array)){
            return res.status(400).json({err:"should be declared the array 'topics' property"})
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
            return res.status(400).json({err:"Error parsing json ["+e+"]"})
        }

        var errors = []
        var datas = []
        var executed = 0
        for(var i in topics){
            bqClient.postMessage(topics[i],message,function(err,data){
               if(err){
                    errors.push(err)
                }
                if(data){
                    datas.push(data)
                }
                executed++
                if(executed == topics.length){
                    if(errors.length>0){
                        return res.status(500).json({err:"An error ocurrs posting the messages","errors":errors})
                    }else{
                        return res.status(201).json(datas)
                    }
                }
            })
        }
    })
    var counter = 0;
    app.get("/topics/:topic/consumers/:consumer/messages",function(req,res){
      counter++
      res.setHeader("X-NodeId",counter)
        try{
            bqClient.getMessage(req.params.topic,req.params.consumer,req.query.visibilityWindow,function(err,data){
                if(err){
                    var err = err.msg || ""+err
                    res.status(err.code || 400).json({"err":err})
                }else{
                    if(data && data.id){
                        Object.keys(data).forEach(function(val){
                            if(typeof(data[val])=== "string" && (data[val].match(/\{.*\}/) || data[val].match(/\[.*\]/))){
                                var orig = data[val]
                                try{
                                    data[val] = JSON.parse(data[val])
                                }catch(e){
                                    //On error do nothing
                                }
                            }
                        })

                        res.status(200).json(data)
                    }else{
                        res.status(204).json({})
                    }
                }
            })
        }catch(e){
            log.log("error", "Error getting message ["+e+"]")
            res.status(500).json({err:"Error processing request ["+e+"]"})
        }
    })

    app.delete("/topics/:topic/consumers/:consumer/messages/:recipientCallback",function(req,res){
        try{
            bqClient.ackMessage(req.params.topic,req.params.consumer,req.params.recipientCallback,function(err){
                if(err){
                    var err = err.msg || ""+err
                    res.status(200).json({"err":err})
                }else{
                    res.status(204).json({})
                }
            })
        }catch(e){
            log.log("error", "Error deleting message ["+e+"]")
            res.status(500).json({err:"Error processing request ["+e+"]"})
        }

    })

    app.get("/topics/:topic/consumers/:consumer/stats",function(req,res){
        try{
            bqClient.getConsumerStats(req.params.topic,req.params.consumer,function(err,data){
                if(err){
                    var err = err.msg || ""+err
                    res.status(err.code || err.code || 400).json({"err":err})
                }else{
                    res.status(200).json(data)
                }
            })
        }catch(e){
            log.log("error", "Error getting the stats for consumer")
            res.status(500).json({err:"Error processing request ["+e+"]"})
        }
    })

    app.get("/topics/:topic/stats",function(req,res){
        try{
            bqClient.getConsumerGroups(req.params.topic,function(err,consumers){
                if(err){
                    log.log("error", "Error getting consumer groups for topic, error: "+err)
                    var err = err.msg || ""+err
                    res.status(err.code || 400).json({"err":err})
                    return
                }
                var total=consumers.length
                var executed=0
                var data=[]
                if(total== 0){
                    res.status(200).json([])
                    return
                }
                consumers.forEach(function(consumer){
                    bqClient.getConsumerStats(req.params.topic,consumer,function(err,stats){
                        if(err){
                            var err = err.msg || ""+err
                            res.status(err.code || 400).json({"err":err})
                        }else{
                            var d = {"consumer":consumer}
                            d.stats=stats
                            data.push(d)
                            executed++
                        }
                        //If an error ocurs the executed never will be equals than total
                        if(executed>=total){
                            res.status(200).json(data)
                        }
                    })
                })
            })
        }catch(e){
            log.log("error", "Error getting the stats for topic")
            res.status(500).json({err:"Error processing request ["+e+"]"})
        }
    })
}



exports.startup = function(config){

    var app = express()
        if(config.loggerConf){
        log.log("info", "Using express logger")
        app.use(morgan(config.loggerConf));
    }
    app.use(bodyParser.json({limit: maxBody}));
    app.use(methodOverride());


    loadApp(app)

    this.socket = app.listen(config.port)
    bqClient = config.bqClientCreateFunction(config.bqConfig)
    console.log("http api running on ["+config.port+"]")
}

exports.shutdown = function(){
  this.socket.close();
}
