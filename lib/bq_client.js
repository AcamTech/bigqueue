var redis = require('redis'),
    fs = require('fs'),
    should = require('should'),
    events = require("events"),
    log = require("../lib/bq_logger.js"),
    bqutils = require("../lib/bq_client_utils.js"),
    async = require("async");

//Class level
var scriptsLoaded = false;

var redisScripts = {};
var scripts = [
  {file: __dirname+'/../scripts/getMessage.lua', name:"getMessage"},
  {file: __dirname+'/../scripts/postMessage.lua', name:"postMessage"},
  {file: __dirname+'/../scripts/createConsumer.lua', name:"createConsumerGroup"},
  {file: __dirname+'/../scripts/createTopic.lua', name:"createTopic"},
  {file: __dirname+'/../scripts/ack.lua', name:"ackMessage"},
  {file: __dirname+'/../scripts/fail.lua', name:"failMessage"},
  {file: __dirname+'/../scripts/deleteTopic.lua', name:"deleteTopic"},
  {file: __dirname+'/../scripts/deleteConsumer.lua', name:"deleteConsumer"},
  {file: __dirname+'/../scripts/resetConsumerGroup.lua', name:"resetConsumerGroup"},
  {file: __dirname+'/../scripts/consumerStats.lua', name:"consumerStats"},
]
async.each(scripts, function(script, cb) {
  fs.readFile(script.file,'ascii',function(err,strFile) {
    redisScripts[script.name] = strFile
    cb(err);
  });
},
function(err) {
  should.not.exist(err);
  scriptsLoaded = true;
});



function BigQueueClient(rConf){
    events.EventEmitter.call( this );
    this.redisConf = rConf
    this.redisLoaded = false
    this.shutdowned = false
    var self = this;
    self.emitReadyWhenAllLoaded()
}

BigQueueClient.prototype = Object.create(require('events').EventEmitter.prototype);

BigQueueClient.prototype.emitReadyWhenAllLoaded = function(){
    var self = this;

    if(scriptsLoaded && this.redisLoaded){
        /**
         * If redis is up but is loading will response but redis is not ready
         * so we will check every sec
         */
        var checkRedisPing = function(){
            if(!self.shutdowned){
                self.redisClient.execute("PING",function(err,data){
                    if(!err && data == "PONG"){
                        self.emit("ready")
                    }else{
                        self.emit("error",err)
                        setTimeout(checkRedisPing,1000)
                    }
                })
            }
        }
        checkRedisPing()
    }else{
        setTimeout(function(){
            self.emitReadyWhenAllLoaded()
        },1);
    }
}

BigQueueClient.prototype.connect = function(){
    var self = this;
    try{
        this.redisClient = redis.createClient(this.redisConf.port,this.redisConf.host,this.redisConf.options)
        this.redisClient.execute = function() {
          var args = [];
          for (var key in arguments) {
            args.push(arguments[key]);
          }
          var command = args.shift();
          var callback = args.pop();
          return self.redisClient.send_command(command, args, callback);
        }

        this.redisClient.on("error",function(err){
          log.log("error", "Error connecting to [cluster_client:"+JSON.stringify(self.redisConf)+"] [error_type:connect_error]",err)
            err["redisConf"] = self.redisConf
            process.nextTick(function(){
                try{
                    self.emit("error",err)
                  }catch(e){
                }
            })
        })
        this.redisClient.on("connect",function(){
            self.emit("connect",self.redisConf)
        })
        this.redisClient.on("ready",function(){
            log.log("debug", "Connected to redis [cluster_client:%j] [error_type:connect_error]", self.redisConf)
            self.redisLoaded = true;
        })
    }catch(e){
        this.emit("error",e)
    }
}

BigQueueClient.prototype.createTopic = function(topic, callback){
    var topic = arguments[0]
    var ttl = -1
    var callback
    if(arguments.length == 3){
        if(ttl != undefined){
            ttl = arguments[1]
        }
        callback = arguments[2]
    }else{
        callback = arguments[1]
    }
    this.redisClient.execute("EVAL",redisScripts["createTopic"],0,topic,ttl,function(err,data){
       if(err){
            log.log("error", "Error creating topic ["+err+"] [error_type:create_topic_error]")
            err = {"msg":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.deleteTopic = function(topic,callback){
    this.redisClient.execute("EVAL",redisScripts["deleteTopic"],0,topic,function(err,data){
        if(err){
            log.log("error", "Error deleting topic ["+err+"] [error_type:delete_topic_error] "+err)
            err = {"msg":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.createConsumerGroup = function(topic, consumerGroup,callback){
    this.redisClient.execute("EVAL",redisScripts["createConsumerGroup"],0,topic,consumerGroup,function(err,data){
        if(err){
            log.log("error", "Error creating group [consumer:"+consumerGroup+"] for topic [topic:"+topic+"] [error_type:create_consumer_error], error: "+err)
            err = {"msg":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.deleteConsumerGroup = function(topic,consumerGroup,callback){
    this.redisClient.execute("EVAL",redisScripts["deleteConsumer"],0,topic,consumerGroup,function(err,data){
        if(err){
            log.log("error", "Error deleting group [consumer:"+consumerGroup+"] for topic [topics:"+topic+"] [error_type:delete_consumer_error], error: "+err)
            err = {"msg":err}
        }
        callCallback(callback,err)
    })

}

BigQueueClient.prototype.resetConsumerGroup = function(topic, consumerGroup,callback){
    this.redisClient.execute("EVAL",redisScripts["resetConsumerGroup"],0,topic,consumerGroup,function(err,data){
        if(err){
            log.log("error", "Error reseting group [consumer:"+consumerGroup+"] for topic [topic:"+topic+"] [error_type:delete_consumer_error], error: "+err)
            err = {"msg":err}
        }
        callCallback(callback,err)
    })
}

BigQueueClient.prototype.postMessage = function(topic, message,callback){
    if(message.msg == undefined){
        callCallback(callback,{"msg":"The message should have the 'msg' property"})
        return
    }
    var self = this
    this.redisClient.execute("EVAL",redisScripts["postMessage"],0,topic,JSON.stringify(message),function(err,data){
        if(err){
            log.log("error", "Error posting message [%j] [cluster_node:%s] [topic:%s] [error_type:create_message_error], error: %j", message, self.redisConf.host, topic, err)
            err = {"msg":""+err}
            callCallback(callback,err)
        }else{
            if(callback)
                callback(undefined,{"id":data,"topic":topic})
        }
    })
}

BigQueueClient.prototype.getMessage = function(topic,consumer,visibilityWindow,callback){
    visibilityWindow = visibilityWindow || -1
    var tms = Math.floor(new Date().getTime() / 1000)
    var self = this
    this.redisClient.execute("EVAL",redisScripts["getMessage"],0,tms,topic,consumer,visibilityWindow,function(err,data){
        if(err){
            log.log("error", "Error getting messages of topic [topic:%s] for consumer [consumer:%s] [error_type:get_message_error] [cluster_node:%s], error: "+err,topic,consumer,self.redisConf.host+":"+self.redisConf.port)
            err = {"msg":typeof(err) == "string" ? err : JSON.stringify(err)}
            callCallback(callback,err)
        }else{
            if(callback){
                var data = bqutils.redisListToObj(data)
                data.recipientCallback = data.id
                callback(undefined,data)
            }
        }
    })
}

BigQueueClient.prototype.ackMessage = function(topic,consumerGroup,id,callback){
  var self = this
    this.redisClient.execute("EVAL",redisScripts["ackMessage"],0,topic,consumerGroup,id,function(err,data){
        if(err){
            log.log("error", "Error doing ack for message [%s] in topic [topic:%s] for consumer [consumer:%s] [error_type:ack_message_error] [cluster_node:%s], error: "+err, id, topic, consumerGroup, self.redisConf.host+":"+self.redisConf.port)
            err = {"msg":typeof(err) == "string" ? err : JSON.stringify(err)}
            callCallback(callback,err)
        }else{
            if(data<=0)
                callCallback(callback,{"msg":"no message was acked"})
            else
                callCallback(callback)
        }

    })
}

BigQueueClient.prototype.failMessage = function(topic,consumerGroup,id,callback){
    this.redisClient.execute("EVAL",redisScripts["failMessage"],0,topic,consumerGroup,id,function(err,data){
       if(err)
        err = {"msg":""+err}
       callCallback(callback,err)
    })
}

BigQueueClient.prototype.getTopicTtl = function(topic,callback){
    this.redisClient.execute("GET","topics:"+topic+":ttl",function(err,data){
        if(err){
            err = {"msg":""+err}
            callback(err)
        }else{
            callback(undefined,data)
        }
    })
}

BigQueueClient.prototype.listTopics = function(callback){
    this.redisClient.execute("SMEMBERS","topics",function(err,data){
        if(err)
            log.log("error", "Error listing topics [%j]", err)
        callCallback(callback,data)
    })

}

BigQueueClient.prototype.getConsumerGroups = function(topic,callback){
    var self = this
    this.redisClient.execute("SISMEMBER","topics",topic,function(err,data){
        if(data == 0){
            callCallback(callback,{"msg":"Error topic not found","code":404})
            return
        }
        self.redisClient.execute("SMEMBERS","topics:"+topic+":consumers",function(err,data){
            if(err){
                log.log("error", "Error getting consumer groups for topic [topic:"+topic+"], error: "+err)
                callback(err)
            }
            if(callback)
                callback(undefined,data)
        })
    })
}

BigQueueClient.prototype.getConsumerStats = function(topic,consumer,callback){
    var self = this
    this.redisClient.execute("EVAL",redisScripts["consumerStats"],0,topic,consumer,function(err,data){
        if(err){
            log.log("error", "Error getting consumer stats for consumer [consumer:"+consumer+"] of topic [topic:"+topic+"] [error_type:error_getting_stats], error: "+err)
            err = {"msg":""+err}
            callCallback(callback,err)
        }else{
            callback(undefined,bqutils.redisListToObj(data))
        }
    })
}

BigQueueClient.prototype.shutdown = function(){
    this.shutdowned = true
    try{
        this.redisClient.quit()
        this.redisClient.closing = true
    }catch(e){}
}

BigQueueClient.prototype.getHeads = function(callback){
    var self = this
    this.listTopics(function(data){
        if(!data){
            callback({"msg":"Error getting topics"},undefined)
            return
        }
        if(data.length == 0)
            callback(undefined,[])
        var dataCount = 0
        var dataRet = {}
        for(var i in data){
            self.getHead(data[i],function(err,head){
                if(err){
                    err = {"msg":""+err}
                    callback(err,undefined)
                    return
                }
                dataRet[head.topic]=head.head
                dataCount++
                if(dataCount == data.length){
                    callback(undefined,dataRet)
                }
            })
        }
    })
}

BigQueueClient.prototype.healthcheck = function(callback) {
  callback();
}

BigQueueClient.prototype.getNodeStats = function(callback) {
  var self = this;
  var resp = {
    sample_date: new Date().getTime(),
    topics_stats:[],
    node_stats: {}
  };

  async.waterfall([
    function(cb) {
      self.listTopics(function(topics) {
        cb(undefined, topics);
      });
    },
    function(topics, cb) {
      //Get Stats for all topics
      async.each(topics, function(topic, fEachCb) {
        var topic_stats = {topic_id: topic, consumers:[]};
        async.parallel([
          //Get Consumer stats
          function(pCb) {
            self.getConsumerGroups(topic, function(err, consumers) {
              if(err) {
                return pCb(err);
              }
              async.each(consumers, function(consumer, cCb) {
                self.getConsumerStats(topic, consumer, function(err, stats) {
                  topic_stats.consumers.push({consumer_id: consumer, consumer_stats: stats});
                  cCb(err);
                });
              }, pCb);
            });
          },
          //Get topic head
          function(pCb) {
            self.getHead(topic, function(err, data) {
              topic_stats["topic_head"] = data && data.head && parseInt(data.head);
              pCb(err);
            });
          }
        ], function(err) {
          resp.topics_stats.push(topic_stats);
          fEachCb(err);
        });
      }, cb);
    },
    //Get node stats
    function(cb) {
      resp.node_stats=self.redisClient && self.redisClient.server_info;
      cb();
    }
    ],function(err, result) {
      callback(err, resp);
  });
}

BigQueueClient.prototype.getHead = function(topic,callback){
    this.redisClient.execute("GET","topics:"+topic+":head",function(err,data){
        if(err){
            err = {"msg":""+err}
            callback(err,undefined)
        }
        if(!data){
            data = -1
        }
        callback(undefined,{"topic":topic,"head":data})
    })
}

function callCallback(){
    if(arguments.length <=0 )
        return;
    var callback = arguments[0]
    var check = arguments[1]
    if(callback){
        if(check){
            callback(check)
        }else{
            callback()
        }
    }

}
exports.bqClient = BigQueueClient
exports.createClient = function(redisConf){


    if(!redisConf.options)
        redisConf.options = {}
    redisConf.options["return_buffers"] = false
    var cli = new BigQueueClient(redisConf)
    cli.connect()
    return cli;
}
