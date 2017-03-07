var redis = require('redis'),
    bqutils = require('../lib/bq_client_utils.js'),
    fs = require('fs'),
    should = require('should'),
    events = require("events"),
    log = require("../lib/bq_logger.js")

/**
 *   scripts load
**/
var TOTAL_SCRIPTS = 2
var redisScripts= {}
fs.readFile(__dirname+'/../scripts/journalWrite.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["journalWrite"] = strFile
})
fs.readFile(__dirname+'/../scripts/journalRetrieve.lua','ascii',function(err,strFile){
    should.not.exist(err)
    redisScripts["journalRetrieve"] = strFile
})


function BigQueueJournalClient(conf){
    if(conf == undefined)
        conf = {}
    if(conf.options == undefined)
        conf.options = {}
    conf.options["return_buffers"] = false
    this.redisConf = conf
    this.redisLoaded = false
    this.shutdowned = false
    var self = this;
    self.emitReadyWhenAllLoaded()

}


BigQueueJournalClient.prototype = Object.create(require('events').EventEmitter.prototype);

BigQueueJournalClient.prototype.emitReadyWhenAllLoaded = function(){
    var self = this;
    var scriptsLoaded = (Object.keys(redisScripts).length == TOTAL_SCRIPTS)
    if(scriptsLoaded && this.redisLoaded){
        var checkRedisPing = function(){
            if(!self.shutdowned){
                self.redisClient.send_command("ping", [], function(err,data){
                    if(!err && data == "PONG") {
                        if(!self.ready) {
                          self.emit("ready")
                          self.ready = true;
                        }
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

BigQueueJournalClient.prototype.connect = function(){
    var self = this;
    try{
        this.redisClient = redis.createClient(this.redisConf.port,this.redisConf.host,this.redisConf.options)
        this.redisClient.on("error",function(err){
            log.log("error", "Error connecting to ["+JSON.stringify(self.redisConf)+"] ["+err+"]")
            err["redisConf"] = self.redisConf
            process.nextTick(function(){
                self.emit("error",err)
            })
        })
        this.redisClient.on("connect",function(){
            self.emit("connect",self.redisConf)
        })
        this.redisClient.on("ready",function(){
            log.log("debug", "Connected to redis [%j]", self.redisConf);
            self.redisLoaded = true;
        })
    }catch(e){
        this.emit("error",e)
    }
}

BigQueueJournalClient.prototype.write = function(node,topic, messageId, message, ttl,callback){
    var self = this
    var timer = log.startTimer()
    this.redisClient.send_command("eval",[redisScripts["journalWrite"],0,node,topic,messageId,JSON.stringify(message),ttl],function(err,data){
        if(err){
            log.log("error", "Error journaling message [%j], error: %s", message, err)
            if(callback)
                callback(err)
        }else{
            if(callback)
                callback(undefined)
        }
    })
}

BigQueueJournalClient.prototype.retrieveMessages = function(node,topic, idFrom, callback){
    this.redisClient.send_command("eval",[redisScripts["journalRetrieve"],0,node,topic, idFrom], function(err,data){
        if(err){
            if(callback)
                callback(err)
        }else{
            var res = []
            for(var msg in data){
                res.push(bqutils.redisListToObj(data[msg]))
            }
            if(callback)
                callback(undefined, res)
        }
    })
}

BigQueueJournalClient.prototype.shutdown = function(){
    this.shutdowned = true 
    try{
        this.redisClient.quit()
        this.redisClient.closing = true

    }catch(e){}
}

exports.createJournalClient = function(redisConf){
    if(!redisConf.options)
        redisConf.options = {}
    if(typeof redisConf.options == 'string') {
        redisConf.options = JSON.parse(redisConf.options);
    }
    var cli = new BigQueueJournalClient(redisConf)
    cli.connect()
    return cli;
}

