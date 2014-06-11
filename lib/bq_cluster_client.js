var fs = require('fs'),
    should = require('should'),
    events = require("events"),
    metricCollector = require('../lib/metricsCollector.js'),
    os = require("os"),
    log = require("winston"),
    NodesMonitor = require("../lib/bq_nodes_monitor.js");

//Default ttl 3 days
var defaultTtl = 3*24*60*60

var DOWN_STATUS="DOWN";

var buckets = ["< 5",">= 5 && < 10",">= 10 && < 20",">= 20 && < 30",">= 30 && < 40",">= 40 && < 50",">= 50 && < 60",">= 60 && < 70", ">= 70 && < 80",">= 80 && < 90",">= 90 && < 100",">= 100"]
var shouldCount = function(val){
           if(val < 5)
               return "< 5"
           else if(val >= 5 && val < 10)
               return ">= 5 && < 10"
           else if(val >= 10 && val < 20)
               return ">= 10 && < 20"
           else if(val >= 20 && val < 30)
               return ">= 20 && < 30"
           else if(val >= 30 && val < 40)
               return ">= 30 && < 40"
           else if(val >= 40 && val < 50)
               return ">= 40 && < 50"
           else if(val >= 50 && val < 60)
               return ">= 50 && < 60"
           else if(val >= 60 && val < 70)
               return ">= 60 && < 70"
           else if(val >= 70 && val < 80)
               return ">= 70 && < 80"
           else if(val >= 80 && val < 90)
               return ">= 80 && < 90"
           else if(val >= 90 && val < 100)
               return ">= 90 && < 100"
           else
               return ">= 100"
       }

var statsInit = exports.statsInit = false
var clusterStats
var getStats
var postStats
var ackStats
var failStats
var clientStats
var journalStats

/**
 *  Object responsible for execute all commands throght the bq cluster
 */
function BigQueueClusterClient(createClientFunction,
                               createJournalFunction,
                               clientTimeout,
                               refreshInterval,
                               cluster,
                               adminApiUrl,
                               statsInterval,
                               statsFile){
    if(!exports.statsInit){
        exports.statsInit = true
        clusterStats = metricCollector.createCollector("clusterStats",buckets,shouldCount,statsFile,statsInterval)
        getStats = metricCollector.createCollector("getStats",buckets,shouldCount,statsFile,statsInterval)
        postStats = metricCollector.createCollector("postStats",buckets,shouldCount,statsFile,statsInterval)
        ackStats = metricCollector.createCollector("ackStats",buckets,shouldCount,statsFile,statsInterval)
        failStats = metricCollector.createCollector("failStats",buckets,shouldCount,statsFile,statsInterval)
        clientStats = metricCollector.createCollector("clientStats",buckets,shouldCount,statsFile,statsInterval)
        journalStats = metricCollector.createCollector("journalStats",buckets,shouldCount,statsFile,statsInterval)
    }

    this.nodes=[]
    this.journals=[]
    this.statsInterval = statsInterval || -1
    this.statsFile = statsFile
    this.createClientFunction = createClientFunction
    this.createJournalFunction = createJournalFunction
    this.loading = true
    this.clientTimeout = clientTimeout || 125
    this.hostname = os.hostname()
    this.uid = 0
    
    this.shutdowned = false
    this.nodeMonitor = new NodesMonitor({adminApi: adminApiUrl, refreshInterval: refreshInterval, cluster: cluster });
    this.nodeMonitor.on("nodeChanged",function(node) {self.nodeDataChange(node)});
    this.nodeMonitor.on("nodeRemoved",function(node) {self.nodeRemoved(node)});
    var self = this
  

    //Metada about the distribution of the clients by zookeeper path
    this.clientsMeta = {}
    this.clientsMeta["node"]= {
        "createClient": self.createClientFunction,
        "clientsList": "nodes"
    }
    this.clientsMeta["journal"]= {
        "createClient": self.createJournalFunction,
        "clientsList": "journals"
    }
    process.nextTick(function() {
      self.emit("ready");
    });

}

BigQueueClusterClient.prototype = Object.create(require('events').EventEmitter.prototype);


BigQueueClusterClient.prototype.getClientById = function(list, id){
    for(var i=0; i<list.length; i++){
        if(list[i] && list[i].id == id)
            return list[i]     
    }
}
/**
 * Look for a node with specific id
 */
BigQueueClusterClient.prototype.getNodeById = function(id){
    return this.getClientById(this.nodes,id)
}
BigQueueClusterClient.prototype.getJournalById = function(id){
    return this.getClientById(this.journals,id)
}

BigQueueClusterClient.prototype.shutdown = function(){
    this.shutdowned = true
    this.nodeMonitor.shutdown();
    clearInterval(this.refreshInterval)
    for(var i in this.clientsMeta){
        var type = this.clientsMeta[i]
        var clients = this[type.clientsList]
        for(var j in clients){
            clients[j].client.shutdown()
        }
    }
}
/**
 * When node is added we0ll create a clusterNode object and if is loading we will notify this load
 */
BigQueueClusterClient.prototype.nodeAdded = function(node){
    var self = this
    if(node.status == DOWN_STATUS) {
      return;
    } 
    var meta = this.clientsMeta[node.type]
    var clusterNode = {}
    clusterNode["id"] = node.id
    clusterNode["data"] = node; 
    
    clusterNode["client"] = meta.createClient(clusterNode.data)
    
    this[meta.clientsList].push(clusterNode)
    clusterNode.client.on("error",function(err){
      log.error(err)
    })
    log.info("Node added [%j]", node)

}
/**
 * When a node is removed we'll remove it from the nodes list
 * and re-send all messages sent by this node
 */
BigQueueClusterClient.prototype.nodeRemoved = function(node){
   var meta = this.clientsMeta[node.type]
   var clusterNode = this.getClientById(this[meta.clientsList],node.id)
    if(!clusterNode)
        return
    if(clusterNode["shutdown"] != undefined)
        clusterNode.shutdown()
    this[meta.clientsList] = this[meta.clientsList].filter(function(val){
        return val.id != clusterNode.id
    })
    log.info("Node removed [%j]", node)
}

/**
 * When a node data is changes we'll load it and if the node chage their status 
 * from up to any other we'll re-send their cached messages
 */
BigQueueClusterClient.prototype.nodeDataChange = function(node){
  var meta = this.clientsMeta[node.type]
  var newData = node
  var clusterNode = this.getClientById(this[meta.clientsList],node.id)
  if(!clusterNode){
      this.nodeAdded(node)
  }else{
    clusterNode.data = newData
    log.info("Node data chaged [%j]", node)
    if(newData.status == DOWN_STATUS) {
      this.nodeRemoved(node);
    }
  }
}

/**
 * Filter the nodes list by UP status
 */
BigQueueClusterClient.prototype.getNodesNoUp = function(){
    return this.nodes.filter(function(val){
        return val && val.data && val.data.status != "UP"
    })
}



/**
 * Call execute a function with all nodes and call callback when all execs are finished
 * the way to detect finishes are using a monitor that will be called by the exec function
 */
BigQueueClusterClient.prototype.withEvery = function(list,run,callback){
    var count = 0
    var total = list.length
    var hasErrors = false
    var errors = []
    var datas = []
    if(list.length == 0)
        callback()
    var monitor = function(err,data){
        if(err){
            hasErrors = true;
            errors.push({"msg":err})
        }
        if(data){
            datas.push(data)
        }
        count++
        if(count >= total){
            if(hasErrors)
                callback(errors,datas)
            else
                callback(undefined,datas)
        }
    }
    for(var i in list){
        run(list[i],monitor)
    }
}

/**
 * Get one node in a round-robin fashion
 */
BigQueueClusterClient.prototype.nextNodeClient = function(){
    var node = this.nodes.shift()
    this.nodes.push(node)
    return node
}

BigQueueClusterClient.prototype.generateClientUID = function(){
    return this.hostname+":"+(new Date().getTime())+":"+(this.uid++)
}

/**
 * Exec a function with one client if an error found we try with other client until
 * there are nodes into the list if the function fails with all nodes the callback will be
 * called with an error
 */
BigQueueClusterClient.prototype.withSomeClient = function(run,cb){
    var self = this
    var calls = 0
    var actualClient
    function monitor(){
        this.timer = log.startTimer()
        var m = this
        m.emited = false
        setTimeout(function(){
            if(!m.emited){
                m.timer("timeout found")
                log.info("Timeout on client: %j", actualClient)
                m.emit("timeout",undefined)
            }
        },self.clientTimeout)

        this.emit = function(err,data,finishWithNoData){
            var shouldEmitWithNoData = true
            if(finishWithNoData != undefined)
                shouldEmitWithNoData = finishWithNoData

            m.timer("Monitor emit called")
            if(m.emited)
                return
            m.emited = true
            calls++
            if(!err && shouldEmitWithNoData){
                m.timer("Finishing monitor")
                cb(err,data)
                return
            }
            if(calls < self.nodes.length){
                actualClient = self.nextNodeClient()
                run(actualClient,new monitor())
            }else{
                var error = err
                m.timer("Finishing monitor exec with errors ["+error+"] ")
                if(error) {
                  var msg = typeof(error) === "object" ? JSON.stringify(error) : error;
                  cb({msg:"Node execution fail ["+msg+"]"},undefined)
                } else {
                    cb()
                }
            }
        }
        this.next = function(){
            this.emit(undefined,undefined,false)
        }
    }

    if(this.nodes.length == 0){
        cb({msg:"No nodes found"},null)
        return
    }
    actualClient = this.nextNodeClient()
    run(actualClient,new monitor())
}

/**
 * Get unix timestamp
 */
BigQueueClusterClient.prototype.tms = function(){
    var d = new Date().getTime()/1000
    return Math.floor(d)
}

/**
 * Post a message to one node generating a uid 
 */
BigQueueClusterClient.prototype.postMessage = function(topic,message,callback){
    var startTime = new Date().getTime()
    var timer = log.startTimer()
    var self = this
    var uid = this.generateClientUID()
    message["uid"] = uid
    this.withSomeClient(
        function(clusterNode,monitor){
            if(!clusterNode || clusterNode.data.status != "UP"){
                monitor.emit("Error with clusterNode",undefined)
                return
            }
            if(clusterNode.data.read_only){
                log.debug("Node ["+JSON.stringify(clusterNode.data)+"] is in readonly")
                return monitor.next()
            }
            var clientTimer = clientStats.timer() 
            clusterNode.client.postMessage(topic,message,function(err,key){
                clientStats.collect(clientTimer.lap())
                if(!err){
                    //If no error, write the data to the journals and load it to the local cache
                    key["uid"] = uid
                    //Load to journal
                    var journalTimer = journalStats.timer()
                    self.writeMessageToJournal(topic,key.id,message,clusterNode,function(err){
                      journalStats.collect(journalTimer.lap())
                      monitor.emit(err,key)
                    })
                    
                }else{
                  monitor.emit(err,key)
                  log.error("Error posting message [%j] [%j] [topic:%s]", err, clusterNode.data, topic)
                }
            })
        },
        function(err,key){
            var time = new Date().getTime() - startTime
            clusterStats.collect(time)
            postStats.collect(time)
            timer("Post message to cluster")
            callback(err,key)
            timer("Posted cluster callback")
        }
   )
}

BigQueueClusterClient.prototype.writeMessageToJournal = function(topic, msgId, message, clusterNode, cb){
    var self = this
    if(!clusterNode.data.journals || clusterNode.data.journals.length == 0 ){
        cb(undefined)
    }else{
        var journalIds = clusterNode.data.journals
        var journals = []
        for(var i in journalIds){
            var journal = this.getJournalById(journalIds[i])
            if(!journal){
                var err = "Error getting journal ["+journalIds[i]+"] warranties not gotten" 
                log.error(err)
                err = {"msg":err}
                return cb(err,undefined)
            }
            if(journal.data.status != "UP"){
                var err = "Error journal ["+journalIds[i]+"] DOWN" 
                log.error(err)
                err = {"msg":err}
                return cb(err,undefined)
            }
            journals.push(journal)
        }
        this.withEvery(journals,
            function(journal, jMonitor){
                journal.client.write(clusterNode.id,topic,msgId,message,self.journalTtl,function(err){
                    jMonitor(err)
                })
            },
            function(err,data){
                cb(err)
            }
        )
    }
}

/**
 * Return one message queued for a consumer group, this method will generates a recipientCallback 
 * that we'll be used 
 */
BigQueueClusterClient.prototype.getMessage = function(topic,group,visibilityWindow,callback){
    var self = this
    var metricsTimer = clusterStats.timer()
    var timer = log.startTimer()
    timer("Getting message")
    this.withSomeClient(
        function(clusterNode,monitor){    
            if(!clusterNode || (clusterNode.data.status != "UP" && clusterNode.data.status != "READONLY")){
              return monitor.emit("Cluster node error or down ["+clusterNode.data.host+"]")
            }
            self.getMessageFromNode(clusterNode.id, topic, group, visibilityWindow, monitor.emit); 
        },
        function(err,data){
          timer("Get message from cluster")
          if(data == {})
              data = undefined
          if(err){
              log.error("Error getting messages ["+err.msg+"]")
              err = {"msg":err.msg}
          }
          clusterStats.collect(metricsTimer.lap())
          getStats.collect(metricsTimer.lap())
          callback(err,data)
          timer("Getted Message from cluster Callback end")
        }
    )
}
BigQueueClusterClient.prototype.createRecipientCallback = function(nodeId, topic, group, msgId) {
  return this.encodeRecipientCallback({"nodeId":nodeId,"topic":topic,"consumerGroup":group,"id":msgId})

}
BigQueueClusterClient.prototype.getMessageFromNode = function(nodeId,topic,group,visibilityWindow,callback){
  var self = this;
  var clusterNode = this.getNodeById(nodeId);
  if(!clusterNode) {
    return callback({"msg":"Node ["+nodeId+"] not found"});
  }
  if(clusterNode.data.status != "UP" && clusterNode.data.status != "READONLY") {
    return callback({"msg":"Node ["+nodeId+"] is not UP ["+clusterNode.data.status+"]"});
  }
  var clientTimer = clientStats.timer()
  clusterNode.client.getMessage(topic,group,visibilityWindow,function(err,data){
      clientStats.collect(clientTimer.lap())
      if(err){
          callback(err,undefined)
      }else{
          if(data.id){
              data["recipientCallback"] = self.createRecipientCallback(nodeId, topic, group, data.id);
              data["nodeId"] = clusterNode.id;
              callback(undefined,data);
          }else{
              callback(undefined,undefined);
          }
      }
  })

}
/**
 * Generates the recipient callback to return at get instance
 */
BigQueueClusterClient.prototype.encodeRecipientCallback = function(data){
    var keys = Object.keys(data)
    var strData = ""
    for(var i in keys){
        strData = strData+":"+keys[i]+":"+data[keys[i]]
    }
    return strData.substring(1)
}

/**
 * Regenerate the data ecoded by #encodeRecipientCallback
 */
BigQueueClusterClient.prototype.decodeRecipientCallback = function(recipientCallback){
   var splitted = recipientCallback.split(":")
   var data = {}
   for(var i = 0; i< splitted.length; i=i+2){
        data[splitted[i]] = splitted[i+1]
   }
   return data
}

BigQueueClusterClient.prototype.ackMessage = function(topic,group,recipientCallback,callback){
    var startTime = clusterStats.timer()
    var repData = this.decodeRecipientCallback(recipientCallback)
    var node = this.getNodeById(repData.nodeId)
    if(!node || node.data.status != "UP"){
        callback({msg:"Node ["+repData.nodeId+"] not found or down","code":404})
        return
    }
    node.client.ackMessage(topic,group,repData.id,function(err){
        var time = new Date().getTime() - startTime
        clientStats.collect(startTime.lap())
        clusterStats.collect(startTime.lap())
        ackStats.collect(startTime.lap())

        if(err){
            log.error("Error doing ack of recipientCallback ["+recipientCallback+"], error: "+err.msg)
            err = {"msg":err.msg}
            return callback(err)
        }
        callback()
     })
}

BigQueueClusterClient.prototype.failMessage = function(topic,group,recipientCallback,callback){
    var startTime = clientStats.timer() 
    var repData = this.decodeRecipientCallback(recipientCallback)
    var node = this.getNodeById(repData.nodeId)
    if(!node || node.data.status != "UP"){
        callback({msg:"Node ["+repData.nodeId+"] not found or down","code":404})
        return
    }
    node.client.failMessage(topic,group,repData.id,function(err){
        clientStats.collect(startTime.lap())
        clusterStats.collect(startTime.lap())
        failStats.collect(startTime.lap())

        if(err){
            err = {"msg":err}
            log.error("Error doing fail of recipientCallback [%s], error: %j", recipientCallback, err)
        }
        callback(err)
    })
}

BigQueueClusterClient.prototype.listTopics = function(callback){
    var topicsPath = this.zkClusterPath+"/topics"
    this.zkClient.a_get_children(topicsPath,false,function(rc,error,children){
        if(rc!=0){
            log.error("Error listing topics [%j]", error)
        }
        callback(children)
    })
}

BigQueueClusterClient.prototype.getConsumerGroups = function(topic,callback){
    if(this.shutdowned)
        return callback("Client shutdowned")
    var consumerGroupsPath = this.zkClusterPath+"/topics/"+topic+"/consumerGroups"
    //Check first if node exists
    var self = this
    this.zkClient.a_exists(consumerGroupsPath,false,function(rc,error){
        if(rc != 0){
            callback({"msg":"Path ["+consumerGroupsPath+"], doesn't exists","code":404})
            return
        }
        self.zkClient.a_get_children(consumerGroupsPath,false,function(rc,error,children){
            if(rc!=0){
                log.error("Error getting consumer groups [%j]", error)
                callback({msg:"Error ["+rc+"-"+error+", "+consumerGroupsPath+"]"},undefined)
                return
            }
            callback(undefined,children)
        })
    })
}

exports.createClusterClient = function(clusterConfig){
  return  new BigQueueClusterClient(clusterConfig.createNodeClientFunction,
                                            clusterConfig.createJournalClientFunction,
                                            clusterConfig.clientTimeout,
                                            clusterConfig.refreshInterval,
                                            clusterConfig.cluster,
                                            clusterConfig.adminApiUrl,
                                            clusterConfig.statsInterval,
                                            clusterConfig.statsFile)
} 
