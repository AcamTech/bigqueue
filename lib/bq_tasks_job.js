var needle = require("needle"),
    async = require("async"),
    log = require("../lib/bq_logger.js");

function TasksJob(config) {
  this.adminApi = config.adminApiUrl;
  this.tasksUrl = this.adminApi+"/tasks";
  this.pendingTasksUrl = this.adminApi+"/tasks?task_status=PENDING&data_node_id="+config.dataNodeId;
  this.bqClient = config.createNodeClientFunction(config.nodeConfig);
}

TasksJob.prototype.applyTask = function(task, callback) {
  var self = this;
  switch(task.task_type) {
    case "CREATE_TOPIC":
      self.bqClient.createTopic(task.task_data.topic_id, task.task_data.ttl, callback);
      break;
    case "CREATE_CONSUMER":
      self.bqClient.createConsumerGroup(task.task_data.topic_id, task.task_data.consumer_id, callback);
      break;
    case "RESET_CONSUMER":
      self.bqClient.resetConsumerGroup(task.task_data.topic_id, task.task_data.consumer_id, callback);
      break;
    case "DELETE_TOPIC":
      self.bqClient.deleteTopic(task.task_data.topic_id, callback);
    break;
    case "DELETE_CONSUMER":
      self.bqClient.deleteConsumerGroup(task.task_data.topic_id, task.task_data.consumer_id, callback);
      break;
    default:
      callback({msg: "Task type ["+task.task_type+"] not valid"});
  }
}

TasksJob.prototype.doProcess = function(callback) {
  var self = this;
  needle.get(this.pendingTasksUrl, function(err, response) {
    if(err) {
      log.log("error", "Tasks job error calling api",err);
      return callback(err);
    }
    if(response.statusCode != 200) {
      log.log("error", "Error getting data from tasks [url: "+self.pendingTasksUrl+"] [status: "+response.statusCode+"] [response: "+response.body+"]")
      return callback({msg: "Error getting pending tasks from ["+self.pendingTasksUrl+"]"});
    }
    var tasks = response.body;
    async.eachSeries(tasks, function(task, cb) {
      async.series([
        function(sCb) {
          log.log("info", "Procesando: ",JSON.stringify(task));
          self.applyTask(task, sCb);
        },
        function(sCb) {
          needle.put(self.tasksUrl+"/"+task.task_id,{task_status: "FINISHED"}, {json:true}, function(err, resp) {
            if(err) {
              log.log("error", "Tasks Job, error puting status as finished", err);
              return sCb(err);
            }
            if(response.statusCode > 299) {
              log.log("error", "Error changing status of ["+task.stas_id+"] [status: "+response.statusCode+"] [response: "+response.body+"]");
              return sCb({msg: "Error changing status of taskId ["+task.task_id+"]"});
            }
            log.log("info", "Estado de tarea finalizado correctamente ["+self.tasksUrl+"/"+task.task_id+"]");
            sCb();
          });
        }
      ], cb);
    }, callback);
  });
}

TasksJob.prototype.doMonitor = function(callback) {
  var self = this;
  function monitor() {
    self.doProcess(function(err) {
      if(err) {
        if(callback)
          callback(err);
      } else if(!self.shutdowned) {
        setTimeout(monitor, self.refreshTime);
      }
    });
  }
  monitor();
}

TasksJob.prototype.shutdown = function() {
  this.shutdowned = true;
}

module.exports = TasksJob;
