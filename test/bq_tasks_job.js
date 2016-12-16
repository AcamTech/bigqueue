var should = require("should"),
    nock = require("nock"),
    async = require("async"),
    TasksJob = require("../lib/bq_tasks_job.js");
describe("Tasks Node Job", function() {


  function MockClient(data) {
    this.ops = [];
    this.createData = data;
    var self = this;
    
    this.createTopic = function(topic, ttl, callback) {
      self.ops.push({op: "CREATE_TOPIC", data: {topic: topic, ttl: ttl}});
      callback(); 
    }
    
    this.createConsumerGroup = function(topic, consumer, callback) {
      self.ops.push({op: "CREATE_CONSUMER", data: {topic: topic, consumer: consumer}});
      callback(); 
    }
 
    this.resetConsumerGroup = function(topic, consumer, callback) {
      self.ops.push({op: "RESET_CONSUMER", data: {topic: topic, consumer: consumer}});
      callback(); 
    }

    this.deleteTopic = function(topic, callback) {
      self.ops.push({op: "DELETE_TOPIC", data: {topic: topic}});
      callback(); 
    }

    this.deleteConsumerGroup = function(topic, consumer, callback) {
      self.ops.push({op: "DELETE_CONSUMER", data: {topic: topic, consumer: consumer}});
      callback(); 
    }
  }

  var tasks_job_config = {
    dataNodeId: "node1",
    createNodeClientFunction: createMock,
    refreshTime: 10,
    adminApiUrl:"http://adminapi.bigqueue.com",
    nodeConfig: {test: true}
  }
  var mock;

  function createMock(data) {
    mock = new MockClient(data);
    return mock; 
  }

  var directResults = [];
  
  var tasksJob;

  before(function() {
    nock("http://adminapi.bigqueue.com")
      .defaultReplyHeaders({"Content-Type":"application/json"})
      .filteringPath(/tasks.*/g, 'tasks')
      .get("/tasks")
      .reply(200, function(req, b) {
      var result = directResults.shift();  
      if(result) {
        if(typeof(result) === "function") {
          return result(req, b);
        } else {
          return result;
        }
      } else {
          return []; 
      }
    })
    .persist();
  });
  after(function() {
    nock.cleanAll();
  });

  beforeEach(function() {
    tasksJob = new TasksJob(tasks_job_config);
  });

  afterEach(function() {
    tasksJob.shutdown();
  });

  it("Should use the config create function and the nodeConfig parameter", function(done) {
    mock.createData.test.should.be.ok;
    done();
  });

  it("Should support CREATE_TOPIC task", function(done) {
    async.series([
      function(cb) {
        nock("http://adminapi.bigqueue.com")
        .put("/tasks/1", {"task_status":"FINISHED"})
        .reply(204, {});
        cb();
      },
      function(cb) {
        directResults.push([{task_id: 1, task_type: "CREATE_TOPIC", task_data:{topic_id: "TEST_TOPIC", ttl: 1000}}]);
        cb();
      },
      function(cb) {
        tasksJob.doProcess(cb);
      },
      function(cb) {
        mock.ops.length.should.equal(1);
        var op = mock.ops.pop();
        op.op.should.equal("CREATE_TOPIC");
        op.data.topic.should.equal("TEST_TOPIC");  
        op.data.ttl.should.equal(1000);
        cb();  
      }
    ], done);

  });
  it("Should support CREATE_CONSUMER task", function(done) {
    async.series([
      function(cb) {
        nock("http://adminapi.bigqueue.com")
        .put("/tasks/1", {"task_status":"FINISHED"})
        .reply(204, {});
        cb();
      },
      function(cb) {
        directResults.push([{task_id: 1, task_type: "CREATE_CONSUMER", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER" }}]);
        cb();
      },
      function(cb) {
        tasksJob.doProcess(cb);
      },
      function(cb) {
        mock.ops.length.should.equal(1);
        var op = mock.ops.pop();
        op.op.should.equal("CREATE_CONSUMER");
        op.data.topic.should.equal("TEST_TOPIC");  
        op.data.consumer.should.equal("TEST_CONSUMER");  
        cb();  
      }
    ], done);

  });
  it("Should support RESET_CONSUMER task", function(done) {
    async.series([
      function(cb) {
        nock("http://adminapi.bigqueue.com")
        .put("/tasks/1", {"task_status":"FINISHED"})
        .reply(204, {});
        cb();
      },
      function(cb) {
        directResults.push([{task_id: 1, task_type: "RESET_CONSUMER", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER" }}]);
        cb();
      },
      function(cb) {
        tasksJob.doProcess(cb);
      },
      function(cb) {
        mock.ops.length.should.equal(1);
        var op = mock.ops.pop();
        op.op.should.equal("RESET_CONSUMER");
        op.data.topic.should.equal("TEST_TOPIC");  
        op.data.consumer.should.equal("TEST_CONSUMER");  
        cb();  
      }
    ], done);

  });
  it("Should support DELETE_TOPIC task", function(done) {
   async.series([
      function(cb) {
        nock("http://adminapi.bigqueue.com")
        .put("/tasks/1", {"task_status":"FINISHED"})
        .reply(204, {});
        cb();
      },
      function(cb) {
        directResults.push([{task_id: 1, task_type: "DELETE_TOPIC", task_data:{topic_id: "TEST_TOPIC"}}]);
        cb();
      },
      function(cb) {
        tasksJob.doProcess(cb);
      },
      function(cb) {
        mock.ops.length.should.equal(1);
        var op = mock.ops.pop();
        op.op.should.equal("DELETE_TOPIC");
        op.data.topic.should.equal("TEST_TOPIC");  
        cb();  
      }
    ], done);
  });
  it("Should support DELETE_CONSUMER task", function(done) {
     async.series([
      function(cb) {
        nock("http://adminapi.bigqueue.com")
        .put("/tasks/1", {"task_status":"FINISHED"})
        .reply(204, {});
        cb();
      },
      function(cb) {
        directResults.push([{task_id: 1, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER"}}]);
        cb();
      },
      function(cb) {
        tasksJob.doProcess(cb);
      },
      function(cb) {
        mock.ops.length.should.equal(1);
        var op = mock.ops.pop();
        op.op.should.equal("DELETE_CONSUMER");
        op.data.topic.should.equal("TEST_TOPIC");  
        op.data.consumer.should.equal("TEST_CONSUMER");  
        cb();  
      }
    ], done);
  });
  it("Should fail if any task of list fail", function(done) {
    async.series([
      function(cb) {
        nock("http://adminapi.bigqueue.com")
        .put("/tasks/1", {"task_status":"FINISHED"})
        .reply(204, {});
        cb();
      },
      function(cb) {
        directResults.push([
         {task_id: 1, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER"}},
         {task_id: 1, task_type: "UNKNOWN_TASK", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER"}},
         {task_id: 2, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC1", consumer_id: "TEST_CONSUMER1"}}
        ]);
        cb();
      },
      function(cb) {
        tasksJob.doProcess(function(err) {
          should.exist(err);
          mock.ops.length.should.equal(1);
          cb();
        });
      }
    ], done);

  });
  it("Should exec tasks in order", function(done) {
    async.series([
      function(cb) {
        nock("http://adminapi.bigqueue.com")
        .put("/tasks/1", {"task_status":"FINISHED"})
        .reply(204, {})
        .put("/tasks/2", {"task_status":"FINISHED"})
        .reply(204, {});

        cb();
      },
      function(cb) {
        directResults.push([
         {task_id: 1, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER"}},
         {task_id: 2, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC1", consumer_id: "TEST_CONSUMER1"}}
        ]);
        cb();
      },
      function(cb) {
        tasksJob.doProcess(function(err) {
          should.not.exist(err);
          mock.ops.length.should.equal(2);
          mock.ops[0].data.topic.should.equal("TEST_TOPIC");  
          mock.ops[1].data.topic.should.equal("TEST_TOPIC1");  
          cb();
        });
      }
    ], done);

  });
  it("Should put a tasks as FINISHED when the task is complete", function(done) {
        nock("http://adminapi.bigqueue.com")
        .put("/tasks/1", {"task_status":"FINISHED"})
        .reply(204, function() {
          done();
        });
        directResults.push([{task_id: 1, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER"}}]);
        tasksJob.doProcess(function(err) {
          should.not.exist(err);
        });
  });
  it("Should enable monitor constantly for new events", function(done) {
    nock("http://adminapi.bigqueue.com")
      .put("/tasks/1", {"task_status":"FINISHED"})
      .reply(204,{}).persist();
  
      async.series([
        function(cb) {
          tasksJob.doMonitor();
          cb();
        },
        function(cb) {
          directResults.push([
           {task_id: 1, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER"}},
           {task_id: 1, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC1", consumer_id: "TEST_CONSUMER1"}}
          ]);
          cb();
        },
        function(cb) {
          setTimeout(cb, 200);
        },
        function(cb) {
          mock.ops.length.should.equal(2);
          cb();
        },
        function(cb) {
          directResults.push([
           {task_id: 1, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC", consumer_id: "TEST_CONSUMER"}},
           {task_id: 1, task_type: "DELETE_CONSUMER", task_data:{topic_id: "TEST_TOPIC1", consumer_id: "TEST_CONSUMER1"}}
          ]);
          cb();
        },
        function(cb) {
          setTimeout(cb, 200);
        },
        function(cb) {
          mock.ops.length.should.equal(4);
          cb();
        }
      ],done);

  });
});
