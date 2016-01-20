var should = require('should'),
    request = require('request'),
    log = require("../lib/bq_logger.js"),
    utils = require('../lib/bq_client_utils.js'),
    bqadm = require("../lib/bq_clusters_adm.js"),
    mysql = require("mysql"),
    async = require("async");

describe("Clusters administration for multicluster purposes",function(){
    try {
      //If any other removes it will fail
      log.remove(log.transports.Console);
    }catch(e) {}
    var mysqlConf = {
     host     : '127.0.0.1',
     user     : 'root',
     password : '',
     database : 'bigqueue',
     connectionLimit: 10,
     waitForConnections: false
    };
    var admClient;
    var mysqlConn = mysql.createConnection(mysqlConf);


    var admConfig = {
        "logLevel":"error",
        "mysqlConf":mysqlConf
    }

    before(function(done) {
        mysqlConn.connect(function(err) {
          done(err);
        });
    });


    beforeEach(function(done) {
      async.parallel([
        function(d) {
          mysqlConn.query("TRUNCATE stats", d);
        },
        function(d) {
          mysqlConn.query("TRUNCATE clusters", d);
        },
        function(d) {
          mysqlConn.query("TRUNCATE data_nodes", d);
        },
        function(d) {
          mysqlConn.query("TRUNCATE endpoints",d);
        },
        function(d) {
          mysqlConn.query("TRUNCATE topics",d);
        },
        function(d) {
          mysqlConn.query("TRUNCATE consumers",d);
        },
        function(d) {
          mysqlConn.query("TRUNCATE node_journals",d);
        },
        function(d) {
          mysqlConn.query("TRUNCATE tasks",d);
        }
      ], function(err) {
        done(err)
      });
    });
    beforeEach(function(done){
        admClient = bqadm.createClustersAdminClient(admConfig)
        admClient.on("ready",function(){
          done()
        })
    })

    afterEach(function(done){
        admClient.shutdown()
        process.nextTick(function(){
          done()
        })
    })

    describe("Create clusters",function(){
        it("should create a cluster",function(done){
          admClient.createBigQueueCluster({name:"test1"},function(err){
           should.not.exist(err)
           mysqlConn.query("SELECT * from clusters", function(err, data) {
             should.not.exist(err);
             data.length.should.equal(1);
             data[0].name.should.equal("test1");
             done();
           });
         });
        });
        it("should fail if the cluster already exists",function(done){
            admClient.createBigQueueCluster({name:"test1"},function(err){
               should.not.exist(err)
                admClient.createBigQueueCluster({name:"test1"},function(err){
                  should.exist(err)
                  done();
                })
            })

        })

        it("should support add journals on create",function(done){
          var clusterData={
              name:"test1",
              journals:[
                  {id:"journal1", host:"journal1","port":123,"status":"DOWN", options:{"test":true}},
                  {id:"journal2", host:"journal2","port":123,"status":"DOWN"}
              ]
          }
          admClient.createBigQueueCluster(clusterData,function(err){
            should.not.exist(err);
            mysqlConn.query("select * from data_nodes where type=? and cluster=? order by id",["journal","test1"], function(err, data) {
              should.not.exist(err);
              data.length.should.equal(2);
              data[0].id.should.equal("journal1");
              data[0].host.should.equal("journal1");
              data[0].port.should.equal(123);
              data[0].status.should.equal("DOWN");
              data[0].options.should.equal('{"test":true}');
              data[0].cluster.should.equal("test1");
              data[1].id.should.equal("journal2");
              data[1].host.should.equal("journal2");
              data[1].port.should.equal(123);
              data[1].status.should.equal("DOWN");
              data[1].options.should.equal('{}');
              data[1].cluster.should.equal("test1");
              done();
            });
          });
        })

        it("should support add nodes on create",function(done){
          var clusterData={
              name:"test1",
              nodes:[
                  {id:"node1",host:"node1","port":123,"status":"DOWN","journals":[], options:{"test":true}},
                  {id:"node2",host:"node2","port":123,"status":"DOWN","journals":[]}
              ]
          }
          admClient.createBigQueueCluster(clusterData,function(err){
            should.not.exist(err);
            mysqlConn.query("select * from data_nodes where type=? and cluster=? order by id",["node","test1"], function(err, data) {
              should.not.exist(err);
              data.length.should.equal(2);
              data[0].id.should.equal("node1");
              data[0].host.should.equal("node1");
              data[0].port.should.equal(123);
              data[0].status.should.equal("DOWN");
              data[0].options.should.equal('{"test":true}');
              data[0].cluster.should.equal("test1");
              data[1].id.should.equal("node2");
              data[1].host.should.equal("node2");
              data[1].port.should.equal(123);
              data[1].status.should.equal("DOWN");
              data[1].options.should.equal('{}');
              data[1].cluster.should.equal("test1");
              done();
            });
          });
        })
        it("should support add entry points on create",function(done){
            var clusterData={
                name:"test1",
                endpoints:[
                    {host:"127.0.0.1",port:8080, description:"endpoint 1"},
                    {host:"127.0.0.2",port:8080, description:"endpoint 2"}
                ]
            }
            admClient.createBigQueueCluster(clusterData,function(err){
              should.not.exist(err);
              mysqlConn.query("SELECT * FROM endpoints ORDER BY host", function(err, data) {
                data[0].host.should.equal("127.0.0.1");
                data[0].port.should.equal(8080);
                data[0].description.should.equal("endpoint 1");
                data[0].cluster.should.equal("test1");
                data[1].host.should.equal("127.0.0.2");
                data[1].port.should.equal(8080);
                data[1].description.should.equal("endpoint 2");
                data[1].cluster.should.equal("test1");

                done();
              });
            });
        });
        it("Should support node journal asociations", function(done) {
            var clusterData={
              name:"test1",
              nodes:[
                  {id:"node1",host:"node1","port":123,"status":"DOWN","journals":["j1"], options:{"test":true}}
              ],
              journals: [
                  {id:"j1",host:"node1","port":123,"status":"DOWN"}
              ]
            }
            admClient.createBigQueueCluster(clusterData,function(err){
              should.not.exist(err);
              mysqlConn.query("SELECT * FROM node_journals", function(err, data) {
                data[0].journal_id.should.equal("j1");
                done();
              });
            });
        });
    })

    describe("Modify clusters",function(){
        beforeEach(function(done){
            admClient.createBigQueueCluster({
                name:"test1",
                nodes:[
                    {id:"node1",host:"127.0.0.1",port:6379,status:"UP",journals:[]},
                    {id:"node2",host:"127.0.0.1",port:6380,status:"UP",journals:[]}
                ],
                journals:[
                    {id:"j1",host:"127.0.0.1",port:6379,status:"UP"}
                ]
            },function(err){
              should.not.exist(err)
              done()
            })
        });
        it("should support add nodes to existing cluster",function(done){
          admClient.addNodeToCluster("test1",{id:"node1",host:"127.0.0.1",port:6381},function(err){
                should.exist(err)
                admClient.addNodeToCluster("test1",{id:"node3"},function(err){
                  should.exist(err)
                    admClient.addNodeToCluster("test1",{id:"node3",host:"127.0.0.1",port:6381,status:"DOWN",journals:[]},function(err){
                      should.not.exist(err)
                        mysqlConn.query("SELECT * FROM data_nodes WHERE id=?", ["node3"], function(err,data) {
                          data[0].id.should.equal("node3");
                          data[0].host.should.equal("127.0.0.1");
                          data[0].port.should.equal(6381);
                          data[0].status.should.equal("DOWN");
                          data[0].options.should.equal('{}');
                          data[0].cluster.should.equal("test1");
                          done();
                        });
                    });
                });
            });
        });

        it("should support node modify on existing cluster",function(done){
          async.series([
            function(cb) {
              admClient.updateNodeData("test1",{id:"node2",status:"DOWN","journals":[]},function(err){
                should.not.exist(err)
                mysqlConn.query("SELECT status FROM data_nodes WHERE id=?",["node2"], function(err, data) {
                  data[0].status.should.equal("DOWN");
                  cb();
                });
              });
            },
            function(cb) {
              admClient.updateNodeData("test1",{id:"node2",journals:["j1","j2"]},function(err){
                should.not.exist(err)
                mysqlConn.query("SELECT * FROM node_journals WHERE node_id=?",["node2"], function(err, data) {
                  data.length.should.equal(2);
                  cb();
                });
              })
            }],
            done);
        })

        it("should support add journals on existing cluster",function(done){
          async.series([
            function(cb) {
              //Journal exists
              admClient.addJournalToCluster("test1",{id:"j1",host:"127.0.0.1",port:6381},function(err){
                  should.exist(err);
                  cb();
              })
            },
            function(cb) {
                admClient.addJournalToCluster("test1",{id:"j2",host:"127.0.0.1",port:6381},function(err){
                  should.not.exist(err)
                  mysqlConn.query("SELECT host, port FROM data_nodes WHERE id=?",["j2"], function(err, data) {
                    data[0].port.should.equal(6381);
                    data[0].host.should.equal("127.0.0.1");
                    cb();
                  });
                });
            }], done);
        });
        it("should support journal modify",function(done){
          admClient.updateJournalData("test1",{id:"j1",status:"DOWN"},function(err){
            should.not.exist(err)
            mysqlConn.query("SELECT status FROM data_nodes WHERE id=?",["j1"], function(err, data) {
              data[0].status.should.equal("DOWN");
              done();
            });
        });
      })
      it("Should create tasks for create topics and consumers on the added node", function(done) {
        async.series([
          function(cb) {
            admClient.createTopic({tenant_id:"test",tenant_name:"test", name:"t1", cluster:"test1", ttl:10},cb);
          },
          function(cb) {
              admClient.createConsumerGroup({topic_id:"test-test-t1", tenant_id:"test",tenant_name:"test", name:"c1"},cb);
          },
          function(cb) {
              admClient.createConsumerGroup({topic_id:"test-test-t1", tenant_id:"test",tenant_name:"test", name:"c2"},cb);
          },
          function(cb) {
            admClient.addNodeToCluster("test1",{id:"node4",host:"127.0.0.1",port:6381},cb);
          },
          function(cb) {
            mysqlConn.query("SELECT * FROM tasks WHERE data_node_id = ? ORDER BY task_id", ["node4"], function(err, data) {
              data[0].task_type.should.equal("CREATE_TOPIC");
              JSON.parse(data[0].task_data).topic_id.should.equal("test-test-t1");
              JSON.parse(data[0].task_data).ttl.should.equal(10);
              data[1].task_type.should.equal("CREATE_CONSUMER");
              JSON.parse(data[1].task_data).consumer_id.should.equal("test-test-c1");
              JSON.parse(data[1].task_data).topic_id.should.equal("test-test-t1");
              data[2].task_type.should.equal("CREATE_CONSUMER");
              JSON.parse(data[2].task_data).consumer_id.should.equal("test-test-c2");
              JSON.parse(data[2].task_data).topic_id.should.equal("test-test-t1");
              cb();
            });
          }
        ], done);
      });

  });
  describe("Create topics and groups",function(){
      beforeEach(function(done){
        async.series([
          function(cb) {
            admClient.createBigQueueCluster({
                  name:"test1",
                  nodes:[
                      {id:"node1-1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                      {id:"node2-1","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                  ]
             },cb);
          },
          function(cb) {
             admClient.createBigQueueCluster({
                  name:"test2",
                  nodes:[
                      {id:"node1-2","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                      {id:"node2-2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
                  ]
             },cb);
          },
          function(cb) {
            mysqlConn.query("UPDATE clusters SET `default`=? WHERE name=?",["Y","test1"], cb);
          }], done);
      });

        it("should enable to create topics into the default clusters",function(done){
          admClient.createTopic({"name":"test","tenant_id":"1234","tenant_name":"456"},function(err){
            mysqlConn.query("SELECT cluster FROM topics WHERE topic_name=?",["test"], function(err, data) {
              data[0].cluster.should.equal("test1");
              done();
            });
          });
        })

        it("should create the id based on topic data", function(done) {
          admClient.createTopic({"name":"test","tenant_id":"1234","tenant_name":"456"},function(err){
            mysqlConn.query("SELECT topic_id FROM topics WHERE topic_name=?",["test"], function(err, data) {
              data[0].topic_id.should.equal("1234-456-test");
              done();
            });
          });
        });

        it("should enable to create topics with an specific ttl",function(done){
          admClient.createTopic({"name":"test","ttl":100, "tenant_id":"1234","tenant_name":"456"},function(err){
             should.not.exist(err)
             mysqlConn.query("SELECT ttl FROM topics WHERE topic_name=?",["test"], function(err, data) {
              data[0].ttl.should.equal(100);
              done();
            });
          })
        })

        it("should enable to create topics into an specific cluster",function(done){
          admClient.createTopic({"name":"test","tenant_id":"1234","tenant_name":"456", "cluster":"test2"},function(err){
            mysqlConn.query("SELECT cluster FROM topics WHERE topic_name=?",["test"], function(err, data) {
              data[0].cluster.should.equal("test2");
              done();
            });
          });
        })
       it("should fail if the topic exist in any cluster",function(done){
          admClient.createTopic({"name":"test","tenant_id":"1234","tenant_name":"456", "cluster":"test2"},function(err){
            should.not.exist(err);
            admClient.createTopic({"name":"test","tenant_id":"1234","tenant_name":"456", "cluster":"test2"},function(err){
              should.exist(err);
              done();
            })
          });
       });
       it("Should fail if cluster doesn't exist", function(done) {
          admClient.createTopic({"name":"test","tenant_id":"1234","tenant_name":"456", "cluster":"test3"},function(err){
            should.exist(err);
            done();
          });
       });
       it("Should fail if tenant_id or tenant_name are not present", function(done) {
          admClient.createTopic({"name":"test","tenant_name":"456"},function(err){
            should.exist(err);
            admClient.createTopic({"name":"test","tenant_id":"1234"},function(err){
              should.exist(err);
              done();
            });
          });
       });
       it("Should enable create consumer", function(done) {
        async.series([
          function(cb) {
            admClient.createTopic({name:"test",tenant_id:"1234",tenant_name:"456", cluster:"test2"},cb);
          },
          function(cb) {
            admClient.createConsumerGroup({name:"test-consumer",tenant_id:"tenant_test",tenant_name:"tenant_name",topic_id:"1234-456-test"},function(err){
              mysqlConn.query("SELECT * FROM consumers", function(err, data) {
                data[0].consumer_id.should.equal("tenant_test-tenant_name-test-consumer");
                data[0].consumer_name.should.equal("test-consumer");
                data[0].tenant_id.should.equal("tenant_test");
                data[0].tenant_name.should.equal("tenant_name");
                data[0].topic_id.should.equal("1234-456-test");
                cb();
              });
            });
          }
        ], done);
       });
       it("Should fail if topic doesn't exists", function(done) {
          admClient.createConsumerGroup({name:"test-consumer",tenant_id:"tenant_test",tenant_name:"tenant_name",topic_id:"1234-456-test"},function(err){
            should.exist(err);
            done();
          });
       });
       it("Should fail if tenant_id or tenant_name are not set", function(done) {
        async.series([
          function(cb) {
            admClient.createTopic({name:"test",tenant_id:"1234",tenant_name:"456", cluster:"test2"},cb);
          },
          function(cb) {
            admClient.createConsumerGroup({name:"test-consumer",tenant_name:"tenant_name",topic_id:"1234-456-test"},function(err){
              should.exist(err);
              cb();
            });
          },
          function(cb) {
            admClient.createConsumerGroup({name:"test-consumer",tenant_id:"tenant_test",topic_id:"1234-456-test"},function(err){
              should.exist(err);
              cb();
            });
          }
        ], done);
       });

       it("Should create tasks for created topic", function(done) {
        async.series([
          function(cb) {
            admClient.createTopic({name:"test",tenant_id:"1234",tenant_name:"456", cluster:"test2", ttl: 5},cb);
          },
          function(cb) {
            mysqlConn.query("SELECT * FROM tasks ORDER BY data_node_id", function(err, data) {
              data[0].data_node_id.should.equal("node1-2");
              data[0].task_type.should.equal("CREATE_TOPIC");
              JSON.parse(data[0].task_data).topic_id.should.equal("1234-456-test");
              JSON.parse(data[0].task_data).ttl.should.equal(5);
              data[1].data_node_id.should.equal("node2-2");
              data[1].task_type.should.equal("CREATE_TOPIC");
              JSON.parse(data[1].task_data).topic_id.should.equal("1234-456-test");
              JSON.parse(data[1].task_data).ttl.should.equal(5);
              cb();
            });
          }
        ], done);

       });
       it("Should create tasks for created consumer", function(done) {
        async.series([
          function(cb) {
            admClient.createTopic({name:"test",tenant_id:"1234",tenant_name:"456", cluster:"test2"},cb);
          },
          function(cb) {
            admClient.createConsumerGroup({name:"test-consumer",tenant_id: "test",tenant_name:"tenant_name",topic_id:"1234-456-test"},function(err){
              should.not.exist(err);
              cb();
            });
          },
          function(cb) {
            mysqlConn.query("SELECT * FROM tasks WHERE task_type = ? ORDER BY data_node_id", ["CREATE_CONSUMER"], function(err, data) {
              data[0].data_node_id.should.equal("node1-2");
              data[0].task_type.should.equal("CREATE_CONSUMER");
              JSON.parse(data[0].task_data).consumer_id.should.equal("test-tenant_name-test-consumer");
              JSON.parse(data[0].task_data).topic_id.should.equal("1234-456-test");
              data[1].data_node_id.should.equal("node2-2");
              data[1].task_type.should.equal("CREATE_CONSUMER");
              JSON.parse(data[1].task_data).consumer_id.should.equal("test-tenant_name-test-consumer");
              JSON.parse(data[1].task_data).topic_id.should.equal("1234-456-test");
              cb();
            });
          }], done);
       });
    });

    describe("Delete topics",function(done){
        beforeEach(function(done){
          async.series([
            function(cb) {
              admClient.createBigQueueCluster({
                name:"test1",
                nodes:[
                    {id:"node1",host:"127.0.0.1",port:6379,status:"UP",journals:[]},
                    {id:"node2",host:"127.0.0.1",port:6380,status:"UP",journals:[]}
                ]
             },cb);
            },
            function(cb) {
             admClient.createBigQueueCluster({
                name:"test2",
                nodes:[
                    {id:"node3",host:"127.0.0.1",port:6379,status:"UP",journals:[]},
                    {id:"node4",host:"127.0.0.1",port:6380,status:"UP",journals:[]}
                ]
             },cb);
            },
            function(cb) {
              admClient.createTopic({name:"test",tenant_id:"test",tenant_name:"test",cluster:"test2"},cb);
            }
          ], done);
        });

        it("should delete topic from specific clusters",function(done){
           admClient.deleteTopic("test-test-test",function(err){
              should.not.exist(err)
              mysqlConn.query("SELECT * FROM topics WHERE topic_id=?",["test-test-test"], function(err, data) {
                data.length.should.equal(0);
                done();
              });
           });
        })
        it("should fail if topic doesn't exist on delete",function(done){
             admClient.deleteTopic("test-no-exist",function(err){
                 should.exist(err)
                 done()
             })
        })
        it("Should fail if exists a consumer asociated to this topic", function(done) {
          admClient.createConsumerGroup({name:"test-consumer",tenant_id:"tenant_test",tenant_name:"tenant_name",topic_id:"test-test-test"},function(err){
            should.not.exist(err);
             admClient.deleteTopic("test-test-test",function(err){
                should.exist(err)
                done();
             });
          });
        });
        it("Should create the tasks to delete the topic", function(done) {
          admClient.deleteTopic("test-test-test",function(err){
            mysqlConn.query("SELECT * FROM tasks WHERE task_type=? ORDER BY data_node_id",["DELETE_TOPIC"], function(err, data) {
              data[0].data_node_id.should.equal("node3");
              data[0].task_type.should.equal("DELETE_TOPIC");
              JSON.parse(data[0].task_data).topic_id.should.equal("test-test-test");
              data[1].data_node_id.should.equal("node4");
              data[1].task_type.should.equal("DELETE_TOPIC");
              JSON.parse(data[1].task_data).topic_id.should.equal("test-test-test");
              done();
            });
         });

        });
    })

    describe("Delete consumers",function(done){
         beforeEach(function(done){
          async.series([
            function(cb) {
              admClient.createBigQueueCluster({
                name:"test1",
                nodes:[
                    {id:"node1",host:"127.0.0.1",port:6379,status:"UP",journals:[]},
                    {id:"node2",host:"127.0.0.1",port:6380,status:"UP",journals:[]}
                ]
             },cb);
            },
            function(cb) {
             admClient.createBigQueueCluster({
                name:"test2",
                nodes:[
                    {id:"node3",host:"127.0.0.1",port:6379,status:"UP","journals":[]},
                    {id:"node4",host:"127.0.0.1",port:6380,status:"UP","journals":[]}
                ]
             },cb);
            },
            function(cb) {
              admClient.createTopic({name:"test",tenant_id:"test",tenant_name:"test",cluster:"test2"},cb);
            },
            function(cb) {
              admClient.createConsumerGroup({name:"3",tenant_id:"1",tenant_name:"2",topic_id:"test-test-test"}, cb);
            }
          ], done);
        })

        it("should delete consumer from specific cluster",function(done){
          admClient.deleteConsumerGroup("test-test-test","1-2-3",function(err){
            should.not.exist(err)
            mysqlConn.query("SELECT * FROM consumers WHERE consumer_id=?",["1-2-3"], function(err, data) {
              data.length.should.equal(0);
              done();
            });
          });
        });

        it("should fail if consumer doesn't exist on delete",function(done){
            admClient.deleteConsumerGroup("test","testConsumer-no-exist",function(err){
                should.exist(err)
                done()
            })
        })
        it("Should create the tasks for delete consumer", function(done) {
          admClient.deleteConsumerGroup("test-test-test","1-2-3",function(err){
            mysqlConn.query("SELECT * FROM tasks WHERE task_type=? ORDER BY data_node_id",["DELETE_CONSUMER"], function(err, data) {
              data[0].data_node_id.should.equal("node3");
              data[0].task_type.should.equal("DELETE_CONSUMER");
              JSON.parse(data[0].task_data).topic_id.should.equal("test-test-test");
              JSON.parse(data[0].task_data).consumer_id.should.equal("1-2-3");
              data[1].data_node_id.should.equal("node4");
              data[1].task_type.should.equal("DELETE_CONSUMER");
              JSON.parse(data[1].task_data).topic_id.should.equal("test-test-test");
              JSON.parse(data[1].task_data).consumer_id.should.equal("1-2-3");
              done();
            });
         });
      })
    });

    describe("Reset consumers", function() {

      beforeEach(function(done){
           async.series([
            function(cb) {
              admClient.createBigQueueCluster({
                name:"test1",
                nodes:[
                  {id:"node1",host:"127.0.0.1",port:6379,status:"UP",journals:[]},
                  {id:"node2",host:"127.0.0.1",port:6380,status:"UP",journals:[]}
                ],
                journals:[
                  {id:"j1",host:"127.0.0.1",port:6379,status:"UP"}
                ]
             },cb);
            },
            function(cb) {
             admClient.createBigQueueCluster({
                name:"test2",
                nodes:[
                    {id:"node3",host:"127.0.0.1",port:6379,status:"UP",journals:[]},
                    {id:"node4",host:"127.0.0.1",port:6380,status:"UP",journals:[]}
                ],
                endpoints: [
                  {name:"t1",host:"127.0.0.1",port:"8080"},
                  {name:"t1",host:"127.0.0.1",port:"8081"}
                ]
             },cb);
            },
            function(cb) {
              admClient.createTopic({name:"test",tenant_id:"test",tenant_name:"test",cluster:"test2"},cb);
            }, function(cb) {
              admClient.createConsumerGroup({name:"c1",tenant_id:"test",tenant_name:"test",topic_id:"test-test-test"},cb)
            }, function(cb) {
              admClient.createConsumerGroup({name:"c2",tenant_id:"test",tenant_name:"test",topic_id:"test-test-test"},cb)
            }
          ], done);
        });

        it("Should create the tasks con consumer reset", function(done) {
          admClient.resetConsumer("test-test-test", "test-test-c1", function(err) {
             mysqlConn.query("SELECT * FROM tasks WHERE task_type=? ORDER BY data_node_id",["RESET_CONSUMER"], function(err, data) {
              data[0].data_node_id.should.equal("node3");
              data[0].task_type.should.equal("RESET_CONSUMER");
              JSON.parse(data[0].task_data).topic_id.should.equal("test-test-test");
              JSON.parse(data[0].task_data).consumer_id.should.equal("test-test-c1");
              data[1].data_node_id.should.equal("node4");
              data[1].task_type.should.equal("RESET_CONSUMER");
              JSON.parse(data[1].task_data).topic_id.should.equal("test-test-test");
              JSON.parse(data[1].task_data).consumer_id.should.equal("test-test-c1");
              done();
            });
          });
        });

        it("Should fail if consumer doesn't exist", function(done) {
          admClient.resetConsumer("test-test-test", "test-test-c3", function(err) {
            should.exist(err);
            done();
          });
        });
    });

    describe("Retrieve information",function(){
         beforeEach(function(done){
           async.series([
            function(cb) {
              admClient.createBigQueueCluster({
                name:"test1",
                nodes:[
                  {id:"node1",host:"127.0.0.1",port:6379,status:"UP",journals:[]},
                  {id:"node2",host:"127.0.0.1",port:6380,status:"UP",journals:[]}
                ],
                journals:[
                  {id:"j1",host:"127.0.0.1",port:6379,status:"UP"}
                ]
             },cb);
            },
            function(cb) {
             admClient.createBigQueueCluster({
                name:"test2",
                nodes:[
                    {id:"node3",host:"127.0.0.1",port:6379,status:"UP",journals:[]},
                    {id:"node4",host:"127.0.0.1",port:6380,status:"UP",journals:[]}
                ],
                endpoints: [
                  {name:"t1",host:"127.0.0.1",port:"8080"},
                  {name:"t1",host:"127.0.0.1",port:"8081"}
                ]
             },cb);
            },
            function(cb) {
              admClient.createTopic({name:"test",tenant_id:"test",tenant_name:"test",cluster:"test2"},cb);
            }, function(cb) {
              admClient.createConsumerGroup({name:"c1",tenant_id:"test",tenant_name:"test",topic_id:"test-test-test"},cb)
            }, function(cb) {
              admClient.createConsumerGroup({name:"c2",tenant_id:"test",tenant_name:"test",topic_id:"test-test-test"},cb)
            }
          ], done);
        });

        it("should return information for an existent node",function(done){
            admClient.getNodeData("test1","node1",function(err,data){
              data.host.should.equal("127.0.0.1")
              data.port.should.equal(6379)
              data.status.should.equal("UP")
              should.not.exist(err)
              done()
            })
        })
        it("should return error for an inexistent node",function(done){
            admClient.getNodeData("test1","node5",function(err,data){
                should.exist(err)
                done()
            })
        })
        it("should return information for an existent journal",function(done){
            admClient.getJournalData("test1","j1",function(err,data){
              data.host.should.equal("127.0.0.1")
              data.port.should.equal(6379)
              data.status.should.equal("UP")
              should.not.exist(err)
              done()
            })
        })
        it("should return error for an inexistent Journal",function(done){
            admClient.getJournalData("test1","j5",function(err,data){
                should.exist(err)
                done()
            })
        })


        it("should get the entry points for a topic",function(done){
          admClient.getTopicData("test-test-test",function(err,data){
              should.not.exist(err)
              should.exist(data)
              data.topic_id.should.equal("test-test-test")
              should.exist(data.ttl)
              data.endpoints[0].host.should.equal("127.0.0.1")
              data.endpoints[1].host.should.equal("127.0.0.1")
              if(data.endpoints[0].port == 8080) {
                data.endpoints[1].port.should.equal(8081)
              } else {
                data.endpoints[1].port.should.equal(8080)
                data.endpoints[0].port.should.equal(8081)
              }
              done()
          })
        })
        it("should get data about cluster",function(done){
            admClient.getClusterData("test1", true,function(err,data){
              data.should.have.keys("cluster","topics","nodes","endpoints","journals")
                done()
            })
        })
        it("should get all topics for a group",function(done){
            admClient.getGroupTopics("test-test-test",function(err,data){
              data.should.have.length(2)
              data[0].consumer_id.should.match(/test-test-(c1|c2)/)
              data[1].consumer_id.should.match(/test-test-(c1|c2)/)
              done()
            })
        })
    })

    describe("Node Stats", function() {
     beforeEach(function(done){
        admClient.createBigQueueCluster({
            name:"test1",
            nodes:[
                {id:"node1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP","journals":[]},
                {id:"node2","host":"127.0.0.1","port":6380,"errors":0,"status":"UP","journals":[]}
            ],
            journals:[
                {id:"j1","host":"127.0.0.1","port":6379,"errors":0,"status":"UP"},
            ],
            endpoints:[
                {name:"e1","host":"127.0.0.1","port":8080},
                {name:"e2","host":"127.0.0.1","port":8081}
            ]
       },function(err){
        admClient.createTopic({tenant_id:"test",tenant_name:"test", name:"t1", cluster:"test1"},function(err){
          should.not.exist(err)
          admClient.createConsumerGroup({topic_id:"test-test-t1", tenant_id:"test",tenant_name:"test", name:"c1"},function(err,data){
           should.not.exist(err);
           done();
          });
        });
       });
     });


      beforeEach(function(done) {
        mysqlConn.query("TRUNCATE stats", function(err) {
          mysqlConn.commit(done);
        });
      });
      it("Should receive node stats", function(done) {
        var time = new Date();
        admClient.updateNodeMetrics("test","node1",{
               "sample_date" : 1401977686996,
               "topics_stats" :
                [
                  {
                   "consumers" :
                      [
                        {
                          "consumer_id" : "consumer1",
                          "consumer_stats" :
                          {
                            "fails" : 2,
                            "lag" : 10,
                            "processing" : 1
                          }
                        },
                        {
                          "consumer_id" : "consumer2",
                          "consumer_stats" :
                            {
                              "fails" : 0,
                              "lag" : 1,
                              "processing" : 0
                            }
                        }
                     ],
                 "topic_head" : 1,
                 "topic_id" : "topic1"
                },
                {
                  topic_id:"topic2",
                  topic_head: 1,
                  consumers: [
                   {
                     consumer_id: "consumer3",
                     consumer_stats: {
                      lag: 4,
                      fails: 3,
                      processing: 1
                     }
                   }
                  ]
                }
              ]
        }, function(err) {
          should.not.exist(err);
          mysqlConn.query("SELECT * FROM stats", function(err, data) {
            data[0].cluster.should.equal("test");
            data[0].node.should.equal("node1");
            data[1].cluster.should.equal("test");
            data[1].cluster.should.equal("test");
            data[2].node.should.equal("node1");
            data[2].node.should.equal("node1");

            data[0].topic.should.equal("topic1");
            data[0].consumer.should.equal("consumer1");
            data[0].lag.should.equal(10);
            data[0].fails.should.equal(2);
            data[0].processing.should.equal(1);

            data[1].topic.should.equal("topic1");
            data[1].consumer.should.equal("consumer2");
            data[1].lag.should.equal(1);
            data[1].fails.should.equal(0);
            data[1].processing.should.equal(0);

            data[2].topic.should.equal("topic2");
            data[2].consumer.should.equal("consumer3");
            data[2].lag.should.equal(4);
            data[2].fails.should.equal(3);
            data[2].processing.should.equal(1);

            done();
          });
        });
      });
      it("Should update value", function(done) {
        var time = new Date();
        admClient.updateNodeMetrics("test","node1",{
            sample_date: time,
            topics_stats: [
              {
                topic_id: "topic1",
                topic_head:1,
                consumers: [
                  {
                   consumer_id: "consumer1",
                   consumer_stats: {
                    lag:10,
                    fails:2,
                    processing:1
                   }
                  }
                ]
              }
            ]
          }, function(err) {
          admClient.updateNodeMetrics("test","node1",{
              sample_date: time,
              topics_stats: [
                {
                  topic_id: "topic1",
                  topic_head: 1,
                  consumers: [
                    {
                     consumer_id: "consumer1",
                     consumer_stats: {
                      lag:1,
                      fails:9,
                      processing:3
                     }
                    }
                  ]
                }
              ]
            }, function(err) {
              should.not.exist(err);
              mysqlConn.query("SELECT * FROM stats", function(err, data) {

                data[0].cluster.should.equal("test");
                data[0].node.should.equal("node1");
                data[0].topic.should.equal("topic1");
                data[0].consumer.should.equal("consumer1");
                data[0].lag.should.equal(1);
                data[0].fails.should.equal(9);
                data[0].processing.should.equal(3);
                done();
              });
            });
          });
      });
      it("Should fails if some data is missing", function(done) {
        admClient.updateNodeMetrics("test","node1",{
            sample_date: new Date().getTime(),
            topics_stats: [
              {
                topic_id: "topic1",
                topic_head: 1,
                consumers_prop_error: [
                  {
                   consumer_id: "consumer1",
                   consumer_stats: {
                    lag:1,
                    fails:9,
                    processing:3
                   }
                  }
                ]
              }
            ]
        }, function(err) {
          should.exist(err);
          done()
      });
    });
    it("Should get consolidate data for topic", function(done) {
      admClient.updateNodeMetrics("test1","node1",{
         sample_date: new Date().getTime(),
            topics_stats: [
              {
                topic_id: "test-test-t1",
                topic_head: 1,
                consumers: [
                  {
                   consumer_id: "test-test-c1",
                   consumer_stats: {
                    lag:10,
                    fails:2,
                    processing:1
                   }
                  }
                ]
              }
            ]
        }, function(err) {
          admClient.updateNodeMetrics("test1","node2",{
            sample_date: new Date().getTime(),
            topics_stats: [
              {
                topic_id: "test-test-t1",
                topic_head: 1,
                consumers: [
                  {
                   consumer_id: "test-test-c1",
                   consumer_stats: {
                    lag:2,
                    fails:3,
                    processing:4
                   }
                  }
                ]
              }
            ]
          }, function(err) {
            should.not.exist(err);
            admClient.getTopicData("test-test-t1", function(err, data) {
              should.not.exist(err);
              should.exist(data);
              data.consumers.length.should.equal(1);
              data.consumers[0].consumer_id.should.equal("test-test-c1");
              data.consumers[0].consumer_stats.lag.should.equal(12);
              data.consumers[0].consumer_stats.fails.should.equal(5);
              data.consumers[0].consumer_stats.processing.should.equal(5);
              done();
            });
          });
       });
    });
    it("Should get consolidate date for consummer", function(done) {
      admClient.updateNodeMetrics("test1","node1",{
          sample_date: new Date().getTime(),
          topics_stats: [
            {
              topic_id: "test-test-t1",
              topic_head: 1,
              consumers: [
                {
                 consumer_id: "test-test-c1",
                 consumer_stats: {
                  lag:10,
                  fails:2,
                  processing:1
                 }
                }
              ]
            }
          ]
        }, function(err) {
          admClient.updateNodeMetrics("test1","node2",{
            sample_date: new Date().getTime(),
            topics_stats: [
              {
                topic_id: "test-test-t1",
                topic_head: 1,
                consumers: [
                  {
                   consumer_id: "test-test-c1",
                   consumer_stats: {
                    lag:2,
                    fails:3,
                    processing:4
                   }
                  }
                ]
              }
            ]
          }, function(err) {
            should.not.exist(err);
            admClient.getConsumerData("test-test-t1", "test-test-c1", function(err, data) {
              should.not.exist(err);
              should.exist(data);
              data.consumer_stats.lag.should.equal(12);
              data.consumer_stats.fails.should.equal(5);
              data.consumer_stats.processing.should.equal(5);
              done();
            });
          });
       });

    });
  });
  describe("Tasks actions", function(done) {
    it("Should enable tasks fetch", function(done) {
      async.series([
        function(cb) {
          admClient.createTasks([
            {data_node_id:"test1", task_type:"TEST", task_data:{test:1}},
            {data_node_id:"test2", task_type:"TEST", task_data:{test:2}}
          ],cb)
        },
        function(cb) {
          admClient.getTasksByCriteria({data_node_id: "test1"}, function(err, data) {
            data[0].task_type.should.equal("TEST");
            data[0].task_data.test.should.equal(1);
            cb();
          });
        },
        function(cb) {
          admClient.getTasksByCriteria({task_type: "TEST"}, function(err, data) {
            data.length.should.equal(2);
            cb();
          });
        },
        function(cb) {
          admClient.getTasksByCriteria({task_type: "TEST-2"}, function(err, data) {
            data.length.should.equal(0);
            cb();
          });
        }
      ], done);
    });

    it("Should enable update task status", function(done) {
      var id = 0;
      async.series([
        function(cb) {
          admClient.createTasks([
            {data_node_id:"test1", task_type:"TEST", task_data:{test:1}},
            {data_node_id:"test2", task_type:"TEST", task_data:{test:2}}
          ],cb)
        },
        function(cb) {
          admClient.getTasksByCriteria({data_node_id: "test1"}, function(err, data) {
            data[0].task_status.should.equal("PENDING");
            id = data[0].task_id
            cb();
          });
        },
        function(cb) {
          admClient.updateTaskStatus(id, "DONE", cb);
        },
        function(cb) {
          admClient.getTasksByCriteria({task_id: id}, function(err, data) {
            data[0].task_status.should.equal("DONE");
            data[0].data_node_id.should.equal("test1");
            cb();
          });
        }
      ],done);
    });
  });
});
