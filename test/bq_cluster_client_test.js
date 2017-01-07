var should = require('should'),
    redis = require('redis'),
    utils = require('../lib/bq_client_utils.js'),
    bq = require('../lib/bq_client.js'),
    bj = require("../lib/bq_journal_client_redis.js"),
    bqc = require('../lib/bq_cluster_client.js'),
    log = require('../lib/bq_logger'),
    fs = require('fs'),
    async = require("async"),
    nock = require('nock');

var j = 0
describe("Big Queue Cluster",function(){
    var bqClientConfig = {
        "refreshInterval":10,
        "cluster": "test",
        "adminApiUrl":"http://adminapi.bigqueue.com",
        "createJournalClientFunction":bj.createJournalClient,
        "createNodeClientFunction":bq.createClient,
        "topicStructRefreshInterval": 10
    }


    var bqClient
    var redisClient1
    var redisClient2
    var nodeClient1;
    var nodeClient2;
    var journalClient1
    var journalClient2
    var clusterData;
    var topicsStruct;
    beforeEach(function() {
      clusterData = {
        name:"test1",
          nodes:[
              {id:"node1","host":"127.0.0.1","port":6379,"status":"UP","journals":["j1"]},
              {id:"node2","host":"127.0.0.1","port":6380,"status":"UP","journals":["j2"]}
          ],
          journals:[
              {id:"j1","host":"127.0.0.1","port":6379,"status":"UP"},
              {id:"j2","host":"127.0.0.1","port":6380,"status":"UP"}
          ],
          endpoints:[
              {name:"e1","host":"127.0.0.1","port":8080},
              {name:"e2","host":"127.0.0.1","port":8081}
          ]
      }
      topicsStruct = {
        topics:[
          {
            topic_id: "123-a-b",
            enabled: true,
            max_new_messages_per_seccond:-1,
            consumers: [{
              consumer_id: "456-c-d",
              enabled: true
            }]
          },{
            topic_id: "JJJ-a-b",
            enabled: true,
            max_new_messages_per_seccond: -1,
            consumers: [{
              consumer_id: "JJJ-c-d",
              enabled: true
            }]
          }
        ]
      }
    });
    before(function() {
      nock("http://adminapi.bigqueue.com")
      .defaultReplyHeaders({"Content-Type":"application/json"})
      .get("/clusters/test")
      .reply(200, function(req, b) {
        return clusterData;
      })
      .persist();
      nock("http://adminapi.bigqueue.com")
      .defaultReplyHeaders({"Content-Type":"application/json"})
      .get("/clusters/test/topics")
      .reply(200, function(req, b) {
        return topicsStruct;
      })
      .persist();
    });

    before(function(done){
        var execute = function() {
          var args = [];
          for (var key in arguments) {
            args.push(arguments[key]);
          }
          var command = args.shift();
          var callback = args.pop();
          return this.send_command(command, args, callback);
        }

        async.parallel([
          function(cb) {
            redisClient1 = redis.createClient(6379,"127.0.0.1",{"return_buffers":false})
            redisClient1.execute = execute;
            redisClient1.on("ready", cb);
          },
          function(cb) {
            redisClient2 = redis.createClient(6380,"127.0.0.1",{"return_buffers":false})
            redisClient2.execute = execute;
            redisClient2.on("ready", cb);
          }
        ],done);
    });

    before(function(done){
      async.parallel([
        function(cb) {
          nodeClient1 = bq.createClient({host:"127.0.0.1",port:6379});
          nodeClient1.on("ready", cb);
        },
        function(cb) {
          nodeClient2 = bq.createClient({host:"127.0.0.1",port:6380});
          nodeClient2.on("ready", cb);
        },
        function(cb) {
          journalClient1 = bj.createJournalClient({host:"127.0.0.1",port:6379});
          journalClient1.on("ready", cb);
        },
        function(cb) {
          journalClient2 = bj.createJournalClient({host:"127.0.0.1",port:6380});
          journalClient2.on("ready", cb);
        }
      ],done);
    });

    beforeEach(function(done){
      async.series([
        function(cb) {
          redisClient1.execute("flushall", cb);
        },
        function(cb) {
          redisClient2.execute("flushall", cb);
        }
      ], done);
    })

    beforeEach(function(done) {
      bqClient = bqc.createClusterClient(bqClientConfig);
      bqClient.on("ready",function() {
        setTimeout(done, 30);
      });
    });
    afterEach(function(done){
        bqClient.shutdown()
        process.nextTick(function(){
            done()
        })
    })

    after(function() {
     nock.cleanAll();
    });
    //End of prepare stage
    describe("#internals",function(){
      it("Should not create client if node is down", function(done) {
        async.series([
          function(cb) {
            clusterData.nodes[3] = {id:"node3","host":"127.0.0.1","port":6379,"status":"DOWN","journals":["j1"]};
            setTimeout(cb, 20);
          },
          function(cb) {
            var node = bqClient.getClientById(bqClient.nodes,"node3")
            should.not.exist(node);
            cb();
          },
          function(cb) {
            clusterData.nodes[3].status = "UP";
            setTimeout(cb, 200);
          },
          function(cb) {
            var node = bqClient.getClientById(bqClient.nodes,"node3")
           should.exist(node);
           cb();
          }
        ],done);
      });
      it("Should remove client if it's set to down", function(done) {
        async.series([
          function(cb) {
            clusterData.nodes[3] = {id:"node3","host":"127.0.0.1","port":6379,"status":"UP","journals":["j1"]};
            setTimeout(cb, 20);
          },
          function(cb) {
            var node = bqClient.getClientById(bqClient.nodes,"node3")
            should.exist(node);
            cb();
          },
          function(cb) {
            clusterData.nodes[3].status = "DOWN";
            setTimeout(cb, 20);
          },
          function(cb) {
           var node = bqClient.getClientById(bqClient.nodes,"node3")
           should.not.exist(node);
           cb();
          }
        ],done);
      });
      it("Should re-add node if flap down and up", function(done) {
        async.series([
          function(cb) {
            clusterData.nodes[3] = {id:"node3","host":"127.0.0.1","port":6379,"status":"UP","journals":["j1"]};
            setTimeout(cb, 200);
          },
          function(cb) {
            var node = bqClient.getClientById(bqClient.nodes,"node3")
            should.exist(node);
            cb();
          },
          function(cb) {
            clusterData.nodes[3].status = "DOWN";
            setTimeout(cb, 20);
          },
          function(cb) {
           var node = bqClient.getClientById(bqClient.nodes,"node3")
           should.not.exist(node);
           cb();
          },
          function(cb) {
            clusterData.nodes[3].status = "UP";
            setTimeout(cb, 20);
          },
          function(cb) {
           var node = bqClient.getClientById(bqClient.nodes,"node3")
           should.exist(node);
           cb();
          }

        ],done);
      });
    });

    describe("#postMessage",function(){
      beforeEach(function(done){
        async.series([
          function(cb) {
            nodeClient1.createTopic("testTopic",cb);
          },
          function(cb) {
            nodeClient1.createTopic("123-a-b",cb);
          },
          function(cb) {
            nodeClient2.createTopic("testTopic",cb);
          },
          function(cb) {
            nodeClient2.createTopic("123-a-b",cb);
          }
        ], done);

      });

      it("should balance the writes",function(done) {
        async.series([
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test1"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test2"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test3"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test4"},cb);
          },
          function(cb) {
            redisClient1.execute("get","topics:testTopic:head",function(err,data){
              should.not.exist(err);
              should.exist(data);
              data.should.equal(""+2);
              cb();
            });
          },
          function(cb) {
            redisClient2.execute("get","topics:testTopic:head",function(err,data){
              should.not.exist(err)
              should.exist(data)
              data.should.equal(""+2)
              cb();
            });
          }
        ], done);
      });

      it("should reject process if topic is disabled (enabled:false)", function(done) {
        async.series([
          function(cb) {

            bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
              should.not.exist(err);
              cb();
            });
          },function(cb) {
            topicsStruct.topics[0].enabled=false;
            topicsStruct.topics[1].enabled=false;
            setTimeout(cb, 100);
          }, function(cb) {
            bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
              should.exist(err);
              cb();
            });
          }
        ],function(cb) {
          done()
        })
      })
      it("should reject process if topic is hitting max amount of posts", function(done) {
        async.series([
          function(cb) {
            bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
              should.not.exist(err);
              cb();
            });
          },function(cb) {
            bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
              should.not.exist(err);
              cb();
            });
          },function(cb) {
            bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
              should.not.exist(err);
              cb();
            });
          },function(cb) {
            topicsStruct.topics[0].max_new_messages_per_seccond=2;
            topicsStruct.topics[1].max_new_messages_per_seccond=2;
            //Clear stats
            setTimeout(cb, 1500);
          },function(cb) {
              bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
                should.not.exist(err);
                cb();
              });
            },function(cb) {
              bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
                should.not.exist(err);
                cb();
              });
            },function(cb) {
              bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
                should.exist(err);
                cb();
              });
            },function(cb) {
              bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
                should.exist(err);
                cb();
              });
            },function(cb) {
              setTimeout(cb, 1500)
            },function(cb) {
                bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
                  should.not.exist(err);
                  cb();
                });
              },function(cb) {
                bqClient.postMessage("123-a-b",{msg:"test1"},function(err) {
                  should.not.exist(err);
                  cb();
                });
              }
        ],function(cb) {
          done()
        })
      })

      it("should try to resend the message to another node if an error ocurrs sending",function(done){
        async.series([
          function(cb) {
            clusterData.nodes[3] = {id:"node3","host":"127.0.0.1","port":6381,"status":"UP","journals":[]}
            setTimeout(cb,20);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test1"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test2"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test3"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test4"},cb);
          },
          function(cb) {
            redisClient1.execute("get","topics:testTopic:head",function(err,data1){
              redisClient2.execute("get","topics:testTopic:head",function(err,data2){
                var sum = parseInt(data1)+parseInt(data2)
                sum.should.equal(4)
                cb();
              });
            });
          }
        ], done);
      });

      it("should write to all journals declared for the node",function(done){
       async.series([
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test1"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"test2"},cb);
          },
          function(cb) {
            journalClient1.retrieveMessages("node1","testTopic",1,function(err,data) {
              should.not.exist(err)
              should.exist(data)
              data.should.have.length(1)
              cb();
            });
          },
          function(cb) {
           journalClient2.retrieveMessages("node2","testTopic",1,function(err,data) {
             should.not.exist(err)
             should.exist(data)
             data.should.have.length(1)
             cb();
           });
          }
        ], done);
      })
     it("should return an error if an error ocurrs writing data to the journal",function(done){
       async.series([
        function(cb) {
          clusterData.journals.push({id:"j3","host":"127.0.0.1","port":6381,"status":"UP"});
          clusterData.nodes[0].journals = ["j1","j2","j3"];
          clusterData.nodes[1].journals = ["j1","j2","j3"];
          setTimeout(cb, 20);
        },
        function(cb) {
          bqClient.postMessage("testTopic",{msg:"test1"},function(err) {
            should.exist(err);
            cb();
          });
        }
       ],done);
     });

     it("should ignore force down status",function(done){
       async.series([
        function(cb) {
          clusterData.nodes[1].status="FORCEDOWN";
          setTimeout(cb, 20);
        },
        function(cb) {
          bqClient.postMessage("testTopic",{msg: "test1"},cb);
        },
        function(cb) {
          bqClient.postMessage("testTopic",{msg: "test2"},cb);
        },
        function(cb) {
          bqClient.postMessage("testTopic",{msg: "test3"},cb);
        },
        function(cb) {
          bqClient.postMessage("testTopic",{msg: "test4"},cb);
        },
        function(cb) {
           redisClient1.execute("get","topics:testTopic:head",function(err,data){
           should.not.exist(err)
           should.exist(data)
           data.should.equal(""+4)
           cb();
         });
        }
       ],done);
     })
     it("should ignore read_only status",function(done){
      async.series([
          function(cb) {
            clusterData.nodes[1].status="READONLY";
            setTimeout(cb, 20);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg: "test1"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg: "test2"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg: "test3"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg: "test4"},cb);
          },
          function(cb) {
             redisClient1.execute("get","topics:testTopic:head",function(err,data){
             should.not.exist(err)
             should.exist(data)
             data.should.equal(""+4)
             cb();
           });
          }
         ],done);
       });
    })
    describe("#getMessage",function(){
        beforeEach(function(done){
          async.series([
            function(cb) {
              nodeClient1.createTopic("testTopic",cb);
            },
            function(cb) {
              nodeClient2.createTopic("testTopic",cb);
            },
           function(cb) {
              nodeClient1.createConsumerGroup("testTopic","testGroup",cb);
            },
            function(cb) {
              nodeClient2.createConsumerGroup("testTopic","testGroup",cb);
            }
          ], done);

        });

        it("should generate and add a recipientCallback to the returned message",function(done){
            //because get message using round-robin
            async.series([
              function(cb) {
                bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
              },
              function(cb) {
                bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
              },
              function(cb) {
                bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
              },
              function(cb) {
                bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                  should.not.exist(err)
                  should.exist(data)
                  data.should.have.property("uid")
                  data.should.have.property("recipientCallback")
                  cb();
                });
              }
            ], done);
        });
        it("should fail if consumer or topic doesnt exists", function(done) {
          async.series([
             function(cb) {
              bqClient.getMessage("fakeTopic","testGroup",undefined,function(err, data) {
                should.exist(err);
                cb();
              });
            },
            function(cb) {
              bqClient.getMessage("testTopic","fakeGroup",undefined,function(err, data) {
                should.exist(err);
                cb();
              });
            },
          ],done);
        })
        it("should get node Id",function(done){
          //because get message using round-robin
          async.series([
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
            },
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
            },
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
            },
            function(cb) {
              bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(err)
                should.exist(data)
                data.should.have.property("nodeId")
                cb();
              });
            }
          ], done);
        })

        it("should balance the gets through all nodes",function(done){
          async.series([
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
            },
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
            },
            function(cb) {
              bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(err);
                should.exist(data);
                data.should.have.property("uid");
                data.should.have.property("nodeId");
                data.should.have.property("recipientCallback");
                cb();
              });
            },
            function(cb) {
              bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(err);
                should.exist(data);
                data.should.have.property("uid");
                data.should.have.property("nodeId");
                data.should.have.property("recipientCallback");
                cb();
              });
            },
            function(cb) {
              bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(err)
                should.not.exist(data)
                cb();
              });
            }
          ],done);
        })
        it("should enable get message from specific nodes",function(done){
          async.series([
              function(cb) {
                bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
              },
              function(cb) {
                bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
              },
              function(cb) {
                bqClient.getMessageFromNode("node1","testTopic","testGroup",undefined,function(err,data){
                  should.not.exist(err)
                  should.exist(data)
                  data.should.have.property("uid")
                  data.should.have.property("nodeId")
                  data.nodeId.should.equal("node1");
                  data.should.have.property("recipientCallback")
                  cb();
                });
              },
              function(cb) {
                bqClient.getMessageFromNode("node2","testTopic","testGroup",undefined,function(err,data){
                  should.not.exist(err)
                  should.exist(data)
                  data.should.have.property("uid")
                  data.should.have.property("nodeId")
                  data.nodeId.should.equal("node2");
                  data.should.have.property("recipientCallback")
                  cb();
                });
              },
              function(cb) {
                bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                  should.not.exist(err)
                  should.not.exist(data)
                  cb();
                });
              }
          ], done);
        })

        it("should read messages from read_only nodes",function(done){
          async.series([
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
            clusterData.nodes[1].status="READONLY";
            setTimeout(cb, 20);
           },
           function(cb) {
            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(err)
              should.exist(data)
              data.should.have.property("uid")
              data.should.have.property("recipientCallback")
              cb();
            });
           },
           function(cb) {
            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(err)
              should.exist(data)
              data.should.have.property("uid")
              data.should.have.property("recipientCallback")
              cb();
            });
           },
           function(cb) {
            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(err)
              should.not.exist(data)
              cb();
            });

           }
          ],done);
        })

        it("should run ok if a node is down",function(done){
          async.series([
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
            clusterData.nodes[1].status="DOWN";
            setTimeout(cb, 20);
           },
           function(cb) {
             bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(err)
              should.exist(data)
              data.should.have.property("uid")
              data.should.have.property("recipientCallback")
              cb();
            })
           }], done());
        })

        it("should run ok if a all nodes are down",function(done){
          async.series([
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
            clusterData.nodes[0].status="DOWN";
            clusterData.nodes[1].status="DOWN";
            setTimeout(cb, 20);
           },
           function(cb) {
             bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(data)
              cb();
             });
           }
          ],done);
        });

        it("should get the uid generated at post instance",function(done){
          var uids = [];
          async.series([
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key){
                uids.push(key.uid);
                cb();
              });
            },
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"testMessage"},function(err,key){
                uids.push(key.uid);
                cb();
              });
            },
            function(cb) {
              bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(err)
                should.exist(data)
                uids.should.include(data.uid);
                cb();
              })
            },
            function(cb) {
              bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(err)
                should.exist(data)
                uids.should.include(data.uid);
                cb();
              })
            }
          ], done);
        })

        it("should return undefined if no message found",function(done){
          async.series([
            function(cb) {
              redisClient1.execute("set","topics:testTopic:head",0,cb);
            },
            function(cb) {
              redisClient2.execute("set","topics:testTopic:head",0,cb);
            },
            function(cb) {
              bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(err)
                should.not.exist(data)
                cb();
              });
            }
          ], done);
        });
        it("should return undefined if error found",function(done){
            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
                should.not.exist(data)
                done()
            })
        })

        it("should fail if the consumer group doesn't exist",function(done){
          async.series([
            function(cb) {
              redisClient1.execute("set","topics:testTopic:head",0,cb);
            },
            function(cb) {
              redisClient2.execute("set","topics:testTopic:head",0,cb);
            },
            function(cb) {
              bqClient.getMessage("testTopic","testGroup-no-exist",undefined,function(err,data){
                should.exist(err);
                cb();
              });
            }
          ], done);
        })

        it("should ignore down status",function(done){
          async.series([
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
            clusterData.nodes[1].status = "FORCEDOWN";
            setTimeout(cb, 20);
           },
           function(cb) {
             bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(err);
              should.exist(data);
              data.should.have.property("uid");
              data.should.have.property("recipientCallback");
              cb();
             });
           },
           function(cb) {
            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(err)
              should.not.exist(data)
              cb();
            });
           }
          ], done);
        });
    });

    describe("#ackMessages",function(){
      beforeEach(function(done){
        async.series([
          function(cb) {
            nodeClient1.createTopic("testTopic",cb);
          },
          function(cb) {
            nodeClient2.createTopic("testTopic",cb);
          },
         function(cb) {
            nodeClient1.createConsumerGroup("testTopic","testGroup",cb);
          },
          function(cb) {
            nodeClient2.createConsumerGroup("testTopic","testGroup",cb);
          }
        ], done);

      });

      it("should receive the recipientCallback ack the message",function(done){
          var recipientCallback;
          var recipientData;
          var client;
          async.series([
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
             bqClient.getMessage("testTopic","testGroup",undefined,function(err,data) {
                recipientCallback = data.recipientCallback
                recipientData = bqClient.decodeRecipientCallback(recipientCallback)
                client
                if(recipientData.nodeId == "node1"){
                    client = redisClient1
                }else{
                    client = redisClient2
                }
                cb();
             });
           },
           function(cb) {
             client.execute("zrangebyscore","topics:testTopic:consumers:testGroup:processing","-inf","+inf",function(err,data){
              should.not.exist(err)
              should.exist(data)
              data.should.have.length(1)
              cb();
             });
           },
           function(cb) {
             bqClient.ackMessage("testTopic","testGroup",recipientCallback,cb);
           },
           function(cb) {
             client.execute("zrangebyscore","topics:testTopic:consumers:testGroup:processing","-inf","+inf",function(err,data){
              should.not.exist(err)
              should.exist(data)
              data.should.have.length(0)
              cb();
             });
           }], done);
      })
      it("should fail if the target node is down",function(done){
          var recipientCallback;
          var recipientData;
          async.series([
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
             bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
           },
           function(cb) {
             bqClient.getMessage("testTopic","testGroup",undefined,function(err,data) {
                recipientCallback = data.recipientCallback
                recipientData = bqClient.decodeRecipientCallback(recipientCallback)
                cb();
             });
           },
           function(cb) {
             if(clusterData.nodes[0].id == recipientData.nodeId) {
              clusterData.nodes[0].status = "DOWN";
            } else {
              clusterData.nodes[1].status = "DOWN";
            }
            cb();
           },
           function(cb) {
             bqClient.ackMessage("testTopic","testGroup",recipientData.id,function(err) {
              should.exist(err);
              cb();
             });
           }
          ], done);
      });
  })

  describe("#fail",function(){
      beforeEach(function(done){
        async.series([
          function(cb) {
            nodeClient1.createTopic("testTopic",cb);
          },
          function(cb) {
            nodeClient2.createTopic("testTopic",cb);
          },
         function(cb) {
            nodeClient1.createConsumerGroup("testTopic","testGroup",cb);
          },
          function(cb) {
            nodeClient2.createConsumerGroup("testTopic","testGroup",cb);
          }
        ], done);
      });

      it("should fail the message using the recipientCallback",function(done){
        var recipientCallback;
        var recipientData;
        var client
        async.series([
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
          },
          function(cb) {
            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(err)
              recipientCallback = data.recipientCallback
              recipientData = bqClient.decodeRecipientCallback(recipientCallback)
              if(recipientData.nodeId == "node1"){
                  client = redisClient1
              }else{
                  client = redisClient2
              }
              cb();
            });
          },
          function(cb) {
            client.execute("lrange","topics:testTopic:consumers:testConsumer:fails",0,-1,function(err,data) {
              should.not.exist(err);
              should.exist(data);
              data.should.have.lengthOf(0);
              cb();
            });
          },
          function(cb) {
            bqClient.failMessage("testTopic","testGroup",recipientCallback,cb);
          },
          function(cb) {
            client.execute("lrange","topics:testTopic:consumers:testGroup:fails",0,-1,function(err,data) {
              should.not.exist(err);
              should.exist(data);
              data.should.have.lengthOf(1);
              cb();
            });
          }
        ],done);
      });

      it("should fail if the target node is down",function(done){
        var recipientCallback;
        var recipientData;
        async.series([
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
          },
          function(cb) {
            bqClient.postMessage("testTopic",{msg:"testMessage"},cb);
          },
          function(cb) {
            bqClient.getMessage("testTopic","testGroup",undefined,function(err,data){
              should.not.exist(err)
              recipientCallback = data.recipientCallback
              recipientData = bqClient.decodeRecipientCallback(recipientCallback)
              cb();
            });
          },
          function(cb) {
            if(clusterData.nodes[0].id == recipientData.nodeId) {
              clusterData.nodes[0].status ="DOWN";
            } else {
              clusterData.nodes[1].status ="DOWN";
            }
            setTimeout(cb, 20);
          },
          function(cb) {
            bqClient.failMessage("testTopic","testGroup",recipientCallback,function(err) {
              should.exist(err);
              cb();
            });
          },
        ],done);
      });
   })

 describe("background",function(){
    beforeEach(function(done){
        async.series([
          function(cb) {
            nodeClient1.createTopic("testTopic",cb);
          },
          function(cb) {
            nodeClient2.createTopic("testTopic",cb);
          },
         function(cb) {
            nodeClient1.createConsumerGroup("testTopic","testGroup",cb);
          },
          function(cb) {
            nodeClient2.createConsumerGroup("testTopic","testGroup",cb);
          }
        ], done);
      });

     it("should collect stats in file",function(done){
         bqClient.shutdown()

          var bqClientConfig = {
            "refreshInterval":10,
            "cluster": "test",
            "adminApiUrl":"http://adminapi.bigqueue.com",
            "createJournalClientFunction":bj.createJournalClient,
            "createNodeClientFunction":bq.createClient,
             "statsInterval":50,
             "statsFile":"/tmp/bigqueueStats.log"
          }


          var dirs = fs.readdirSync("/tmp")
          if(dirs.lastIndexOf("bigqueueStats.log") != -1){
              fs.unlinkSync("/tmp/bigqueueStats.log")
          }
          bqc.statsInit = false
          bqClient = bqc.createClusterClient(bqClientConfig)
          async.series([
            function(cb) {
              setTimeout(cb, 300);
            },
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"test1"},cb);
            },
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"test2"},cb);
            },
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"test3"},cb);
            },
            function(cb) {
              bqClient.postMessage("testTopic",{msg:"test4"},cb);
            },
            function(cb) {
              setTimeout(cb, 210);
            },
            function(cb) {
              var dirs = fs.readdirSync("/tmp")
              dirs.lastIndexOf("bigqueueStats.log").should.not.equal(-1)
              cb();
            }
          ],done);
     })
  })
})
