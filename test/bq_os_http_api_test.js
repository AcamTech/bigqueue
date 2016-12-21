var should = require('should'),
    bq = require('../lib/bq_client.js'),
    httpApi = require("../ext/openstack/bq_os_http_api.js")
    redis = require('redis'),
    request = require('request'),
    log = require("../lib/bq_logger.js")

describe("Open stack http api",function(){
    var redisClient
    var http_api
    var redisConf= {host:"127.0.0.1",port:6379}

    var httpApiConf = {
        "port": 8082,
        "bqConfig": redisConf,
        "bqClientCreateFunction": bq.createClient,
        "logLevel":"critical",
        "singleNodeMaxReCall":2
    }

    var getMessage = bq.bqClient.prototype.getMessage;
    var bqClient;
    before(function() {
      bq.bqClient.prototype.getMessageFromNode = function(node,topic,consumer,vW,cb) {
        getMessage.call(this,topic,consumer,vW,function(err, data) {
          if(data) {
            data["nodeId"] = node || "redis1";
          }
          cb(err,data);
        });
      }
      bq.bqClient.prototype.getMessage = function(topic,consumer,vW,cb) {
        this.getMessageFromNode("redis1",topic,consumer,vW,cb);
      };

    });
    after(function() {
      bq.bqClient.prototype.getMessage = getMessage;
    });

    before(function(done){
        bqClient = bq.createClient(redisConf)
        bqClient.on("ready",function(){
            done()
        })
    });

    before(function(done){
        redisClient = redis.createClient(redisConf.port,redisConf.host,{"return_buffers":false})
        redisClient.on("ready",function(){
            httpApi.startup(httpApiConf,function(err){
                done()
            })
            done()
        })
    })

    beforeEach(function(done){
        redisClient.send_command("flushall", [], function(err,data){
            done()
        })
    })

    after(function(done){
        httpApi.shutdown()
        done()
    })

    describe("Exchange Messages",function(done){
        beforeEach(function(done){
            bqClient.createTopic("testTopic",function(err){
                should.not.exist(err)
                bqClient.createTopic("testTopic2",function(err){
                    should.not.exist(err)
                    bqClient.createConsumerGroup("testTopic","testConsumer1",function(err){
                        should.not.exist(err)
                        bqClient.createConsumerGroup("testTopic","testConsumer2",function(err){
                            should.not.exist(err)
                            bqClient.createConsumerGroup("testTopic2","testConsumer2",function(err){
                                should.not.exist(err)
                                done()
                            })
                        })
                    })
                })
            })

        })
        it("should get an error if a post message receive an invalid json",function(done){
            request({
                url:"http://127.0.0.1:8082/messages",
                method:"POST",
                body:"foo"
            },function(error,response,body){
                response.statusCode.should.equal(400)
                done()
            })
        })

        it("should receive posted messages on multi-topic post",function(done){
            request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic","testTopic2"]}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                response.body.should.have.length(2)
                var postId1
                var postId2
                if(response.body[0].topic = "testTopic"){
                    postId1 = response.body[0].id
                    postId2 = response.body[1].id
                }else{
                    postId1 = response.body[1].id
                    postId2 = response.body[0].id
                }
                request({
                    uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId1)
                    body.msg.should.equal("testMessage")
                    request({
                        uri:"http://127.0.0.1:8082/topics/testTopic2/consumers/testConsumer2/messages",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        should.exist(response)
                        response.statusCode.should.equal(200)
                        body.should.have.property("id")
                        body.should.have.property("msg")
                        body.id.should.equal(""+postId2)
                        body.msg.should.equal("testMessage")
                        done()
                    })
                })
            })
        })

        it("Should return remaining and node if there are remaining messages", function(done) {
            request({
                url:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
              response.statusCode.should.equal(201)
              request({
                  url:"http://127.0.0.1:8082/messages",
                  method:"POST",
                  json:{msg:"testMessage",topics:["testTopic"]}
              },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                    method:"GET",
                    json:true
                },function(error,response,body){
                  response.statusCode.should.equal(200)
                  response.headers["x-remaining"].should.equal("1");
                  response.headers["x-nodeid"].should.equal("redis1@1");
                  request({
                      uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                      method:"GET",
                      json:true
                  },function(error,response,body){
                    response.statusCode.should.equal(200)
                    should.not.exist(response.headers["x-remaining"]);
                    should.not.exist(response.headers["x-nodeid"]);
                    done();
                 });
               });
            });
          });
        });

      it("Should enable to select node to get", function(done) {
          request({
              url:"http://127.0.0.1:8082/messages",
              method:"POST",
              json:{msg:"testMessage",topics:["testTopic"]}
          },function(error,response,body){
            response.statusCode.should.equal(201)
            request({
                url:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
              response.statusCode.should.equal(201)
              request({
                  uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                  method:"GET",
                  headers:{"X-NodeId":"redis2@1"},
                  json:true
              },function(error,response,body){
                response.statusCode.should.equal(200)
                response.headers["x-remaining"].should.equal("1");
                response.headers["x-nodeid"].should.equal("redis2@2");
                done();
               });
             });
          });
        });

        it("Should not reponse node id if max call reached", function(done) {
          request({
              url:"http://127.0.0.1:8082/messages",
              method:"POST",
              json:{msg:"testMessage",topics:["testTopic"]}
          },function(error,response,body){
            response.statusCode.should.equal(201)
            request({
                url:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
              response.statusCode.should.equal(201)
              request({
                  uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                  method:"GET",
                  headers:{"X-NodeId":"redis1@2"},
                  json:true
              },function(error,response,body){
                response.statusCode.should.equal(200)
                should.not.exist(response.headers["x-remaining"]);
                should.not.exist(response.headers["x-nodeid"]);
                done();
               });
             });
          });
        });

        it("Should return error if invalid node header", function(done) {
          request({
              url:"http://127.0.0.1:8082/messages",
              method:"POST",
              json:{msg:"testMessage",topics:["testTopic"]}
          },function(error,response,body){
            response.statusCode.should.equal(201)
            request({
                url:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
              response.statusCode.should.equal(201)
              request({
                  uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                  method:"GET",
                  headers:{"X-NodeId":"redis1"},
                  json:true
              },function(error,response,body){
                response.statusCode.should.not.equal(200)
                done();
               });
             });
          });
        });

        it("should fail if fail writting to unexistent topic",function(done){
           request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic","testTopic-no-existent","testTopic-no-existent2"]}
            },function(error,response,body){
                response.statusCode.should.equal(500)
                response.body.errors.should.have.length(2)
                done()
            })
        })
        it("should support write a message to multiple topics",function(done){
           request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body[0].should.have.property("id")
                var postId = body[0].id
                request({
                    uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    done()
                })
            })

        })

        it("should receive json's as messages and transform it's to string, when the message come back should be as json format",function(done){
            request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:{test:"message"},topics:["testTopic"]}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body[0].should.have.property("id")
                var postId = body[0].id
                request({
                    uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId)
                    body.msg.should.have.property("test")
                    body.msg.test.should.equal("message")
                    done()
                })
            })
        })
        it("should return message if the _json property exists and the message is not json",function(done){
            request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",_json:"true",topics:["testTopic"]}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body[0].should.have.property("id")
                var postId = body[0].id
                request({
                    uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    done()
                })
            })

        })
        it("should return 204 http status if no data could be getted",function(done){
            request({
                uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(204)
                done()
            })
        })
        it("should get an error if we try to get messages from non existent topic",function(done){
            request({
                uri:"http://127.0.0.1:8082/topics/testTopic-dsadsa/consumers/testConsumer1/messages",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(400)
                done()
            })
        })
        it("should can get the same message if there are 2 consumer groups",function(done){
            request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body[0].should.have.property("id")
                var postId = body[0].id
                request({
                    uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    request({
                        uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer2/messages",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        should.exist(response)
                        response.statusCode.should.equal(200)
                        body.should.have.property("id")
                        body.should.have.property("msg")
                        body.id.should.equal(""+postId)
                        body.msg.should.equal("testMessage")
                        done()
                    })
                })
            })
        })
        it("should get different messages if 2 members of the same consumer group do a 'get message'",function(done){
            request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body[0].should.have.property("id")
                var postId1 = body[0].id
                request({
                    uri:"http://127.0.0.1:8082/messages",
                    method:"POST",
                    json:{msg:"testMessage",topics:["testTopic"]}
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(201)
                    body[0].should.have.property("id")
                    var postId2 = body[0].id
                    request({
                        uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        should.exist(response)
                        response.statusCode.should.equal(200)
                        body.should.have.property("id")
                        body.should.have.property("msg")
                        body.id.should.equal(""+postId1)
                        body.msg.should.equal("testMessage")
                        request({
                            uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            should.exist(response)
                            response.statusCode.should.equal(200)
                            body.should.have.property("id")
                            body.should.have.property("msg")
                            body.id.should.equal(""+postId2)
                            body.msg.should.equal("testMessage")
                            done()
                        })
                    })
                })
            })
        })
        it("should receive the same message if the visibility window is rached",function(done){
            request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body[0].should.have.property("id")
                var postId = body[0].id
                request({
                    uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages?visibilityWindow=1",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.should.have.property("recipientCallback")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    setTimeout(function(){
                        request({
                            uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages?visibilityWindow=1",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            should.exist(response)
                            response.statusCode.should.equal(200)
                            body.should.have.property("id")
                            body.should.have.property("msg")
                            body.should.have.property("recipientCallback")
                            body.id.should.equal(""+postId)
                            body.msg.should.equal("testMessage")
                            done()
                        })
                    },1100)
                })
            })
        })
        it("should enable to do a DELETE of a message so this message shouldn't be received another time",function(done){
            request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:{msg:"testMessage",topics:["testTopic"]}
            },function(error,response,body){
                should.exist(response)
                response.statusCode.should.equal(201)
                body[0].should.have.property("id")
                var postId = body[0].id
                request({
                    uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages?visibilityWindow=1",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    should.exist(response)
                    response.statusCode.should.equal(200)
                    body.should.have.property("id")
                    body.should.have.property("msg")
                    body.id.should.equal(""+postId)
                    body.msg.should.equal("testMessage")
                    request({
                        uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages/"+body.recipientCallback,
                        method:"DELETE",
                        json:true
                    },function(err,response,body){
                        response.statusCode.should.equal(204)
                        setTimeout(function(){
                            request({
                                uri:"http://127.0.0.1:8082/topics/testTopic/consumers/testConsumer1/messages?visibilityWindow=1",
                                method:"GET",
                                json:true
                            },function(error,response,body){
                                should.exist(response)
                                response.statusCode.should.equal(204)
                                done()
                            })
                        },1100)
                    })
                })
            })
        })
    })

    describe("Limits",function(){
        it("should get an error if a posted message have more than 64kb",function(done){
            var msg_json = {"msg":""}
            for(var i=0;i<(129*1024)+10;i++) {
              msg_json.msg=msg_json.msg+"a"
            }
            request({
                uri:"http://127.0.0.1:8082/messages",
                method:"POST",
                json:msg_json,
                headers:{"content-length":JSON.stringify(msg_json).length}
            },function(err,response,body){
                should.not.exist(err)
                response.statusCode.should.equal(413)
                done()
            })
        })
    })
})
