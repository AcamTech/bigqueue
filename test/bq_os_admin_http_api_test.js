var should = require('should'),
    http = require('http'),
    redis = require("redis"),
    bq = require('../lib/bq_client.js'),
    httpApi = require("../ext/openstack/bq_os_admin_http_api.js")
    request = require('request'),
    log = require("../lib/bq_logger.js"),
    utils = require('../lib/bq_client_utils.js'),
    bj = require('../lib/bq_journal_client_redis.js'),
    express = require('express'),
    mysql = require('mysql'),
    async = require("async");

describe("openstack admin http api",function(){

     var fake_sock;
     var mysqlConf = {
        host     : 'localhost',
        user     : 'root',
        password : '',
        database : 'bigqueue',
        connectionLimit: 5,
        waitForConnections: true
    };

    var mysqlConn = mysql.createConnection(mysqlConf);


    var admConfig = {
        "logLevel":"error",
        "adminRoleId":3,
        "defaultCluster":"test",
        "mysqlConf":mysqlConf
    }

    var httpConfig = {
        "admConfig":admConfig,
        "port":8081,
        "maxTtl":500,
        "logLevel":"critical",
        "useCache": false
   }

    var keystoneConfig = {
        "keystoneUrl":"http://localhost:35357/v2.0",
        "adminToken":"admin",
        "foceAuth":false
    }

    before(function(done) {
      mysqlConn.connect(function(err) {
        done(err);
      });
    });


    beforeEach(function(done){
    var redisCli =  redis.createClient(6379,"127.0.0.1",{"return_buffers":false})
        redisCli.on("ready",function(){
            redisCli.send_command("flushall", [], function(err){
                should.not.exist(err)
                redisCli.quit()
                done()
            })
        })
    })


    var api

    beforeEach(function(){
        api = httpApi.startup(httpConfig)
    })
    afterEach(function(done){
        api.shutdown()
        process.nextTick(function(){
            done()
        })
   })

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
        }
      ], function(err) {
        done(err)
      });
    });
    describe("Cluster Admin",function(){
        it("should enable to create new clusters",function(done){
        async.series([
          function(cb) {
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"GET",
                json:true
            },function(error,response,body){
                should.not.exist(error)
                response.statusCode.should.equal(200)
                body.should.have.length(0)
              cb();
            })
          },
          function(cb) {
                request({
                    url:"http://127.0.0.1:8081/clusters",
                    method:"POST",
                    json:{"name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
              cb();
            })
          },
          function(cb) {
                    request({
                        url:"http://127.0.0.1:8081/clusters",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.should.have.length(1)
                        body[0].should.equal("test")
              cb();
            })
          }], done);
        })
        it("Should get the cluster information",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.cluster.should.equal("test")
                    done()
                })
            })
        })

        it("should support add nodes",function(done){
          async.series( [
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                cb();
              });
            },
            function(cb) {
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.nodes.should.have.length(0)
                cb();
              });
            },
            function(cb) {
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/nodes",
                        method:"POST",
                json:{"id":"test1","host":"127.0.0.1","port":6379,"status":"DOWN","journals":[]}
                    },function(err,response,body){

                        response.statusCode.should.equal(201)
                cb();
              })
            },
            function(cb) {
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.nodes.should.have.length(1)
                            body.nodes[0].host.should.equal("127.0.0.1")
                            body.nodes[0].port.should.equal(6379)
                            body.nodes[0].host.should.equal("127.0.0.1")
                            done()
              });
            }], done);
        });
        it("should support modify nodes",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.nodes.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/nodes",
                        method:"POST",
                        json:{"id":"test1","host":"127.0.0.1","port":6379,"journals":[],"status":"DOWN"}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.nodes.should.have.length(1)
                            body.nodes[0].host.should.equal("127.0.0.1")
                            body.nodes[0].port.should.equal(6379)
                            request({
                                url:"http://127.0.0.1:8081/clusters/test/nodes/test1",
                                method:"PUT",
                                json:{status:"UP"}
                            },function(err,response,body){
                                response.statusCode.should.equal(200)
                                request({
                                    url:"http://127.0.0.1:8081/clusters/test",
                                    method:"GET",
                                    json:true
                                },function(err,response,body){
                                    response.statusCode.should.equal(200)
                                    body.nodes.should.have.length(1)
                                    body.nodes[0].host.should.equal("127.0.0.1")
                                    body.nodes[0].port.should.equal(6379)
                                    body.nodes[0].status.should.equal("UP")
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })

        it("should support modify journal",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.nodes.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/journals",
                        method:"POST",
                        json:{"id":"j1","host":"127.0.0.1","port":6379,"status":"DOWN"}
                    },function(err,response,body){
                      response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.journals.should.have.length(1)
                            body.journals[0].host.should.equal("127.0.0.1")
                            body.journals[0].port.should.equal(6379)
                            request({
                                url:"http://127.0.0.1:8081/clusters/test/journals/j1",
                                method:"PUT",
                                json:{status:"UP"}
                            },function(err,response,body){
                                response.statusCode.should.equal(200)
                                request({
                                    url:"http://127.0.0.1:8081/clusters/test",
                                    method:"GET",
                                    json:true
                                },function(err,response,body){
                                    response.statusCode.should.equal(200)
                                    body.journals.should.have.length(1)
                                    body.journals[0].host.should.equal("127.0.0.1")
                                    body.journals[0].port.should.equal(6379)
                                    body.journals[0].status.should.equal("UP")
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })
        it("should support add endpoints",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.endpoints.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/endpoints",
                        method:"POST",
                        json:{"name":"test1","host":"127.0.0.1","port":6379}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.endpoints.should.have.length(1)
                            body.endpoints[0].host.should.equal("127.0.0.1")
                            body.endpoints[0].port.should.equal(6379)
                            done()
                        })
                    })

                })
            })
        })
        it("Should enable get info for nodes and journals",function(done){
            request({
                    url:"http://127.0.0.1:8081/clusters",
                    method:"POST",
                    json:{"name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/nodes",
                        method:"POST",
                        json:{"id":"test1","name":"test1","host":"127.0.0.1","port":6379,"journals":[],"status":"DOWN"}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test/journals",
                            method:"POST",
                            json:{"id":"j1","name":"j1","host":"127.0.0.1","port":6379,"journals":[],"status":"DOWN"}
                        },function(err,response,body){
                            response.statusCode.should.equal(201)
                            request({
                                url:"http://127.0.0.1:8081/clusters/test/nodes/test1",
                                method:"GET",
                                json:true
                            },function(error,response,body){
                                response.statusCode.should.equal(200)
                                body.host.should.equal("127.0.0.1")
                                body.port.should.equal(6379)
                                 request({
                                    url:"http://127.0.0.1:8081/clusters/test/journals/j1",
                                    method:"GET",
                                    json:true
                                },function(error,response,body){
                                    response.statusCode.should.equal(200)
                                    body.host.should.equal("127.0.0.1")
                                    body.port.should.equal(6379)
                                    done()
                                })
                            })
                        })
                    })
                })
        })

        it("should support delete endpoints")
        it("should support delete journals")
        it("should support add journals",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.journals.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test/journals",
                        method:"POST",
                        json:{"id":"test1","host":"127.0.0.1","port":6379}
                    },function(err,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/clusters/test",
                            method:"GET",
                            json:true
                        },function(err,response,body){
                            response.statusCode.should.equal(200)
                            body.journals.should.have.length(1)
                            body.journals[0].host.should.equal("127.0.0.1")
                            body.journals[0].port.should.equal(6379)
                            done()
                        })
                    })

                })
            })
        })
        it("should support cluster deletes")

    })

    describe("Topics and consumers",function(){
        beforeEach(function(done){
          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                            "id":"redis1",
                            "host":"127.0.0.1",
                            "port":6379,
                              "status":"UP"
                          }],
                            "journals":[]
                         }
            },function(error,response,body){
                response.statusCode.should.equal(201)
                  cb();
              })
            },
            function(cb) {
                request({
                    url:"http://127.0.0.1:8081/clusters",
                    method:"POST",
                    json:{"name":"test2",
                          "nodes":[{
                            "id":"redis2",
                                "host":"127.0.0.1",
                                "port":6379,
                                "status":"UP",
                            }],
                                "journals":[]
                            }
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    cb()
            })
            },
            function(cb) {
              mysqlConn.query("UPDATE clusters SET `default`=? WHERE name = ?",["Y","test"],cb);
            }], done);
        })
        it("should support create topics into the default cluster using your tenant id",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters/test?format=complete",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenant_id":"1234","tenant_name":"test","name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test?format=complete",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.topics.should.have.length(1)
                        done()
                    })
                })
            })
        })
        it("should get an error if the topic contains ':'", function(done) {
           request({
                url:"http://127.0.0.1:8081/clusters/test?format=complete",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenantId":"1234","name":"test:la"}
                },function(error,response,body){
                    response.statusCode.should.equal(400);
                    done()
                })
            })
        });
        it("Should return full data if format complet if not only nodes and basic info", function(done) {
              request({
                  url:"http://127.0.0.1:8081/clusters/test?format=complete",
                  method:"GET",
                  json:true
              },function(error,response,body){
                  response.statusCode.should.equal(200)
                  should.exist(body.topics)
                  request({
                      url:"http://127.0.0.1:8081/clusters/test",
                      method:"GET",
                      json:true
                  },function(error,response,body){
                      response.statusCode.should.equal(200)
                      should.not.exist(body.topics)
                      done()
                  })
              })

        });
        it("should support create topics into any cluster using your tenant id",function(done){
             request({
                url:"http://127.0.0.1:8081/clusters/test2?format=complete",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenant_id":"1234","name":"test","cluster":"test2", "tenant_name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/clusters/test2?format=complete",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.topics.should.have.length(1)
                        done()
                    })
                })
            })
        })
        it("should support create consumers in any topic using your tenant id",function(done){
          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201);
                cb();
            })
          },
          function(cb) {
                request({
                  url:"http://127.0.0.1:8081/topics/1234-test-test",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.consumers.should.have.length(0)
                  cb();
              });
          },
          function(cb){
                    request({
                url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                        method:"POST",
                json:{"tenant_id":"456","tenant_name":"test", "name":"test-consumer"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
              cb();
            });
          },
          function(cb) {
                        request({
                url:"http://127.0.0.1:8081/topics/1234-test-test",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.consumers.should.have.length(1)
                body.consumers[0].consumer_id.should.equal("456-test-test-consumer")
                cb();
            })
          }], done);
        })

        it("should support list all topics",function(done){
          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                  json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                  response.statusCode.should.equal(201);
                  cb();
              })
            },
            function(cb) {
                     request({
                        url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                  json:{"tenant_id":"12345", "tenant_name":"test","name":"test2"}
                    },function(error,response,body){
                  response.statusCode.should.equal(201);
                  cb();
              })
            },
            function(cb) {
                        request({
                  url:"http://127.0.0.1:8081/topics",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.length(2)
                  body.should.include("1234-test-test")
                  body.should.include("12345-test-test2")
                  cb()
                    })
            }], done);
        })

        it("should support list all topics of a tenantId",function(done){
          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                  json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                  response.statusCode.should.equal(201);
                  cb();
              })
            },
            function(cb) {

                request({
                    url:"http://127.0.0.1:8081/topics?tenant_id=1234",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(1)
                    body.should.include("1234-test-test")
                    cb();
                });
            },
            function(cb) {
                     request({
                        url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                  json:{"tenant_id":"1234", "tenant_name":"test","name":"test2"}
                    },function(error,response,body){
                  response.statusCode.should.equal(201);
                  cb();
              })
            },
            function(cb) {
                        request({
                  url:"http://127.0.0.1:8081/topics?tenant_id=1234",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.length(2)
                  body.should.include("1234-test-test")
                  body.should.include("1234-test-test2")
                  cb()
                    })
            }], done);
                })
        it("should get all topic data on create",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                body.should.have.property("topic_id","1234-test-test")
                body.should.have.property("ttl")
                body.should.have.property("cluster")
                body.should.have.property("endpoints")
                body.should.have.property("consumers")
                done()
            })

        })
        it("should get all consumer data on create",function(done){
          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201);
                cb();
            })
          },
          function(cb) {
              request({
                  url:"http://127.0.0.1:8081/topics/1234-test-test",
                  method:"GET",
                  json:true
            },function(error,response,body){
                  response.statusCode.should.equal(200)
                  body.consumers.should.have.length(0)
                  cb();
              });
          },
          function(cb){
                request({
                url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                method:"POST",
                json:{"tenant_id":"456","tenant_name":"test", "name":"test-consumer"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                body.should.have.property("topic_id","1234-test-test")
                body.should.have.property("consumer_id","456-test-test-consumer")
                    body.should.have.property("cluster")
                    body.should.have.property("endpoints")
                cb();
            });
          }], done);
        })

        it("should get all information about a topic",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test-test",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.property("topic_id","1234-test-test")
                    body.should.have.property("ttl")
                    body.should.have.property("cluster")
                    body.should.have.property("endpoints")
                    body.should.have.property("consumers")
                    done()
                })
            })

        })
        it("should get all information about a consumer",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                    method:"POST",
                    json:{"tenant_id":"456","tenant_name":"test","name":"test1"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test-test/consumers/456-test-test1",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.should.have.property("topic_id","1234-test-test")
                        body.should.have.property("consumer_id","456-test-test1")
                        body.should.have.property("cluster")
                        body.should.have.property("endpoints")
                        done()
                    })
                })
            })
        })

        it("should support list all consumer of a topic",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                        method:"POST",
                        json:{"tenant_id":"456","tenant_name":"test","name":"test1"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        request({
                            url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.length(1)
                            body[0].consumer_id.should.equal("456-test-test1")
                            done()
                        })
                    })
                })
            })

        })
        it("should get an error if the consumer contains a ':'", function(done) {
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(0)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                        method:"POST",
                        json:{"tenant_id":"456","tenant_name":"test","name":"test:withSemiColon"}
                    },function(error,response,body){
                        response.statusCode.should.equal(400);
                        done();
                    });
                });
            });
        });

        it("should support topic deletes",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics?tenant_id=1234",
                    method:"GET",
                    json:true
                },function(error,response,body){
                    response.statusCode.should.equal(200)
                    body.should.have.length(1)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test-test",
                        method:"DELETE",
                    },function(error,response,body){
                        response.statusCode.should.equal(204)
                        request({
                            url:"http://127.0.0.1:8081/topics?tenant_id=1234",
                            method:"GET",
                            json:true
                        },function(error,response,body){
                            response.statusCode.should.equal(200)
                            body.should.have.length(0)
                            done()
                        })
                    })
                })
            })
        })

        it("should support consumers delete",function(done){
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                    method:"POST",
                    json:{"tenant_id":"456","tenant_name":"test","name":"test1"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8081/topics/1234-test-test",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        body.consumers.length.should.equal(1)
                        request({
                            url:"http://127.0.0.1:8081/topics/1234-test-test/consumers/456-test-test1",
                            method:"DELETE",
                        },function(error,response,body){
                            response.statusCode.should.equal(204)
                            request({
                                url:"http://127.0.0.1:8081/topics/1234-test-test",
                                method:"GET",
                                json:true
                            },function(error,response,body){
                                body.consumers.length.should.equal(0)
                                done()
                            })
                        })
                    })
                })
            })
        })
        it("should support consumers reset",function(done){
          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/topics",
                method:"POST",
                json:{"tenant_id":"1234", "tenant_name":"test","name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201);
                cb();
              });
            },
            function(cb) {
                request({
                url:"http://127.0.0.1:8081/topics/1234-test-test/consumers",
                    method:"POST",
                json:{"tenant_id":"456","tenant_name":"test","name":"test1"}
                },function(error,response,body){
                response.statusCode.should.equal(201);
                cb();
              });
            },
            function(cb) {
                    request({
                  url:"http://127.0.0.1:8081/topics/1234-test-test/consumers/456-test-other",
                  method:"PUT",
                    },function(error,response,body){
                  response.statusCode.should.equal(404);
                  cb();
              });
            },
            function(cb) {
                        request({
                  url:"http://127.0.0.1:8081/topics/1234-test-test/consumers/456-test-test1",
                            method:"PUT",
                        },function(error,response,body){
                  response.statusCode.should.equal(204);
                  cb();
              });
            }
          ],done);
        });
    })

    describe("Limits",function(done){
        it("should limit the ttl time to the default ttl",function(done){
          async.series([
            function(cb) {
            request({
                    url:"http://127.0.0.1:8081/clusters",
                    method:"POST",
                    json:{"name":"test",
                    "nodes":[{
                          "id":"redis1",
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP",
                            "journals":[]
                            }]
                         }
            },function(error,response,body){
                response.statusCode.should.equal(201)
                  cb();
              })
            },
            function(cb) {
              mysqlConn.query("UPDATE clusters SET `default`=? WHERE name = ?",["Y","test"],cb);
            },
            function(cb) {
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                    json:{"tenant_id":"1234","tenant_name":"test","name":"test","ttl":500}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    cb();
                })
            },
            function(cb) {
                    request({
                        url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                    json:{"tenant_id":"1234","tenant_name":"test","name":"test2","ttl":501}
                    },function(error,response,body){
                        response.statusCode.should.equal(406)
                    cb()
                });
            }], done);
        })
    })
    describe("Keystone authorization", function(){
        var fakekeystone
        before(function(){
            fakekeystone = express()
            fakekeystone.get("/v2.0/tokens/:token",function(req,res){
                if(req.params.token === "user123" && req.headers["x-auth-token"] === "admin"){
                    return res.json(200,{
                        "access": {
                            "token":{
                                "tenant":
                                     {
                                         "id": "1",
                                         "name": "1234"
                                     }

                            },
                            "user":{
                                "roles": [
                                    {
                                        "id": "3",
                                        "name": "Admin",
                                        "tenantId": "1"
                                    }
                                ]
                            }
                        }

                    })
                }
                if(req.params.token === "user345" && req.headers["x-auth-token"] === "admin"){
                    return res.json(200, {
                        "access": {
                            "token":{
                                "tenant":
                                     {
                                         "id": "3",
                                         "name": "345"
                                     }

                            },
                            "user":{
                            }
                        }

                    })
                }

                if(req.params.token === "someone" && req.headers["x-auth-token"] === "admin"){
                    return res.json(200, {
                        "access": {
                            "token":{
                                "tenant":
                                     {
                                         "id": "2",
                                         "name": "someone"
                                     }

                            },
                            "user":{}
                        }

                    })
                }

                return res.json(404,{err:"token not found"})
            })
            fake_sock = fakekeystone.listen(35357)
        })

        after(function(){
            if(fake_sock){
                fake_sock.close()
            }
        })

        beforeEach(function(){
            api.shutdown()
            var intConf = httpConfig
            intConf["keystoneConfig"] = keystoneConfig
            api = httpApi.startup(intConf)
        })

        it("Should validate the token at cluster creation",function(done){
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(401)
                request({
                    url:"http://127.0.0.1:8081/clusters",
                    method:"POST",
                    json:{"name":"test"},
                    headers:{"X-Auth-Token":"123"}
                },function(error,response,body){
                    response.statusCode.should.equal(401)
                    request({
                        url:"http://127.0.0.1:8081/clusters",
                        method:"POST",
                        json:{"name":"test"},
                        headers:{"X-Auth-Token":"user123"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                        done()
                    })

                })
            })
        })

        it("Should validate tenant on topic creation",function(done){
          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                          "id":"redis1",
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP",
                            "journals":[]
                           }]
                      },
                headers:{"X-Auth-Token":"user123"}
            },function(error,response,body){
                  response.statusCode.should.equal(201);
                  cb();
              })
            },
            function(cb) {
              mysqlConn.query("UPDATE clusters SET `default`=? WHERE name = ?",["Y","test"],cb);
            },
            function(cb) {
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                  json:{"name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(401)
                  cb();
              })
            },
            function(cb) {
                    request({
                        url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                  json:{"name":"test"},
                  headers:{"X-Auth-Token":"lala"}
                    },function(error,response,body){
                  response.statusCode.should.equal(401);
                  cb();
              });
            },
            function(cb) {
                        request({
                            url:"http://127.0.0.1:8081/topics",
                            method:"POST",
                  json:{"name":"test"},
                            headers:{"X-Auth-Token":"user123"}
                        },function(error,response,body){
                            response.statusCode.should.equal(201)
                body.topic_id.should.equal("1-1234-test");
                cb();
            })
            }], done);
        })
        it("Should validate tenant on consumer creation",function(done){
          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                          "id":"redis1",
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP",
                            "journals":[]
                        }]
                    },
                headers:{"X-Auth-Token":"user123"}
            },function(error,response,body){
                  response.statusCode.should.equal(201);
                  cb();
              })
            },
            function(cb) {
              mysqlConn.query("UPDATE clusters SET `default`=? WHERE name = ?",["Y","test"],cb);
            },
            function(cb) {
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                  json:{"name":"test"},
                  headers:{"X-Auth-Token":"user123"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                body.topic_id.should.equal("1-1234-test");
                cb();
              })
            },
            function(cb) {
                    request({
                  url:"http://127.0.0.1:8081/topics",
                        method:"POST",
                  json:{"name":"test", topic_id:"1-1234-test"},
                    },function(error,response,body){
                        response.statusCode.should.equal(401)
                cb();
              })
            },
            function(cb) {
                        request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test/consumers",
                            method:"POST",
                  json:{"name":"test"},
                  headers:{"X-Auth-Token":"sometoken"}
                        },function(error,response,body){
                            response.statusCode.should.equal(401)
                cb();
                            })
            },
            function(cb) {
           request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test/consumers",
                method:"POST",
                  json:{"name":"test-consumer", topic_id:"1-1234-test"},
                headers:{"X-Auth-Token":"user123"}
            },function(error,response,body){
                            response.statusCode.should.equal(201)
                body.topic_id.should.equal("1-1234-test");
                body.consumer_id.should.equal("1-1234-test-consumer");
                cb();
        })
            }

          ], done);
        });

        it("should check topic owner before delete topic",function(done){

          async.series([
            function(cb) {
            request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                          "id":"redis1",
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP",
                            "journals":[]
                           }]
                      },
                headers:{"X-Auth-Token":"user123"}
            },function(error,response,body){
                  response.statusCode.should.equal(201);
                  cb();
              })
            },
            function(cb) {
              mysqlConn.query("UPDATE clusters SET `default`=? WHERE name = ?",["Y","test"],cb);
            },
            function(cb) {
               request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                  json:{"name":"test"},
                        headers:{"X-Auth-Token":"user123"}
                    },function(error,response,body){
                        response.statusCode.should.equal(201)
                body.topic_id.should.equal("1-1234-test");
                cb();
                    })
            },
            function(cb) {
           request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test",
                  method: "DELETE"
            },function(error,response,body){
                response.statusCode.should.equal(401);
                cb();
              });
            },
            function(cb) {
               request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test",
                  method: "DELETE",
                    headers:{"X-Auth-Token":"someone"}
                },function(error,response,body){
                response.statusCode.should.equal(404);
                cb();
              });
            },
            function(cb) {
                    request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test",
                  method: "DELETE",
                        headers:{"X-Auth-Token":"user345"}
                    },function(error,response,body){
                response.statusCode.should.equal(404);
                cb();
              });
            },
            function(cb) {
                        request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test",
                  method: "DELETE",
                            headers:{"X-Auth-Token":"user123"}
                        },function(error,response,body){
                response.statusCode.should.equal(204);
                cb();
              });
            }
          ], done);
        });
        it("should check consumer owner before delete",function(done){
          async.series([
            function(cb) {
           request({
                url:"http://127.0.0.1:8081/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                          "id":"redis1",
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP",
                            "journals":[]
                           }]
                      },
                headers:{"X-Auth-Token":"user123"}
            },function(error,response,body){
                  response.statusCode.should.equal(201);
                  cb();
              })
            },
            function(cb) {
              mysqlConn.query("UPDATE clusters SET `default`=? WHERE name = ?",["Y","test"],cb);
            },
            function(cb) {
                request({
                    url:"http://127.0.0.1:8081/topics",
                    method:"POST",
                  json:{"name":"test"},
                    headers:{"X-Auth-Token":"user123"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                body.topic_id.should.equal("1-1234-test");
                cb();
              })
            },
            function(cb) {
              request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test/consumers",
                  method:"POST",
                  json:{"name":"test_consumer"},
                  headers:{"X-Auth-Token":"user345"}
              },function(error,response,body){
                response.statusCode.should.equal(201)
                body.consumer_id.should.equal("3-345-test_consumer");
                cb();
              })
            },
            function(cb) {
                    request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test/consumers/3-345-test_consumer",
                        method:"DELETE",
                    },function(error,response,body){
                response.statusCode.should.equal(401);
                cb();
              });
            },
            function(cb) {
                        request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test/consumers/3-345-test_consumer",
                            method:"DELETE",
                            headers:{"X-Auth-Token":"someone"}
                        },function(error,response,body){
                response.statusCode.should.equal(404);
                cb();
              });
            },
            function(cb) {
                            request({
                  url:"http://127.0.0.1:8081/topics/1-1234-test/consumers/3-345-test_consumer",
                                method:"DELETE",
                  headers:{"X-Auth-Token":"user345"}
                            },function(error,response,body){
                response.statusCode.should.equal(204);
                cb();
              });
            }
          ], done);
        });
                            })
    describe("Tasks", function() {
      it("Should enable list tasks by criteria", function(done) {
        async.series([
          function(cb) {
            mysqlConn.query("TRUNCATE tasks", cb);
          },
          function(cb) {
            mysqlConn.query("INSERT INTO tasks (data_node_id, task_group, task_type, task_data) VALUES (?,?,?,?)", ["node1","1","CREATE_TOPIC",'{"topic_id":"test1"}'], cb);
          },
          function(cb) {
            mysqlConn.query("INSERT INTO tasks (data_node_id, task_group, task_type, task_data) VALUES (?,?,?,?)", ["node2","1","CREATE_TOPIC",'{"topic_id":"test2"}'], cb);
          },
          function(cb) {
            request({
                url:"http://127.0.0.1:8081/tasks?data_node_id=node2&task_status=PENDING",
                method: "GET",
                json: true
            },function(error,response,body){
              response.statusCode.should.equal(200);
              body.length.should.equal(1);
              body[0].task_id.should.equal(2);
              body[0].data_node_id.should.equal("node2");
              body[0].task_type.should.equal("CREATE_TOPIC");
              body[0].task_data.topic_id.should.equal("test2");
              cb();
            });
          },
          function(cb) {
                request({
                url:"http://127.0.0.1:8081/tasks?task_type=CREATE_TOPIC&task_status=PENDING",
                method: "GET",
                json: true
                },function(error,response,body){
              response.statusCode.should.equal(200);
              body.length.should.equal(2);
              cb();
            });
          },
          function(cb) {
                    request({
                url:"http://127.0.0.1:8081/tasks/1",
                method: "GET",
                json: true
                    },function(error,response,body){
              response.statusCode.should.equal(200);
              body.data_node_id.should.equal("node1");
              body.task_type.should.equal("CREATE_TOPIC");
              body.task_status.should.equal("PENDING");
              body.task_data.topic_id.should.equal("test1")
              cb();
            });
          },
          function(cb) {
                        request({
                url:"http://127.0.0.1:8081/tasks?task_status=FINISHED",
                method: "GET",
                json: true
                        },function(error,response,body){
              response.statusCode.should.equal(200);
              body.length.should.equal(0);
              cb();
            });
          }
        ], done);
      });
      it("Should enable update tasks", function(done) {
        async.series([
          function(cb) {
            mysqlConn.query("TRUNCATE tasks", cb);
          },
          function(cb) {
            mysqlConn.query("INSERT INTO tasks (data_node_id, task_group, task_type, task_data) VALUES (?,?,?,?)", ["node1","1","CREATE_TOPIC",'{"topic_id":"test1"}'], cb);
          },
          function(cb) {
                            request({
                url:"http://127.0.0.1:8081/tasks/1",
                method: "PUT",
                json: {"task_status":"DONE"}
                            },function(error,response,body){
              response.statusCode.should.equal(204);
              cb();
            });
          },
          function(cb) {
                                request({
                url:"http://127.0.0.1:8081/tasks/1",
                method: "GET",
                json: true
                                },function(error,response,body){
              response.statusCode.should.equal(200);
              body.task_status.should.equal("DONE");
              cb();
            });
          }
         ], done);
      });
    });
    describe("Node Stats", function(){
      beforeEach(function(done) {
        mysqlConn.query("TRUNCATE stats", function(err) {
          mysqlConn.commit(done);
        });
      });
      it("Should receive node stats", function(done) {
          var time = new Date();
          request({
            url:"http://127.0.0.1:8081/clusters/test/nodes/node1/stats",
            method: "POST",
            json: {
               "sample_date" : 1401977686996,
               "topics_stats" :
                [
                  {
                   "consumers" :
                      [
                        {
                          "consumer_id" : "testConsumer2",
                          "consumer_stats" :
                          {
                            "fails" : 0,
                            "lag" : 1,
                            "processing" : 0
                          }
                        },
                        {
                          "consumer_id" : "testConsumer",
                          "consumer_stats" :
                            {
                              "fails" : 2,
                              "lag" : 10,
                              "processing" : 1
                            }
                        }
                     ],
                 "topic_head" : 1,
                 "topic_id" : "testTopic"
                }
              ]
            }
          }, function(error, response, body) {
            should.not.exist(error);
            response.statusCode.should.equal(200);
            mysqlConn.query("SELECT * FROM stats", function(err, data) {
              data[0].cluster.should.equal("test");
              data[0].topic.should.equal("testTopic");
              data[0].consumer.should.equal("testConsumer");
              data[0].lag.should.equal(10);
              data[0].fails.should.equal(2);
              data[0].processing.should.equal(1);
              done();
            });
        });
      });
    });
});
