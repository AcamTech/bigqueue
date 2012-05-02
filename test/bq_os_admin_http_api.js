var should = require('should'),
    ZK = require("zookeeper"),
    redis = require("redis"),
    bq = require('../lib/bq_client.js'),
    httpApi = require("../ext/openstack/bq_os_admin_http_api.js")
    request = require('request'),
    log = require("node-logging"),
    utils = require('../lib/bq_client_utils.js'),
    bj = require('../lib/bq_journal_client_redis.js')

describe("openstack admin http api",function(){
   
   var bqPath = "/bq"
    
    var zkConfig = {
        connect: "localhost:2181",
        timeout: 200000,
        debug_level: ZK.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false
    }   

    var admConfig = {
        "zkConfig":zkConfig,
        "zkBqPath":bqPath,
        "createNodeClientFunction":bq.createClient,
        "createJournalClientFunction":bj.createJournalClient,
        "logLevel":"error",
        "defaultCluster":"test"
    }
    
    var httpConfig = {
        "admConfig":admConfig,
        "port":8080,
        "basePath":"/bigqueue",
        "logLevel":"critical"
    }

    var zk = new ZK(zkConfig)

    before(function(done){
        zk.connect(function(err){
            if(err){
                done(err)
            }else{
                done()  
            }
        })
    })

    beforeEach(function(done){
    var redisCli =  redis.createClient()
        redisCli.on("ready",function(){
            redisCli.flushall(function(err){
                should.not.exist(err)
                redisCli.quit()
                done()
            })
        })
    })

    beforeEach(function(done){
        zk.a_create("/bq","",0,function(rc,error,path){
            utils.deleteZkRecursive(zk,"/bq/clusters",function(){
                utils.deleteZkRecursive(zk,"/bq/admin",function(){
                    zk.a_create("/bq/clusters","",0,function(rc,error,path){
                        zk.a_create("/bq/admin","",0,function(rc,error,path){
                            zk.a_create("/bq/admin/indexes","",0,function(rc,error,path){
                                zk.a_create("/bq/admin/indexes/topics","",0,function(rc,error,path){
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })
    })

    after(function(){
        zk.close()
    })

    var api

    beforeEach(function(){
        api = httpApi.startup(httpConfig)
    })
    afterEach(function(){
        api.shutdown()
    })



    describe("Cluster Admin",function(){
        it("should enable to create new clusters",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"GET",
                json:true
            },function(error,response,body){
                should.not.exist(error)
                response.statusCode.should.equal(200)
                body.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/clusters",
                    method:"POST",
                    json:{"name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/clusters",
                        method:"GET",
                        json:true
                    },function(error,response,body){
                        response.statusCode.should.equal(200)
                        body.should.have.length(1)
                        body[0].should.equal("test")
                        done()
                    })
                })
            })
        })
        it("Should get the cluster information",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"POST",
                json:{"name":"test"}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/clusters/test",
                    method:"GET",
                    json:true
                },function(err,response,body){
                    response.statusCode.should.equal(200)
                    body.cluster.should.equal("test")
                    done()
                })
            })
        })
        it("should support cluster deletes")
        it("if keystone is set should validate the token")
    })
    
    describe("Topics and consumers",function(){
        beforeEach(function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters",
                method:"POST",
                json:{"name":"test",
                      "nodes":[{
                        "name":"redis1", 
                        "config":{
                            "host":"127.0.0.1",
                            "port":6379,
                            "status":"UP"
                         }
                      }]}
            },function(error,response,body){
                response.statusCode.should.equal(201)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/clusters",
                    method:"POST",
                    json:{"name":"test2",
                          "nodes":[{
                            "name":"redis1", 
                            "config":{ 
                                "host":"127.0.0.1",
                                "port":6379,
                                "status":"UP"
                            }
                         }]}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    done()
                })
            })
        })
        it("should support create topics into the default cluster using your tenant id",function(done){
            request({
                url:"http://127.0.0.1:8080/bigqueue/clusters/test",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics/1234",
                    method:"POST",
                    json:{"name":"test"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/clusters/test",
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
        it("should support create topics into any cluster using your tenant id",function(done){
             request({
                url:"http://127.0.0.1:8080/bigqueue/clusters/test2",
                method:"GET",
                json:true
            },function(error,response,body){
                response.statusCode.should.equal(200)
                body.topics.should.have.length(0)
                request({
                    url:"http://127.0.0.1:8080/bigqueue/topics/1234",
                    method:"POST",
                    json:{"name":"test","cluster":"test2"}
                },function(error,response,body){
                    response.statusCode.should.equal(201)
                    request({
                        url:"http://127.0.0.1:8080/bigqueue/clusters/test2",
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
        it("should support create consumers in any topic using your tenant id")
        it("should support topic deletes")
        it("should support consumers delete")
        it("if keyston is set should validate the token")
    })

    describe("stats",function(){
        it("should retrive stats for clusters")
        it("should retrive stats for topics using the tenant id")
        it("should retrive stats for consumer groups using the tenant id")
    })

})
