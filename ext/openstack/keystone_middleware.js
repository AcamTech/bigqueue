var  request = require('request')

module.exports.auth= function(config){

    var keystoneAdminUrl = config.keystoneUrl
    var adminToken = config.adminToken
    var forceAuth = config.forceAuth || false

    //TODO: cache the results

    return function(req,res,next){
        //If token will validate and get the user data
        req["keystone"] = {}
        req.keystone["authorized"] = false
        if(req.headers["x-auth-token"]){
            request({
                "url":keystoneAdminUrl+"/tokens/"+req.headers["x-auth-token"],
                "method":"GET",
                "headers":{"X-Auth-Token":adminToken},
                "json":true
            },function(error,response,body){
                if(error || response.statusCode >= 500){
                    return res.status(500).json({err:"Error checking token with keyston","desc":body})
                }
                if(response.statusCode == 200){
                    req["keystone"]["authorized"] = true
                    req["keystone"]["userData"] = body
                }else{
                    if(forceAuth)
                        return res.status(401).json({"err":body})
                    else
                        req["keystone"]["authorized"] = false

                }
                next()
            })

        }else{
            //If no token and
            if(forceAuth){
                return res.status(401).json({"err":"X-Auth-Token header is required"})
            }
            next()
        }
    }
}
