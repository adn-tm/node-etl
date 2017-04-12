var fs = require('fs');
var path = require('path');
var async = require('async');
var _ = require('underscore');
var objectPath =require("object-path");

var request = require("request");
var through = require("through2");
var ftp = require("ftp");
var Url = require('url');
var Buffer = require('buffer').Buffer;
var streamToMongoDB = require("stream-to-mongo-db").streamToMongoDB;
var config = require('../config/environment');

module.exports.getWriteStreamByUrl=function (url, options, callback) {
	if(!callback && _.isFunction(options)) {
		callback=options;
		options={};
	}
	var _istr=false;
	if (!url) return callback(new Error("WriteStream: Undefined URL: "+url));
	if (url.indexOf("$")==0) {
		url=url.split("/");
		var varname=url[0].substr(1);
		if (!config[varname]) return callback(new Error("Unknown URL parameter: "+varname));
		url[0]=config[varname];
		url=url.join("/");
	}

	var parsedUrl=Url.parse(url);
	if (!parsedUrl) return callback(new Error("Bad URL: "+url));
	if (parsedUrl.protocol=="file:") {
		// console.log("Switch tot next file ", url);
		var pathName=url.substring("file:".length);
		console.log("Write to file ", pathName);
		callback(null, fs.createWriteStream(pathName) );
	} else 
	if (parsedUrl.protocol=="http:" || parsedUrl.protocol=="https:") {
		console.log("Uploading: ", url);
		options = options || {};
		options.url=url;
		if (!options.method || options.method.toLowerCase()=="post") {
			callback(null, request.post(options));
		}
		else if (options.method.toLowerCase()=="get") {
			callback(null, request.get(options));
		}
		else if (options.method.toLowerCase()=="put") {
			callback(null, request.put(options));
		}
	} else 
	if (parsedUrl.protocol=="mongodb:") {
		var paths=parsedUrl.pathname.split("/");
		var collection=paths.pop();
		parsedUrl.pathname = paths.join("/");
		return callback(null, streamToMongoDB({ dbURL : parsedUrl.href, collection : collection }));
	} else 
		callback(new Error("Unknown protocol for URL "+url));
}
module.exports.getReadStreamByUrl=function (url, options, callback) {
	if(!callback && _.isFunction(options)) {
		callback=options;
		options={};
	}
	var _istr=false;
	if (!url) return callback(new Error("ReadStream: Undefined URL: "+url));
	if (url.indexOf("$")==0) {
		url=url.split("/");
		var varname=url[0].substr(1);
		if (!config[varname]) return callback(new Error("Unknown URL parameter: "+varname));
		url[0]=config[varname];
		url=url.join("/");
	}


	var parsedUrl=Url.parse(url);
	if (!parsedUrl) return callback(new Error("Bad URL: "+url));
	if (parsedUrl.protocol=="file:") {
		// console.log("Switch tot next file ", url);
		var pathName=url.substring("file:".length);
		console.log("Read from file ", pathName);
		callback(null, fs.createReadStream(pathName) );
	} else 
	if (parsedUrl.protocol=="http:" || parsedUrl.protocol=="https:") {
		console.log("Loading: ", url);
		options = options || {};
		options.url=url;
		if (!options.method || options.method.toLowerCase()=="get") {
			// console.log("GET ", url, "with headers ", options);
			callback(null, request.get(options));
		}
		else if (options.method.toLowerCase()=="post") {
			// console.log("POST ", url, "with headers ", options);
			callback(null, request.post(options));
		}
		else if (options.method.toLowerCase()=="delete") {
			// console.log("POST ", url, "with headers ", options);
			callback(null, request.delete(options));
		}
	} else 
	if (parsedUrl.protocol=="ftp:") {
		// console.log("Process as ftp ", url);
		if (parsedUrl.auth) {
			var u_p=parsedUrl.auth.split(":");
			parsedUrl.user=u_p[0];
			if (u_p.length>1)
				parsedUrl.password=u_p[1]+"@";
		}
		 var c = new Client();
		 c.on('ready', function() {
			c.get(parsedUrl.path, function(err, stream) {
			  if (err) return callback(err);
			  stream.once('close', function() { c.end(); });
			  callback(null, stream);
			});
		 });
		 c.on('error', function(err) {
			callback(err);
		 });
		  // connect to localhost:21 as anonymous 
		 c.connect(parsedUrl);
	} else 
		callback(new Error("Unknown protocol for URL "+url));
}

module.exports.startQuarterDate = function (year, quarter) {
	if (quarter==2)
		return year+"-04-01";	
	if (quarter==3 || quarter==34)
		return year+"-07-01";	
	if (quarter==4)
		return year+"-10-01";	
	return year+"-01-01";	
}
module.exports.endQuarterDate=function (year, quarter) {
	if (quarter==1)
		return year+"-03-31";
	if (quarter==2 || quarter==12)
		return year+"-06-30";	
	if (quarter==3)
		return year+"-09-30";	
	return year+"-12-31";	
}

module.exports.replaceParams=function _replaceParams(params, obj, key) {
        if (_.isString(obj) ) {
            var s=obj;
            var k=s.indexOf("{%");
            while (k>=0) {  
                  var f=s.indexOf("%}", k);
                  var path=s.substring(k+2, f>=1?f:undefined);
                  var valObj;
                  var replacement=""
                  if (path /* && !(path in dates) */ ) {
                    valObj=objectPath.get(params, path) || "";
                    if (valObj._id)
                            valObj = valObj._id;
                     replacement = valObj?valObj:""
                    s=s.replace("{%"+path+"%}",replacement );
                  }
                  k=s.indexOf("{%", k+1+replacement.length );
            } 
            return s;
        } else if(_.isObject(obj)) {
       //     console.log("_replaceParams", key);
            var result={}
            for(var key in obj)
                  result[key]=_replaceParams(params, obj[key], key)
            return result;
        }
        else return obj;
    } 


module.exports.tryParseSetter=function(a) {
	if (!_.isString(a)) return a;
	try { 
		var b=JSON.parse(a);
		return b;
	} catch(e) { }
	return a;
}