var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var async = require("async");
var objectPath  =require("object-path");
var vm = require("vm")
var utils = require("../../utils.js");
var Job = require("../job.js");

var aSchema = new Schema({
			childrenKey:{type:String, default:"children", required:true},
			parentKeyPath:{type:String, default:"parent", required:true},
			idKeyPath:{type:String, default:"id"},

			destination:{type:String},
			reqOptions:{type:Schema.Types.Mixed, set:utils.tryParseSetter},

		}, { discriminatorKey: 'type' });


aSchema.methods.toStream=function(agrs, callback) { 
	if (!callback && agrs) {
		callback=agrs;
		agrs=false;
	}
	var that = this,
		data=that.agrs || {};
		agrs =  agrs || {};
	var spy=agrs.spy && _.isFunction(agrs.spy[this.id])?agrs.spy[this.id]:null;
		data = _.extend(data, agrs);
		data.tree={};
	var context;
	if (agrs.isContext) {
		context= agrs;
		if (data)
		for(var key in this.agrs) 
			context[key]=this.agrs[key];
	} else { 
		context = new vm.createContext(data);
		context.isContext=true;
	}

	// var context =new vm.createContext(data);
		that.initWithContext(context);
		if (context instanceof Error) return callback(context);
//		var proc=that.processorInContext(context);
//		if (proc instanceof Error || !proc) return callback(proc || new Error("Reduce function is undefined for reducer job "+that.name+"(id="+that._id+")"));

		var maps={};
		var tree=[];

	
		var combiner=pi.thru.obj(function (data, encoding, onDone) {
			// console.log("One done", i++)
			// this.push(JSON.stringify(data) );
			// return onDone();
			
		try {	
			if (!data) { 
				
				return onDone();
			}
			
			var id=objectPath.get(data, that.idKeyPath);
			var parentId=objectPath.get(data, that.parentKeyPath);
			


			if (!id) { 
				
				return onDone();
			}
			/* if (id in maps) {
				return onDone(new Error("Not unique id in data:"+id));
			} */
			

			var r=_.omit(data, [that.childrenKey, that.parentKeyPath])
			maps[id]=maps[id] || {};
			for(var key in r)  maps[id][key]=r[key];

		
			
			
			
				if (!parentId)
					tree.push(maps[id])
				else { 
					
					maps[parentId]=maps[parentId] || {};
					maps[parentId][that.childrenKey] = maps[parentId][that.childrenKey] || [];
					maps[parentId][that.childrenKey].push(maps[id]);
					
				}
			} catch(e) {console.error(e); }
			
			return onDone();
			// this.push("A");
			
		}, function(finshed) {
				console.log("tree");
				if (tree.length==0)
					this.push("{}");
				else if (tree.length==1)
					this.push( JSON.stringify(tree[0]) );
				else 
					this.push( JSON.stringify(tree) );
				finshed();
		});

		var reqOptions=context.reqOptions || that.reqOptions || {};
		if (reqOptions.auth) {
			reqOptions.headers=reqOptions.headers || {};
			reqOptions.headers={"Authorization": "Basic "+(new Buffer(reqOptions.auth.user+":"+reqOptions.auth.password) ).toString("base64") };
			reqOptions=_.omit(reqOptions, "auth");
		}

		utils.getWriteStreamByUrl(context.destination || that.destination, reqOptions, function(err, _stream) {
			if (err) { console.error(err); return callback(err);	}
			if (spy) 
				return callback(null, pi.pipeline([ combiner,  Job.spyStream(spy), _stream]) );
			return callback(null,  pi.pipeline([combiner, _stream]) );
		});

		
}

aSchema.post('init', function(doc) {
 	doc.isWriteable=true;
	doc.isReadable=false;
});

aSchema.pre("save", function(next){
	this.isWriteable=true;
	this.isReadable=false;
	next();
});

module.exports = Job.discriminator('WriterJSONTree', aSchema);
