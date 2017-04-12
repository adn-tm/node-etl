var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var async = require("async");
var objectPath  =require("object-path");
var vm = require('vm');

var Job = require("../job.js");

function _idOrString(v) {
	if(_.isString(v)) return v;
	return v.id || v;
}

var aSchema = new Schema({
			source: { type: String, required:true, set:_idOrString },
		}, { discriminatorKey: 'type' });

aSchema.methods.getLinkedJobs=function() {
	return [];
}
aSchema.methods.attachExtJob=function(brunch, link) {
	this.source=brunch.id;
}
aSchema.methods.getLinkedJobs=function(parent, callback) {
	if (parent && !callback) {callback =parent; parent = null;  }
	var that=this;
	this.ensureDepts(function(e, source){
		if (e) return callback(e);
		source.getLinkedJobs(that.id, function(e, sourceRes) { 
			if (e ) return callback(e);
			var result=_.omit(that.serialize(), "source");
				result.nodes=[];
				result.links=[];	
				result.parent=parent;		
				result.links.push({ 
						source:(sourceRes.last || sourceRes.id), 
						target: result.id,
				  		key: 	"source"
				});
				if (sourceRes.links)
					result.links=result.links.concat(sourceRes.links);
				if (sourceRes.nodes)
					result.nodes=result.nodes.concat(sourceRes.nodes);
				if (!(sourceRes.first && sourceRes.last) )
					result.nodes.push(_.omit(sourceRes, ["nodes", "links"]) );

				callback(null, result);
		})
	});
}

aSchema.methods.toStream=function(agrs, callback) { 
	if (!callback && agrs) {
		callback=agrs;
		agrs=false;
	}
	var that = this;
	var data=that.agrs || {};
		agrs =  agrs || {};
		var spy=agrs.spy && _.isFunction(agrs.spy[this.id])?agrs.spy[this.id]:null;
		agrs.spy=_.omit(agrs, "spy");
		data = _.extend(data, agrs);
		var context;
		if (agrs.isContext) {
			context= agrs;
			if (this.agrs)
				for(var key in this.agrs) 
					context[key]=this.agrs[key];
			context.initer= undefined;
			context.processor= undefined;			
		} else { 
			context = new vm.createContext(data);
			context.isContext=true;
		}

		// var context = new vm.createContext(data);
		that.initWithContext(context);
		
		var proc=that.processorInContext(context);
		if (proc instanceof Error || !proc) return callback(proc || new Error("Map function is undefined for joinDetail job "+that.name+"(id="+that.id+")"));

		this.ensureDepts(function(err, source) { 
			if (err || !source) return callback(err || new Error("Source Job is undefined for joinDetail job "+that.name+"(id="+that.id+")") );
			var combiner=pi.thru.obj(function (masterData, encoding, onDone) {
				var stream=this;
//				console.log("masterData.inn=", masterData.general.inn);
				context.master=masterData;
				source.toStream(context, function(err, detailStream){
					if (err || !detailStream) {
						console.error(err || "Detail stream not ready");
						stream.push(masterData);
						return onDone(err);
					};
					var buffer=[];
					detailStream
					.on("error", function(e){ stream.emit('error', e); onDone(e); })
					.on("data", function(data){ buffer.push(data);  })
					.on('end', function(data) { 
						console.log("Details loaded ", buffer.length);
						if (data) buffer.push(data);
						var joined=proc.call(context, masterData, buffer);
						// console.log("joined.details", joined.details || joined);
						if (joined) {
							stream.push(joined);
							if(spy) spy(joined)
						}
						onDone();
					});
				});
			});
			return callback(null, combiner);
		});

}
aSchema.methods.ensureDepts = function(cb) {
	var prop="source";
	var that=this;
	if (!this[prop]) return cb();
	Job.findOne({id:this[prop]}, function(e, r){
		if (e || !r)  return cb(new Error("Link "+prop+" is broken. Linked entity not exists "+that[prop] ));
		cb(null, r);
	})
}

aSchema.post('init', function(doc) {
 	doc.isWriteable=true;
	doc.isReadable=true;
});

aSchema.pre("save", function(next){
	this.isWriteable=true;
	this.isReadable=true;
	next();
});

module.exports = Job.discriminator('JoinDetail', aSchema);
