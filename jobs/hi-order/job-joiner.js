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
			vocabs:[{
			   job: { type: String, required:true, set:_idOrString },
			   idPath:{type:String, required:true, default:"id"},
			   key:{type:String, required:true},
			   _id:false
			}]
		}, { discriminatorKey: 'type' });

aSchema.methods.attachExtJob=function(brunch, link) {
	console.log("Joiner.attachExtJob=", brunch, link);
	this.vocabs=this.vocabs || [];
	link = link || {}
	var a={job:brunch.id, key:(link.key || ("key_"+this.vocabs.length))};
	if ("idPath" in link)
		a.idPath=link.idPath;
	this.vocabs.push(a);
}
aSchema.methods.toStream=function(agrs, callback) { 
	if (!callback && agrs) {
		callback=agrs;
		agrs=false;
	}
	var that=this;
	this.ensureDepts(function(err, vocabs){
		if (err || !that) return callback(err);
		var data=that.agrs || {};
			agrs =  agrs || {};
		
		var spy=agrs.spy && _.isFunction(agrs.spy[this.id])?agrs.spy[this.id]:null;
			data = _.extend(data, agrs);
			data.maps={};
		async.each(vocabs, function(voc, next) {
			if (!voc || !voc.job) return next();
			voc.job.toStream({}, function(err, mapStream) {
				if (err || !mapStream) { 
					console.error("Joiner Voc stream error", err || mapStream);
					return next(err);
				}
				var buf=[];
				mapStream.on("data", function(d){ d && buf.push(d); }).on("error", next).on("end", function(){
					if (voc.idPath) {
						data.maps[voc.key]={};
						buf.forEach(function(a){
							var id=objectPath.get(a, voc.idPath);
							data.maps[voc.key][id]=a;
						})
						//console.log("data.maps=", voc.key, data.maps[voc.key]);
					} else data.maps[voc.key]=buf;
					next();
				})
			})
		}, function(e) {
				if (e) return callback(e);
				var context;
				if (agrs.isContext) {
					context= agrs;
					if (that.agrs)
						for(var key in that.agrs) 
							context[key]=that.agrs[key];
					context.initer= undefined;
					context.processor= undefined;					
				} else { 
					context = new vm.createContext(data);
					context.isContext=true;
				}

				// var context = new vm.createContext(data);
				that.initWithContext(context);
				if (context instanceof Error) return callback(context);
				var proc=that.processorInContext(context);
				if (proc instanceof Error || !proc) return callback(proc || new Error("Predicate function is undefined for filter job "+that.name+"(id="+that.id+")"));

				return callback(null, pi.map(function(obj) { 
					try {
						var res=proc.call(context, obj);
						if (spy) spy(res);
					} catch(e) {
						console.error(e);
						res=obj;
					}
					return res;
				}) );
		})
	});
}

aSchema.methods.getLinkedJobs=function(parent, callback) {
	if (parent && !callback) {callback =parent; parent = null;  }
	var that=this;
	this.ensureDepts(function(e, vocabs){
		if (e) return callback(e);
		// console.log("Vocabs", e || vocabs);
		async.map(vocabs, 
			function(voc, next) { 
				voc.job.getLinkedJobs(that.id, 
					function(e, r) { 
						console.log("Joined", e || r);
						if (r) {
							
				  		}
						next(e,r);
				}); 
			}, 
			function(e, children) {
				if (e ) return callback(e);
				var result=_.omit(that.serialize(), "vocabs");

				result.nodes=[];
				result.links=[];
				
				result.parent=parent;

				if (!children || !_.isArray(children)) return callback(null, result);
				children=children.filter(function(a){return !!a; })
				
				for(var i=0;i<children.length; i++) {
					result.links.push({ 
						source:(children[i].last || children[i].id), 
						target: result.id,
						idPath: vocabs[i].idPath,
				  		key: 	vocabs[i].key,
				  		index: i
					});
				}
				
				children.forEach(function(c, i){
					if (c.links)
						result.links=result.links.concat(c.links);
					if (c.nodes)
						result.nodes=result.nodes.concat(c.nodes);
					if (!(c.first && c.last) )
						result.nodes.push(_.omit(c, ["nodes", "links"]) );
				})
				
				callback(null, result);
		})
	});
}
aSchema.methods.ensureDepts = function(cb) {
	var prop="vocabs";
	var that=this;
	if (!this[prop] || !this[prop].length) return cb(null, []);
	
	var ids=this.vocabs.map(function(v){ return v.job; }).filter(function(v) {return !!v; } );
	if (!ids.length) return cb(null, []);
	//console.log({id:{$in:ids}, isReadable:true})
	Job.find({id:{$in:ids}, isReadable:true}, function(e, r){
		if (e || !r.length) return cb(e, r);
		if (ids.length!=r.length) return cb(new Error("Chain of "+prop+" is broken.Exists "+r.length+" of "+ids.length ));
		var result=that[prop].map(function(voc) {
			var res=_.extend({}, voc);
			res.job=_.find(r, function(one){ return one.id==voc.job; });
			return res;
		}).filter(function(a){ return a && a.job && _.isFunction(a.job.toStream); })
		cb(null, result);
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

module.exports = Job.discriminator('Joiner', aSchema);
