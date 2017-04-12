var pi = require('pipe-iterators');
var Schema = require('mongoose').Schema;
var async = require("async");
var _ = require("underscore");
var vm = require('vm');

var Job = require("../job.js");


function _idOrString(v) {
	if (_.isArray(v))
		return v.map(_idOrString);
	if(_.isString(v)) return [v];
	return v.id || v;
}

var aSchema = new Schema({
			receivers:{  type: [String] /*, required:true*/, set:_idOrString }
		}, { discriminatorKey: 'type' });
aSchema.methods.attachExtJob=function(brunch, link) {
	this.receivers=this.receivers || [];
	this.receivers.push(brunch.id);
}
aSchema.methods.getLinkedJobs=function(parent, callback) {
	if (parent && !callback) {callback =parent; parent = null;  }
	var that=this;
	this.ensureDepts(function(e, receivers){
		if (e) return callback(e);
		async.map(receivers, 
			function(job, next) { job.getLinkedJobs(that.id, next); }, 
			function(e, children) {
				if (e ) return callback(e);
				var result=_.omit(that.serialize(), "receivers");

				result.nodes=[];
				result.links=[];
				
				result.parent=parent;

				if (!children || !_.isArray(children)) return callback(null, result);
				children=children.filter(function(a){return !!a; })
				
				for(var i=0;i<children.length; i++) {
					result.links.push({ source:result.id, target: (children[i].first || children[i].id) });
				}
				
				children.forEach(function(c, i){
					if (c.links)
						result.links=result.links.concat(c.links);
					if (c.nodes)
						result.nodes=result.nodes.concat(c.nodes);
					if (!(c.first && c.last) )
						result.nodes.push(_.omit(c, ["nodes", "links"]) );
				})
				if (parent) {
					result.first=result.id;
					// result.last=result.nodes[result.nodes.length-1].id;
				}
				callback(null, result);
		})
	});
} 

// match
aSchema.methods.toStream=function(agrs, callback) {  
	if (!callback && agrs) {
		callback=agrs;
		agrs=false;
	}
	
	var data=this.agrs || {};
		agrs =  agrs || {};
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
		this.initWithContext(context);
		if (context instanceof Error) return callback(context);
	var proc=this.processorInContext(context);
		if (proc instanceof Error) return callback(proc || new Error("Comparator function is undefined for sorter job "+this.name+"(id="+this.id+")"));
	// console.log("Splitted data", data);
	var that=this;
	this.ensureDepts(function(e, receivers){
		// console.log(e || receivers);
		if (e) { console.error(e);  return callback(e); }
		if ( (receivers.length!=that.receivers.length) || !that.receivers.length) {
			console.error("Not all pipes or empty set of receivers in splitter reciervers "+that.name); 
			return callback(new Error("Not all pipes or empty set of receivers in splitter reciervers "+that.name)); 
		};
		async.map(receivers, 
				function(job, next) { 
					// console.log("Split into ", job.type, job.id);
					job.toStream(data, next); 
				}, 
				function(e, pipeline) {
					if (e) { console.error(e); return callback(e); }
					if (pipeline.length!=receivers.length) {
						console.error("Not all pipes in splitter reciervers"); return callback(new Error("Not all pipes in splitter reciervers "+that.name)); 
					}

					if (!proc) {
						console.log("Fork stream for ", receivers.length);
						return callback(null, pi.fork(pipeline));
					}
					var splitter=pi.writable.obj(function (doc, encoding, onDone) {
						var destStream=proc.call(context, doc, pipeline);
						// console.log("Push ", destStream);
						if (pi.isStream(pipeline[destStream]) ) {

							pipeline[destStream].write(doc);
						}
						onDone();
					});
					splitter.on('finish', function(){
						// console.log("Finised ");
						pipeline.forEach(function(a){
							a.end();
						})
					});
					return callback(null, splitter);
				})
	})
}

aSchema.methods.ensureDepts = function(cb) {
	var prop="receivers";
	var that=this;
	if (!this[prop] || !this[prop].length) return cb(null, []);
	Job.find({id:{$in:this[prop]}, isWriteable:true}, function(e, r){
		if (e || !r.length) return cb(e, r);
		if (that[prop].length!=r.length) return cb(new Error("Chain of "+prop+" is broken.Exists "+r.length+" of "+that[prop].length ));
		var result=that[prop].map(function(uuid) {
			return _.find(r, function(one){ return one.id==uuid; });
		}).filter(function(a){ return a && _.isFunction(a.toStream); })
		cb(null, result);
	})
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


module.exports = Job.discriminator('Splitter', aSchema);
