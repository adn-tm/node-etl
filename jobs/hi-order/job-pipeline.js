var pi = require('pipe-iterators');
var Schema = require('mongoose').Schema;
var async = require("async");
var _ = require("underscore");


var Job = require("../job.js");

function _idOrString(v) {
	if (_.isArray(v))
		return v.map(_idOrString);
	if(_.isString(v)) return [v];
	return v.id || v;
}

var aSchema = new Schema({
			// sources:[{type:String}] // [{type: Schema.Types.ObjectId, ref: 'Datasource'}]
			chain:[{  type: String, set:_idOrString} ], 
			mainPipe: {type: String, set:_idOrString }
		}, { discriminatorKey: 'type' });

// aSchema.path("chain").set();
aSchema.methods.stop=function() {
	if (this.headOfPipe && pi.isReadable(this.headOfPipe)) {
		this.headOfPipe.unpipe();
	}
	else
	if (this.stream && _.isFunction(this.stream.unpipe)) {
		this.stream.unpipe();
		
	}
}
aSchema.methods.toStream=function(args, callback) {  
	
	if (!callback && args) {
		callback=args;
		args=false;
	}
	var data=this.args || {};
		args =  args || {};
		for(var key in data)
			args[key]=data[key];
		// data = _.extend(data, args);
	var that = this;
	this.ensureDepts(function(e, chain){
		if (e) return callback(e);
		async.map(chain, 
				function(job, next) {  job.toStream(args, next); }, 
				function(e, pipeline) {
					if (e) return callback(e);
					that.headOfPipe=pipeline[0]?pipeline[0]:null;
					/* 
					console.log("pipeline.length=", pipeline.length);
					pipeline.forEach((p)=>{
						console.log("pipeline: ", pi.isReadable(p), pi.isWritable(p));
					}) 
					*/
					if (!pi.isWritable(that.headOfPipe) ) {
						pipeline.splice(0,1);
						if (!pipeline.length)
							that.stream=that.headOfPipe
						else if (pipeline.length==1)
							that.stream=that.headOfPipe.pipe(pipeline[0]);
						else
							that.stream=that.headOfPipe.pipe(pi.pipeline(pipeline));
					} else that.stream=pi.pipeline(pipeline)
					callback(null, that.stream);
				})
	})
}

/*

	console.log("Setup spy for ", job.id);
	var spyStream=Job.spyStream(data.spy[job.id]);
	job.toStream(data, function(e, stream){
		if (e || !stream) return next(e, stream);
		if (pi.isReadable(stream))
			return next(null, pi.pipeline([stream, spyStream]) );
		if (pi.isWritable(stream))
			return next(null, pi.pipeline([spyStream, stream]) );
		next(null, stream);
	}); 

*/
aSchema.statics.run=function(chain, args, callback) { 
	if(!callback && args) {
		callback=args;
		args={}
	}
	if (!chain || !_.isArray(chain) || !chain.length) return callback(new Error("Empty or wrong job chain in Pipline.run ") );
	var that = new this({chain:chain.map(function(a) {return a.id || a; }), name:"Pipeline job autocreated at "+new Date().toISOString()});
	var jobs=chain.concat([that])
	async.each(jobs, 
		function(a, next) {a.save(next); }, 
		function(e) { 
				that.run(args, function(){
					async.each(jobs, 
						function(job, next) { job.remove(next); },
						callback
					);
				});
		}
	);
	return that;
}

aSchema.methods.getLinkedJobs=function(parent, callback) {
	if (parent && !callback) {callback =parent; parent = null;  }
	var that=this;
	this.ensureDepts(function(e, chain){
		if (e) return callback(e);
		var result=_.omit(that.serialize(), "chain");
		if (!chain || !chain.length) {
			result.parent=parent;
			return callback(null, result);
		}

		async.map(chain, 
			function(job, next) { job.getLinkedJobs(that.id, next); }, 
			function(e, children) {
				if (e ) return callback(e);

				result.nodes=[];
				result.links=[];
				
				result.parent=parent;

				if (!children || !_.isArray(children)) return callback(null, result);
				children=children.filter(function(a){return !!a; })
				
				for(var i=0;i<children.length-1; i++) {
					result.links.push({ source: (children[i].last || children[i].id),  target: (children[i+1].first || children[i+1].id), isMain:!parent });
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
					result.first=result.nodes[0].id;
					result.last=result.nodes[result.nodes.length-1].id;
				}
				callback(null, result);
		})
	});
} 

aSchema.methods.ensureDepts = function(cb) {
	var prop="chain";
	var that=this;
	if (!this[prop] || !this[prop].length) return cb(null, []);
	Job.find({id:{$in:this[prop]}}, function(e, r){
		if (e || !r.length) return cb(e, r);
		if (that[prop].length!=r.length) return cb(new Error("Chain of "+prop+" is broken.Exists "+r.length+" of "+that[prop].length ));
		var result=that[prop].map(function(uuid) {
			return _.find(r, function(one){ return one.id==uuid; });
		}).filter(function(a){ return a && _.isFunction(a.toStream); })
		cb(null, result);
	})
}


aSchema.methods.run=function(args, callback) { 
	if(!callback && args) {
		callback=args;
		args={}
	}
	var that=this;
	if (!this.chain || !this.chain.length) return callback(new Error("Empty job chain in pipline "+(this.name || this.id)) );
	this.toStream(args, function(e, stream){
		if (e) return callback(e);
		var counter=0;
		var replied=false;
		stream.on("data", function(d){
			// console.log(++counter);
		})
		.on("error", function(e){ if (!replied) {replied=true; callback(e); } that.stream=null;that.headOfPipe = null; })
		.on("finish", function(){ if (!replied) {replied=true; callback(); } that.stream=null; that.headOfPipe = null; })
		.on("end", function(){ if (!replied) {replied=true; callback(); } that.stream=null;	that.headOfPipe = null;	})
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


module.exports = Job.discriminator('Pipeline', aSchema);
