var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var Readable = require('stream').Readable;
var glob = require("glob")
var vm = require("vm");



var Buffer = require('buffer').Buffer;
var Job = require("../job.js");
var utils = require("../../utils.js");
var config = require('../../config.js');

function _idOrString(v) {
	if(_.isString(v)) return v;
	return v.id || v;
}

var aSchema = new Schema({
			source:{type:String},
		//	reqOptions:{type:Schema.Types.Mixed, set:utils.tryParseSetter},
			parser:  {  type: String, set:_idOrString }, // { type: Schema.Types.ObjectId, ref: 'Job'/*, required:true */},
		}, { discriminatorKey: 'type' });

aSchema.methods.attachExtJob=function(brunch, link) {
	this.parser=brunch.id;
}
aSchema.methods.getLinkedJobs=function(parent, callback) {
	if (parent && !callback) {callback =parent; parent = null;  }
	var that=this;
	this.ensureDepts(function(e, parser){
		if (e) return callback(e);
		if (parser)
			parser.getLinkedJobs(that.id, function(e, sourceRes) { 
				if (e ) return callback(e);
				var result=_.omit(that.serialize(), "parser");
				result.nodes=[];
				result.links=[];	
				result.parent=parent;		
				result.links.push({ 
						source:result.id,
						target: (sourceRes.first || sourceRes.id), 
				  		key: 	"parser"
				});
				if (sourceRes.links)
					result.links=result.links.concat(sourceRes.links);
				if (sourceRes.nodes)
					result.nodes=result.nodes.concat(sourceRes.nodes);
				if (!(sourceRes.first && sourceRes.last) ) {
					result.nodes.push(_.omit(sourceRes, ["nodes", "links"]) );
				}
				// result.last=sourceRes.id;
				result.last=result.id;
				callback(null, result);
			})
		else 
			callback(null, _.omit(that.serialize(), "parser"));
	});
}
aSchema.methods.ensureDepts = function(cb) {
	var prop="parser";
	var that=this;
	if (!this[prop]) return cb();
	Job.findOne({id:this[prop]}, function(e, r){
		if (e)  return cb(new Error("Link "+prop+" is broken. Linked entity not exists "+that[prop] ));
		cb(null, r);
	})
}
aSchema.methods.toStream=function(agrs, callback) { 
	if (!callback && agrs) {
		callback=agrs;
		agrs=false;
	}
	agrs =  agrs || {};
	var that=this;
	
	var spy=agrs.spy && _.isFunction(agrs.spy[this.id])?agrs.spy[this.id]:null;
	var cntxt=_.extend(this.agrs || {}, agrs);
		

	//	cntxt.state={lastPageRows:-1, rows:-1, page:0, totalRowsProcessed:0, totalRows:-1, totalPages:-1};
	//	cntxt.url="";

	var context;
	if (agrs.isContext) {
		context= agrs;
		if (this.agrs)
			for(var key in cntxt) 
				context[key]=this.agrs[key];
		context.initer= undefined;
		context.processor= undefined;		
	} else { 
		context = new vm.createContext(cntxt);
		context.isContext=true;
	}

	// var context = new vm.createContext(cntxt);
	that.initWithContext(context);
	console.log(context, agrs);
/*
	var reqOptions=this.reqOptions || {};
	if (reqOptions.auth) {
		reqOptions.headers=reqOptions.headers || {};
		reqOptions.headers={"Authorization": "Basic "+(new Buffer(reqOptions.auth.user+":"+reqOptions.auth.password) ).toString("base64") };
		reqOptions=_.omit(reqOptions, "auth");
	}
*/
	var source = context.source || that.source
	if (!source) return callback(new Error("Source folder is not defined") );
	if (source.indexOf("$")==0) {
		var url=source.split("/");
		url.splice(0,1);
		url="/"+url.join("/");
	}
	console.log(url);
	glob(url, { root: config.ETL_ROOT_DIR, nodir:true }, function (er, files) {
		console.log(er || files)
		that.processor= that.processor || "var processor=function(){return true;}";
		var proc=that.processorInContext(context);
		if (!proc) 
		if (proc instanceof Error) return callback(proc || new Error("Predicate function is undefined for filter job "+that.name+"(id="+that.id+")"));
		proc = proc || function() { return true; };
		var glueStream = new Readable();
		var fileIndex=0;

		glueStream.getNext= function() {
			var stream = this;
			if (fileIndex>=files.length) {
				console.log("Finishing with index ", fileIndex, files);
				stream.push(null);
				return;
			}
			
			
			if (!files[fileIndex] || !proc.call( context, files[fileIndex] )) {
				console.log("get next file")
				fileIndex++;
				return stream.getNext();
			}

			var url="file:"+ files[fileIndex];// path.join(config.ETL_ROOT_DIR, files[fileIndex] ); //, that.this._schema.parseSourceUrlWithState(this.state);
			
			console.log(url);

			utils.getReadStreamByUrl(url, /* reqOptions, */ function(err, _stream) { 
				if (err) return stream.emit("error", e);
				_stream.on("data", function(chunk) {
					// console.log(chunk.toString("utf8"));
					  if (!stream.push(chunk))  stream._source.pause();
					})
					.on("error", (e) => {
						stream.emit("error", e); console.error(e);
					})
					.on("end", () => {
						stream._source = null;
						console.log("get next file")
						fileIndex++;
						stream.getNext();
					});
				stream._source=_stream;
			});
		}
		glueStream._read = function(size) {
			if (!this._source) {
				console.log("get next file"); 
				// fileIndex++; 
				this.getNext(); 
			}
			if (this._source) this._source.resume();
		}

		function parserStream(cb) {
			if (!that.parser) return cb();
			that.ensureDepts(function(e, parser) {
				if (e || !parser) return cb(e);
				parser.toStream(cb);
			});
		}

		var counter = pi.thru.obj(function (data, encoding, onDone) {
				this.push(data);
				if (data) {
					cntxt.state.rows++;
					cntxt.state.totalRowsProcessed++;
				}
				onDone();
		});

		parserStream(function(e, pars){
			if (e) return callback(e);
			var resStream=glueStream;
			if (pars) { 
				console.log("Parser ready")
				resStream=glueStream.pipe(pars);
			}
			if (spy) {
				console.log("Set paged spy");
				return  callback(null, resStream.pipe(Job.spyStream(spy)) );
			}
			return  callback(null, resStream);
		})
	});
}


aSchema.post('init', function(doc) {
 	doc.isWriteable=false;
	doc.isReadable=true;
});

aSchema.pre("save", function(next){
	this.isWriteable=false;
	this.isReadable=true;
	next();
});
module.exports = Job.discriminator('ReaderFolder', aSchema);