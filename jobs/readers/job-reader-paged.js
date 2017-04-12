var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");

var utils = require("../../utils.js");
var Buffer = require('buffer').Buffer;
var Job = require("../job.js");
var Readable = require('stream').Readable;

var vm = require("vm");

function _idOrString(v) {
	if(_.isString(v)) return v;
	return v.id || v;
}

var aSchema = new Schema({
			source:{type:String},
			reqOptions:{type:Schema.Types.Mixed, set:utils.tryParseSetter},
			parser:  {  type: String, set:_idOrString }, // { type: Schema.Types.ObjectId, ref: 'Job'/*, required:true */},
		}, { discriminatorKey: 'type' });

function urlTplProcessor(template, state) { 
		state = state || {};
		var page=state.page || 0;
		var listLimit=100;
		state.offset=state.from=listLimit*page
		state.to=listLimit*(page+1);
		state.limit=listLimit;
		state.since=(state.since || "");
		// var url="https://all.culture.ru/api/2.2/organizations?type=mincult&offset={%offset%}&limit={%limit%}"; //  || "";
		var url= template || "";
		for(var key in state) url=url.replace("{%"+key+"%}", state[key]); 
		return url;
}
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
	// console.log("this.parser=", this.parser);
	if (!this[prop]) return cb();
	Job.findOne({id:this[prop]}, cb);
}
aSchema.methods.toStream=function(agrs, callback) { 
	if (!callback && agrs) {
		callback=agrs;
		agrs=false;
	}

	var that=this,
		agrs =  agrs || {};
	// console.log(this.name, agrs);
	var spy=(agrs.spy && _.isFunction(agrs.spy[this.id]))?agrs.spy[this.id]:null;
	var cntxt=_.extend(this.agrs || {}, agrs);
		cntxt.state= cntxt.state || {}
		cntxt.state[that.id]={lastPageRows:-1, rows:-1, page:0, totalRowsProcessed:0, totalRows:-1, totalPages:-1, url:""};
		
	// console.log("agrs.spy=", agrs.spy, this.id)

	var context;
	if (agrs.isContext) {
		context= agrs;
		if (this.agrs)
			for(var key in cntxt) 
				context[key]=this.agrs[key];
	} else { 
		context = new vm.createContext(cntxt);
		context.isContext=true;
	}

	// var context = new vm.createContext(cntxt);
	// context.isContext=true;

	that.initWithContext(context);
	that.processor= that.processor || "var processor="+urlTplProcessor.toString();
	var proc=that.processorInContext(context);
	if (proc instanceof Error) return callback(proc || new Error("Predicate function is undefined for filter job "+that.name+"(id="+that.id+")"));

	var reqOptions=this.reqOptions || {};
	if (reqOptions.auth) {
		reqOptions.headers=reqOptions.headers || {};
		reqOptions.headers={"Authorization": "Basic "+(new Buffer(reqOptions.auth.user+":"+reqOptions.auth.password) ).toString("base64") };
		reqOptions=_.omit(reqOptions, "auth");
	}

	var Glue = new Readable();
	Glue.getLength=function(){ return 0; };
	Glue.getNext= function() {
		var stream = this;
		var urlTpl=(context.source || that.source);
		cntxt.state[that.id].url=proc.call(context, context.source || that.source, cntxt.state[that.id]); //, that.this._schema.parseSourceUrlWithState(this.state);
		if (!cntxt.state[that.id].url) {
				console.log("Finishing with state", cntxt.state[that.id]);
			stream.push(null);
			cntxt.state[that.id].lastPageRows=cntxt.state[that.id].rows;
			cntxt.state[that.id].rows=0;
			return;
		}
		// console.log("Locading", cntxt.state[that.id]);
		if (cntxt.state[that.id].rows>=0) cntxt.state[that.id].lastPageRows=cntxt.state[that.id].rows;
		cntxt.state[that.id].rows=0;

		utils.getReadStreamByUrl(cntxt.state[that.id].url, reqOptions, function(err, _stream){ 
			if (err) return stream.emit("error", err);
			_stream.on("data", function(chunk) {
				// 
					// if (cntxt.state[that.id].organizations==4924)  console.log(chunk.toString("utf8"));
				 	if (!stream.push(chunk))  stream._source.pause();
				})
				.on("error", (e) => {
					cntxt.state[that.id].rows=0;
					stream.emit("error", e); console.error(e);
				})
				.on("end", () => {
					cntxt.state[that.id].page++;
					stream._source = null;
					

					// console.log(cntxt.url+" finished", cntxt.state[that.id].totalRowsProcessed, "/", cntxt.state[that.id].totalRows);
					// if (cntxt.state[that.id].organizations==4924) console.log("Page finished with state", cntxt.state);
					if ((cntxt.state[that.id].rows && cntxt.state[that.id].lastPageRows) || 
						(cntxt.state[that.id].totalRows>0 && cntxt.state[that.id].totalRows>cntxt.state[that.id].totalRowsProcessed) ) {
						stream.getNext();
					} else {
						setTimeout(function(){
							if ((cntxt.state[that.id].rows && cntxt.state[that.id].lastPageRows) || (cntxt.state[that.id].totalRows>0 && cntxt.state[that.id].totalRows>cntxt.state[that.id].totalRowsProcessed) ) {
								stream.getNext();
							} else {
								console.log(cntxt.state);
								stream.push(null);		
							}
						}, 1000) 
						
					}
				});
			stream._source=_stream;
		});
	}
	Glue._read = function(size) {
		if (!this._source) {
				this.getNext();
		}
		if (this._source) this._source.resume();
	}
	function parserStream(cb) {
		if (!that.parser) return cb();

		that.ensureDepts(function(e, parser){
			if (e || !parser) {
				console.warn(that.id, " parser not found", e);
			 return cb(e);
			}
			// console.log(that.id, " parser ", parser.id);
			parser.toStream(context, cb);
		})
	}

	var counter = pi.thru.obj(function (data, encoding, onDone) {
			if (data) {
				cntxt.state[that.id].rows++;
				cntxt.state[that.id].totalRowsProcessed++;
				// console.log("counting", cntxt.state[that.id].rows, data.toString("utf8").substr(0, 50));
			}
			this.push(data);
			onDone();
	});

	parserStream(function(e, pars){
		if (e) return callback(e);
		var resStream;
		if (pars) {
			resStream=Glue.pipe(pars).pipe(counter);
		} else 
			resStream=Glue.pipe(counter);
		if (spy) {
			console.log("Set paged spy");
			return  callback(null, resStream.pipe(Job.spyStream(spy)) );
		}
		return  callback(null, resStream);
	})
}


aSchema.post('init', function(doc) {
 	doc.isWriteable=false;
	doc.isReadable=true;
	
	if (!doc.processor)
		doc.processor="var processor="+urlTplProcessor.toString();

});

aSchema.pre("save", function(next){
	this.isWriteable=false;
	this.isReadable=true;
	next();
});
module.exports = Job.discriminator('ReaderPaged', aSchema);