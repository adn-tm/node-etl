var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var async = require("async");
var objectPath  =require("object-path");
var vm = require('vm');

var Job = require("../job.js");

var aSchema = new Schema({
			
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
		data._buffer=[];
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
		if (context instanceof Error) return callback(context);
		var proc=that.processorInContext(context);
		if (proc instanceof Error || !proc) return callback(proc || new Error("Comparator function is undefined for sorter job "+that.name+"(id="+that.id+")"));

		var combiner=pi.thru.obj(function (doc, encoding, onDone) {
			data._buffer.push(doc);
			onDone();
		}, function(onDone) {
			var stream=this;
			data._buffer.sort(proc).forEach(function(a){ stream.push(a); });
			onDone();
		});
		if (spy) 
			return callback(null,  combiner.pipe(Job.spyStream(spy)) );
		return callback(null, combiner);
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

module.exports = Job.discriminator('Sorter', aSchema);