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
			childrenKey:{type:String, default:"children", required:true},
			parentKey:{type:String, default:"parent"},
			idKeyPath:{type:String, default:"id"},
			levelKey:{type:String},
			mpathKey:{type:String},
			mpathKeySeparator:{type:String, default:"#"},
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
		data.maps={};
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

//		var proc=that.processorInContext(context);
//		if (proc instanceof Error || !proc) return callback(proc || new Error("Reduce function is undefined for reducer job "+that.name+"(id="+that._id+")"));
		
		function putOne(stream, data, parent, level) {
			var a=_.omit(data, that.childrenKey);
			
			a[that.parentKey]=parent?objectPath.get(parent, that.idKeyPath):undefined;
			if (that.levelKey)
				a[that.levelKey]=level;
			if(that.mpathKey)
				a[that.mpathKey]=(parent ? (parent[that.mpathKey]+that.mpathKeySeparator):"") + objectPath.get(a, that.idKeyPath);
			stream.push(a);
			if (_.isArray(data[that.childrenKey]) )
				data[that.childrenKey].forEach(function(ch) {
					putOne(stream, ch, a, level+1);
				})
			
		}
		var buffer=[]
		var combiner=pi.thru.obj(function (data, encoding, onDone) {
			buffer.push(data+"");
			onDone();
		}, function(onDone){
			try {
				var data=JSON.parse(buffer.join(""));
				putOne(this, data, null, 0);
			} catch(e) {
				console.error(e);
				return onDone(e);
			}
			onDone();
		});

		if (spy) 
			return callback(null,  pi.pipeline([combiner, Job.spyStream(spy)]) );
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

module.exports = Job.discriminator('ParserJSONTree', aSchema);
