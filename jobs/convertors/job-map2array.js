var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var async = require("async");
var objectPath  =require("object-path");
var vm = require('vm');

var Job = require("../job.js");

function _stringSplitter(v) {
	if(_.isArray(v)) return v;
	if(!_.isString(v) || !v) return [];
	return v.split(",").map(function(a){ return a.trim() });
}
var aSchema = new Schema({
			fields:{type:String}, // set:_stringSplitter}],
			idKey:{type:String, default:"id", required:true},
			valueKey:{type:String, default:"value", required:true},
		}, { discriminatorKey: 'type' });

aSchema.methods.__stream=function(context, proc, callback) { 
	var that = this;
	var fields=_stringSplitter(that.fields);
	var combiner=pi.thru.obj(function (data, encoding, onDone) {
		(fields || []).forEach(function(f){
			var propValue=objectPath.get(data, f);
			if (_.isObject(propValue) && !_.isArray(propValue) ) {
				var buf=[];
				for (var key in propValue) {
					var r={}; 
					r[that.idKey]=key;
					r[that.valueKey]=propValue[key];
					buf.push(r);
				}
				objectPath.set(data, f, buf);
			}
		})
		this.push(data);
		onDone();
	} );
	return callback(null, combiner);
}
/*
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
		
		var fields=_stringSplitter(that.fields);
		var combiner=pi.thru.obj(function (data, encoding, onDone) {
			(fields || []).forEach(function(f){
				if (_.isObject(data[f]) && !_.isArray(data[f]) ) {
					var b=[];
					for (var key in data[f]) {
						var r={}; 
						r[that.idKey]=data[f][key];
						buf.push(r);
					}
					data[f]=buf;
				}
			})
		} );
		if (!spy)
			return callback(null, combiner);
		else 
			return callback(null, combiner.pipe(Job.spyStream(spy)));
}
*/
aSchema.post('init', function(doc) {
 	doc.isWriteable=true;
	doc.isReadable=true;
});

aSchema.pre("save", function(next){
	this.isWriteable=true;
	this.isReadable=true;
	next();
});

module.exports = Job.discriminator('Map2Array', aSchema);
