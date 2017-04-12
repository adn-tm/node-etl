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
		}, { discriminatorKey: 'type' });

aSchema.methods.__stream=function(context, proc, callback) { 
	var that = this;
	var fields=_stringSplitter(that.fields);
	var combiner=pi.thru.obj(function (data, encoding, onDone) {
		(fields || []).forEach(function(f){
			var propValue=objectPath.get(data, f);
			if (_.isArray(propValue) ) {
				var buf={};
				propValue.forEach(function(one){
					if (!_.isObject(one)) return;
					var id=one[that.idKey];
					if (id!=undefined) {
						var val=_.omit(one, that.idKey);
						var keys=Object.keys(val);
						
						if (keys.length==0) buf[id]=null
						else 
						if (keys.length==1)
							buf[id]=val[keys[0]]
						else 
							buf[id]=val
					}
				});
				objectPath.set(data, f, buf);
			};// else console.log("data["+f+"]=", data[f]);
		})
		this.push(data);
		onDone();
	} );
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

module.exports = Job.discriminator('Array2Map', aSchema);
