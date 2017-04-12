var path = require("path");
var pi = require('pipe-iterators');
var Schema = require('mongoose').Schema;
var _ = require("underscore");

var Job = require("../job.js");

var aSchema = new Schema({}, { discriminatorKey: 'type' });

aSchema.methods.__stream=function(context, proc, callback) {  
	if (!_.isFunction(proc))
		return callback(new Error ("Undefined Filtering predicate for"+this.name));
	return callback(null, pi.filter(function(obj) {
		 return proc.call(context, obj); 
	}) );
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

module.exports = Job.discriminator('Filter', aSchema);
