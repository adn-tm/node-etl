var path = require("path");
var pi = require('pipe-iterators');
var Schema = require('mongoose').Schema;
var _ = require("underscore");

var Job = require("./job.js");

var aSchema = new Schema({}, { discriminatorKey: 'type' });

aSchema.methods.__stream=function(context, proc, callback) {  
	return callback(null, pi.thru.obj(function (data, encoding, onDone) { onDone(); }) );
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

module.exports = Job.discriminator('Nothing', aSchema);
