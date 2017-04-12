var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");


var Job = require("../job.js");

var aSchema = new Schema({
			
		}, { discriminatorKey: 'type' });


aSchema.methods.__stream=function(context, proc, callback) {  
	var pushed=[];
	var combiner=pi.thru.obj(function (data, encoding, onDone) {
		var hash=proc.call(context, data);
		if (pushed.indexOf(hash)<0) {
			pushed.push(hash);
			this.push(data);
		};
		onDone();
	});
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

module.exports = Job.discriminator('Uniquer', aSchema);
