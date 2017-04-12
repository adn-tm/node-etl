var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var JSONStream = require("jsonstream2");

var Job = require("../job.js");

var aSchema = new Schema({
			rootNode:{type:String, required:true, default:"*"},
		}, { discriminatorKey: 'type' });


aSchema.methods.__stream=function(context, proc, callback) { 
	if (!proc)
		return callback(null, JSONStream.parse(this.rootNode) );
	return callback(null, pi.pipeline(JSONStream.parse(this.rootNode), pi.map(function(obj) { 
		return proc.call(context, obj); 
	}) ) );
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

module.exports = Job.discriminator('ParserJSON', aSchema);
