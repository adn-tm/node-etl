var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var saxStream = require('sax-stream');

var Job = require("../job.js");

var aSchema = new Schema({
			xmlNode:{type:String, required:true, default:"item"},
		}, { discriminatorKey: 'type' });


aSchema.methods.__stream=function(context, proc, callback) { 
	callback(null, saxStream({ tag: this.xmlNode } ) );
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

module.exports = Job.discriminator('ParserXML', aSchema);
