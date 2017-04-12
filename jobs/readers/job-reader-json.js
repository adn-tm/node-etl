var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var JSONStream = require("jsonstream2");
var utils = require("../../utils.js");
var Buffer = require('buffer').Buffer;
var Job = require("../job.js");

var aSchema = new Schema({
			source:{type:String},
			reqOptions:{type:Schema.Types.Mixed, set:utils.tryParseSetter},
			rootNode:{type:String, required:true, default:"*"},
		}, { discriminatorKey: 'type' });


aSchema.methods.__stream=function(context, proc, callback) { 
	var that=this;
	var reqOptions=context.reqOptions || this.reqOptions || {};
	if (reqOptions.auth) {
		reqOptions.headers=reqOptions.headers || {};
		reqOptions.headers={"Authorization": "Basic "+(new Buffer(reqOptions.auth.user+":"+reqOptions.auth.password) ).toString("base64") };
		reqOptions=_.omit(reqOptions, "auth");
	}

	utils.getReadStreamByUrl(context.source || this.source, reqOptions, function(err, _stream) { 
		if (err || !_stream) {
			console.log("_getSources get stream error", err)
			return callback(err);
		}
		var parser=JSONStream.parse(that.rootNode)
		callback(null, _stream.pipe(parser) );
	});
}

aSchema.post('init', function(doc) {
 	doc.isWriteable=false;
	doc.isReadable=true;
});

aSchema.pre("save", function(next){
	this.isWriteable=false;
	this.isReadable=true;
	next();
});

module.exports = Job.discriminator('ReaderJSON', aSchema);
