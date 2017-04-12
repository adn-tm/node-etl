var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var Buffer = require('buffer').Buffer;
var utils = require("../../utils.js");

var Job = require("../job.js");

var aSchema = new Schema({
			destination:{type:String},
			reqOptions:{type:Schema.Types.Mixed, set:utils.tryParseSetter},
		}, { discriminatorKey: 'type' });


aSchema.methods.__stream=function(context, proc, callback) { 
	var reqOptions=context.reqOptions || this.reqOptions || {};
	if (reqOptions.auth) {
		reqOptions.headers=reqOptions.headers || {};
		reqOptions.headers={"Authorization": "Basic "+(new Buffer(reqOptions.auth.user+":"+reqOptions.auth.password) ).toString("base64") };
		reqOptions=_.omit(reqOptions, "auth");
	}

	utils.getWriteStreamByUrl(context.destination || this.destination, reqOptions, function(err, _stream) { 
		if (err || !_stream) {
			console.log("_getSources get stream error", err)
			return callback(err);
		}
		var combiner=pi.thru.obj(function (data, encoding, onDone) {
			// console.log("Writing", data);
			if (data!=undefined) 
				this.push(JSON.stringify(data).replace(/\n/g, " ")+"\n");
			onDone();
		});
		callback(null, pi.head(combiner, _stream) );
	});
}

aSchema.post('init', function(doc) {
 	doc.isWriteable=true;
	doc.isReadable=false;
});

aSchema.pre("save", function(next){
	this.isWriteable=true;
	this.isReadable=false;
	next();
});

module.exports = Job.discriminator('WriterJSONS', aSchema);
