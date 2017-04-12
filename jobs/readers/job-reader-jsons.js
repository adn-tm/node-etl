var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var Buffer = require('buffer').Buffer;
var utils = require("../../utils.js");
var Job = require("../job.js");

var aSchema = new Schema({
			source:{type:String},
			reqOptions:{type:Schema.Types.Mixed, set:utils.tryParseSetter},
		}, { discriminatorKey: 'type' });


aSchema.methods.__stream=function(context, proc, callback) { 
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
		var tail="";
		var combiner=pi.thru.obj(function (data, encoding, onDone) {
			var stream=this;
			var p=tail+(data+"");
			var slices=p.split("\n");
			if (p.substr(p.length-1)!="\n") tail=slices.pop(); else tail="";
			slices.forEach(function(one) { 
				if (one)
					try {
						var a = JSON.parse(one);
						// console.log(a, typeof one);
						stream.push(a)
					} catch(e) {
						console.warn(one, e);
					}
			})
			onDone();
		});
		return callback(null, _stream.pipe(combiner) );
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

module.exports = Job.discriminator('ReaderJSONS', aSchema);
