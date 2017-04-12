var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var JSONStream = require("jsonstream2");

var Job = require("../job.js");

var aSchema = new Schema({
		}, { discriminatorKey: 'type' });


aSchema.methods.__stream=function(context, proc, callback) { 
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

module.exports = Job.discriminator('ParserJSONS', aSchema);
