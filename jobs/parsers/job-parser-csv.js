var path = require("path");
var pi = require('pipe-iterators');
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var _ = require("underscore");
var iconv = require("iconv-lite");
var csv = require("fast-csv");

var Job = require("../job.js");

var aSchema = new Schema({
			encoding:{type:String},
			delimiter:{type:String, default:','},
			escape:{type:String, default:'"'},
			headers:{type:Boolean, default:false},
		}, { discriminatorKey: 'type' });

aSchema.methods.__stream=function(context, proc, callback) { 
	 var options=this.toObject();
	 	 options.quote='"';

	if (this.encoding) {
		return callback(null, pi.pipeline([iconv.decodeStream(this.encoding), csv.parse(options ) ] ) );
	} else { 
		return callback(null, csv.parse(options ) ); 
	}
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

module.exports = Job.discriminator('ParserCSV', aSchema);
