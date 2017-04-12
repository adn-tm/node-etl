var path = require("path");
var pi = require('pipe-iterators');
var _ = require("underscore");
var utils = require("../utils.js");
var mongoose = require('mongoose'),
	Schema = mongoose.Schema;
var mongooseTemp = require('mongoose-temporary');
var uuid  = require("uuid");
const vm = require('vm');	

function _iToString(a) {
	if (_.isFunction(a)) return "var initer="+a.toString();
	return a;
}

function _pToString(a) {
	if (_.isFunction(a)) return "var processor="+a.toString();
	return a;
}

var aSchema =new Schema({
			name: { type: String, index:true },
			id: {type:String, index: true, default: uuid.v4 /*{ unique: true }*/ },
			initer:{type:String,  set:_iToString},
			processor:{type:String,  set:_pToString},

			isWriteable:{type: Schema.Types.Boolean},
			isReadable:{type: Schema.Types.Boolean},

			// ender: {type:String},
			args:{type:Schema.Types.Mixed, set:utils.tryParseSetter},
			
			//  type:{type: String, enum:["R", "J", "P", "F", "MR", "W"], required:true},
			// properties:{ type: Schema.Types.Mixed },
			// kind:{type: String, enum:["R", "J", "P", "F", "MR", "W"], required:true},
			// parent: {  type: Schema.Types.ObjectId, ref: 'Job' },
			// chain:[ {  type: Schema.Types.ObjectId, ref: 'Job' } ]
			_fns:{type: Schema.Types.Mixed, temporary:true},
	}, { discriminatorKey: 'type' });

	aSchema.plugin(mongooseTemp);

	aSchema.index()
aSchema
	.virtual('info')
	.get(function() {
		 return this.get('name');
	});
aSchema.statics.deserialize=function(desc) {
	var a = new this();
	for(var key in desc)
		a.set(key, desc[key]);
	if (!a.id) 
		a.id=uuid.v4().toLowerCase();
	a.id=a.id.toLowerCase();
	if (!a.id.match(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/) )
		a.id=uuid.v4().toLowerCase();
	return a;
}
aSchema.methods.serialize = function(){
	 // var a=_.pick(this.toObject(), ["name", "type"]); // , "isWriteable", "isReadable", "initer", "processor"
	 var a=_.omit(this.toObject(), ["_id", "_fns" , "isWriteable", "isReadable", "__v", "__t",  "created", "modified"]); //, "initer", "processor"
	 return a;
}


aSchema.methods.toStream=function(agrs, callback) { 
	if (!callback && agrs) {
		callback=agrs;
		agrs=false;
	}
	var that=this;
	var Job=this.constructor;
	var context;
	var data=this.agrs || {};
			agrs =  agrs || {};
			data = _.extend(data, agrs)
	
	var spy=agrs.spy && _.isFunction(agrs.spy[this.id])?agrs.spy[this.id]:null;
	
	if (agrs.isContext) {
		context= agrs;
		if (this.agrs)
			for(var key in this.agrs) 
				context[key]=this.agrs[key];
		context.initer= undefined;
		context.processor= undefined;
	} else { 
		context = new vm.createContext(data);
		context.isContext=true;
	}

	this.initWithContext(context);
	if (context instanceof Error)
		return callback(context);
	
	var proc=this.processorInContext(context);
	if (proc instanceof Error) return callback(proc || new Error("Predicate function is undefined for filter job "+this.name+" (id="+this.id+")"));
	if (!spy)
		this.__stream(context, proc, callback)
	else 
		this.__stream(context, proc, function(e, stream){
			if (e || !stream || !pi.isStream(stream)) {
				console.warn("Updefiined result stream for ", that.type, that.id);
				return  callback(e, stream);
			}
			if (that.isReadable) {
				if (that.isWriteable)
					return callback(e, pi.pipeline([stream, Job.spyStream(spy)]) );
				else {
					console.log(that.type, " as Readable, spy=", spy)
					return callback(e, stream.pipe(Job.spyStream(spy)) );
				}
			}
			return  callback(e, pi.pipeline([Job.spyStream(spy), stream]) );
		})
}

// Owerride this
aSchema.methods.__stream=function(context, proc, callback) {  
	callback(new Error("Method _stream is abstract for JobType="+this.type) );
}

aSchema.virtual('initFn').get(function() {
	this._fns = this._fns || {};
	if (this._fns.initer instanceof vm.Script || this._fns.initer instanceof  Error) return this._fns.initer;
	if (this.initer) {
		try {
			this._fns.initer=new vm.Script(this.initer)
		} catch(e) {
			this._fns.initer=e;
		}
	} else this._fns.initer=false;
	return this._fns.initer;
})

aSchema.virtual('processFn').get(function() {
	this._fns = this._fns || {};
	if (this._fns.processor instanceof vm.Script || this._fns.processor instanceof  Error) return this._fns.processor;
	// if (this.processor) {
		
		try {
			this._fns.processor=new vm.Script(this.processor); //  || "function processor(a){ return a; }"
		} catch(e) {
			console.error(e);
			this._fns.processor=e;
		}
	// } else this._fns.processor=false;
	return this._fns.processor;
})
/*
aSchema.virtual('enderFn').get(function() {
	if (this._fns.ender instanceof vm.Script || this._fns.ender instanceof  Error) return this._fns.ender;
	if (this.ender) {
		this._fns = this._fns || {};
		try {
			this._fns.ender=new vm.Script(this.initer)
		} catch(e) {
			this._fns.ender=e;
		}
	} else this._fns.ender=false;
	return this._fns.ender;
});
*/

aSchema.methods.processorInContext=function(context) {
//	console.log(this.constructor.modelName, this.name, context);
	var processor=this.processFn;
	if (processor instanceof Error || !processor) 
	 	return processor || new Error("Process function is undefined for job "+this.name+"(id="+this.id+")");
	if (processor) {
		
 		processor.runInContext(context);
 	}
  	if (_.isFunction(context.processor) )
  		return context.processor;
  	return false; // function(a){ return a }; // new Error("Process is not a function for job "+this.name+"(id="+this.id+")")
}


aSchema.methods.getLinkedJobs=function(parent, callback) {
	if (parent && !callback) {callback =parent; parent = null;  };
	var a=this.serialize(); a.parent=parent;
	return callback(null, a); //  links:[], children
}

aSchema.methods.initWithContext=function(context) {
	var initer=this.initFn;
	
	if (initer instanceof Error) 
		return initer;
	if (!initer) return;
	console.log(this.constructor.modelName, this.name, initer);
 	initer.runInContext(context);
  	if (_.isFunction(context.initer) ) {
  	  		context.initer.call(context, context);
  	  		// console.log("Inited context", context);
  	}
  	return context;
}

aSchema.statics.spyStream=function(spy) {
	var i=0;
	return pi.thru.obj(function(data, encoding, cb){
		this.push(data);
		i++;
		spy(data, i);
		cb();
	});
}


aSchema.pre("save", function(next){
	if (!this.id) 
		this.id=uuid.v4().toLowerCase();
	this.id=this.id.toLowerCase();
	if (!this.id.match(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/) )
		this.id=uuid.v4().toLowerCase();
	next();
});

module.exports = mongoose.model('Job', aSchema);
