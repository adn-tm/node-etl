var mongoose = require('mongoose');
var async = require("async");
var _ = require("underscore");
var path = require("path");
var fs=  require("fs-extra");

var J=require("../jobs");




// Connect to database
mongoose.connect('mongodb://localhost/examples', { db: { safe: true} } );
mongoose.connection.on('error', function(err) {
	console.error('MongoDB connection error: ' + err);
	process.exit(-1);
});


function _sources(runParams, cb) {
	cb = cb || function() {};
	console.log("_sources")
	var jVocReader  = new J.ReaderJSON({name:"Read Stat sources", source:"$LOCALFS/DWC/sources.json" });
	
	var jParser=new J.ParserJSON({name:"JSON data.* parser", rootNode:"data.*"});
	var jReader  = new J.ReaderPaged({name:"Read subordinates", 
		source:"https://esb.mkrf.ru/cdm/v2/subordinates?offset={%offset%}&limit={%limit%}", reqOptions:{auth:{user:"opendata", password:"tGXK4zSn"}}, 
		 parser:jParser});
	var jMapper  = new J.Mapper({name:"Cleanup", processor:function(a){  return a.general; } });
	var jJoiner  = new J.Joiner({name:"Joiner with sources map",
							vocabs:[{ job: jVocReader.id, idPath:"aname", key:"source"}],
							processor:function (a){  
								if (a && a.kopuk && this.maps && this.maps.source) {
									var dwc=this.maps.source[a.kopuk];
									a.mapping=a.mapping || [];
									if (dwc) {
										a.mapping.push({src:"DWC", id:dwc.vernum, text:dwc.adesc || dwc.aname});
									}
								}
								a.parent="0";
								a.id=a.inn;
								return a;
							}
						});
	var jThru = new J.Thru({name:"Add parent node", processor:function(stream, data, onReady) { 
		if (!this.root) { 
			this.root={ "name": "Подведомственные учреждения", "key": ["subordinate","category_institutions","subordinate"], "id":"0"};
			stream.push(this.root);
		} 
		if (!this.ci) { this.ci={}; this.ciIndex=0; }
		var ciKey;
		if (!this.ci[data.category_institutions]) {
			var ciKey="ci"+this.ciIndex;
			this.ciIndex++;
			this.ci[data.category_institutions]={id:ciKey, name:data.category_institutions, parent:this.root.id };
			stream.push(this.ci[data.category_institutions]);
		} else 	ciKey=this.ci[data.category_institutions].id;
		data.parent=ciKey;
		data.ciName=data.category_institutions;
		data.category_institutions=ciKey;
		stream.push(data);
		onReady();
	} });

	var jWriter  = new J.WriterJSONTree({name:"Write Sources Dim", destination:"$LOCALFS/temp/dim-sources.json" });

	var main=new J.Pipeline({name:"0. BOR+Stat subordinates", chain:[jReader, jMapper, jJoiner, jThru, jWriter ] });
	console.log(main.name);
	var jobs=[jVocReader, jParser, jReader, jJoiner, jMapper, jWriter,jThru,  main]; 

	var spy={};
	var i=0;
	spy[jReader.id]=function(data) {
		console.log("Spy: ", i, data);
		i++;
	}
	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			main.run(runParams, function() { async.each(jobs, function(a, next) {a.remove(next)}, function(err) { cb(); } ) });
		} else cb(err, main);
	})
}

function _dwcFacts(runParams, cb) {
	cb = cb || function() {};
	var jSrcReader  = new J.Reader({name:"Read Stat sources", source:"$LOCALFS/dims/sources.json" });
	var jSrcTreeParser  = new J.ParserJSONTree({name:"Parse Sources tree" });
	
	// var jSrcMapper  = new J.Mapper({name:"Cleanup src", processor:function(a){ return {id: a.id, dwcId:a.dwcId, inn:a.inn, ci:a.category_institutions }; } });
	var jSrcThru = new J.Thru({name:"Add parent node", processor:function(stream, data, onReady) { 
			if (data && data.mapping && data.mapping.forEach) {
				data.mapping.forEach(function(a){
					if (a.src=="DWC")
						stream.push({id: data.id, dwcId:String(a.id), inn:data.inn, ci:data.category_institutions })
				});
			}
			onReady();
		}
	});	
	var jVocPipeline=new J.Pipeline({name:"Sources", chain:[jSrcReader, jSrcTreeParser, jSrcThru] }); // jSrcMapper
	
	

	var jParser = new J.ParserJSONS({name:"JSONS parser"});
	var jReader = new J.ReaderFolder({name:"Read Stat facts", parser:jParser,	
		 // source:"$LOCALFS/DWC/2014/*.jsons",
		initer:function() { 
				this.year=this.year || "2016";
				this.source="$LOCALFS/DWC/"+this.year+"/*.jsons"
				return this;
		}  });
	//  var jReader = new J.ReaderJSONS({name:"Read Stat facts", source:"$LOCALFS/DWC/2014/data-12.jsons" });

	var jJoiner  = new J.Joiner({name:"Joiner with sources map",
						vocabs:[{ job: jVocPipeline.id, idPath:"dwcId", key:"source"}],
						processor:function(a) {
							if (a) {
								var sourceId= String(a.sourceId || a.src_VERNUM);
								if (sourceId && this.maps && this.maps.source) {
									var subordinate=this.maps.source[sourceId];
									a.subordinate=subordinate?(subordinate.inn || subordinate):null;
									a.category_institutions=subordinate?subordinate.ci:null;
								}
							}
							return a;
						}
	});
	
	var jMapper  = new J.Mapper({name:"Cleanup", processor:function(a){ 
			if (a) {
				a.key="f_"+a.ffo_VERNUM+"_p_"+a.fpa_VERNUM+"_r_"+a.fro_VERNUM+"_g_"+a.fgr_VERNUM;
				a.id=a.src_VERNUM=a.fro_VERNUM=a.fgr_VERNUM=a.seq_NUMBER=a.ffo_VERNUM=a.fpa_VERNUM=undefined;
			}
			return a;
			}
		});
	var jFilter  = new J.Filter({name:"Remove non-subords", processor:function(a){ return !!a.subordinate && (a.key in a);	} });

	var jWriter  = new J.WriterJSONS({name:"Write Stat facts", 
		// destination:"$LOCALFS/temp/stat-2014.jsons",
		initer:function() { 
				this.year=this.year || "2016";
				this.destination="$LOCALFS/temp/"+this.year+"-stat.jsons"
				return this;
			}  });

	var main=new J.Pipeline({name:"1. Stat Facts", chain:[jReader, jJoiner,  jMapper, jFilter,  jWriter ] });
	jVocPipeline.mainPipe=main;
	var jobs=[jSrcReader, jSrcTreeParser, jParser, jReader, jJoiner, jMapper, jFilter, jWriter, main, jVocPipeline, jSrcThru]; // jSrcMapper

	
	var i=0;
	var spy={};
	 spy[jVocPipeline.id]=function(data) {
		// if (i==0) console.log(this.maps);
		// if ((i% 100)==0) 
			console.log("Spy: ", i, data);
		i++;
	}

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			console.log("Running with", runParams)
			// jVocPipeline.run({ spy:spy }, cb)
			main.run(runParams, function() { async.each(jobs, function(a, next) {a.remove(next)}, function(err) { cb(); } ) });
		} else cb(err, main);
	})

}
function _BORFacts(runParams, cb) {
	cb = cb || function() {};
	
	var jParser=new J.ParserJSON({name:"JSON data.* parser", rootNode:"data.*"});
	var jReader  = new J.ReaderPaged({name:"Read subordinates", 
			// source:"https://esb.mkrf.ru/cdm/v2/subordinates?year=2016&quartal=1&offset={%offset%}&limit={%limit%}", 
			initer:function() { 
				this.year=this.year || "2016";
				this.quarter=this.quarter || "4";
				this.source="https://esb.mkrf.ru/cdm/v2/subordinates?year="+this.year+"&quartal="+this.quarter+"&offset={%offset%}&limit={%limit%}";
				return this;
			},
			reqOptions:{auth:{user:"opendata", password:"tGXK4zSn"}}, 
		 parser:jParser.id});

	// ci pipeline
	var jSrcReader  = new J.Reader({name:"Read Stat sources", source:"$LOCALFS/dims/sources.json" });
	var jSrcTreeParser  = new J.ParserJSONTree({name:"Parse Sources tree" });
	var jSrcMapper  = new J.Mapper({name:"Cleanup src", processor:function(a){ return {id: a.id, inn:a.inn, ci:a.category_institutions }; } });
	var jVocPipeline=new J.Pipeline({name:"Sources", chain:[jSrcReader, jSrcTreeParser, jSrcMapper ] });

	var jJoiner  = new J.Joiner({name:"Joiner with sources map",
							vocabs:[{ job: jVocPipeline.id, idPath:"inn", key:"source"}],
							processor:function (a){  
								if (a) {
									var sourceId= String(a.subordinate);
									if (sourceId && this.maps && this.maps.source) {
										var subordinate=this.maps.source[sourceId];
										a.category_institutions=subordinate?subordinate.ci:null;
									}
								}
								return a;
							}
						});

	// data pipeline
	var jMapper  = new J.Mapper({name:"Bor data", processor:function(a){  
		
		var year = this.year || "2016";
		var quarter = this.quarter || "4";
		var datetime = this.datetime || (year+"-"+(quarter*3)+"-30T20:59:00Z");
		
		var general = a.general || {};
		var result={subordinate:general.inn, date:datetime};

		if (general["bor-statistic"] && general["bor-statistic"]["management-report"]) {
			var mng=general["bor-statistic"]["management-report"];
			result.type=mng.type || "0";
			var prefix="y_"+year+"__"; 
			
			for(var key in mng) {
				var parts=key.split("_");
				if (parts.length==2) {
					var aType=parts[0];
					var aCode=Number(parts[1]);
					if (typeof mng[key] == "number")
						result[prefix+"t_"+mng.type+"__"+key]=mng[key];
					else if (typeof mng[key] == "object") {
						result[prefix+"t_"+mng.type+"__"+key+"__plan"]=mng[key].plan;
						result[prefix+"t_"+mng.type+"__"+key+"__fact"]=mng[key].fact;
					}
				} 
			}
			
			if (mng["otr-indexes"] && mng["otr-indexes"].length) {
				var otr=mng["otr-indexes"]
				for(var i=0; i<otr.length; i++) {
					var key=prefix+"t_"+mng.type+"__otr_"+otr[i].id;
					if (otr[i].value) {
						result[key]=otr[i].value;
					} else {
						result[key+"__plan"]=otr[i].plan;
						result[key+"__fact"]=otr[i].fact;
					}
				}
			} 
		}
		return result;
	} });
	var jFilter  = new J.Filter({name:"Remove empty vals", processor:function(a){ return ("type" in a);	} }); // 
	var jWriter  = new J.WriterJSONS({name:"Write Stat facts", 
			// destination:"$LOCALFS/temp/bor-2016-1.jsons",
			initer:function() { 
				this.year=this.year || "2016";
				this.quarter=this.quarter || "4";
				this.destination="$LOCALFS/temp/"+this.year+"-"+this.quarter+"-bor.jsons"
				return this;
			} });
/*
	// msrs pipeline
	var jThru = new J.Thru({name:"mng_XXX measures", processor:function(stream, a, onReady) { 
			for(var key in a) {
				var parts=key.split("__");
				if (parts.length>1) {
					while (parts.length) {
						var id=parts.join("__");
						parts.pop();
						var parent=parts.join("__");
						stream.push({id:id, name:id, parent:parent});
					}
				}					
			};
			onReady();
	}});

	var jMsrsUniquer =  new J.Uniquer({name:"Unique measures", processor: function (a) { return a.id; } });
	var jMsrsWriter = new J.WriterJSONTree({name:"Write Measures Dim", //  destination:"$LOCALFS/measures/bor.json" });
			initer:function() { 
				this.year=this.year || "2016";
				this.destination="$LOCALFS/measures/bor-"+this.year+".json"
				return this;
			} });

	
	var jSubPipe=new J.Pipeline({name:"BOR msrs", chain:[ jThru, jMsrsUniquer, jMsrsWriter ] });


	var jSplitter  = new J.Splitter({name:"Splitter", receivers:[jWriter, jSubPipe ] });

	var main=new J.Pipeline({name:"2. BOR facts", chain:[jReader, jMapper, jJoiner, jFilter, jSplitter ] }); // 

	jSubPipe.mainPipe = main;	
	*/
	var main=new J.Pipeline({name:"2. BOR facts", chain:[jReader, jMapper, jJoiner, jFilter, jWriter ] }); 
	jVocPipeline.mainPipe = main;

	var jobs=[jParser, jReader, jMapper, jWriter, jFilter, /* jSplitter, jSubPipe, jThru, jMsrsUniquer, jMsrsWriter, */ main, jJoiner, jVocPipeline, jSrcReader, jSrcTreeParser, jSrcMapper]; 

	var spy={};
	var i=0;
	 spy[jWriter.id]=function(data) {
		// if (i==0) console.log(this.maps);
			console.log("Spy: ", data);
		i++;
	}

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			main.run(runParams, function() { async.each(jobs, function(a, next) {a.remove(next)}, function(err) { cb(); } ) });
		}
		else cb(err, main);
	})
}

function _BORFactsOnly(runParams, cb) {
	cb = cb || function() {};
	
	var jParser=new J.ParserJSON({name:"JSON data.* parser", rootNode:"data.*"});
	var jReader  = new J.ReaderPaged({name:"Read subordinates", 
			// source:"https://esb.mkrf.ru/cdm/v2/subordinates?year=2016&quartal=1&offset={%offset%}&limit={%limit%}", 
			initer:function() { 
				this.year=this.year || "2016";
				this.quarter=this.quarter || "4";
				this.source="https://esb.mkrf.ru/cdm/v2/subordinates?year="+this.year+"&quartal="+this.quarter+"&offset={%offset%}&limit={%limit%}";
				return this;
			},
			reqOptions:{auth:{user:"opendata", password:"tGXK4zSn"}}, 
		 parser:jParser.id});

	// ci pipeline
	var jSrcReader  = new J.Reader({name:"Read Stat sources", source:"$LOCALFS/dims/sources.json" });
	var jSrcTreeParser  = new J.ParserJSONTree({name:"Parse Sources tree" });
	var jSrcMapper  = new J.Mapper({name:"Cleanup src", processor:function(a){ return {id: a.id, inn:a.inn, ci:a.category_institutions }; } });
	var jVocPipeline=new J.Pipeline({name:"Sources", chain:[jSrcReader, jSrcTreeParser, jSrcMapper ] });

	var jJoiner  = new J.Joiner({name:"Joiner with sources map",
							vocabs:[{ job: jVocPipeline.id, idPath:"inn", key:"source"}],
							processor:function (a){  
								if (a) {
									var sourceId= String(a.subordinate);
									if (sourceId && this.maps && this.maps.source) {
										var subordinate=this.maps.source[sourceId];
										a.category_institutions=subordinate?subordinate.ci:null;
									}
								}
								return a;
							}
						});

	// data pipeline
	var jMapper  = new J.Mapper({name:"Bor data", processor:function(a){  
		
		var year = this.year || "2016";
		var quarter = this.quarter || 4;
		var datetime = this.datetime || (year+"-"+(quarter*3)+"-30T20:59:00Z");
		
		var general = a.general || {};
		var result={subordinate:general.inn, date:datetime};

		if (general["bor-statistic"] && general["bor-statistic"]["management-report"]) {
			var mng=general["bor-statistic"]["management-report"];
			result.type=mng.type || "0";
			var prefix="y_"+year+"__"; 
			
			for(var key in mng) {
				var parts=key.split("_");
				if (parts.length==2) {
					var aType=parts[0];
					var aCode=Number(parts[1]);
					if ( (aType=="mng")  && (aCode>510) && (aCode<520)) {
						if (typeof mng[key] == "number")
							result[prefix+"t_"+mng.type+"__"+key]=mng[key];
						else if (typeof mng[key] == "object") {
							result[prefix+"t_"+mng.type+"__"+key+"__plan"]=mng[key].plan;
							result[prefix+"t_"+mng.type+"__"+key+"__fact"]=mng[key].fact;
						}
					} else {
						if (typeof mng[key] == "number")
							result[prefix+key]=mng[key];
						else  if (typeof mng[key] == "object") {
							result[prefix+key+"__plan"]=mng[key].plan;
							result[prefix+key+"__fact"]=mng[key].fact;
						}
					}
				} 
			}
			
			if (mng["otr-indexes"] && mng["otr-indexes"].length) {
				var otr=mng["otr-indexes"]
				for(var i=0; i<otr.length; i++) {
					var key=prefix+"t_"+mng.type+"__otr_"+otr[i].id;
					if (otr[i].value) {
						result[key]=otr[i].value;
					} else {
						result[key+"__plan"]=otr[i].plan;
						result[key+"__fact"]=otr[i].fact;
					}
				}
			} 
		}
		return result;
	} });
	var jFilter  = new J.Filter({name:"Remove empty vals", processor:function(a){ return ("type" in a);	} }); // 
	var jWriter  = new J.WriterJSONS({name:"Write Stat facts", 
			// destination:"$LOCALFS/temp/bor-2016-1.jsons",
			initer:function() { 
				this.year=this.year || "2016";
				this.quarter=this.quarter || "4";
				this.destination="$LOCALFS/temp/"+this.year+"-"+this.quarter+"-bor.jsons"
				return this;
			} });
	var main=new J.Pipeline({name:"2. BOR facts", chain:[jReader, jMapper, jJoiner, jFilter, jWriter ] }); // 

	//jSubPipe.mainPipe = main;	
	jVocPipeline.mainPipe = main;

	var jobs=[jParser, jReader, jMapper, jWriter, jFilter, main, jJoiner, jVocPipeline, jSrcReader, jSrcTreeParser, jSrcMapper]; 

	var spy={};
	var i=0;
	 spy[jWriter.id]=function(data) {
		// if (i==0) console.log(this.maps);
			console.log("Spy: ", data);
		i++;
	}

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			main.run(runParams, function() { async.each(jobs, function(a, next) {a.remove(next)}, function(err) { cb(); } ) });
		}
		else cb(err, main);
	})
}

function _BORAxis1(runParams, cb) {
	cb = cb || function() {};
	
	var jParser=new J.ParserJSON({name:"JSON data.* parser", rootNode:"mng.pay_types.*", processor:function(a){ a.year=this.year; return a; }});
	var jReader  = new J.ReaderPaged({name:"Read subordinates", 
			// source:"https://esb.mkrf.ru/cdm/v2/subordinates?year=2016&quartal=1&offset={%offset%}&limit={%limit%}", 
			initer:function() { 
				this.fromYear=this.fromYear || 2016;
				this.toYear=this.toYear || 2016;
				this.quarter=4;
				this.source="http://api.bor.mkrf.ru/mng/mng/?year={%year%}&quartal=4&key=9c7Fgrv4MKRFtZ";
				return this;
			}, 
			processor:function(template, state) { 
					if (!this.year) this.year=this.fromYear;
					else this.year++;
					if (this.year>this.toYear) 
						return null;
					var url=template;
					for(var key in state) url=url.replace("{%year%}", this.year); 
					return url;
			},
		 parser:jParser.id});
	var jThru = new J.Thru({name:"mng_XXX measures", processor:function(stream, a, onReady) { 
		stream.push({
    		id:"#",
    		name:"БОР. Управленческая отчетность"
    	});
    	var key="y_"+a.year;
    	stream.push({
    		id:key,
    		name:a.year,
    		parent:"#"
    	});
    	var p=key;
    	key=key+"__t_"+a.type_id;
    	stream.push({
    		id:key,
    		name:key+"_"+a.type_name,
    		parent:p
    	});
    	if (a.pay_sources && a.pay_sources.forEach)
    		a.pay_sources.forEach(function(b){
    			stream.push({
		    		id:key+"__"+b.code,
		    		name:key+"__"+b.code+"_"+b.name,
		    		parent:key
		    	});
		    	stream.push({
		    		id:key+"__"+b.code+"__plan",
		    		name:key+"__"+b.code+"__plan", // b.code+"/"+b.name+" (план)",
		    		parent:key+"__"+b.code
		    	});
		    	stream.push({
		    		id:key+"__"+b.code+"__fact",
		    		name:key+"__"+b.code+"__fact", // b.code+"/"+b.name+" (факт)",
		    		parent:key+"__"+b.code
		    	});
    		})
    	onReady();
	} });
	var jMsrsUniquer =  new J.Uniquer({name:"Unique measures", processor: function (a) { return a.id; } });
	var jWriter  = new J.WriterJSONTree({name:"Write Bor MNG/KOSGU measures", 
			// destination:"$LOCALFS/temp/bor-2016-1.jsons",
			initer:function() { 
				this.destination="$LOCALFS/temp/axis-bor.json"
				return this;
			} });
	var main=new J.Pipeline({name:"2. BOR MNG measures", chain:[jReader, jThru, jMsrsUniquer, jWriter ] }); // 

	var jobs=[jParser, jReader, jMsrsUniquer, jThru, jWriter,  main]; 

	var spy={};
	var i=0;
	 spy[jWriter.id]=function(data) {
		// if (i==0) console.log(this.maps);
		//	console.log("Spy: ", data);
		i++;
	}

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			main.run(runParams, function() { async.each(jobs, function(a, next) {a.remove(next)}, function(err) { cb(); } ) });
		}
		else cb(err, main);
	})
}

function _BORAxis2(runParams, cb) {
	cb = cb || function() {};
	
	var jParser=new J.ParserJSON({name:"JSON data.* parser", rootNode:"mng.otr_indexes.*", processor:function(a){ a.year=this.year; return a; }});
	var jReader  = new J.ReaderPaged({name:"Read subordinates", 
			// source:"https://esb.mkrf.ru/cdm/v2/subordinates?year=2016&quartal=1&offset={%offset%}&limit={%limit%}", 
			initer:function() { 
				this.fromYear=this.fromYear || 2016;
				this.toYear=this.toYear || 2016;
				this.quarter=4;
				this.source="http://api.bor.mkrf.ru/mng/mng/?year={%year%}&quartal=4&key=9c7Fgrv4MKRFtZ";
				return this;
			}, 
			processor:function(template, state) { 
					if (!this.year) this.year=this.fromYear;
					else this.year++;
					if (this.year>this.toYear) 
						return null;
					var url=template;
					for(var key in state) url=url.replace("{%year%}", this.year); 
					return url;
			},
		 parser:jParser.id});
	var jThru = new J.Thru({name:"mng_XXX measures", processor:function(stream, a, onReady) { 
		stream.push({
    		id:"OTR",
    		name:"БОР. Отраслевая отчетность"
    	});
    	// y_2012__t_1__otr_2__plan
    	var key="y_"+a.year;
    	stream.push({
    		id:key,
    		name:a.year,
    		parent:"OTR"
    	});
    	var p=key;
    	key=key+"__t_"+a.org_type;
    	stream.push({
    		id:key,
    		name:key,
    		parent:p
    	});
    	stream.push({
    		id:key+"__otr_"+a.id,
    		name:key+"__otr_"+a.id+"_"+a.name,
    		parent:key
    	});

		stream.push({
    		id:key+"__otr_"+a.id+"__plan",
    		name:key+"__otr_"+a.id+"__plan", // b.code+"/"+b.name+" (план)",
    		parent:key+"__otr_"+a.id
    	});
    	stream.push({
    		id:key+"__otr_"+a.id+"__fact",
    		name:key+"__otr_"+a.id+"__fact", // b.code+"/"+b.name+" (факт)",
    		parent:key+"__otr_"+a.id
    	});

    	onReady();
	} });
	var jMsrsUniquer =  new J.Uniquer({name:"Unique measures", processor: function (a) { return a.id; } });
	var jWriter  = new J.WriterJSONTree({name:"Write Bor OTR measures", 
			// destination:"$LOCALFS/temp/bor-2016-1.jsons",
			initer:function() { 
				this.destination="$LOCALFS/temp/axis-otr-bor.json"
				return this;
			} });
	var main=new J.Pipeline({name:"2. BOR OTR measures", chain:[jReader, jThru, jMsrsUniquer, jWriter ] }); // 

	var jobs=[jParser, jReader, jMsrsUniquer, jThru, jWriter,  main]; 

	var spy={};
	var i=0;
	 spy[jWriter.id]=function(data) {
		// if (i==0) console.log(this.maps);
		//	console.log("Spy: ", data);
		i++;
	}

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			main.run(runParams, function() { async.each(jobs, function(a, next) {a.remove(next)}, function(err) { cb(); } ) });
		}
		else cb(err, main);
	})
}


function _reduceFacts(runParams, cb) {
	cb = cb || function() {};

	var jParser = new J.ParserJSONS({name:"JSONS parser"});
	var jReader = new J.ReaderFolder({name:"Read Stat facts", parser:jParser,	source:"$LOCALFS/temp/*.jsons" });
	var jReducer =new J.Reducer({name:"Reduce by inn & date", processor:
		function(map, a){ 
			var d=a.date || ""
			var key=a.date.substr(0,7)+'-'+a.subordinate;
			map[key]=map[key] || {};
			for(var f in a)	
				// if (a[f])
					map[key][f]=a[f];
		}
	});
	var jMapper  = new J.Mapper({name:"Cleanup", processor:function(a){ a.value.key=a.id; return a.value; } });


	var jSrcReader  = new J.Reader({name:"Read Stat sources", source:"$LOCALFS/dims/sources.json" });
	var jSrcTreeParser  = new J.ParserJSONTree({name:"Parse Sources tree" });
	// var jSrcMapper  = new J.Mapper({name:"Cleanup src", processor:function(a){ return {id: a.id, category_institutions:a.category_institutions }; } });
	var jVocPipeline=new J.Pipeline({name:"Sources", chain:[jSrcReader, jSrcTreeParser] }); // jSrcMapper

	var jJoiner  = new J.Joiner({name:"Joiner with sources map",
						vocabs:[{ job: jVocPipeline.id, idPath:"id", key:"source"}],
						processor:function(a) {
							if (a) {
								var sourceId= String(a.subordinate);
								if (sourceId && this.maps && this.maps.source) {
									var subordinate=this.maps.source[sourceId];
									a.category_institutions=subordinate?subordinate.category_institutions:null;
								}
							}
							return a;
						}
	});

	var jWriter  = new J.WriterJSONS({name:"Write Stat facts", destination:"$LOCALFS/facts/all.jsons"})
	
	var main=new J.Pipeline({name:"3. Reduce Facts", chain:[jReader, jReducer, jMapper, jJoiner, jWriter ] }); // [jSrcReader, jSrcTreeParser, jWriter]}); // 
	jVocPipeline.mainPipe=main;

	var jobs=[jParser, jReader, jReducer, jMapper, jWriter, jVocPipeline,jSrcReader, jSrcTreeParser, jJoiner, main]; 

	var spy;
	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			main.run(runParams, function() { async.each(jobs, function(a, next) {a.remove(next)}, function(err) { cb(); } ) });
		} else cb(err, main);
	})
}

function readEIPSKevents(runParams, cb) {
	
	
	var jSubParser=new J.ParserJSON({name:"JSON events.* parser", rootNode:"data.*"});
	var mainReader  = new J.ReaderPaged({name:"Read subordinates", 
		source:"https://esb.mkrf.ru/cdm/v2/subordinates?offset={%offset%}&limit=100", reqOptions:{auth:{user:"opendata", password:"tGXK4zSn"}}, 
		 parser:jSubParser, 
		 propcessor:function(template, state) { 
			state = state || {};
			var page=state.page || 0;
			var listLimit=10;
			state.offset=state.from=listLimit*page
			state.to=listLimit*(page+1);
			state.limit=listLimit;
			state.since=(state.since || "");
			// var url="https://all.culture.ru/api/2.2/organizations?type=mincult&offset={%offset%}&limit={%limit%}"; //  || "";
			var url= template || "";
			for(var key in state) url=url.replace("{%"+key+"%}", state[key]); 
			return url;
	}
		});
	
	var jParser=new J.ParserJSON({name:"JSON events.* parser", rootNode:"events.*"});
	var detailReader  =  new J.ReaderPaged({name:"Read Bus", source:"https://all.culture.ru/api/2.2/events?status=accepted&organizations={%organizations%}&offset={%offset%}&limit=100",  
		processor:function(template, state) { 
					if (!this.master || !this.master.general.extension || !this.master.general.extension.externalIds || !this.master.general.extension.externalIds.eipskId) return null;
					if (state.rows==0) return null;
					state.page = state.page || 0;
					state.offset=state.from=100*(state.page || 0);
					state.organizations=this.master.general.extension.externalIds.eipskId;
					var url=template;
					for(var key in state) url=url.replace("{%"+key+"%}", state[key] ); 
					return url;
		},
		parser:jParser});

	var jJoinDetail = new J.JoinDetail({name:"Joiner readerer",  source:detailReader, processor:function(master, details) { master.events=details; return master; }	});
	
	var jThru = new J.Thru({name:"Extract events", processor:function(stream, data, onReady) { 	
			if (!data.events || !data.events.length) return onReady();
			data.events.forEach(function(evt){
				if (!evt.start) return;
				var start=new Date(evt.start);
				var YQ=start.getFullYear()+'-'+Math.trunc((start.getMonth()/3)+1)*3;
				stream.push({
					subordinate:data.general.inn,
					date:YQ,
					key:YQ+'-'+data.general.inn
				});
			})
			onReady();
		}
	});

	var jReducer  = new J.Reducer({name:"Reduce counts", processor:function(map, a){ 
		var key=a.key || "undefined"
		map[key]=map[key] || a;
		map[key].events  = map[key].events || 0;
		map[key].events++;
	}});
	var jMapper  = new J.Mapper({name:"Cleanup", processor:function(a){ var b=a.value; b.date=a.value.date+"-30T20:59:00Z"; return b; } });

	var jWriter  = new J.WriterJSONS({name:"Write ./job-data/eipsk-events.jsons", destination:"$LOCALFS/temp/eipsk-events.jsons" });

	
	var main=new J.Pipeline({name:"EIPSK events", chain:[mainReader, jJoinDetail, jThru, jReducer, jMapper, jWriter]}); // 
	var jobs=[main,jParser, mainReader, detailReader, jJoinDetail, jThru, jReducer, jMapper, jWriter, jSubParser ]; 
	var spy={};
	var i=0;
	spy[jMapper.id]=function(data) {
		console.log("Spy: ", data);
		i++;
	}

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			main.run(runParams, function() { 
				async.each(jobs, function(a, next) {/* a.remove(next)*/ next(); }, function(err) {
					console.log("ready"); 
				});
			})
		} else cb(err, main);
	})
}


 _sources(false, function(){ console.log("Ready _sources"); });
 readEIPSKevents(false,  function(){ console.log("Ready readEIPSKevents"); });
 _BORFacts(false,  function(){ console.log("Ready _BORFacts"); }); // {  "year": 2016, "quarter":3}
 _dwcFacts( false , function(e){ console.log("Ready _dwcFacts", e); }); // {  "year": 2015, "datetime":"2015-12-31T20:59:00Z"}

 _reduceFacts(false, function(e){ console.log("Ready _reduceFacts", e); }); // {  "year": 2015, "datetime":"2015-12-31T20:59:00Z"}
 _BORAxis1(false , function(e){ console.log("Ready _BORAxis1", e); }); //  {  "fromYear": 2012, "toYear":2016 }
 _BORAxis2(false , function(e){ console.log("Ready _BORAxis2", e); }); //{  "fromYear": 2012, "toYear":2016 }


