require("../../R/console.js");

var config 		= require('../../config/environment');

var mongoose = require('mongoose');
var async = require("async");
var _ = require("underscore");
var path = require("path");
var fs=  require("fs-extra");

var Job=require("../jobs/job.js");
// readers
var JReader=require("../jobs/job-reader-json.js");
var JReaderS=require("../jobs/job-reader-jsons.js");
var JReaderPaged=require("../jobs/job-reader-paged.js");
var Reader=require("../jobs/job-reader.js");

// 1:1 transformers
var JFilter=require("../jobs/job-filter.js");
var JMapper=require("../jobs/job-mapper.js");
var JSorter = require("../jobs/job-sorter.js");
var JThru=require("../jobs/job-thru.js");
var JParserJSON=require("../jobs/job-parser-json.js");
var JReducer=require("../jobs/job-reducer.js");
var JUniquer=require("../jobs/job-uniquer.js");

// 1:N transformers
var JJoiner=require("../jobs/job-joiner.js");
var JSplitter=require("../jobs/job-splitter.js");
var JPipeline=require("../jobs/job-pipeline.js");
var JJoinDetail=require("../jobs/job-join-detail.js");

// writers
var JWriter=require("../jobs/job-writer-json.js");
var JWriterS=require("../jobs/job-writer-jsons.js");

var TreeWriter=require("../jobs/job-tree-writer.js");
var TreeParser=require("../jobs/job-tree-parser.js");



// Connect to database
mongoose.connect(config.mongo.uri, config.mongo.options);
mongoose.connection.on('error', function(err) {
	console.error('MongoDB connection error: ' + err);
	process.exit(-1);
});


function testRW() {


	var jReader  = new JReader({name:"Read ./job-data/1.json", source:"file:"+path.join(__dirname, "job.data/1.json") });
	var jWriter  = new JWriter({name:"Read ./job-data/2.json", destination:"file:"+path.join(__dirname, "job.data/1-rw.json") });
	jReader.toStream(function (err, r) {
		if (err) { console.error("Read stream error", err); return; }
		jWriter.toStream(function (err, w) {
			if (err) { console.error("Write stream error", err);  return; }
			r.pipe(w).on("finish", function(){ console.log("ready"); process.exit(0);});
		})	
	})
}

function testRsWs() {


	var jReader  = new JReaderS({name:"Read ./job-data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data.jsons") });
	var jWriter  = new JWriterS({name:"Read ./job-data/data-2.json", destination:"file:"+path.join(__dirname, "job.data/data-2.jsons") });
	jReader.toStream(function (err, r) {
		if (err) { console.error("Read stream error", err); return; }
		jWriter.toStream(function (err, w) {
			if (err) { console.error("Write stream error", err);  return; }
			r.pipe(w).on("finish", function(){ console.log("ready"); process.exit(0);});
		})	
	})
}


function testR_Map_W() {
	

	var jReader  = new JReader({name:"Read ./job-data/1.json", source:"file:"+path.join(__dirname, "job.data/1.json") });
	var jWriter  = new JWriter({name:"Read ./job-data/2.json", destination:"file:"+path.join(__dirname, "job.data/1-map-id.json") });
	
	var jMapper  = new JMapper({name:"Map only IDs", processor:function(a){ return {id: a.id, name:(a.name || a.aname || a.caption) }; } } );

	jReader.toStream(function (err, r) {
		if (err) { console.error("Read stream error", err); return; }
		jMapper.toStream(function (err, m) {
			if (err) { console.error("Map stream error", err); return; }
			jWriter.toStream(function (err, w) {
				if (err) { console.error("Write stream error", err);  return; }
				r.pipe(m).pipe(w).on("finish", function(){ console.log("ready"); process.exit(0);});
			})	
		})
	})
}
// "src_VERNUM":1007,"fro_VERNUM":61,"fgr_VERNUM":76,"seq_NUMBER":0,"ffo_VERNUM":3,"fpa_VERNUM":13,"sourceId":1007,"3_p_13_r_61_g_76"

function testR_Filter_W() {

	
	var jFilter  = new JFilter({name:"Filter with value IDs", 
		processor:function (a){ 
			var aId=a.ffo_VERNUM+"_p_"+a.fpa_VERNUM+"_r_"+a.fro_VERNUM+"_g_"+a.fgr_VERNUM;
			return !!a[aId]; 
		}
	});
	var jReader  = new JReaderS({name:"Read ./job-data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data-50.jsons") });
	var jWriter  = new JWriterS({name:"Read ./job-data/data-filtered.json", destination:"file:"+path.join(__dirname, "job.data/data-50-filtered.jsons") });

	jReader.toStream(function (err, r) {
		if (err) { console.error("Read stream error", err); return; }
		jFilter.toStream(function (err, f) {
			if (err) { console.error("Filter stream error", err); return; }
			jWriter.toStream(function (err, w) {
				if (err) { console.error("Write stream error", err);  return; }
				r.pipe(f).pipe(w);
			})	
		})
	})
}


function testR_Uniq_W() {

	

	
	var jReader  = new JReaderS({name:"Read ./job-data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data-2.jsons") });
	var jUniquer = new JUniquer({name:"Uniq with value IDs", processor: function (a) { return a.sourceId || "undefined"; } });
	var jMapper  = new JMapper({name:"Map only IDs", processor: function (a) { return a.sourceId || "undefined"; } });

	var jWriter  = new JWriterS({name:"Read ./job-data/data-filtered.json", destination:"file:"+path.join(__dirname, "job.data/data-2-unique-id.jsons") });

	JPipeline.run([jReader, jUniquer, jMapper,  jWriter], function(){ console.log("ready")});
}

function testR_Sort_W() {

	
	var jSorter = new JSorter({name:"Sort with value IDs", 
			processor: function (a,b) { 
				a = a || {};
				b = b || {};
				return a.id>b.id?1:a.id<b.id?-1:0
			}
	});
	var jReader  = new JReaderS({name:"Read ./job-data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data-2.jsons") });
	var jWriter  = new JWriterS({name:"Read ./job-data/data-filtered.json", destination:"file:"+path.join(__dirname, "job.data/data-2-ordered.jsons") });

	JPipeline.run([jReader, jSorter,   jWriter], function(){ console.log("ready")});
}





function testR_Join_R_W() {

	

	var jReader  = new JReader({name:"Read ./job.data/1.json", source:"file:"+path.join(__dirname, "job.data/1.json") });
	var jWriter  = new JWriterS({name:"Read ./job.data/1-joined.job-writer-jsons", destination:"file:"+path.join(__dirname, "job.data/1-joined.jsons") });
	var jVocReader  = new JReader({name:"Read ./job.data/sources.json", source:"file:"+path.join(__dirname, "job.data/sources.json") });

	var jJoiner  = new JJoiner({name:"Joiner with sources map",
							vocabs:[{ job: jVocReader.id, idPath:"aname", key:"source"}],
							processor:function (a){  
								if (a.kopuk && this.maps && this.maps.source) 
									a.DWC=this.maps.source[a.kopuk];
								return a 
							}
						});
	jVocReader.save(function(err) {
		if (err) { console.error("Read stream error", err); return; }
		jJoiner.save(function(err){

			if (err) { console.error("Read stream error", err); return; }
			jReader.toStream(function (err, r) {
				if (err) { console.error("Read stream error", err); return; }
				jJoiner.toStream(function (err, f) {
					if (err) { console.error("Filter stream error", err); return; }
					jWriter.toStream(function (err, w) {
						if (err) { console.error("Write stream error", err);  return; }
						r.pipe(f).pipe(w).on("finish", function(){
							jVocReader.remove();
							jJoiner.remove();
						 console.log("ready"); process.exit(0);});
						
					})	
				})
			})
		})
	})

	
}



function testR_Split_W_W() {


	var jReader  = new JReaderS({name:"Read ./job.data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data.jsons") });

	var jWriter1  = new JWriterS({name:"Read ./job.data/job.data/data-fork1.jsons", destination:"file:"+path.join(__dirname, "job.data/data-fork1.jsons") });
	var jWriter2  = new JWriter({name:"Read ./job.data/data-fork2.json", destination:"file:"+path.join(__dirname, "job.data/data-fork2.json") });

	var jSplitter  = new JSplitter({name:"Split into 2 destinantions", receivers:[jWriter1.id, jWriter2.id ], });
		jWriter1.save(function(err) {
			jWriter2.save(function(err) {
			if (err) { console.error("Read stream error", err); return; }
			jReader.toStream(function (err, r) {
				if (err) { console.error("Read stream error", err); return; }
				jSplitter.toStream(function (err, s) {
					if (err) { console.error("Filter stream error", err); return; }
					r.pipe(s).on("finish", function(){
						jWriter2.remove();
						jWriter1.remove();
						console.log("ready"); process.exit(0);});
				})
			})
		})
	})

}

function testR_SwitchSplit_W_W() {

	var jReader  = new JReaderS({name:"Read ./job.data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data-50.jsons") });

	
	var jWriter1  = new JWriterS({name:"Read ./job.data/job.data/data-fork1.jsons", destination:"file:"+path.join(__dirname, "job.data/data-odd.jsons") });
	var jWriter2  = new JWriter({name:"Read ./job.data/data-fork2.json", destination:"file:"+path.join(__dirname, "job.data/data-even.json") });

	var jSplitter  = new JSplitter({name:"Split into 2 destinantions", receivers:[jWriter1, jWriter2], processor:function(doc, streams) { return ((doc.id || 0) % 2 ) }});

	jWriter1.save(function(err) {
		jWriter2.save(function(err) {
			if (err) { console.error("Read stream error", err); return; }
			JPipeline.run([jReader, jSplitter], function(){ jWriter2.remove(); jWriter1.remove(); console.log("ready")});
		})
	})

}

function testR_Pipeline_W() {

	var jReader  = new JReaderS({name:"Read ./job.data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data-50.jsons") });
	
	var jMapper  = new JMapper({name:"Map only IDs", processor:function (a){ a.nativeId=a.ffo_VERNUM+"_p_"+a.fpa_VERNUM+"_r_"+a.fro_VERNUM+"_g_"+a.fgr_VERNUM; a.value=a[a.nativeId]; return a; }});
	
	
	var jFilter  = new JFilter({name:"Filter with value IDs", processor:function (a) { return !!a.value; } });

	var jWriter1  = new JWriterS({name:"Read ./job.data/job.data/data-piped.jsons", destination:"file:"+path.join(__dirname, "job.data/data-piped.jsons") });
	var jWriter2  = new JWriterS({name:"Read ./job.data/job.data/data-piped-m.jsons", destination:"file:"+path.join(__dirname, "job.data/data-piped-m.jsons") });

	var jPipeline  = new JPipeline({name:"Piped", chain:[jMapper.id, jFilter.id, jWriter2.id ] });
	var jSplitter  = new JSplitter({name:"Splitter", receivers:[jPipeline.id, jWriter1.id ] });

	var jobs=[jReader, jMapper, jWriter1, jFilter, jWriter2,  jPipeline, jSplitter];

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
			if (err) { console.error("Read stream error", err); return; }
			jReader.toStream(function (err, r) {
				if (err) { console.error("Read stream error", err); return; }
				jSplitter.toStream(function (err, s) {
					if (err) { console.error("Filter stream error", err); return; }
					r.pipe(s).on("end", function(){console.log("ready");  });
				})
			})
	})
}


function testPipelineRun() {


	var jReader  = new JReaderS({name:"Read ./job.data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data-50.jsons") });

	
	var jMapper  = new JMapper({name:"Map only IDs", 
		processor:function (a){ a.nativeId=a.ffo_VERNUM+"_p_"+a.fpa_VERNUM+"_r_"+a.fro_VERNUM+"_g_"+a.fgr_VERNUM; a.value=a[a.nativeId]; return a; } });
	
	
	var jFilter  = new JFilter({name:"Filter with value IDs", processor:function (a) { return !!a.value; } });

	var jWriter1  = new JWriterS({name:"Read ./job.data/job.data/data-piped.jsons", destination:"file:"+path.join(__dirname, "job.data/data-piped.jsons") });
	var jWriter2  = new JWriterS({name:"Read ./job.data/job.data/data-piped-m.jsons", destination:"file:"+path.join(__dirname, "job.data/data-piped-m.jsons") });

	var jPipeline  = new JPipeline({name:"Piped", chain:[jMapper.id, jFilter.id, jWriter2.id ] });
	var jSplitter  = new JSplitter({name:"Splitter", receivers:[jPipeline.id, jWriter1.id ] });

	var main = new JPipeline({name:"Main pipe", chain:[jReader, jSplitter] });
		jPipeline.mainPipe=main;
	async.each([jReader, main, jMapper, jSplitter, jFilter, jWriter2, jPipeline, jWriter1 ], function(a, n) {a.save(n)}, function(e) {
		JPipeline.findOne({id:main.id}, function(e, a){
			var spy={};
			var i=0;
			spy[jFilter.id]=function(data) {
				console.log("Spy: ", i);
				if (i>10) a.stop();
				i++;
			}
			a.run({spy:spy}, function(){console.log("ready");  }); 
		})
	});
}

function testR_Reduce_W() {
	

	var jReader  = new JReader({name:"Read ./job-data/1.json", source:"file:"+path.join(__dirname, "job.data/1.json") });
	var jWriter  = new JWriter({name:"Read ./job-data/1-reduced.json", destination:"file:"+path.join(__dirname, "job.data/1-reduced.json") });
	
	var jReducer  = new JReducer({name:"Reduce counts", processor:function(map, a){ 
		var key=a.curator || "undefined"
		map[key]=map[key] || 0;
		map[key]++;
	}});

	jReader.toStream(function (err, r) {
		if (err) { console.error("Read stream error", err); return; }
		jReducer.toStream(function (err, m) {
			if (err) { console.error("Map stream error", err); return; }
			jWriter.toStream(function (err, w) {
				if (err) { console.error("Write stream error", err);  return; }
				r.pipe(m).pipe(w).on("finish", function(){ console.log("ready"); process.exit(0);});
			})	
		})
	})
}


function testRP_W() {
	/*  default page URL replaces
	function processor(template) { 
		// var state = this.state || {};
		state = state || {};
		var page=state.page || 0;
		var listLimit=100;
		state.offset=state.from=listLimit*page
		state.to=listLimit*(page+1);
		state.limit=listLimit;
		state.since=(state.since || "");
		var url=template;
		for(var key in state) url=url.replace("{%"+key+"%}", state[key]); 
		return url;
	}
	*/


	var jParser=new JParserJSON({name:"JSON data.* parser", rootNode:"data.*"});
	var jReader  = new JReaderPaged({name:"Read Bus", source:"https://esb.mkrf.ru/cdm/v2/subordinates?offset={%offset%}&limit={%limit%}", reqOptions:{auth:{user:"opendata", password:"opendata"}}, 
		 parser:jParser.id});

	var jWriter  = new JWriter({name:"Read ./job-data/eip-loaded.json", destination:"file:"+path.join(__dirname, "job.data/eip-loaded.json") });

	jParser.save(function (err) {
		if (err) { console.error("jParser saveerror", err); return; };
		jReader.toStream(function (err, r) {
			if (err) { console.error("Read stream error", err); return; }
			jWriter.toStream(function (err, w) {
				if (err) { console.error("Write stream error", err);  return; }
				r.pipe(w).on("finish", function(){ jParser.remove(); console.log("ready"); process.exit(0);});
				
			})
		})
	})
}


function loadEipskSubords() {
	

	var jParser=new JParserJSON({name:"JSON organizations.* parser", rootNode:"organizations.*"});
	var jReader  = new JReaderPaged({name:"Read Bus", source:"https://all.culture.ru/api/2.2/organizations?type=mincult&offset={%offset%}&limit={%limit%}",  
		parser:jParser.id});
	// var jMapper  = new JMapper({name:"Map EIPSK organization", destination:"file:"+path.join(__dirname, "job.data/eip-loaded.json") });
	
	

	var jFilter  = new JFilter({name:"Filter with value IDs", processor:function(a){ return !!a.inn; }});

	var jWriter  = new JWriterS({name:"Read ./job-data/eipsk-organizations.jsons", destination:"file:"+path.join(__dirname, "job.data/eipsk-organizations.jsons") });

	jParser.save(function (err) {
		if (err) { console.error("jParser saveerror", err); return; };
		jReader.toStream(function (err, r) {
			if (err) { console.error("Read stream error", err); return; }
			jFilter.toStream(function (err, f) {
				jWriter.toStream(function (err, w) {
					if (err) { console.error("Write stream error", err);  return; }
					r.pipe(f).pipe(w);
					jParser.remove();
				})
			});
		})
	})
}



function test_JoinDetails() {

	function initer(cntxt) {
		source="https://test-portal.trudvsem.ru/opendata/api/v1/vacancies/company/inn/"+master.inn;
	}
	function predicate(a){ return !!a.inn; }
	function joiner(master, details) { 
		master.details=details;
		return master;
	}


	var jParser=new JParserJSON({name:"JSON organizations.* parser", rootNode:"organizations.*"});

	var mainReader  = new JReaderPaged({name:"Read Bus", source:"https://all.culture.ru/api/2.2/organizations?type=mincult&offset={%offset%}&limit={%limit%}",  
		// processor:processor.toString(),
		parser:jParser});

	var jFilter  = new JFilter({name:"Filter with value IDs", processor:"var processor="+predicate.toString()});

	var detailReader  = new JReader({name:"Read Bus", rootNode:"*" , initer:initer.toString()  });

	// var jPipeline  = new JPipeline({name:"Piped", chain:[jMapper.id, jFilter.id, jWriter2.id ] });


	var jJoinDetail = new JJoinDetail({name:"Joiner readerer",  source:detailReader, processor:"var processor="+joiner.toString()	});
	var jWriter  = new JWriterS({name:"Write ./job-data/eipsk-detailed.jsons", destination:"file:"+path.join(__dirname, "job.data/eipsk-detailed.jsons") });

	 var jobs=[detailReader, jParser]; 
	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		JPipeline.run([mainReader, jFilter, jJoinDetail, jWriter], function() { console.log("ready"); })
	 }) 
}

function test_SankeyDataPipes(){
	
	var reader1  = new JReaderS({name:"Read ./job.data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data.jsons") });
	var filter1  = new JFilter({name:"Filter with value IDs", processor: function (a){ return !!a.inn; } });
	
	var writer1  = new JWriterS({name:"Write ./job-data/1.jsons", destination:"file:"+path.join(__dirname, "job.data/eipsk-1.jsons") });
	
	

	var mapper  = new JMapper({name:"Map only IDs", 
		processor:function (a){ a.nativeId=a.ffo_VERNUM+"_p_"+a.fpa_VERNUM+"_r_"+a.fro_VERNUM+"_g_"+a.fgr_VERNUM; a.value=a[a.nativeId]; return a; } });

	var filter2  = new JFilter({name:"Filter2", processor: function (a){ return !!a.inn; } });
	var writer2  = new JWriterS({name:"Write ./job-data/2.jsons", destination:"file:"+path.join(__dirname, "job.data/eipsk-2.jsons") });
	var pipe2= new JPipeline({name:"Subpipe 2", chain:[mapper.id, filter2.id ] });
	
	// var splitter  = new JSplitter({name:"Split into 2 destinantions", receivers:[pipe.id, writer1.id ], });
	var pipe1  = new JPipeline({name:"Subpipe 1", chain:[pipe2, writer2.id], });
	
	var main=new JPipeline({name:"Main chain 1", chain:[reader1.id, filter1.id, pipe1.id ] });
	
	pipe1.mainPipe=main;
	pipe2.mainPipe=main;
	console.log(main.id, pipe1.mainPipe);
	var jobs=[reader1, filter1, writer1, mapper,filter2,  writer2, pipe1, pipe2, main]; 

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		main.getLinkedJobs(function(e, r) { 
			fs.writeJson(path.join(__dirname, "../public/d3/tree-sankey.json"), r);
		})
		// main.run()
	}) 


}
function test_SankeyDataSplitter(){
	
	var reader1  = new JReaderS({name:"Read ./job.data/data.jsons", source:"file:"+path.join(__dirname, "job.data/data.jsons") });
	var filter1  = new JFilter({name:"Filter with value IDs", processor: function (a){ return !!a.inn; } });
	
	var writer1  = new JWriterS({name:"Write ./job-data/1.jsons", destination:"file:"+path.join(__dirname, "job.data/eipsk-1.jsons") });
	
	

	var mapper  = new JMapper({name:"Map only IDs", 
		processor:function (a){ a.nativeId=a.ffo_VERNUM+"_p_"+a.fpa_VERNUM+"_r_"+a.fro_VERNUM+"_g_"+a.fgr_VERNUM; a.value=a[a.nativeId]; return a; } });

	var filter2  = new JFilter({name:"Filter2", processor: function (a){ return !!a.inn; } });
	var writer2  = new JWriterS({name:"Write ./job-data/2.jsons", destination:"file:"+path.join(__dirname, "job.data/eipsk-2.jsons") });
	var pipe2= new JPipeline({name:"subpipe 2", chain:[mapper.id, filter2.id, writer2.id ] });
	
	var splitter  = new JSplitter({name:"Split into 2 destinantions", receivers:[pipe2.id, writer1.id ], });
	
	var main=new JPipeline({name:"Main chain 1", chain:[reader1.id, filter1.id, splitter.id ] });
	pipe2.mainPipe=main;

	var jobs=[reader1, filter1, splitter, writer1, mapper,filter2,  writer2, pipe2, main]; 

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		main.getLinkedJobs(function(e, r) { 
			fs.writeJson(path.join(__dirname, "../public/d3/tree-sankey.json"), r);
			process.exit(0);
		})
		// main.run()
	}) 


}
function test_SankeyDataJoiner(){
	
	var jReader  = new JReader({name:"Read ./job.data/1.json", source:"file:"+path.join(__dirname, "job.data/1.json") });
	var jWriter  = new JWriterS({name:"Read ./job.data/1-joined.job-writer-jsons", destination:"file:"+path.join(__dirname, "job.data/1-joined.jsons") });
	var jVocReader  = new JReader({name:"Read ./job.data/sources.json", source:"file:"+path.join(__dirname, "job.data/sources.json") });

	var jJoiner  = new JJoiner({name:"Joiner with sources map",
							vocabs:[{ job: jVocReader.id, idPath:"aname", key:"source"}],
							processor:function (a){  
								if (a.kopuk && this.maps && this.maps.source) 
									a.DWC=this.maps.source[a.kopuk];
								return a 
							}
						});
	var main=new JPipeline({name:"Main chain", chain:[jReader.id, jJoiner.id, jWriter.id ] });


	var jobs=[main, jReader, jWriter, jVocReader, jJoiner]; 

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		main.getLinkedJobs(function(e, r) { 
			fs.writeJson(path.join(__dirname, "../public/d3/tree-sankey.json"), r);
		})
		// main.run()
	}) 
}
function test_SankeyDataDetJoiner(){
	
	function initer(cntxt) {
		source="https://test-portal.trudvsem.ru/opendata/api/v1/vacancies/company/inn/"+master.inn;
	}
	
	function joiner(master, details) { 
		master.details=details;
		return master;
	}


	var jParser=new JParserJSON({name:"JSON parser", rootNode:"organizations.*"});

	var mainReader  = new JReaderPaged({name:"MainReader", source:"https://all.culture.ru/api/2.2/organizations?type=mincult&offset={%offset%}&limit={%limit%}",  
		// processor:processor.toString(),
		parser:jParser});

	var jFilter  = new JFilter({name:"Filter", processor:function (a){ return !!a.inn; }});

	var detailReader  = new JReader({name:"Read Bus", rootNode:"*" , initer:initer});

	var jJoinDetail = new JJoinDetail({name:"Joiner detailer",  source:detailReader, processor:joiner });
	var jWriter  = new JWriterS({name:"Main Writer", destination:"file:"+path.join(__dirname, "job.data/eipsk-detailed.jsons") });
	var main= new JPipeline({name:"Main chain 2",  chain:[mainReader, jFilter, jJoinDetail, jWriter]})

	var jobs=[main, mainReader, jFilter, jJoinDetail, jWriter, detailReader, jParser]; 
	// console.log(main);
	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		main.getLinkedJobs(function(e, r) { 
			fs.writeJson(path.join(__dirname, "../public/d3/tree-sankey.json"), r);
			// process.exit(0);
			// async.each(jobs, function(a, next) {a.remove(next)}, function(err) { process.exit(0); });
		})
	}) 
}


function testR_FromTree_W() {
	var jReader  = new Reader({name:"Read ./job-data/msrs-65.json", source:"file:"+path.join(__dirname, "job.data/msrs-65.json") }); // data-50.jsons
	var treeParser  = new TreeParser({name:"Map only IDs", processor:function(a){ return {id: a.id, name:(a.name || a.aname || a.caption) }; } } );
	var jWriter  = new JWriter({name:"Write ./job-data/msrs-65-flat.json", destination:"file:"+path.join(__dirname, "job.data/msrs-65-flat.json") });

	var jobs=[jReader, treeParser,  jWriter]; 
	var spy={};
	// spy[jReader.id]=function(data) {}

	async.each(jobs,  function(a, next) {a.save(next)}, function(err) {
		JPipeline.run([jReader, treeParser, jWriter], {spy:spy}, function(e){ 
			console.log(e || "ready")
			async.each(jobs, function(a, next) {a.remove(next)}, function(err) { process.exit(0); });
		});
	});
}

function testR_WTree() {
	var jReader  = new JReader({name:"Read JSON ./job-data/msrs-65.json", source:"file:"+path.join(__dirname, "job.data/msrs-65-flat.json") }); // data-50.jsons
	var jWriter  = new TreeWriter({name:"Write ./job-data/msrs-65-flat.json", destination:"file:"+path.join(__dirname, "job.data/msrs-65-tree.json") });
	//
	var jFilter  = new JFilter({name:"Filter", processor:function (a){ return !!a; }});
	var jobs=[jReader, jFilter, jWriter]; 
	var spy={};
	var i=0;
	// spy[jWriter.id]=function(data) { console.log(data, i++);	}

	async.each(jobs,  function(a, next) {a.save(next)}, function(err) {
		JPipeline.run([jReader,jFilter, jWriter], {spy:spy}, function(e){ 
			console.log(e || "ready")
			async.each(jobs, function(a, next) {a.remove(next)}, function(err) { /*process.exit(0); */ });
		});
	}) 
}


// var =require("../jobs/job-to-tree-writer.js");

// test tree convertion
// testR_FromTree_W();
 testR_WTree();

// make chart data tests
//test_SankeyDataPipes();
// test_SankeyDataSplitter();
//test_SankeyDataDetJoiner();

// testRW()
// testRsWs()
 // testR_Map_W()
// testR_Filter_W()
 // testR_Join_R_W();
 // testR_Split_W_W();
 // testR_SwitchSplit_W_W();

 // testR_Pipeline_W();
 // testR_Reduce_W()
 // testRP_W();
 // loadEipskSubords();
 // testPipelineRun();
// test_JoinDetails();

// testR_Sort_W();
 // testR_Uniq_W();

