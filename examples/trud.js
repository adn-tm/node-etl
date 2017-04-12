var mongoose = require('mongoose');
var async = require("async");
var _ = require("underscore");
var path = require("path");
var fs=  require("fs-extra");
var J=require("./jobs.js");



mongoose.connect('mongodb://localhost/TrudVsem', { db: { safe: true} } );
mongoose.connection.on('error', function(err) {
	console.error('MongoDB connection error: ' + err);
	process.exit(-1);
});
/*

function readEIPSKinn(runParams) {
	var jParser=new J.ParserJSON({name:"JSON organizations.* parser", rootNode:"organizations.*"});

	var mainReader  = new J.ReaderPaged({name:"Read Bus", source:"https://all.culture.ru/api/2.2/organizations?type=mincult&offset={%offset%}&limit={%limit%}",  
		// processor:processor.toString(),
		parser:jParser});
	var jMapper  = new J.Mapper({name:"Cleanup", processor:function(a){  return {name:a.name, inn:a.inn}; } });

	var jFilter  = new J.Filter({name:"Filter with value IDs", processor:function(a){ return !!a.inn; }});

	var jWriter  = new J.WriterJSONS({name:"Write ./job-data/eipsk-detailed.jsons", destination:"file:"+path.join(__dirname, "trud.data/eipsk-inn.jsons") });

	var jobs=[ mainReader,jParser,  jFilter, jMapper, jWriter ]; 
	var main=new J.Pipeline({name:"RosTrud vacansies", chain:[mainReader, jMapper, jFilter,jWriter]});
	var spy={};
	var i=0;
	spy[jWriter.id]=function(data) {
		console.log("Spy: ", i);
		i++;
	}

	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {
			runParams.spy=spy;
			main.run(runParams, function() { 
				async.each(jobs, function(a, next) {a.remove(next)}, function(err) {
					console.log("ready"); 
				});
			})
		} else cb(err, main);
	})
}
*/

function readRosTrud(runParams) {
	

	var mainReader  = new J.ReaderJSONS({name:"Read Bus", source:"file:"+path.join(__dirname, "trud.data/eipsk-inn.jsons") });
	var detailReader  = new J.ReaderJSON({name:"Read Bus", rootNode:"results.vacancies.*" , initer:function (cntxt) { source="https://test-portal.trudvsem.ru/opendata/api/v1/vacancies/company/inn/"+master.inn;	}  });

	var jJoinDetail = new J.JoinDetail({name:"Joiner readerer",  source:detailReader, processor:function(master, details) { master.vacancies=details; return master; }	});
	var jFilter  = new J.Filter({name:"Filter with value IDs", processor:function(a){ return (a.vacancies && a.vacancies.length); }});

	var jWriter  = new J.WriterJSONS({name:"Write ./job-data/eipsk-detailed.jsons", destination:"file:"+path.join(__dirname, "trud.data/eipsk-rt.jsons") });

	var jobs=[ mainReader, detailReader, jJoinDetail, jFilter, jWriter ]; 

	var main=new J.Pipeline({name:"RosTrud vacansies", chain:[mainReader, jJoinDetail, jFilter, jWriter]});
	async.each(jobs, function(a, next) {a.save(next)}, function(err) {
		if (runParams && !err) {

			main.run(runParams, function() { 
				async.each(jobs, function(a, next) {a.remove(next)}, function(err) {
					console.log("ready"); 
				});
			})
		} else cb(err, main);
	})
}

// readEIPSKinn({})
readRosTrud({})

