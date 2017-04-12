
var JOBS={};

JOBS.Job=require("./jobs/job.js");
// readers
JOBS.Reader=		require("./jobs/readers/job-reader.js");
JOBS.ReaderJSON=	require("./jobs/readers/job-reader-json.js");
JOBS.ReaderJSONS=	require("./jobs/readers/job-reader-jsons.js");
JOBS.ReaderPaged=	require("./jobs/readers/job-reader-paged.js");
JOBS.ReaderFolder=	require("./jobs/readers/job-reader-folder.js");

// 1:1 transformers
JOBS.Filter=	require("./jobs/convertors/job-filter.js");
JOBS.Mapper=	require("./jobs/convertors/job-mapper.js");
JOBS.Sorter = 	require("./jobs/convertors/job-sorter.js");
JOBS.Thru=		require("./jobs/convertors/job-thru.js");
JOBS.Reducer=	require("./jobs/convertors/job-reducer.js");
JOBS.Uniquer=	require("./jobs/convertors/job-uniquer.js");

// array <-> map transformers
JOBS.Array2Map=require("./jobs/convertors/job-array2map.js");
JOBS.Map2Array=require("./jobs/convertors/job-map2array.js");


JOBS.ParserJSON=	require("./jobs/parsers/job-parser-json.js");	
JOBS.ParserJSONS=	require("./jobs/parsers/job-parser-jsons.js");
JOBS.ParserCSV=	require("./jobs/parsers/job-parser-csv.js");
JOBS.ParserXML=	require("./jobs/parsers/job-parser-xml.js");
JOBS.ParserJSONTree=	require("./jobs/parsers/job-tree-parser.js");


// 1:N transformers
JOBS.Joiner=	require("./jobs/hi-order/job-joiner.js");
JOBS.Splitter=	require("./jobs/hi-order/job-splitter.js");
JOBS.Pipeline=	require("./jobs/hi-order/job-pipeline.js");
JOBS.JoinDetail=require("./jobs/hi-order/job-join-detail.js");

// writers
JOBS.WriterJSON=	require("./jobs/writers/job-writer-json.js");
JOBS.WriterJSONS=	require("./jobs/writers/job-writer-jsons.js");
JOBS.WriterJSONTree=	require("./jobs/writers/job-tree-writer.js");





module.exports = JOBS; 