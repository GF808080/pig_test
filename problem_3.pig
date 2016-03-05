

register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- later you will load to other files, example:
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray); 

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);
RDFAsubset = FILTER ntriples BY subject MATCHES '.*rdfabout.com.*';

ntriplesB = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subjectB:chararray,predicateB:chararray,objectB:chararray);
RDFBsubset = FILTER ntriplesB BY subjectB MATCHES '.*rdfabout.com.*';

FullSet = JOIN RDFAsubset by object, RDFBsubset by subjectB;

fullFinal = DISTINCT FullSet;

FinalGroup= GROUP fullFinal ALL;
finalCount = FOREACH FinalGroup GENERATE COUNT(fullFinal);
store finalCount into '/user/hadoop/businesstest' using PigStorage();
dump finalCount;

