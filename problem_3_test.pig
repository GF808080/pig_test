

register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- later you will load to other files, example:
raw = 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray); 

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);
RDFAsubset = FILTER ntriples BY subject MATCHES '.*business.*';

ntriplesB = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subjectB:chararray,predicateB:chararray,objectB:chararray);
RDFBsubset = FILTER ntriplesB BY subjectB MATCHES '.*business.*';

FullSet = JOIN RDFAsubset by subject, RDFBsubset by subjectB;

fullFinal = DISTINCT FullSet;

ct = GENERATE COUNT(fullFinal)
dump ct;


