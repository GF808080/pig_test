register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar
-- load the test file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray);

-- parse each line into ntriple
ntriples = FOREACH raw GENERATE FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

---group triples by subject
subjects = GROUP ntriples BY (subject);

-- flatten the subjects out (because group by produces a tuple of each subject
-- in the first column, and we want each subject ot be a string, not a tuple),
-- and count the number of tuples associated with each subject

count_by_subject = FOREACH subjects GENERATE flatten($0) as subj, COUNT($1) as counted;

--group by counts
Hcounts = GROUP count_by_subject by counted;



Histograms = FOREACH Hcounts GENERATE group as Subject_Count ,COUNT(count_by_subject.subj); 
store Histograms into '/user/hadoop/full-results' using PigStorage();


