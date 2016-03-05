register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar
-- load the test file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);

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

dump Hcounts;


Histograms = FOREACH Hcounts GENERATE group as Subject_Count ,COUNT(count_by_subject.subj); 
store Histograms into '/user/hadoop/full-results' using PigStorage();
dump Histograms;


### Stackoverflow example ###
input:
3858241 3634889,3557384,3398406,1324234,956203 3858242 3707004,3668705,3319261,1515701 3858243 3684611,3681785,3574238,3221341,3156927,3146465,2949611 3858244 2912700,2838924,2635670,2211676,17445,14040 3858245 3755824,3699969,3621837,3608095,3553737,3176316,2072303 3858246 3601877,3503079,3451067 3858247 3755824,3694819,3621837,2807431,1600859 


X = LOAD 'pigpatient.txt' using PigStorage(' ') AS (pid:int,str:chararray);

X1 = FOREACH X GENERATE pid,STRSPLIT(str, ',') AS (y:tuple());

X2 = FOREACH X1 GENERATE  pid,SIZE(y) as numofcitan;

dump X2;
  (3858241,5)
  (3858242,4)
  (3858243,7)
  (3858244,6)
  (3858245,7)
  (3858246,3)
  (3858247,5)

X3 = group X2 by numofcitan;

Histograms = foreach X3 GENERATE group as numofcitan,COUNT(X2.pid); 

dump Histograms;
(3,1)
(4,1)
(5,2)
(6,1)
(7,2)