%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;


/*HW2 1. Funda - segment file consistency
Select the firms in the segment files that have a single business segment. Compare the SIC industry code with the SICH industry code of the firm. 
Which percentage of firms that have a single segment have the same firm-level 4-digit industry code and segment industry code?*/

rsubmit;
libname segm "/wrds/comp/sasdata/naa/segments_current";
proc sql;
	create table h_segment as 
	select * from segm.WRDS_SEGMERGED;
quit;
proc download data=h_segment out=h_segment;run;


libname myfiles "~"; /* ~ is home directory in Linux, e.g. /home/ufl/imp */
proc sql;
	create table myfiles.q1 as
		select gvkey, fyear, datadate, sics1, sics2, sics3, sid, stype, naicss2, naiss2, count(*)
	  	from h_segment 
  	where 		
		2010 <= fyear <= 2019
	and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
quit;

proc sql;
create table myfiles.funda as
select gvkey, fyear, datadate, sich, sale, ni from comp.funda
where 		
		2010 <= fyear <= 2019
	and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;

/* drop doubles */
proc sort data=myfiles.a_funda nodupkey; by gvkey fyear;run;

/* create unique key for each firm-year */
data myfiles.a_funda;
	set myfiles.a_funda;
	key = gvkey || fyear;
run;

proc download data=myfiles.a_funda out=a_funda;run;

endrsubmit;

proc sql;
    create table left_join as
    select a.*, b.sich
    from myfiles.q1 a left join a_funda b
    on a.gvkey = b.gvkey;
quit;

data q1_2;
set left_join;
first_id = input(substr(sics1,1,1), 8.);
run;
 
proc sql;
    create table q1_3 as select *,count(*) as count_num from q1_2;
quit;
 
proc sql;
    create table q1_4 as select gvkey, sid, first_id, count_num, count(*) as same_num from q1_3 where sid = first_id;
    quit;
 
data q1_5;
    set q1_4;
    proportion = same_num / count_num;
run; 



/*2. Relative industry ROA
Create a dataset from Funda with gvkey, fyear, etc, and return on assets. 
Then, using proc sql, construct a measure that is the average return on assets of the other firms in the industry-year, excluding the firm itself.*/

proc sql;
     create table q2 as
          select *, sumroa - roa as sum2, n-1 as n2, 
          calculated sum2 / calculated n2 * 100 as avg2 label "self-excluding industry-year average of ROA" 
          from (select *, sum(roa) as sumroa, count(*) as n from a_funda
               where roa ne .
               group by sich, fyear)
          group by sich, fyear;
quit;


/*3. Entropy measure
Using the firm's segment files, construct the entropy measure of how dispersed the segment's sales are.
The entropy measure is the sum of P x P x ln ( 1 / P), 
ere P is the proportion of the segment sales as a percentage of the firm's sales.*/


data segm3 (keep = GVKEY datadate STYPE SID IAS CAPXS NAICS NAICSH NAICSS1 NAICSS2 NIS OPS SALES SICS1 SICS2 SNMS SOPTP1 INTSEG);
set segm;
if srcdate eq datadate;
/* select business/operating (or, industrial) segments */
if stype IN ("BUSSEG", "OPSEG");
/* keep segments that have SIC industry code */
if SICS1 ne "";
/* keep segments with positive sales */
if sales > 0;
run;


data segm4;
set segm3;
by gvkey datadate;
retain sum_sales; 
if first.datadate then sum_sales = 0;
sum_sales = sum_sales + sales; /* Smilimar to loop in R*/
run;


proc sort data=segm4; by gvkey datadate descending sum_sales;run;


data segm5;
set segm4;
by gvkey datadate;
retain newsum_sales;
if first.datadate then do;
newsum_sales = sum_sales;
end;
run;


data segm6;
set segm5;
portion = sales / newsum_sales;
run;


data segm7;
set segm6;
entroypy = portion * portion * log(1/portion);
run;


data segm8;
set segm7;
by gvkey datadate;
retain entropy_final;
if first.datadate then entropy_final = 0;
entropy_final = entropy_final + entroypy;
if last.datadate then output;
run;

