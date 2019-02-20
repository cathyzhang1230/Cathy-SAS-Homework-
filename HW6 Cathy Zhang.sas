/*1. Number of 8-K filings
Write a SAS macro that appends the number of 8-K filings the firm has made during the fiscal year.
Use cik that is on funda (this is the current cik), and the cik on the WRDS SEC link file if 
that one doesn't give any matches (in case the cik has changed):*/

/* WRDS SEC Analytics Suite: wcilink_gvkey table, provides gvkey => cik link 
http://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=124
*/
rsubmit;endrsubmit;
%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;
rsubmit;
proc download data=WRDSSEC.wciklink_gvkey out=wciklink_gvkey; run; 
endrsubmit;

/*Funda, Edgar save in local*/
%SYSLPUT year1=&year1;
%SYSLPUT year2=&year2;
rsubmit;
data getf_1 (keep = key gvkey fyear datadate cik);
set comp.funda;
if &year1 <= fyear <= &year2;
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
key = gvkey || fyear;
run;
proc download data=getf_1 out=funda;run; 

/* download linktable */
proc download data=WRDSSEC.wciklink_gvkey out=wciklink_gvkey; run; /*linktable by gvkey and cik*/
endrsubmit;

Proc sql;
create table b_matches as
select a.gvkey, a.fyear, a.cik, count(*) as num8Ks
from a_funda a, edgar.filings b
where a.datadate -365 <= b.date <=a.datadate /*roughly filing date is within fyear*/
and formtype IN ('8-K', '8-KSB') /*if you want multi types*/
and input (a.cik, best.)eq b.cik
group by a.gvkey, a.fyear;
quit;

/*
say I have a dataset a_filings (for 8K filings based on funda cik) and I want to 
find firms that have no matches ie i need to find their cik based on the wrds gvkey-cik lintable)
 starting dataset: a_funda (gvkey, fyear, cik)
current dataset with some of those matches: b_matches (gvkey, fyear, cik, num8Ks) byt only for 
ciks that were found (#obs in b_matches is lower than a_funda

things may be easier if a_funda has a key var: key = gvkey || "_" || fyear;
Unique keys can be very handy and efficient*/

Proc sql;
create table want as
select a.*
from a_funda a
where a.key not in (select key from b_matches) /*not in instead of in, works the same way*/
/*what if you dont have the key created here, then you have to be....*/
quit;

/*without key*/
proc sql;
create table want as
select a.gvkey, a.fyear, count(*) as numObs
from a_funda a left join b_matches b
on a.gvkey = b.gvkey and a.fyear = b.fyear
having num8Ks eq . /*'where' clause doesnt work here.*/
/*group by a.gvkey, a.fyear
having numObs eq 0*/
;
quit;




/*2. Absorb*/
proc import out=Franchise
datafile = 'Downloads\franchise.dta'
dbms=DTA replace;
run;

/*refer to the new post on Github*/
proc sort data=Franchisebrands; by city; run;
proc standard data=Franchisebrands
out=Fran_mean mean=0;
by city;
var math_scr str;
run;

proc reg data=Fran_city
outest=Reg1 noprint;
model math_scr = str;
quit;
* regression output through proc standard;
proc glm data=Franchisebrands;
absorb city;
ods output ParameterEstimates  = Reg2;
model math_scr = str / solution; run;
quit;
proc glm data=Franchisebrands;
class city;
ods output ParameterEstimates  = Reg2;
model math_scr = str county / solution; run;
quit;

/*results are the same*/


/*3. Matched sample*/
/*Get funda again*/
rsubmit;
proc sql;
    create table q3funda as select
    gvkey, fyear, datadate, cik, ni, (ni > 0) as loss, (wcap/at) as wcap, (mkvalt/ceq) as mktobk, conm, sich
    from comp.funda
    where 
    2012 <= fyear <= 2016 and
    indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C';
quit;
endrsubmit;

%macro doLogistic(dsin=, dep=, vars=);
	proc logistic data=&dsin descending  ;
	  model &dep = &vars / RSQUARE SCALE=none ;
	  /* not needed here, but out= captures fitted, errors, etc */
	  output out = logistic_predicted  PREDICTED=predicted ;

	  	ods output	ParameterEstimates  = _outp1
					OddsRatios 			= _outp2
					Association 		= _outp3
					RSquare  			= _outp4
					ResponseProfile 	= _outp5
					GlobalTests   		= _outp6			
					NObs 				= _outp7 ;
	%runquit;
%mend;

%doLogistic(dsin=q3funda, dep = loss, vars= fyear wcap mktobk);
proc sort data=q3funda; by conm;run;
data q3_2;
set q3funda;
if 'A' <= substr(conm, 1, 1) <= 'L' then atol = 1 else atol=0;
run;


proc sql;
  create table matched_smample as
  select a.gvkey as gvkey_t, b.gvkey as gvkey_c, a.fyear, 
  abs(a.predicted - b.predicted) as difference,
  a.predicted as predicted_t, b.predicted as predicted_c
  from logistic_predicted a, lgistic_predicted b /*self join*/
  where 
    a.loss eq 1 and b.loss eq 0
    and a.fyear eq b.fyear
    and a.sich eq b.sich
  group by
    gvkey, fyear
  having
    difference = min(difference) and
    difference < 0.01; /* potentially multiple duplicated matches */
quit;

/* drop duplicates */
proc sort data =.. ; by fyear gvkey_c difference; run;
proc sort data =.. ; nodupkey; by fyear gvkey_c; run;

/*then iterate, go back and get all the gvkey_t that
aren't matched and repeat----a loop---ask for code if needed*/
proc sort data =.. ; by fyear gvkey_t difference; run;
proc sort data =.. ; nodupkey; by fyear gvkey_t; run;



/* logistic regression */

%macro doLogistic(dsin=, dep=, vars=);
	proc logistic data=&dsin descending  ;
	  model &dep = &vars  d2010-d2013 ff12_1-ff12_12 / RSQUARE SCALE=none ;
	  /* not needed here, but out= captures fitted, errors, etc */
	  output out = logistic_predicted  PREDICTED=predicted ;

	  	ods output	ParameterEstimates  = _outp1
					OddsRatios 			= _outp2
					Association 		= _outp3
					RSquare  			= _outp4
					ResponseProfile 	= _outp5
					GlobalTests   		= _outp6			
					NObs 				= _outp7 ;
	%runquit;
%mend;

/* helper macro to export the 7 tables for each logistic regression */
%macro exportLogit(j, k);
	%myExport(dset=_outp1, file=&exportDir\logistic_&j._&k._coef.csv);
	%myExport(dset=_outp2, file=&exportDir\logistic_&j._&k._odds.csv);
	%myExport(dset=_outp3, file=&exportDir\logistic_&j._&k._assoc.csv);
	%myExport(dset=_outp4, file=&exportDir\logistic_&j._&k._rsqr.csv);
	%myExport(dset=_outp5, file=&exportDir\logistic_&j._&k._response.csv);
	%myExport(dset=_outp6, file=&exportDir\logistic_&j._&k._globaltest.csv);
	%myExport(dset=_outp7, file=&exportDir\logistic_&j._&k._numobs.csv);
%mend;

/*	Do logistic regression */

/* 1 */
%doLogistic(dsin=c_ff , dep=loss, vars=beta delay_rank);
%exportLogit(t1,col1);

/* 2 */
%doLogistic(dsin=c_ff , dep=loss, vars=beta delay_rank unex1_p unex2_p );
%exportLogit(t1,col2);
