%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;

rsubmit;


/*1. Reporting delay
Extend the class example (dataset with unexpected earnings based on the last
analyst forecast and earnings announcement stock return) by adding a new
variable reporting delay, computed as the number of days between the earnings
announcement date and the earnings announcement date of the first company in
the same 4-digit SICH to report earnings. Only include firms with end-of-year
in March, June, September or December (such that the observations in the sample
have an end-of-quarter at the same time).

Create decile ranks of the delay, and repeat the regression on CAR on UNEX by
the ranked variable (10 regressions).
Note: you can either use firm-years or adjust the code to use firm-quarters*/

/* funda */
data a_funda (keep = key gvkey fyear datadate conm sich);
set comp.funda;
if 2010= fyear <= 2016;
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
key = gvkey || fyear;
run;

/* merge with fundq by left join*/
proc sql;
create table b_fundq as
select a.* , b.datadate as datadate_fundq, b.fqtr 
from a_funda a left join comp.fundq b
on a.gvkey = b.gvkey and a.fyear = b.fyearq;
quit;

proc sql;
  create table c_permno as
  select a.*, b.lpermno as permno
  from b_fundq a left join crsp.ccmxpf_linktable b
    on a.gvkey = b.gvkey
    and b.lpermno ne .
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS")
    and b.linkprim IN ("C", "P") 
    and ((a.datadate_fundq >= b.LINKDT) or b.LINKDT = .B) and 
       ((a.datadate_fundq <= b.LINKENDDT) or b.LINKENDDT = .E)   ;
quit;

proc sql;
  create table d_cusip as
  select a.*, b.ncusip
  from c_permno a, crsp.dsenames b
  where 
        a.permno = b.PERMNO
    and b.namedt <= a.datadate_fundq <= b.nameendt
    and b.ncusip ne "";
  quit;
 
/* unique obs */
proc sort data=d_cusip nodupkey; by gvkey fyear fqtr;run;

proc sql;
create table e_ibes as
  select distinct a.*, b.ticker as ibes_ticker
  from d_cusip a, ibes.idsum b
  where 
        a.NCUSIP = b.CUSIP
    and a.datadate_fundq > b.SDATES 
;
quit;

/* anndats : announce date (forecast)
anntims : announce time (forecast)
anndatts_act : announce date of actual
anntims_act : announce time of actual
fpedats : forecast period end date (target forcast date)
*/

proc sql; create table f_delays as select 
	a.*, b.actual as actual, b.value as forecast, b.anntims, b.anndats as anndate,
	dhms(b.anndats,     0, 0, b.anntims) as datetime_f, 
    dhms(b.anndats_act, 0, 0, b.anntims_act) as datetime_a,
    min(b.anntims_act) as first_indust_ann_dt,
    day(b.anndats_act - calculated first_indust_ann_dt) as reporting_delay,
    b.actual - b.value as unex
from 
	e_ibes a, ibes.det_epsus b
where 
	a.ibes_ticker eq b.ticker
    and a.sich ne .
	and b.FPI eq '6'   
	and a.datadate_fundq - 7 <= b.fpedats <= a.datadate_fundq + 7
	and b.anndats_act > b.anndats
    and month(a.datadate) in (3,6,9,12) /*fiscal year end on specific monthes*/
group by 
	a.sich, a.datadate_fundq;
quit;


/* create return window dates: mStart - mEnd */
data getb_1;
set f_delays; 
/* drop obs with missing estimation date */
if anndate ne .;
mStart=INTNX('Month',anndate, -12, 'E'); 
mEnd=INTNX('Month',anndate, -1, 'E'); 
if permno ne .;  
format mStart mEnd date.;
run;
  
/* get stock and market return */
proc sql;
  create table getb_2 as
  select a.*, b.date, b.ret, c.vwretd
  from  getb_1 a, crsp.msf b, crsp.msix c
  where a.mStart-5 <= b.date <= a.mEnd +5
  and a.permno = b.permno
  and missing(b.ret) ne 1
  and b.date = c.caldt;
quit;

/* force unique obs */  
proc sort data = getb_2 nodup;by key date;run;

proc reg outest=getb_3 data=getb_2;
   id key;
   model  ret = vwretd  / noprint EDF ;
   by key;
run;

/* create output dataset */
proc sql;
  create table g_beta as 
	select a.*, b.vwretd as beta 
	from getb_2 a left join getb_3 b on a.key=b.key and b._EDF_ > 10;
quit;

proc sql;
	create table g1 as
	select a.*, b.ret, c.vwretd, a.beta * c.vwretd as expected_ret /* assuming alpha of zero */
	from g_beta a, crsp.dsf b, crsp.dsix c
	where a.permno = b.permno
	and a.date = b.date
	and b.date = c.caldt
	and missing(b.ret) ne 1; 
quit;

/* sum abnret - thanks Lin */
proc sql;
	create table g2 as 
	select key, exp(sum(log(1+ret)))-exp(sum(log(1+expected_ret))) as abnret
	from g1 group by key;
quit;

/* create output dataset */
proc sql;
	create table h_car as
	select a.*, b.abnret as car
	from g1 a left join g2 b
	on a.key = b.key;
quit;

/*get delay decile */
proc rank data = h_car out=i_rank groups = 10;
var reporting_delay ; 		
ranks reporting_delay_d; 
run;

proc sort data = i_rank nodup;by reporting_delay_d key;run; 

proc reg data=i_rank;
id key;
by reporting_delay_d;
model car = unex;
run;



/*2. Measurement error in earnings surprise
Extend the class example (dataset with unexpected earnings based on
the last analyst forecast and earnings announcement stock return) .
As time passes between the measurement of the last analyst forecast 
and the earnings announcement, the forecast may be somewhat 'stale'. 
During this window, the 'real' expected earnings are updated, and 
reflected in the stock price, but not in the measured unexpected earnings. 
Create a control variable that measures the raw stock return in this window 
(i.e., between the forecast date and the day before the earnings announcement
event) and add this to the regression. Before running the regression: 
do you expect a positive or negative coefficient for this variable?

Note: you can either use firm-years or adjust the code to use firm-quarters*/

/*Before running the regression, I expect a negative coefficient for this variable*/

%macro getFunda(dsout=, vars=, laggedvars=, year1=2010, year2=2013);
%SYSLPUT dsout=&dsout;
%SYSLPUT vars=&vars;
%SYSLPUT laggedvars=&laggedvars;
%SYSLPUT year1=&year1;
%SYSLPUT year2=&year2;


rsubmit;
filename m1 url 'http://www.wrds.us/macros/array_functions.sas'; %include m1;
filename m2 url 'http://www.wrds.us/macros/runquit.sas'; %include m2;
/* Funda data */
data getf_1 (keep = key gvkey fyear datadate &vars);
set comp.funda;
if &year1 <= fyear <= &year2;
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
key = gvkey || fyear;
run;

/* 	Keep first record; */
proc sort data =getf_1 nodupkey; by gvkey fyear;run;

/* lagged assets */
%if "&laggedvars" ne "" %then %do;
	/* add lagged vars */
	proc sql;
		create table getf_2 as
select a.*, %do_over(values=&laggedvars, between=comma, phrase=b.? as ?_lag)
		from  getf_1 a left join  getf_1 b
		on a.gvkey = b.gvkey and a.fyear -1 = b.fyear;
	quit;
%end;
%else %do;
	/* do not add lagged vars */
	data getf_2; set getf_1; run;
%end;

/* Permno for adate*/
proc sql; 
  create table getf_3 as 
  select a.*, b.lpermno as permno
  from getf_2 a left join crsp.ccmxpf_linktable b 
    on a.gvkey eq b.gvkey 
    and b.lpermno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim IN ("C", "P")  
    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E)   ; 
quit; 

/* retrieve historic cusip */
proc sql;
  create table getf_4 as
  select a.*, b.ncusip
  from getf_3 a, crsp.dsenames b
  where 
        a.permno = b.PERMNO
    and b.namedt <= a.datadate <= b.nameendt
    and b.ncusip ne "";
  quit;
 
/* No duplicates*/
proc sort data=getf_4 nodupkey; by key;run;
 
/* get ibes ticker */
proc sql;
  create table &dsout as
  select distinct a.*, b.ticker as ibes_ticker
  from getf_4 a left join ibes.idsum b
  on 
        a.NCUSIP = b.CUSIP
    and a.datadate > b.SDATES ;

	quit;

/* No duplicates*/
proc sort data=&dsout nodupkey; by key;run;

/*clean up*/
proc datasets library=work; delete getf_1 - getf_4; quit;
endrsubmit;
%mend;










  
