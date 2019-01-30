rsubmit;endrsubmit;
%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;


/*1. Stock return over window 									  				  		  				  *
* 	 compute yearly abnormal stock return (monthly firm return - monthly index return) over 2006-2009	  *
* 	 for firms in the financial industry (6000-6999). 						  				  			  *
* 	 Use the vwretd (value weighted) index return in MSIX. Abnormal stock return is the compounded monthly stock return - the compounded monthly index return.*/


/*downloading from wrds funda crsp comp msf to computer*/
proc sql;
	create table a_funda as
		select gvkey, fyear, datadate, sich, sale, ni, at
	  	from comp.funda 
  	where 		
		2006 <= fyear <= 2009	
		6000 <= sich <= 6999
	and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
quit;

/* drop potential duplicates*/
proc sort data = a_funda nodupkey; by gvkey fyear; run;

proc sql;
	create table stockm as
	select permno, date, ret,
	exp(sum(log(1+ret)))-1 as ret_comp from crsp.msf
	where 6000 <= hsiccd <= 6999
	and 2006 <= year(date) <= 2009
	and ret ne .
	group by permno, year(date)
;
quit;


/* CRSP.MSIX -- monthly index data */
proc sql;
	create table indexm as
	select caldt, vwretd, 
	exp(sum(log(1+vwretd)))-1 as reti_comp from crsp.msix
	where 2006 <= year(caldt) <= 2009
	and vwretd ne .
	group by year(caldt)
;
quit;

/* merge */
proc sql;
	create table abnormal as select
	a.*, b.*, a.ret_comp - b.reti_comp as abn_ret
	from stockkm a leftjoin indexm b
	on a.gvkey = b.gvkey 
	and on year(a.date) = year(b.caldt)
	and month(a.date) = month(b.caldt)
quit;
proc sort data=q1_abnormal; by permno date; run;

/*  2. Macro stock return -- Turn the code for part 1 into a macro                                          
Invoke with arguments: %getReturn(dsin=, dsout=, start=datadate, end=enddate)                        
where dsin already holds the financial firms and their permnos                                     
(gvkey, fyear, permno, sich, datadate, enddate) (generate enddate as enddate = datadate + 360;)*/

/* merge for permno */
proc sql; 
	create table ccMerged as 
	select a.*, b.lpermno as permno
	from funda a left join crsp.ccmxpf_linktable b 
    on a.gvkey = b.gvkey 
    and b.lpermno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim in ("C", "P")  
    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E)  
	; 
quit; 
/* MACRO part */
%macro getReturn(dsin=, dsout=, start=datadate, end=enddate, sich=);
data &dsout;
	set &dsin;
	if permno ne .;
	&end = &start + 360;
	format &end date9.;
	run;
/*the following steps are similar in Q1*/
proc sql;
	create table stockm as
	select permno, date, ret,
	exp(sum(log(1+ret)))-1 as ret_comp from crsp.msf
	where 6000 <= hsiccd <= 6999
	and 2006 <= year(date) <= 2009
	and ret ne .
	group by permno, year(date)
;
quit;

/* CRSP.MSIX -- monthly index data */
proc sql;
	create table indexm as
	select caldt, vwretd, 
	exp(sum(log(1+vwretd)))-1 as reti_comp from crsp.msix
	where 2006 <= year(caldt) <= 2009
	and vwretd ne .
	group by year(caldt)
;
quit;

/* merge */
proc sql;
	create table &dsout as select
	a.*, b.*, a.ret_comp - b.reti_comp as abn_ret
	from stockkm a leftjoin indexm b
	on a.gvkey = b.gvkey 
	and on year(a.date) = year(b.caldt)
	and month(a.date) = month(b.caldt)
quit;
proc sort data=q1_abnormal; by permno date; run;

%mend;


%getReturn(dsin=q2_data, dsout=q2_out, start=datadate, end=enddate);


/*3. Matching with CCM linktable					  				  		  				              *
* 	 - For a sample of firms from Comp.Funda, match on the CCM linktable using datadate (end of fyear)    *
*      to get permno. Use permno to collect stock return for the 12 months of the fiscal year.            *
*    - Then, repeat the above, but instead of matching with the CCM linktable with datadate,              *
*      use 12 end of months of the fiscal year to get to permno for each month.                           *
*      e.g. if the fiscal year end is December 31, 2010, then the end of months will be                   * 
            January 31, 2010, February 28, 2010, etc through December 31, 2010.   */

/* Remove missing permno from ccMerged in Q2*/
data ccMerged;
	set ccMerged;
	if permno ne .;
run;

/*Merge*/
create table q3_i as
	select a.*, b.date, b.ret
	from ccMerged a left join crsp.msf b
	on a.permno = b.permno
	and year(a.datadate) = year(b.date)
	and month(a.datadate) = month(b.date)
	and b.ret ne .
	and a.permno ne .
;
quit;

proc sort data=q3_i; by permno datadate; run;

/*Then repeat the above, but instead of matching with the CCM linktable with datadate, use 12 end of months of 
the fiscal year to get to permno for each month*/

data funda;
	set funda;
	do i = 0 to 11;
		monthdate = intnx('month', datadate, -i, 'E');
		output;
	end;
	format monthdate date9.;
run;

/* Create ccMerged for merge */
proc sql; 
	create table ccMerged as 
	select a.*, b.lpermno as permno
	from funda1 a left join crsp.ccmxpf_linktable b 
    on a.gvkey = b.gvkey 
    and b.lpermno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim in ("C", "P")  
    and ((a.monthdate >= b.LINKDT) or b.LINKDT eq .B) 
	and ((a.monthdate <= b.LINKENDDT) or b.LINKENDDT eq .E)  
	; 
quit; 

/* Remove missing permno */
data ccMerged;
	set ccMerged;
	if permno ne .;
run;

/* Merge */
proc sql;
	create table q3_2 as
	select a.*, b.date, b.ret
	from ccMerged a left join crsp.msf b
	on a.permno = b.permno
	and year(a.monthdate) = year(b.date)
	and month(a.monthdate) = month(b.date)
	and b.ret ne .
	and a.permno ne .
;
quit;

proc sort data=q3_2; by permno monthdate; run;
