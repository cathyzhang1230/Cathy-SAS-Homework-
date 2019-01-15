/*HW1 */

/*
1. Life-time industry sales
Compute the aggregate life-time sales for each firm in Funda. 
Then, aggregate by 4-digit SICH (historical industry code) to compute life-time sales for each industry. 
Drop industries with less than 20 firms in it.
*/

%let wrds = wrds-cloud.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;

rsubmit;
data myComp (keep = gvkey fyear sich sale prcc_f cik conm);
/*keep all variables for all questions in HW1*/
set comp.funda;
/* require fyear to be 2000 or after */
if fyear >= 2000;
if cmiss(of sale) eq 0;
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
run;
proc download data=myComp out=myComp;run;
endrsubmit;


proc sort data=myComp; by gvkey fyear ;run;

data lifesales;
set myComp (keep = gvkey fyear sale sich);
retain sum_sales;
by gvkey;
if first.gvkey then sum_sales = 0; 
sum_sales = sum_sales + sale; 
if last.gvkey then output;
run;


/* aggregate by 4-digit SICH to compute life-time sales for each industry. 
Drop industries with less than 20 firms in it.
*/

proc sort data=lifesales; by gvkey; run;
data firmsales;
set lifesales;
retain sumsale_firm;
by gvkey;
if first.gvkey then sumsale_firm = sum_sales;
/* only output last observation of each gvkey */
run;

/* life-time sales for each industry */
proc sort data=firmsales; by sich gvkey fyear; run;
data indsales;
set firmsales;
retain indsum;
by sich;
if first.sich then do;
	indsum = 0;
end;
indsum = indsum + sum_sales;
if missing(sich) then indsum = .;
run;

data sum_ind;
set indsales;
by sich gvkey;
firmnum + first.gvkey ;
if first.sich then firmnum = 1;
/* only output last observation of each industry */
* if last.sich then output;
run;

/* drop industries with < 20 firms*/
proc sort data = sum_ind; by sich descending firmnum; run;
data finalsum;
set sum_ind;
retain finalfirmnum finalindsum;
by sich;
if first.sich then do;
	finalfirmnum = firmnum;
	finalindsum = indsum;
end;
run;

data finalsum_more (keep = sich gvkey fyear sale sumsale_firm finalindsum finalfirmnum);
set finalsum;
if finalfirmnum < 20 then delete;
if missing(sich) then delete;   
run;


/*
2. Missing SICH
*/
proc sort data = mycomp; by fyear sich; run;
data missingind;
set mycomp (keep = gvkey sich fyear);
retain n nmiss;
by fyear;
if first.fyear then do;
	nmiss = 0;
	n = 0;
end;
if missing(sich) then nmiss = nmiss + 1;
n = n + 1;
run;

proc sort data = missingind; by fyear descending n; run;
data missind;
set missingind;
retain n_new nmiss_new;
by fyear;
if first.fyear then do;
	n_new = n;
	nmiss_new = nmiss;
end; 
run;

data m_ind1;
set missind;
missing = nmiss_new/n_new*100;
run;

data missingSICH;
set m_ind1 (keep = sich gvkey fyear missing);
run;
proc sort data=missingSICH nodupkey; by fyear missing; run;

/*
3. Pre-IPO data points
For each firm in Funda, count the number of years of data before prcc_f is non-missing. 
(For example, if a firm is added to Funda in 2004, and prcc_f only is available for the first time in 2006, 
then there are 2 years of data for that firm)

Then, give an overview of the frequency (how often 0 years missing, how often 1 year missing, etc).
*/

proc sort data=mycomp; by gvkey fyear; run;
data d3;
set mycomp (keep = sich gvkey fyear prcc_f);
retain nm n;
by gvkey;
if first.gvkey then do; 
	nm = 0;
	n = 0;
end;
n = n + 1;
if missing(prcc_f) then nm = nm + 1;
run;

proc sort data= d3; by gvkey descending nm; run;
data d3_2;
set d3;
retain nm_new;
by gvkey;
if first.gvkey then nm_new = nm;
if last.gvkey then output;
run;

proc sort data = d3_2; by nm_new; run;
proc means data = d3_2 noprint;
output out=q3_freq n= /autoname;
var nm_new;
by nm_new;
run;


/*4. Header variables*/
proc sort data=mycomp; by gvkey fyear; run;
data d4;
set mycomp (keep = sich gvkey fyear cik conm);;
n1 = 0;
n2 = 0;
by gvkey;
if cik ne lag(cik) then n1 = 1;
if conm ne lag(conm) then n2 = 1;
if first.gvkey then do;
	n1 = 0;
	n2 = 0;
end;
run;

proc means data=d4;
	var n1 n2;
run;


/*5. Wide to long*/
data somedata;
  input @01 id        1.
        @03 year2010  4.
		@08 year2011  4.
        @13 year2012  4.
		@18 year2013  4.
	;
datalines;
1 1870 1242 2022 1325
2 9822 3186 1505 8212
3      1221 4321 9120
4 4701 2323 3784
run;

data test;
set somedata;
value = year2010; year = 2010; output;
value = year2011; year = 2011; output;
value = year2012; year = 2012; output;
value = year2013; year = 2013; output;
run;







