rsubmit;endrsubmit;
%let wrds = wrds.wharton.upenn.edu 4016;options comamid = TCP remote=WRDS;
signon username=_prompt_;

/*1. Max value
Write a macro that adds the maximum value as an additional field to a dataset. 
The variables to consider are passed as an argument. Invoke the macro as 
%selectMax(dsin=a_funda, vars=sale at ceq, maxvar=myMax). This macro would add a variable myMax
and would set the value to the largest of sale, at and ceq.*/

proc datasets library = work kill; quit;
dm "log; clear;"; dm "out; clear;";
filename m1 url 'http://www.wrds.us/macros/array_functions.sas'; %include m1;
filename m2 url 'http://www.wrds.us/macros/runquit.sas'; %include m2;

%macro selectMax(dsin= , vars=, maxvar=);
data &dsin; set &dsin;
	&maxvar = max(of &vars);
	run;
%mend;

%selectMax(dsin=a_funda, vars=sale at ceq, maxvar=myMax);


/*2. %Array
Load the variables of a dataset into an Array 
(using Ted Clay's Array macro, not the native SAS array).
So, if some dataset has variables gvkey, fyear, datadate, 
roa and ceq, then these variable names need to be set in the array.
Then, use the %do_over macro to rename all the variables in the
dataset where '_new' is added to the variable name 
(for example, gvkey will be renamed to gvkey_new).
*/

/* Retrieve variables in Funda */
ods listing close; 
ods output variables  = varsFunda; 
proc datasets lib = work; contents data=a_funda; quit;run; 
ods output close; 
ods listing; 

/*push the column variable of dataset varsfunda into an array*/
%array (myarray, data = varsfinda, var = variable );
%put length of myArray: &myArrayN; 

&array = %array_new;






/*3. Unexpected earnings
Merge Funda and Fundq for a sample of firms 
(fiscal years 2012 through 2016), and compute
the earnings surprise (actual earnings minus 
analyst forecast), where the most recent analyst
forecast is used (last forecast issued before the
earnings announcement). Use IBES dataset 
'DET_EPSUS' 
https://wrds-web.wharton.upenn.edu/wrds/tools/variable.cfm?library_id=40&file_id=53681.*/

/*Merge*/
proc sql;
	create table temp1 as
	select a.*, b.*
	from &dsin a, &ibes. b
	where a.gvkey = b.gvkey
	and  2012 <= fyear <= 2016
quit;

%Let earnings_surprise = actual - value 
where max (anndats) <= anndats_act;



/*4. Worldwide Governance Indicators
Use Clay's do_over macro to import the worksheets
of the wgi dataset (see folder datasets). Create 
the output dataset in 'long' format, with variables
country code, year, the year and the various governance
variables (voiceandaccountability, political stability, etc).

You can use the following sample code as a starting point
(wrap this inside a macro to make the sheet property flexible).*/

proc import 
  OUT       = work.myData DATAFILE= "C:\temp\myData.xlsx" 
  DBMS      = xlsx REPLACE;
  SHEET     = "worksheet1"; 
  GETNAMES  = YES;
run;

%do_over(value = ....worksheetnames, macro = myImport);

/*funda/fundq: gvkey (cusip here not nice bcoz of the backfills; 
ccm: permno; dsenames cusip; ibes. idsum: ibes ticker. 
can use it from ibes and clay class file from last week. 
So do match funda with fundq first using gvkey and then match fundq with ibes using cusip*/ 

%do_over (value = 



