*=====================================================================;
*PROGRAM DETAILS;
*=====================================================================;
*Program: 	SAS_to_SQLite.sas;
*Version: 	1.0;
*Author:    S. Brad Daughdrill;
*Date:		03.19.2012;
*Purpose:	Export and Import of WRDS tables to SAS and SQLite;
*=====================================================================;
*MACROS USED;
*=====================================================================;
*-------------------------------Name----------------------------------;
* %sas2sqlite(sastable=,path=,database=);
* %sqlite2sas(sqlitetable=,path=,database=);
*=====================================================================;
*Sets initial options (clears output window and log, sets linesize,etc.);
*=====================================================================;
title;
footnote;

options ls=102;
options ps=80;
options label;
options nocenter;

ods results off;
ods listing close; 

*options nosource;    *no echo statements;
options source;      *echo statements;

options nosource2;   *no echo includes;
options nomfile;
options nospool;

/*
options source2;     *echo includes;
options mfile;
options spool;
*/

/*
options symbolgen;   *macro debugging;
options macrogen;    *macro debugging;
options mlogic;      *macro debugging;
options mprint;      *echo macro text;
options notes;       *time-used notes;
*/

options nosymbolgen; *no macro debugging;
options nomacrogen;  *no macro debugging;
options nomlogic;    *no macro debugging;
options nomprint;    *no echo macro text;
options nonotes;     *no time-used notes;

*=====================================================================;
*Delete Macro Variables;
*=====================================================================;
%include "C:\Users\bdaughdr\Dropbox\Research\SAS\Macros\Brad\DeleteMacros.sas"; *Office;
*%include "\\tsclient\C\Users\bdaughdr\Dropbox\Research\SAS\Macros\Brad\DeleteMacros.sas"; *CoralSea from Office;
*%include "\\tsclient\C\Users\Brad\Dropbox\Research\SAS\Macros\Brad\DeleteMacros.sas";  *CoralSea from Home;
%DeleteMacros;
*=====================================================================;
*Create Macro Directory;
*=====================================================================;
%global fullmacrodirectory;
%let fullmacrodirectory=C:\Users\bdaughdr\Dropbox\Research\SAS\Macros; *Office;
*%let fullmacrodirectory=\\tsclient\C\Users\bdaughdr\Dropbox\Research\SAS\Macros; *CoralSea from Office;
*%let fullmacrodirectory=\\tsclient\C\Users\Brad\Dropbox\Research\SAS\Macros;  *CoralSea from Home;
*=====================================================================;
*Create Local Directories;
*=====================================================================;
*%symdel projectrootdirectory;
%global projectrootdirectory;
%let projectrootdirectory=C:\Users\bdaughdr\Dropbox\Research\3rd-Year_Paper\SAS;
*=====================================================================;
*Macro includes;
*=====================================================================;
*%include "&fullmacrodirectory.\IncludeMacros.sas";
*%IncludeMacros (projfolder=&projectrootdirectory.,researchfolder=&fullmacrodirectory.);
%include "&projectrootdirectory.\sas2sqlite.sas"; *Office;
%include "&projectrootdirectory.\sqlite2sas.sas"; *Office;
*=====================================================================;
*Clear output window and log;
*=====================================================================;
DM 'clear output';    	
DM 'clear log';
DM 'GRAPH; CANCEL;'; 

proc greplay igout=work.gseg nofs;
	delete _all_;
	run;
quit;
proc datasets lib=work nolist; delete vars ; quit; run; 

*Clear results viewer;
ods html close; 
*ods html;

*ods listing close;
ods listing;

*=====================================================================;
*Define log and output path;
*=====================================================================;
Proc printto log="&projectrootdirectory.\mylog.log";
run;
proc printto print="&projectrootdirectory.\myoutput.lst";
run;
Proc printto log=log;
run;
Proc printto print=print;
run;
*=====================================================================;
*Define Global Macro Variables;
*=====================================================================;
*%GlobalMacroVars;
*=====================================================================;
*Assign local libnames so that datasets can be viewed in SAS Explorer;
*=====================================================================;
%let crspmfdatalibrary=crspmf;
libname &crspmfdatalibrary. "H:\Research\Mutual_Funds\Data\CRSP_MF";
%let trdatalibrary=tr;
libname &trdatalibrary. "H:\Research\Mutual_Funds\Data\TR";
%let mflinksdatalibrary=mflinks;
libname &mflinksdatalibrary. "H:\Research\Mutual_Funds\Data\MFLinks";
%let secdatalibrary=sec;
libname &secdatalibrary. "H:\Research\Mutual_Funds\Data\SEC_Analytics";
%let textdatalibrary=text;
libname &secdatalibrary. "H:\Research\Mutual_Funds\Data\Text_Analysis";
*********************************************************************************;
*STEP 0: MOVE SAS DATA TO WORK DATA LIBRARY;
*********************************************************************************;
data Fund_hdr;
  set &crspmfdatalibrary..Fund_hdr;
run;
data Fund_hdr_hist;
  set &crspmfdatalibrary..Fund_hdr_hist;
run;
data Fund_style;
  set &crspmfdatalibrary..Fund_style;
run;
data Fund_names;
  set &crspmfdatalibrary..Fund_names;
run;
data Fund_summary;
  set &crspmfdatalibrary..Fund_summary;
run;
data Fund_summary2;
  set &crspmfdatalibrary..Fund_summary2;
run;
data Monthly_tna_ret_nav;
  set &crspmfdatalibrary..Monthly_tna_ret_nav;
run;
*********************************************************************************;
*STEP 1: EXPORT FROM SAS TO SQLITE;
*********************************************************************************;
%let path=C:\Research_temp;
proc export data=Fund_hdr file="&path.\Fund_hdr.db" DBMS=DB REPLACE;
run;
