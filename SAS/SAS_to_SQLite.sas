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
%let crspadatalibrary=crspa;
libname &crspadatalibrary. "H:\Research\Mutual_Funds\Data\CRSPA";
%let crspmfdatalibrary=crspmf;
libname &crspmfdatalibrary. "H:\Research\Mutual_Funds\Data\CRSP_MF";
%let trdatalibrary=tr;
libname &trdatalibrary. "H:\Research\Mutual_Funds\Data\TR";
%let mflinksdatalibrary=mflinks;
libname &mflinksdatalibrary. "H:\Research\Mutual_Funds\Data\MFLinks";
%let msddatalibrary=msd;
libname &msddatalibrary. "H:\Research\Mutual_Funds\Data\Morningstar_Direct";
%let secdatalibrary=sec;
libname &secdatalibrary. "H:\Research\Mutual_Funds\Data\SEC_Analytics";
%let textdatalibrary=text;
libname &secdatalibrary. "H:\Research\Mutual_Funds\Data\Text_Analysis";
*********************************************************************************;
*STEP 0: MOVE SAS DATA TO WORK DATA LIBRARY;
*********************************************************************************;
data Crspa_msi;
  set &crspadatalibrary..Crspa_msi;
run;
data Daily_returns;
  set &crspmfdatalibrary..Daily_returns;
run;
data Fund_fees;
  set &crspmfdatalibrary..Fund_fees;
run;
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
data Mdmf_data_raw;
  set &msddatalibrary..Mdmf_data_raw;
run;
*********************************************************************************;
*STEP 1: EXPORT FROM SAS TO SQLITE (METHOD 1);
*********************************************************************************;
%macro del_temp_files(sastable = , path = );
  filename sqlFile "&path\&sastable..sql";
  data _null_;
    rc = fdelete('sqlFile') ;
  run;
  filename sqlFile CLEAR ;

  filename txtFile "&path\&sastable..txt";
  data _null_;
    rc = fdelete('txtFile') ;
  run;
  filename txtFile CLEAR ;
%mend del_temp_files;

%sas2sqlite(sastable=Fund_hdr, path=C:\Research_temp, database=CRSPMF.s3db);
%sas2sqlite(sastable=Fund_hdr_hist, path=C:\Research_temp, database=CRSPMF.s3db);
%sas2sqlite(sastable=Fund_style, path=C:\Research_temp, database=CRSPMF.s3db);
%sas2sqlite(sastable=Fund_names, path=C:\Research_temp, database=CRSPMF.s3db);
%sas2sqlite(sastable=Fund_summary, path=C:\Research_temp, database=CRSPMF.s3db);
%sas2sqlite(sastable=Fund_summary2, path=C:\Research_temp, database=CRSPMF.s3db);
%sas2sqlite(sastable=Monthly_tna_ret_nav, path=C:\Research_temp, database=CRSPMF.s3db);
%del_temp_files(sastable=Fund_hdr, path=C:\Research_temp);
%del_temp_files(sastable=Fund_hdr_hist, path=C:\Research_temp);
%del_temp_files(sastable=Fund_style, path=C:\Research_temp);
%del_temp_files(sastable=Fund_names, path=C:\Research_temp);
%del_temp_files(sastable=Fund_summary, path=C:\Research_temp);
%del_temp_files(sastable=Fund_summary2, path=C:\Research_temp);
%del_temp_files(sastable=Monthly_tna_ret_nav, path=C:\Research_temp);
*********************************************************************************;
*STEP 1: EXPORT FROM SAS TO SQLITE (METHOD 2);
*********************************************************************************;
*Daily Returns Table;
%let tablenm=Daily_returns;
proc sql;
   connect to odbc (complete="dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\CRSPMF.s3db");
   execute(drop table IF EXISTS &tablenm.) by odbc;
   disconnect from odbc;
quit;
proc sql;
   connect to odbc (complete="dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\CRSPMF.s3db");
   execute(create table &tablenm. (crsp_fundno integer, caldt integer, dret double)) by odbc;
   disconnect from odbc;
quit;

%let crspmfdblibrary=crspmfdb;
libname &crspmfdblibrary. odbc complete = "dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\CRSPMF.s3db";
/*
libname &crspmfdblibrary. odbc complete = "dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\CRSPMF.s3db" 
                               preserve_col_names=yes 
                               connection=global
                               use_odbc_cl=yes;
*/
proc sql; 
    insert into &crspmfdblibrary..&tablenm.(crsp_fundno,caldt,dret) 
    select crsp_fundno,caldt,dret 
    from &tablenm.; 
 quit;
libname &crspmfdblibrary. clear ; * release the db  back to the system ;


**************************Daily Returns Table;
/*
proc contents data = Crspa_msi out = Crspa_msi_cols;
run;
*/
%let tablenm=Crspa_msi;
proc sql;
   connect to odbc (complete="dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\CRSPMF.s3db");
   execute(drop table IF EXISTS &tablenm.) by odbc;
   disconnect from odbc;
quit;
proc sql;
   connect to odbc (complete="dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\CRSPMF.s3db");
   execute(create table &tablenm. (CALDT integer, TOTVAL double, TOTCNT integer, USDVAL double, USDCNT integer, 
                                   SPRTRN double, SPINDX double, VWRETD double, VWRETX double, EWRETD double, 
                                   EWRETX double, VWINDD double, VWINDX double, EWINDD double, EWINDX double, 
                                   CAP1RET double, CAP2RET double, CAP3RET double, CAP4RET double, CAP5RET double, 
                                   CAP6RET double, CAP7RET double, CAP8RET double, CAP9RET double, CAP10RET double, 
                                   CAP1IND double, CAP2IND double, CAP3IND double, CAP4IND double, CAP5IND double, 
                                   CAP6IND double, CAP7IND double, CAP8IND double, CAP9IND double, CAP10IND double)) by odbc;
   disconnect from odbc;
quit;


%let crspmfdblibrary=crspmfdb;
libname &crspmfdblibrary. odbc complete = "dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\CRSPMF.s3db";
proc sql; 
    insert into &crspmfdblibrary..&tablenm.(CALDT, TOTVAL, TOTCNT, USDVAL, USDCNT, SPRTRN, SPINDX, VWRETD, VWRETX, 
                                            EWRETD, EWRETX, VWINDD, VWINDX, EWINDD, EWINDX, 
                                            CAP1RET, CAP2RET, CAP3RET, CAP4RET, CAP5RET, CAP6RET, CAP7RET, CAP8RET, CAP9RET, CAP10RET, 
                                            CAP1IND, CAP2IND, CAP3IND, CAP4IND, CAP5IND, CAP6IND, CAP7IND, CAP8IND, CAP9IND, CAP10IND) 
    select CALDT, TOTVAL, TOTCNT, USDVAL, USDCNT, SPRTRN, SPINDX, VWRETD, VWRETX, 
           EWRETD, EWRETX, VWINDD, VWINDX, EWINDD, EWINDX, 
           CAP1RET, CAP2RET, CAP3RET, CAP4RET, CAP5RET, CAP6RET, CAP7RET, CAP8RET, CAP9RET, CAP10RET, 
           CAP1IND, CAP2IND, CAP3IND, CAP4IND, CAP5IND, CAP6IND, CAP7IND, CAP8IND, CAP9IND, CAP10IND
    from &tablenm.; 
 quit;
libname &crspmfdblibrary. clear ; * release the db  back to the system ;

**************************Morningstar Direct Table;
/*
proc contents data = Mdmf_data_raw out = Mdmf_data_raw_cols;
run;
*/
%let tablenm=Mdmf_data_raw;
proc sql;
   connect to odbc (complete="dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\MDMF.s3db");
   execute(drop table IF EXISTS &tablenm.) by odbc;
   disconnect from odbc;
quit;
proc sql;
   connect to odbc (complete="dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\MDMF.s3db");
   execute(create table &tablenm. (Name varchar(45), Ticker varchar(12), Broad_Cat_Group varchar(22), Global_Cat varchar(41), 
                                   MS_Cat varchar(35), MS_Inst_Cat varchar(45), MS_Rating_Overall integer, US_Broad_Asset_Class varchar(32), 
                                   Domicile varchar(14), Region_of_Sale varchar(21), Equity_Style_Box_Long varchar(31), 
                                   Fixed_Inc_Style_Box_Long varchar(35), MS_Anal_Rating varchar(22), MS_Anal_Rating_Process_Pillar varchar(43), 
                                   MS_Anal_Rating_Perform_Pillar varchar(43), MS_Anal_Rating_People_Pillar varchar(41), 
                                   MS_Anal_Rating_Parent_Pillar varchar(40), MS_Anal_Rating_Price_Pillar varchar(39), 
                                   Global_Fund_Report_Anal_Date integer, Global_Fund_Report_Availability varchar(43), 
                                   Firm_Name varchar(44), Branding_Name varchar(46), Inception_Date integer, 
                                   Manager_Name varchar(558), Manager_History varchar(3139), Manager_Ownership_Level varchar(925), 
                                   Manager_Tenure_Longest double, Manager_Tenure_Average double, Prim_Prosp_Benchmark varchar(251), 
                                   Prim_Prosp_Benchmark_Id varchar(36), Prim_Prosp_Benchmark_Incep_Date integer, Net_Assets_Date integer, 
                                   Net_Assets_Share_Class_Base_Curr integer, Fund_Size_Date integer, Fund_Size_Base_Currency integer, 
                                   Shares_Outstanding_Date integer, Shares_Outstanding integer, Holding_of_an_Investment varchar(35), 
                                   Oldest_Share_Class varchar(27), _12_Mo_Yield_Date integer, _12_Mo_Yield double, Latest_Div_Date integer, 
                                   Latest_Div_Base_Curr integer, Latest_Div_NAV_Base_Curr integer, Latest_Cap_Gain_Date integer, 
                                   Latest_Cap_Gain_Base_Curr double, Latest_Cap_Gain_NAV_Base_Curr double, Latest_Cap_Gain_Date_LT integer, 
                                   Latest_Cap_Gain_LT_Base_Curr double, Latest_Cap_Gain_Date_MT integer, Latest_Cap_Gain_MT_Base_Curr double, 
                                   Latest_Cap_Gain_Date_ST integer, Latest_Cap_Gain_ST_Base_Curr double, Latest_ROC_Date integer, 
                                   Latest_ROC_Base_Currency double, Portfolio_Date integer, Return_Date_Daily integer, Base_Currency varchar(20), 
                                   Virtual_Class varchar(18), NAV_Daily_Base_Currency double, Tot_Ret_1_Day_D_Base_Curr double, 
                                   Tot_Ret_YTD_D_Base_Curr double, Tot_Ret_1Yr_D_Base_Curr double, Tot_Ret_Annlzd_2Yr_D_Base_Curr double, 
                                   Tot_Ret_Annlzd_3Yr_D_Base_Curr double, Tot_Ret_Annlzd_5Yr_D_Base_Curr double, Management_Fee double, 
                                   Annual_Report_Net_Exp_Ratio double, Annual_Report_Gross_Exp_Ratio double, Prospectus_Net_Expense_Ratio double, 
                                   Prospectus_Gross_Expense_Ratio double, Prospectus_Objective varchar(33), Turnover_Ratio_Percent double, 
                                   Unannualized varchar(18), Share_Class_Type varchar(24), SecId varchar(16), PerformanceId varchar(20), 
                                   FundId varchar(16), CUSIP varchar(13), Morningstar_Page integer, Analysis_Date integer, 
                                   Performance_Data_Ready varchar(34), Price_Data_Ready varchar(24), Operations_Data_Ready varchar(32), 
                                   Portfolio_Data_Ready varchar(29), Asset_Change_Date integer, MS_Category_Start_Date integer, 
                                   Note_Effective_Date varchar(28), Primary_Share_in_GIFS_Class varchar(75), Target_Date_Report_Date integer, 
                                   Investment_Strategy varchar(1115), Prospectus_Objective_1 varchar(33))) by odbc;
   disconnect from odbc;
quit;

%let msddblibrary=msddb;
libname &msddblibrary. odbc complete = "dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\MDMF.s3db";
proc sql; 
    insert into &msddblibrary..&tablenm.(Name, Ticker, Broad_Cat_Group, Global_Cat, MS_Cat, MS_Inst_Cat, MS_Rating_Overall, 
                                         US_Broad_Asset_Class, Domicile, Region_of_Sale, Equity_Style_Box_Long, Fixed_Inc_Style_Box_Long, 
                                         MS_Anal_Rating, MS_Anal_Rating_Process_Pillar, MS_Anal_Rating_Perform_Pillar, MS_Anal_Rating_People_Pillar, 
                                         MS_Anal_Rating_Parent_Pillar, MS_Anal_Rating_Price_Pillar, Global_Fund_Report_Anal_Date, 
                                         Global_Fund_Report_Availability, Firm_Name, Branding_Name, Inception_Date, Manager_Name, 
                                         Manager_History, Manager_Ownership_Level, Manager_Tenure_Longest, Manager_Tenure_Average, 
                                         Prim_Prosp_Benchmark, Prim_Prosp_Benchmark_Id, Prim_Prosp_Benchmark_Incep_Date, Net_Assets_Date, 
                                         Net_Assets_Share_Class_Base_Curr, Fund_Size_Date, Fund_Size_Base_Currency, Shares_Outstanding_Date, 
                                         Shares_Outstanding, Holding_of_an_Investment, Oldest_Share_Class, _12_Mo_Yield_Date, _12_Mo_Yield, 
                                         Latest_Div_Date, Latest_Div_Base_Curr, Latest_Div_NAV_Base_Curr, Latest_Cap_Gain_Date, 
                                         Latest_Cap_Gain_Base_Curr, Latest_Cap_Gain_NAV_Base_Curr, Latest_Cap_Gain_Date_LT, 
                                         Latest_Cap_Gain_LT_Base_Curr, Latest_Cap_Gain_Date_MT, Latest_Cap_Gain_MT_Base_Curr, 
                                         Latest_Cap_Gain_Date_ST, Latest_Cap_Gain_ST_Base_Curr, Latest_ROC_Date, Latest_ROC_Base_Currency, 
                                         Portfolio_Date, Return_Date_Daily, Base_Currency, Virtual_Class, NAV_Daily_Base_Currency, 
                                         Tot_Ret_1_Day_D_Base_Curr, Tot_Ret_YTD_D_Base_Curr, Tot_Ret_1Yr_D_Base_Curr, 
                                         Tot_Ret_Annlzd_2Yr_D_Base_Curr, Tot_Ret_Annlzd_3Yr_D_Base_Curr, Tot_Ret_Annlzd_5Yr_D_Base_Curr, 
                                         Management_Fee, Annual_Report_Net_Exp_Ratio, Annual_Report_Gross_Exp_Ratio, Prospectus_Net_Expense_Ratio, 
                                         Prospectus_Gross_Expense_Ratio, Prospectus_Objective, Turnover_Ratio_Percent, Unannualized, 
                                         Share_Class_Type, SecId, PerformanceId, FundId, CUSIP, Morningstar_Page, Analysis_Date, 
                                         Performance_Data_Ready, Price_Data_Ready, Operations_Data_Ready, Portfolio_Data_Ready, 
                                         Asset_Change_Date, MS_Category_Start_Date, Note_Effective_Date, Primary_Share_in_GIFS_Class, 
                                         Target_Date_Report_Date, Investment_Strategy, Prospectus_Objective_1) 

    select Name, Ticker, Broad_Cat_Group, Global_Cat, MS_Cat, MS_Inst_Cat, MS_Rating_Overall, US_Broad_Asset_Class, Domicile, Region_of_Sale, 
           Equity_Style_Box_Long, Fixed_Inc_Style_Box_Long, MS_Anal_Rating, MS_Anal_Rating_Process_Pillar, MS_Anal_Rating_Perform_Pillar, 
           MS_Anal_Rating_People_Pillar, MS_Anal_Rating_Parent_Pillar, MS_Anal_Rating_Price_Pillar, Global_Fund_Report_Anal_Date, 
           Global_Fund_Report_Availability, Firm_Name, Branding_Name, Inception_Date, Manager_Name, Manager_History, Manager_Ownership_Level, 
           Manager_Tenure_Longest, Manager_Tenure_Average, Prim_Prosp_Benchmark, Prim_Prosp_Benchmark_Id, Prim_Prosp_Benchmark_Incep_Date, 
           Net_Assets_Date, Net_Assets_Share_Class_Base_Curr, Fund_Size_Date, Fund_Size_Base_Currency, Shares_Outstanding_Date, Shares_Outstanding, 
           Holding_of_an_Investment, Oldest_Share_Class, _12_Mo_Yield_Date, _12_Mo_Yield, Latest_Div_Date, Latest_Div_Base_Curr, 
           Latest_Div_NAV_Base_Curr, Latest_Cap_Gain_Date, Latest_Cap_Gain_Base_Curr, Latest_Cap_Gain_NAV_Base_Curr, Latest_Cap_Gain_Date_LT, 
           Latest_Cap_Gain_LT_Base_Curr, Latest_Cap_Gain_Date_MT, Latest_Cap_Gain_MT_Base_Curr, Latest_Cap_Gain_Date_ST, Latest_Cap_Gain_ST_Base_Curr, 
           Latest_ROC_Date, Latest_ROC_Base_Currency, Portfolio_Date, Return_Date_Daily, Base_Currency, Virtual_Class, NAV_Daily_Base_Currency, 
           Tot_Ret_1_Day_D_Base_Curr, Tot_Ret_YTD_D_Base_Curr, Tot_Ret_1Yr_D_Base_Curr, Tot_Ret_Annlzd_2Yr_D_Base_Curr, Tot_Ret_Annlzd_3Yr_D_Base_Curr, 
           Tot_Ret_Annlzd_5Yr_D_Base_Curr, Management_Fee, Annual_Report_Net_Exp_Ratio, Annual_Report_Gross_Exp_Ratio, Prospectus_Net_Expense_Ratio, 
           Prospectus_Gross_Expense_Ratio, Prospectus_Objective, Turnover_Ratio_Percent, Unannualized, Share_Class_Type, SecId, PerformanceId, FundId, 
           CUSIP, Morningstar_Page, Analysis_Date, Performance_Data_Ready, Price_Data_Ready, Operations_Data_Ready, Portfolio_Data_Ready, 
           Asset_Change_Date, MS_Category_Start_Date, Note_Effective_Date, Primary_Share_in_GIFS_Class, Target_Date_Report_Date, Investment_Strategy, 
           Prospectus_Objective_1
    from &tablenm.; 
 quit;
libname &msddblibrary. clear ; * release the db  back to the system ;




/*
proc sql;
   connect to odbc (complete="dsn=SQLiteODBC;Driver={SQLite3 ODBC Driver};Database=C:\Research_temp\CRSPMF.s3db");
   execute(create table Dr2 (Name varchar(8), sex varchar(1),Age double, height double, weight double)) by odbc;
   disconnect from odbc;
quit;
*/
 

data &crspmfdblibrary..Daily_returns;
  set Daily_returns;
run;
data Fund_fees;
  set &crspmfdatalibrary..Fund_fees;
run;
data &crspmfdblibrary..Fund_hdr;
  set Fund_hdr;
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
data &crspmfdblibrary..Monthly_tna_ret_nav;
  set Monthly_tna_ret_nav;
run;


*********************************************************************************;
*STEP 1: EXPORT FROM SAS TO SQLITE (METHOD 3);
*********************************************************************************;
proc export data=Fund_hdr outfile= "C:\Research_temp\Fund_hdr.csv" DBMS=CSV REPLACE;
run;
proc export data=Fund_hdr_hist outfile= "C:\Research_temp\Fund_hdr_hist.csv" DBMS=CSV REPLACE;
run;
proc export data=Fund_style outfile= "C:\Research_temp\Fund_style.csv" DBMS=CSV REPLACE;
run;
proc export data=Fund_names outfile= "C:\Research_temp\Fund_names.csv" DBMS=CSV REPLACE;
run;
proc export data=Fund_summary outfile= "C:\Research_temp\Fund_summary.csv" DBMS=CSV REPLACE;
run;
proc export data=Fund_summary2 outfile= "C:\Research_temp\Fund_summary2.csv" DBMS=CSV REPLACE;
run;
proc export data=Monthly_tna_ret_nav outfile= "C:\Research_temp\Monthly_tna_ret_nav.csv" DBMS=CSV REPLACE;
run;

*********************************************************************************;
*STEP 2: IMPORT TO SAS FROM SQLITE;
*********************************************************************************;
%sqlite2sas(sqlitetable = Fund_hdr, path = C:\Research_temp, database = sas_to_sqlite_db.s3db);

