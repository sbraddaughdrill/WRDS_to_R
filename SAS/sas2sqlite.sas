/*******************READ ME*********************************************
* - Macros communicate SQLite and SAS without ODBC -
*
* SAS VERSION:    9.1.3
* SQLITE VERSION: 3.7.4
* DATE:           14may2011
* AUTHOR:         hchao8@gmail.com
*
****************END OF READ ME******************************************/

****************(1) MODULE-BUILDING STEP********************************;
******(1.1) BUILD A MACRO FROM SAS TO SQLITE****************************;
%macro sas2sqlite(sastable = , path = , database = );
   /*****************************************************************
   *  MACRO:      sas2sqlite()
   *  GOAL:       output a dataset in SAS to a table in SQLite
   *  PARAMETERS: sastable  = dataset in SAS for SQLite
   *              path      = destinate file path for SQLite database
   *              database  = name of SQLite database
   *****************************************************************/
  /* %let sastable=Fund_hdr;
   %let path=C:\Research_temp;
   %let database=sas_to_sqlite_db.s3db;
*/

   %let outpath=&path.;
   %let outtxtfile=&sastable..txt;
    %let outfilepath=&outpath.\&outtxtfile.;

   proc export data=&sastable. outfile="&outfilepath." dbms=tab replace;
     putnames = no;
   run;

/*
   proc export data = &sastable outfile = "&path.\sas_2_sqlite2.txt" dbms = tab replace;
        putnames = no;
   run;
*/
	/*
   proc export data=&sastable. outfile="&path.\sas_2_sqlite.txt" dbms=tab replace;
   run;
*/

   ods listing close;
   ods output variables = _varlist;
   proc contents data = &sastable.; 
   run;
   proc sort data = _varlist;
      by num;
   run;

   data _tmp01; 
      set _varlist;
      if lowcase(type) = "num" then vartype = "real";
      else if lowcase(type) = "char" then vartype = "text";
   run;
   proc sql noprint;
      select trim(variable) ||" "|| trim(vartype) 
            into: table_value separated by ", "
      from _tmp01
   ;quit;

   proc sql;
      create table _tmp02 (string char(32767));
      insert into _tmp02  
      /*set string = ".stats on"*/
      set string = "create table sas_table(sas_table_value);"
      set string = '.separator "\t"'
      set string = ".import 'sas_path\sas_txt.txt' sas_table"
   ;quit;

   data _tmp03;
      set _tmp02;
      string = tranwrd(string, "sas_table_value", "&table_value.");
      string = tranwrd(string, "sas_table", "&sastable.");
      string = tranwrd(string, "sas_path", "&path.");
      string = tranwrd(string, "sas_txt", "&sastable.");
   run;
   data _null_;
     set _tmp03;
     file "&path.\&sastable..sql";
     put string;
   run;
   options noxsync noxwait;
   x "sqlite3 -init &path.\&sastable..sql &path.\&database.";

   proc datasets;
      delete _:;
   quit;
   ods listing;

%mend;
