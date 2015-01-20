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
******(1.2) BUILD A MACRO FROM SQLITE TO SAS***************************;
%macro sqlite2sas(sqlitetable = , path = , database = );
   /*****************************************************************
   *  MACRO:      sqlite2sas()
   *  GOAL:       output a table in SQLite to a dataset in SAS  
   *  PARAMETERS: sqlitetable  = table in SQLite for SAS
   *              path         = target file path for SQLite database
   *              database     = name of SQLite database
   *****************************************************************/
   proc sql;
      create table _tmp0 (string char(800));
      insert into _tmp0  
      set string = ".output 'output_path\sqlite_2_sas.txt' "
      set string = '.separator "\t" '
      set string = '.headers on'
      set string = 'select * from sqlite_table;'
      set string = '.output stdout'
   ;quit;
   data _tmp1;
      set _tmp0;
      string = tranwrd(string, "sqlite_table", "&sqlitetable");
      string = tranwrd(string, "output_path", "&path");
   run;
   data _null_;
     set _tmp1;
     file "&path\sas_2_sqlite.sql";
     put string;
   run;

   options noxsync noxwait;
   x "sqlite3 -init &path\sas_2_sqlite.sql &path\&database ";

   proc import datafile = "&path\sqlite_2_sas.txt" out = &sqlitetable 
               dbms = dlm replace;
     delimiter = '09'x;
     guessingrows = 10000;
   run;
   proc datasets nolist;
      delete _:;
   run;
%mend;
