library(data.table)
library(foreign)
library(Hmisc)
library(sas7bdat)
library(sqldf)
library(RSQLite)

#input_directory <- normalizePath("H:/Research/Import_Data/Data/CRSP_MF/",winslash = "\\", mustWork = NA)
input_directory <- normalizePath("C:/Users/bdaughdr/Documents/temp_out/",winslash = "\\", mustWork = NA)

#output_directory <- normalizePath("C:/Research_temp/",winslash = "\\", mustWork = NA)
output_directory <- normalizePath("C:/Users/bdaughdr/Documents/temp_out/",winslash = "\\", mustWork = NA)

#function_directory <- normalizePath("C:/Users/Brad/Dropbox/Research/R/",winslash = "\\", mustWork = NA)                     #HOME
#function_directory <- normalizePath("C:/Users/bdaughdr/Dropbox/Research/R/",winslash = "\\", mustWork = NA)                 #WORK
function_directory <- normalizePath("//tsclient/F/Dropbox/Research_Methods/R/",winslash = "\\", mustWork = NA)                 #WORK

source(file=paste(function_directory,"functions_db.R",sep=""),echo=FALSE)
source(file=paste(function_directory,"functions_utilities.R",sep=""),echo=FALSE)

#########################################################
# From .csv
#########################################################

crsp_out_db <- paste(output_directory,"CRSPMF2.s3db",sep="")

#driver <- dbDriver("SQLite")
#connect <- dbConnect(driver, dbname=crsp_db);

#Import Fund_hdr tables

#import_file <- paste(output_directory,"Fund_hdr.csv",sep="")
#read.csv.sql(import_file, sql="create table Fund_hdr as select * from file",
#             dbname=crsp_db,header=TRUE,stringsAsFactors=FALSE,row.names=FALSE)

#dbWriteTable(conn=connect,name="Fund_hdr", value=import_file,row.names=FALSE, header=TRUE)

#close_open_res(connect)
#dbDisconnect(connect)
#dbUnloadDriver(driver)

#Crspa_msi <- as.data.frame(fread(paste(output_directory,"Crspa_msi.csv",sep=""),na.strings="NA",stringsAsFactors=FALSE),stringsAsFactors=FALSE)
Crspa_msi <- read.csv(file=paste(output_directory,"Crspa_msi.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Crspa_msi",Crspa_msi)
rm(Crspa_msi)
capture.output(gc(),file='NUL')

#Daily_returns <- read.csv(file=paste(output_directory,"Daily_returns.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
#ExportTable(crsp_out_db,"Daily_returns",Daily_returns)
#rm(Daily_returns)
#capture.output(gc(),file='NUL')

#Fund_fees <- as.data.frame(fread(paste(output_directory,"Fund_fees.csv",sep=""),na.strings="NA",stringsAsFactors=FALSE),stringsAsFactors=FALSE)
Fund_fees <- read.csv(file=paste(output_directory,"Fund_fees.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Fund_fees",Fund_fees)
rm(Fund_fees)
capture.output(gc(),file='NUL')

#Fund_hdr <- as.data.frame(fread(paste(output_directory,"Fund_hdr.csv",sep=""),na.strings="NA",stringsAsFactors=FALSE),stringsAsFactors=FALSE)
Fund_hdr <- read.csv(file=paste(output_directory,"Fund_hdr.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Fund_hdr",Fund_hdr)
rm(Fund_hdr)
capture.output(gc(),file='NUL')

Fund_hdr_hist <- read.csv(file=paste(output_directory,"Fund_hdr_hist.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Fund_hdr_hist",Fund_hdr_hist)
rm(Fund_hdr_hist)
capture.output(gc(),file='NUL')

Fund_names <- read.csv(file=paste(output_directory,"Fund_names.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Fund_names",Fund_names)
rm(Fund_names)
capture.output(gc(),file='NUL')

Fund_style <- read.csv(file=paste(output_directory,"Fund_style.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Fund_style",Fund_style)
rm(Fund_style)
capture.output(gc(),file='NUL')

Fund_summary <- read.csv(file=paste(output_directory,"Fund_summary.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Fund_summary",Fund_summary)
rm(Fund_summary)
capture.output(gc(),file='NUL')

Fund_summary2 <- read.csv(file=paste(output_directory,"Fund_summary2.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Fund_summary2",Fund_summary2)
rm(Fund_summary2)
capture.output(gc(),file='NUL')

Monthly_tna_ret_nav <- read.csv(file=paste(output_directory,"Monthly_tna_ret_nav.csv",sep=""),header=TRUE,na.strings="NA",stringsAsFactors=FALSE)
ExportTable(crsp_out_db,"Monthly_tna_ret_nav",Monthly_tna_ret_nav)
rm(Monthly_tna_ret_nav)
capture.output(gc(),file='NUL')

crsp_out_db_tables <- ListTables(crsp_out_db)
crsp_out_db_fields <- ListFields(crsp_out_db)

#########################################################
# From .sas7bdat
#########################################################

crsp_out_db2 <- paste(output_directory,"CRSPMF3.s3db",sep="")

#fund_names1 <- read.sas7bdat(paste(output_directory,"fund_names.sas7bdat",sep=""))

directory <- "H:/Research/Mutual_Funds/Data/CRSP_MF"
sashome <- "/Program Files/SASHome/SASFoundation/9.3"

#tbl <- read.ssd("C:/Research_temp", "fund_names",tmpXport=tempfile(), tmpProgLoc=tempfile(),sascmd = file.path(sashome, "sas.exe"))

#Daily_returns <- sas.get(libraryName=directory, member="Daily_returns", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
#Daily_returns <- unfactorize2(Daily_returns)
#ExportTable(crsp_out_db2,"Daily_returns",Daily_returns)
#rm(Daily_returns)
#capture.output(gc(),file='NUL')

Fund_fees <- sas.get(libraryName=directory, member="Fund_fees", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
Fund_fees <- unfactorize2(Fund_fees)
ExportTable(crsp_out_db2,"Fund_fees",Fund_fees)
rm(Fund_fees)
capture.output(gc(),file='NUL')

Fund_hdr <- sas.get(libraryName=directory, member="Fund_hdr", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
Fund_hdr <- unfactorize2(Fund_hdr)
ExportTable(crsp_out_db2,"Fund_hdr",Fund_hdr)
rm(Fund_hdr)
capture.output(gc(),file='NUL')

Fund_hdr_hist <- sas.get(libraryName=directory, member="Fund_hdr_hist", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
Fund_hdr_hist <- unfactorize2(Fund_hdr_hist)
ExportTable(crsp_out_db2,"Fund_hdr_hist",Fund_hdr_hist)
rm(Fund_hdr_hist)
capture.output(gc(),file='NUL')

Fund_names <- sas.get(libraryName=directory, member="Fund_names", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
Fund_names <- unfactorize2(Fund_names)
ExportTable(crsp_out_db2,"Fund_names",Fund_names)
rm(Fund_names)
capture.output(gc(),file='NUL')

Fund_style <- sas.get(libraryName=directory, member="Fund_style", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
Fund_style <- unfactorize2(Fund_style)
ExportTable(crsp_out_db2,"Fund_style",Fund_style)
rm(Fund_style)
capture.output(gc(),file='NUL')

Fund_summary <- sas.get(libraryName=directory, member="Fund_summary", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
Fund_summary <- unfactorize2(Fund_summary)
ExportTable(crsp_out_db2,"Fund_summary",Fund_summary)
rm(Fund_summary)
capture.output(gc(),file='NUL')

Fund_summary2 <- sas.get(libraryName=directory, member="Fund_summary2", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
Fund_summary2 <- unfactorize2(Fund_summary2)
ExportTable(crsp_out_db2,"Fund_summary2",Fund_summary2)
rm(Fund_summary2)
capture.output(gc(),file='NUL')

Monthly_tna_ret_nav <- sas.get(libraryName=directory, member="Monthly_tna_ret_nav", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
Monthly_tna_ret_nav <- unfactorize2(Monthly_tna_ret_nav)
ExportTable(crsp_out_db2,"Monthly_tna_ret_nav",Monthly_tna_ret_nav)
rm(Monthly_tna_ret_nav)
capture.output(gc(),file='NUL')

crsp_out_db2_tables <- ListTables(crsp_out_db2)
crsp_out_db2_fields <- ListFields(crsp_out_db2)