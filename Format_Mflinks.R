library(gdata)
library(sqldf)
library(RSQLite)

setwd("C:/Research_temp/")     
output_directory <- normalizePath("C:/Research_temp/",winslash = "\\", mustWork = NA)

#function_directory <- normalizePath("C:/Users/Brad/Dropbox/Research/R/",winslash = "\\", mustWork = NA)                     #HOME
function_directory <- normalizePath("C:/Users/bdaughdr/Dropbox/Research/R/",winslash = "\\", mustWork = NA)                 #WORK

source(file=paste(function_directory,"functions_db.R",sep=""),echo=FALSE)
source(file=paste(function_directory,"functions_utilities.R",sep=""),echo=FALSE)

in_db <- paste(output_directory,"MFLinks.s3db",sep="")
out_db <- paste(output_directory,"MFLinks_Formatted.s3db",sep="")

in_tables <- ListTables(in_db)
in_fields <- ListFields(in_db)

###Note this approach imports the .sas7bdat files that were obtained with sas.get from the Import_Mflinks.R file
### Important because of the formats used

####################################################
#Import mfl_exceptions Data
####################################################

mfl_exceptions_fields <- in_fields[in_fields[,1]=="mfl_exceptions",]
mfl_exceptions <- runsql("SELECT * FROM mfl_exceptions",in_db)

for (i in 1:ncol(mfl_exceptions))
{
  mfl_exceptions[,i] <- unknownToNA(mfl_exceptions[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  mfl_exceptions[,i] <- ifelse(is.na(mfl_exceptions[,i]),NA, mfl_exceptions[,i])
} 

ExportTable(out_db,"mfl_exceptions",mfl_exceptions)
rm(mfl_exceptions)
capture.output(gc(),file='NUL')


####################################################
#Import mflink1 Data
####################################################

mflink1_fields <- in_fields[in_fields[,1]=="mflink1",]
mflink1 <- runsql("SELECT * FROM mflink1",in_db)

mflink1_num_to_pad_cols <- c("crsp_fundno")
for (i in 1:length(mflink1_num_to_pad_cols))
{
  mflink1[,mflink1_num_to_pad_cols[i]] <- paste("", formatC(as.integer(mflink1[,mflink1_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  mflink1[,mflink1_num_to_pad_cols[i]] <- trim(mflink1[,mflink1_num_to_pad_cols[i]])
} 

for (i in 1:ncol(mflink1))
{
  mflink1[,i] <- unknownToNA(mflink1[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  mflink1[,i] <- ifelse(is.na(mflink1[,i]),NA, mflink1[,i])
} 

ExportTable(out_db,"mflink1",mflink1)
rm(mflink1_num_to_pad_cols,mflink1)
capture.output(gc(),file='NUL')


####################################################
#Import mflink2 Data
####################################################

mflink2_fields <- in_fields[in_fields[,1]=="mflink2",]
mflink2 <- runsql("SELECT * FROM mflink2",in_db)

mflink2_num_to_date_cols <- c("sdate1","sdate2")
for (i in 1:length(mflink2_num_to_date_cols))
{
  mflink2[,mflink2_num_to_date_cols[i]] <- as.character(as.Date(as.integer(mflink2[,mflink2_num_to_date_cols[i]]), origin="1970-01-01"))
  mflink2[,mflink2_num_to_date_cols[i]] <- ifelse(mflink2[,mflink2_num_to_date_cols[i]]=="0",NA, mflink2[,mflink2_num_to_date_cols[i]])
  mflink2[,mflink2_num_to_date_cols[i]] <- trim(mflink2[,mflink2_num_to_date_cols[i]])
} 

for (i in 1:ncol(mflink2))
{
  mflink2[,i] <- unknownToNA(mflink2[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  mflink2[,i] <- ifelse(is.na(mflink2[,i]),NA, mflink2[,i])
} 

ExportTable(out_db,"mflink2",mflink2)
rm(mflink2_num_to_date_cols,mflink2)
capture.output(gc(),file='NUL')

####################################################
#Check in output db
####################################################

out_tables <- ListTables(out_db)
out_fields <- ListFields(out_db)