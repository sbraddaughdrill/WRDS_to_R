library(data.table)
library(Hmisc)
library(sqldf)
library(RSQLite)


setwd("C:/Research_temp/")    

output_directory <- normalizePath("C:/Research_temp/",winslash = "\\", mustWork = NA)

#function_directory <- normalizePath("C:/Users/Brad/Dropbox/Research/R/",winslash = "\\", mustWork = NA)                     #HOME
function_directory <- normalizePath("C:/Users/bdaughdr/Dropbox/Research/R/",winslash = "\\", mustWork = NA)                 #WORK

source(file=paste(function_directory,"functions_db.R",sep=""),echo=FALSE)
source(file=paste(function_directory,"functions_utilities.R",sep=""),echo=FALSE)

###############################################################################
#Import MFLinks Data
###############################################################################

out_db <- paste(output_directory,"MFLinks.s3db",sep="")

directory <- "H:/Research/Mutual_Funds/Data/MFLinks"
sashome <- "/Program Files/SASHome/SASFoundation/9.3"

mfl_exceptions <- sas.get(libraryName=directory, member="mfl_exceptions", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
mfl_exceptions <- unfactorize2(mfl_exceptions)
ExportTable(out_db,"mfl_exceptions",mfl_exceptions)
rm(mfl_exceptions)
capture.output(gc(),file='NUL')

mflink1 <- sas.get(libraryName=directory, member="mflink1", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
mflink1 <- unfactorize2(mflink1)
ExportTable(out_db,"mflink1",mflink1)
rm(mflink1)
capture.output(gc(),file='NUL')

mflink2 <- sas.get(libraryName=directory, member="mflink2", formats=FALSE, as.is=FALSE, sasprog=file.path(sashome, "sas.exe"))
mflink2 <- unfactorize2(mflink2)
ExportTable(out_db,"mflink2",mflink2)
rm(mflink2)
capture.output(gc(),file='NUL')


mflinks_tables <- ListTables(out_db)
mflinks_fields <- ListFields(out_db)