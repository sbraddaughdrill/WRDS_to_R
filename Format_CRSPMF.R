library(data.table)
library(gdata)
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
source(file=paste(function_directory,"functions_statistics.R",sep=""),echo=FALSE)
source(file=paste(function_directory,"functions_utilities.R",sep=""),echo=FALSE)

temp_db <- paste(output_directory,"temp.s3db",sep="")
crsp_in_db <- paste(output_directory,"CRSPMF2.s3db",sep="")
crsp_out_db <- paste(output_directory,"CRSPMF_Formatted.s3db",sep="")

CRSP_in_tables <- ListTables(crsp_in_db)
CRSP_in_fields <- ListFields(crsp_in_db)

#crsp_tables <- c("Fund_hdr", "Fund_hdr_hist", "Fund_names", "Fund_summary", "Fund_summary2", "Monthly_tna_ret_nav")
#for (i in 1:length(crsp_tables))
#{
#  
#  assign(crsp_tables[i],runsql(paste("SELECT * FROM ",crsp_tables[i],"",sep=""),"CRSPMF.s3db"), envir = .GlobalEnv)
#  
#} 
#rm(i)

###Note this approach imports the .sas7bdat files that were obtained with sas.get from the Import_CRSPMF.R file
### Important because of the formats used


####################################################
#Import Crspa_msi Data
####################################################

Crspa_msi_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Crspa_msi",]
Crspa_msi <- runsql("SELECT * FROM Crspa_msi",crsp_in_db)

#Crspa_msi_num_to_date_cols <- c("CALDT")
Crspa_msi_num_to_date_cols <- c("DATE")
for (i in 1:length(Crspa_msi_num_to_date_cols))
{
  # i <- 1
  Crspa_msi[,Crspa_msi_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Crspa_msi[,Crspa_msi_num_to_date_cols[i]])
  Crspa_msi[,Crspa_msi_num_to_date_cols[i]] <- as.numeric(Crspa_msi[,Crspa_msi_num_to_date_cols[i]])
  Crspa_msi[,Crspa_msi_num_to_date_cols[i]] <- as.integer(Crspa_msi[,Crspa_msi_num_to_date_cols[i]])
  Crspa_msi[,Crspa_msi_num_to_date_cols[i]] <- as.character(Crspa_msi[,Crspa_msi_num_to_date_cols[i]])
  Crspa_msi[,Crspa_msi_num_to_date_cols[i]] <- trim(Crspa_msi[,Crspa_msi_num_to_date_cols[i]])
  Crspa_msi[,Crspa_msi_num_to_date_cols[i]] <- ifelse(Crspa_msi[,Crspa_msi_num_to_date_cols[i]]=="0",NA, Crspa_msi[,Crspa_msi_num_to_date_cols[i]])
  Crspa_msi[,Crspa_msi_num_to_date_cols[i]] <- ifelse(Crspa_msi[,Crspa_msi_num_to_date_cols[i]]=="",NA, Crspa_msi[,Crspa_msi_num_to_date_cols[i]])
  Crspa_msi[,Crspa_msi_num_to_date_cols[i]] <- as.Date(Crspa_msi[,Crspa_msi_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
} 
rm(i)

#for (i in 1:ncol(Crspa_msi))
for (i in which(sapply(Crspa_msi,class)!="Date")) 
{
 Crspa_msi[,i] <- unknownToNA(Crspa_msi[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
 Crspa_msi[,i] <- ifelse(is.na(Crspa_msi[,i]),NA, Crspa_msi[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Crspa_msi",Crspa_msi)
rm2(Crspa_msi_num_to_date_cols)
#rm2(Crspa_msi)


####################################################
#Import Daily_returns Data
####################################################
# 
# Daily_returns_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Daily_returns",]
# Daily_returns <- runsql("SELECT * FROM Daily_returns",crsp_in_db)
# 
# Daily_returns_num_to_pad_cols <- c("crsp_fundno")
# for (i in 1:length(Daily_returns_num_to_pad_cols))
# {
#   Daily_returns[,Daily_returns_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Daily_returns[,Daily_returns_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
#   Daily_returns[,Daily_returns_num_to_pad_cols[i]] <- trim(Daily_returns[,Daily_returns_num_to_pad_cols[i]])
# } 
# rm(i)
# 
# Daily_returns_num_to_date_cols <- c("caldt")
# for (i in 1:length(Daily_returns_num_to_date_cols))
# {
#   # i <- 1
#   Daily_returns[,Daily_returns_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Daily_returns[,Daily_returns_num_to_date_cols[i]])
#   Daily_returns[,Daily_returns_num_to_date_cols[i]] <- as.numeric(Daily_returns[,Daily_returns_num_to_date_cols[i]])
#   Daily_returns[,Daily_returns_num_to_date_cols[i]] <- as.integer(Daily_returns[,Daily_returns_num_to_date_cols[i]])
#   Daily_returns[,Daily_returns_num_to_date_cols[i]] <- as.character(Daily_returns[,Daily_returns_num_to_date_cols[i]])
#   Daily_returns[,Daily_returns_num_to_date_cols[i]] <- trim(Daily_returns[,Daily_returns_num_to_date_cols[i]])
#   Daily_returns[,Daily_returns_num_to_date_cols[i]] <- ifelse(Daily_returns[,Daily_returns_num_to_date_cols[i]]=="0",NA, Daily_returns[,Daily_returns_num_to_date_cols[i]])
#   Daily_returns[,Daily_returns_num_to_date_cols[i]] <- ifelse(Daily_returns[,Daily_returns_num_to_date_cols[i]]=="",NA, Daily_returns[,Daily_returns_num_to_date_cols[i]])
#   Daily_returns[,Daily_returns_num_to_date_cols[i]] <- as.Date(Daily_returns[,Daily_returns_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
#   
#   #Daily_returns[,Daily_returns_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Daily_returns[,Daily_returns_num_to_date_cols[i]]), origin="1960-01-01"))
#   #Daily_returns[,Daily_returns_num_to_date_cols[i]] <- ifelse(Daily_returns[,Daily_returns_num_to_date_cols[i]]=="0",NA, Daily_returns[,Daily_returns_num_to_date_cols[i]])
#   #Daily_returns[,Daily_returns_num_to_date_cols[i]] <- trim(Daily_returns[,Daily_returns_num_to_date_cols[i]])
# } 
# rm(i)
# 
# #for (i in 1:ncol(Daily_returns))
# for (i in which(sapply(Daily_returns,class)!="Date")) 
# {
#   Daily_returns[,i] <- unknownToNA(Daily_returns[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
#   Daily_returns[,i] <- ifelse(is.na(Daily_returns[,i]),NA, Daily_returns[,i])
# } 
# rm(i)
# 
# ExportTable(crsp_out_db,"Daily_returns",Daily_returns)
# rm2(Daily_returns_num_to_pad_cols,Daily_returns_num_to_date_cols)
# #rm2(Daily_returns)


####################################################
#Import Fund_fees Data
####################################################

Fund_fees_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Fund_fees",]
Fund_fees <- runsql("SELECT * FROM Fund_fees",crsp_in_db)

Fund_fees_num_to_pad_cols <- c("crsp_fundno")
for (i in 1:length(Fund_fees_num_to_pad_cols))
{
  Fund_fees[,Fund_fees_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Fund_fees[,Fund_fees_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  Fund_fees[,Fund_fees_num_to_pad_cols[i]] <- trim(Fund_fees[,Fund_fees_num_to_pad_cols[i]])
} 
rm(i)

Fund_fees_num_to_date_cols <- c("begdt","enddt","fiscal_yearend")
for (i in 1:length(Fund_fees_num_to_date_cols))
{
  # i <- 1
  Fund_fees[,Fund_fees_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Fund_fees[,Fund_fees_num_to_date_cols[i]])
  Fund_fees[,Fund_fees_num_to_date_cols[i]] <- as.numeric(Fund_fees[,Fund_fees_num_to_date_cols[i]])
  Fund_fees[,Fund_fees_num_to_date_cols[i]] <- as.integer(Fund_fees[,Fund_fees_num_to_date_cols[i]])
  Fund_fees[,Fund_fees_num_to_date_cols[i]] <- as.character(Fund_fees[,Fund_fees_num_to_date_cols[i]])
  Fund_fees[,Fund_fees_num_to_date_cols[i]] <- trim(Fund_fees[,Fund_fees_num_to_date_cols[i]])
  Fund_fees[,Fund_fees_num_to_date_cols[i]] <- ifelse(Fund_fees[,Fund_fees_num_to_date_cols[i]]=="0",NA, Fund_fees[,Fund_fees_num_to_date_cols[i]])
  Fund_fees[,Fund_fees_num_to_date_cols[i]] <- ifelse(Fund_fees[,Fund_fees_num_to_date_cols[i]]=="",NA, Fund_fees[,Fund_fees_num_to_date_cols[i]])
  Fund_fees[,Fund_fees_num_to_date_cols[i]] <- as.Date(Fund_fees[,Fund_fees_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
  
  #Fund_fees[,Fund_fees_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Fund_fees[,Fund_fees_num_to_date_cols[i]]), origin="1960-01-01"))
  #Fund_fees[,Fund_fees_num_to_date_cols[i]] <- ifelse(Fund_fees[,Fund_fees_num_to_date_cols[i]]=="0",NA, Fund_fees[,Fund_fees_num_to_date_cols[i]])
  #Fund_fees[,Fund_fees_num_to_date_cols[i]] <- trim(Fund_fees[,Fund_fees_num_to_date_cols[i]])
} 
rm(i)

#for (i in 1:ncol(Fund_fees))
for (i in which(sapply(Fund_fees,class)!="Date")) 
{
  Fund_fees[,i] <- unknownToNA(Fund_fees[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Fund_fees[,i] <- ifelse(is.na(Fund_fees[,i]),NA, Fund_fees[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Fund_fees",Fund_fees)
rm2(Fund_fees_num_to_pad_cols,Fund_fees_num_to_date_cols)
#rm2(Fund_fees)


####################################################
#Import Fund_hdr Data
####################################################

Fund_hdr_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Fund_hdr",]
Fund_hdr <- runsql("SELECT * FROM Fund_hdr",crsp_in_db)

Fund_hdr_num_to_pad_cols <- c("crsp_fundno","merge_fundno")
for (i in 1:length(Fund_hdr_num_to_pad_cols))
{
  Fund_hdr[,Fund_hdr_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Fund_hdr[,Fund_hdr_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  Fund_hdr[,Fund_hdr_num_to_pad_cols[i]] <- trim(Fund_hdr[,Fund_hdr_num_to_pad_cols[i]])
} 
rm(i)

Fund_hdr_num_to_char_cols <- c("crsp_portno","crsp_cl_grp")
for (i in 1:length(Fund_hdr_num_to_char_cols))
{
  Fund_hdr[,Fund_hdr_num_to_char_cols[i]] <- as.character(as.integer(Fund_hdr[,Fund_hdr_num_to_char_cols[i]]))
  Fund_hdr[,Fund_hdr_num_to_char_cols[i]] <- ifelse(Fund_hdr[,Fund_hdr_num_to_char_cols[i]]=="0",NA, Fund_hdr[,Fund_hdr_num_to_char_cols[i]])
  Fund_hdr[,Fund_hdr_num_to_char_cols[i]] <- trim(Fund_hdr[,Fund_hdr_num_to_char_cols[i]])
} 
rm(i)

Fund_hdr_num_to_date_cols <- c("first_offer_dt","mgr_dt","end_dt")
for (i in 1:length(Fund_hdr_num_to_date_cols))
{
  # i <- 1
  Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
  Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- as.numeric(Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
  Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- as.integer(Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
  Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- as.character(Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
  Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- trim(Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
  Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- ifelse(Fund_hdr[,Fund_hdr_num_to_date_cols[i]]=="0",NA, Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
  Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- ifelse(Fund_hdr[,Fund_hdr_num_to_date_cols[i]]=="",NA, Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
  Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- as.Date(Fund_hdr[,Fund_hdr_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
  
  #Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Fund_hdr[,Fund_hdr_num_to_date_cols[i]]), origin="1960-01-01"))
  #Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- ifelse(Fund_hdr[,Fund_hdr_num_to_date_cols[i]]=="0",NA, Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
  #Fund_hdr[,Fund_hdr_num_to_date_cols[i]] <- trim(Fund_hdr[,Fund_hdr_num_to_date_cols[i]])
} 
rm(i)

#for (i in 1:ncol(Fund_hdr))
for (i in which(sapply(Fund_hdr,class)!="Date")) 
{
  Fund_hdr[,i] <- unknownToNA(Fund_hdr[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Fund_hdr[,i] <- ifelse(is.na(Fund_hdr[,i]),NA, Fund_hdr[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Fund_hdr",Fund_hdr)
rm2(Fund_hdr_num_to_pad_cols,Fund_hdr_num_to_char_cols,Fund_hdr_num_to_date_cols)
#rm2(Fund_hdr)


####################################################
#Import Fund_hdr_hist Data
####################################################

Fund_hdr_hist_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Fund_hdr_hist",]
Fund_hdr_hist <- runsql("SELECT * FROM Fund_hdr_hist",crsp_in_db)

Fund_hdr_hist_num_to_pad_cols <- c("crsp_fundno")
for (i in 1:length(Fund_hdr_hist_num_to_pad_cols))
{
  Fund_hdr_hist[,Fund_hdr_hist_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Fund_hdr_hist[,Fund_hdr_hist_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  Fund_hdr_hist[,Fund_hdr_hist_num_to_pad_cols[i]] <- trim(Fund_hdr_hist[,Fund_hdr_hist_num_to_pad_cols[i]])
} 
rm(i)

Fund_hdr_hist_num_to_char_cols <- c("crsp_portno","crsp_cl_grp")
for (i in 1:length(Fund_hdr_hist_num_to_char_cols))
{
  Fund_hdr_hist[,Fund_hdr_hist_num_to_char_cols[i]] <- as.character(as.integer(Fund_hdr_hist[,Fund_hdr_hist_num_to_char_cols[i]]))
  Fund_hdr_hist[,Fund_hdr_hist_num_to_char_cols[i]] <- ifelse(Fund_hdr_hist[,Fund_hdr_hist_num_to_char_cols[i]]=="0",NA, Fund_hdr_hist[,Fund_hdr_hist_num_to_char_cols[i]])
  Fund_hdr_hist[,Fund_hdr_hist_num_to_char_cols[i]] <- trim(Fund_hdr_hist[,Fund_hdr_hist_num_to_char_cols[i]])
} 
rm(i)

Fund_hdr_hist_num_to_date_cols <- c("chgdt","chgenddt","first_offer_dt","mgr_dt")
for (i in 1:length(Fund_hdr_hist_num_to_date_cols))
{
  # i <- 1
  Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
  Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- as.numeric(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
  Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- as.integer(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
  Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- as.character(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
  Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- trim(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
  Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- ifelse(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]]=="0",NA, Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
  Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- ifelse(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]]=="",NA, Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
  Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- as.Date(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
  
  #Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Fund_hdr_hist[,names(Fund_hdr_hist)==Fund_hdr_hist_num_to_date_cols[i]]), origin="1960-01-01"))
  #Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- ifelse(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]]=="0",NA, Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
  #Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]] <- trim(Fund_hdr_hist[,Fund_hdr_hist_num_to_date_cols[i]])
} 
rm(i)

#for (i in 1:ncol(Fund_hdr_hist))
for (i in which(sapply(Fund_hdr_hist,class)!="Date")) 
{
  Fund_hdr_hist[,i] <- unknownToNA(Fund_hdr_hist[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Fund_hdr_hist[,i] <- ifelse(is.na(Fund_hdr_hist[,i]),NA, Fund_hdr_hist[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Fund_hdr_hist",Fund_hdr_hist)
rm2(Fund_hdr_hist_num_to_pad_cols,Fund_hdr_hist_num_to_char_cols,Fund_hdr_hist_num_to_date_cols)
#rm2(Fund_hdr_hist)


####################################################
#Import Fund_names Data
####################################################

Fund_names_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Fund_names",]
Fund_names <- runsql("SELECT * FROM Fund_names",crsp_in_db)

Fund_names_num_to_pad_cols <- c("crsp_fundno","merge_fundno")
for (i in 1:length(Fund_names_num_to_pad_cols))
{
  Fund_names[,Fund_names_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Fund_names[,Fund_names_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  Fund_names[,Fund_names_num_to_pad_cols[i]] <- trim(Fund_names[,Fund_names_num_to_pad_cols[i]])
} 
rm(i)

Fund_names_num_to_char_cols <- c("crsp_portno","crsp_cl_grp")
for (i in 1:length(Fund_names_num_to_char_cols))
{
  Fund_names[,Fund_names_num_to_char_cols[i]] <- as.character(as.integer(Fund_names[,Fund_names_num_to_char_cols[i]]))
  Fund_names[,Fund_names_num_to_char_cols[i]] <- ifelse(Fund_names[,Fund_names_num_to_char_cols[i]]=="0",NA, Fund_names[,Fund_names_num_to_char_cols[i]])
  Fund_names[,Fund_names_num_to_char_cols[i]] <- trim(Fund_names[,Fund_names_num_to_char_cols[i]])
} 
rm(i)

Fund_names_num_to_date_cols <- c("chgdt","chgenddt","mgr_dt","first_offer_dt","end_dt")
for (i in 1:length(Fund_names_num_to_date_cols))
{
  # i <- 1
  Fund_names[,Fund_names_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Fund_names[,Fund_names_num_to_date_cols[i]])
  Fund_names[,Fund_names_num_to_date_cols[i]] <- as.numeric(Fund_names[,Fund_names_num_to_date_cols[i]])
  Fund_names[,Fund_names_num_to_date_cols[i]] <- as.integer(Fund_names[,Fund_names_num_to_date_cols[i]])
  Fund_names[,Fund_names_num_to_date_cols[i]] <- as.character(Fund_names[,Fund_names_num_to_date_cols[i]])
  Fund_names[,Fund_names_num_to_date_cols[i]] <- trim(Fund_names[,Fund_names_num_to_date_cols[i]])
  Fund_names[,Fund_names_num_to_date_cols[i]] <- ifelse(Fund_names[,Fund_names_num_to_date_cols[i]]=="0",NA, Fund_names[,Fund_names_num_to_date_cols[i]])
  Fund_names[,Fund_names_num_to_date_cols[i]] <- ifelse(Fund_names[,Fund_names_num_to_date_cols[i]]=="",NA, Fund_names[,Fund_names_num_to_date_cols[i]])
  Fund_names[,Fund_names_num_to_date_cols[i]] <- as.Date(Fund_names[,Fund_names_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
 
  #Fund_names[,Fund_names_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Fund_names[,Fund_names_num_to_date_cols[i]])
  #Fund_names[,Fund_names_num_to_date_cols[i]] <- as.numeric(Fund_names[,Fund_names_num_to_date_cols[i]])
  #Fund_names[,Fund_names_num_to_date_cols[i]] <- as.integer(Fund_names[,Fund_names_num_to_date_cols[i]])
  #Fund_names[,Fund_names_num_to_date_cols[i]] <- as.Date(as.integer(Fund_names[,Fund_names_num_to_date_cols[i]]),format="%Y%m%d",origin="1960-01-01")
  #Fund_names[,Fund_names_num_to_date_cols[i]] <- ifelse(Fund_names[,Fund_names_num_to_date_cols[i]]==0,NA, Fund_names[,Fund_names_num_to_date_cols[i]])
  #Fund_names[,Fund_names_num_to_date_cols[i]] <- as.character(Fund_names[,Fund_names_num_to_date_cols[i]])
  #Fund_names[,Fund_names_num_to_date_cols[i]] <- trim(Fund_names[,Fund_names_num_to_date_cols[i]])
} 
rm(i)

#for (i in 1:ncol(Fund_names))
for (i in which(sapply(Fund_names,class)!="Date")) 
{
  Fund_names[,i] <- unknownToNA(Fund_names[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Fund_names[,i] <- ifelse(is.na(Fund_names[,i]),NA, Fund_names[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Fund_names",Fund_names)
rm2(Fund_names_num_to_pad_cols,Fund_names_num_to_char_cols,Fund_names_num_to_date_cols)
#rm2(Fund_names)


####################################################
#Import Fund_style Data
####################################################

Fund_style_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Fund_style",]
Fund_style <- runsql("SELECT * FROM Fund_style",crsp_in_db)

Fund_style_num_to_pad_cols <- c("crsp_fundno")
for (i in 1:length(Fund_style_num_to_pad_cols))
{
  Fund_style[,Fund_style_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Fund_style[,Fund_style_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  Fund_style[,Fund_style_num_to_pad_cols[i]] <- trim(Fund_style[,Fund_style_num_to_pad_cols[i]])
} 
rm(i)

Fund_style_num_to_date_cols <- c("begdt","enddt")
for (i in 1:length(Fund_style_num_to_date_cols))
{
  # i <- 1
  Fund_style[,Fund_style_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Fund_style[,Fund_style_num_to_date_cols[i]])
  Fund_style[,Fund_style_num_to_date_cols[i]] <- as.numeric(Fund_style[,Fund_style_num_to_date_cols[i]])
  Fund_style[,Fund_style_num_to_date_cols[i]] <- as.integer(Fund_style[,Fund_style_num_to_date_cols[i]])
  Fund_style[,Fund_style_num_to_date_cols[i]] <- as.character(Fund_style[,Fund_style_num_to_date_cols[i]])
  Fund_style[,Fund_style_num_to_date_cols[i]] <- trim(Fund_style[,Fund_style_num_to_date_cols[i]])
  Fund_style[,Fund_style_num_to_date_cols[i]] <- ifelse(Fund_style[,Fund_style_num_to_date_cols[i]]=="0",NA, Fund_style[,Fund_style_num_to_date_cols[i]])
  Fund_style[,Fund_style_num_to_date_cols[i]] <- ifelse(Fund_style[,Fund_style_num_to_date_cols[i]]=="",NA, Fund_style[,Fund_style_num_to_date_cols[i]])
  Fund_style[,Fund_style_num_to_date_cols[i]] <- as.Date(Fund_style[,Fund_style_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
  
  #Fund_style[,Fund_style_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Fund_style[,Fund_style_num_to_date_cols[i]]), origin="1960-01-01"))
  #Fund_style[,Fund_style_num_to_date_cols[i]] <- ifelse(Fund_style[,Fund_style_num_to_date_cols[i]]=="0",NA, Fund_style[,Fund_style_num_to_date_cols[i]])
  #Fund_style[,Fund_style_num_to_date_cols[i]] <- trim(Fund_style[,Fund_style_num_to_date_cols[i]])
} 
rm(i)

#for (i in 1:ncol(Fund_style))
for (i in which(sapply(Fund_style,class)!="Date")) 
{
  Fund_style[,i] <- unknownToNA(Fund_style[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Fund_style[,i] <- ifelse(is.na(Fund_style[,i]),NA, Fund_style[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Fund_style",Fund_style)
rm2(Fund_style_num_to_pad_cols,Fund_style_num_to_date_cols)
#rm2(Fund_style)


####################################################
#Import Fund_summary Data
####################################################

Fund_summary_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Fund_summary",]
Fund_summary <- runsql("SELECT * FROM Fund_summary",crsp_in_db)

Fund_summary_num_to_pad_cols <- c("crsp_fundno")
for (i in 1:length(Fund_summary_num_to_pad_cols))
{
  Fund_summary[,Fund_summary_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Fund_summary[,Fund_summary_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  Fund_summary[,Fund_summary_num_to_pad_cols[i]] <- trim(Fund_summary[,Fund_summary_num_to_pad_cols[i]])
} 
rm(i)

Fund_summary_num_to_date_cols <- c("caldt","nav_latest_dt","tna_latest_dt","nav_52w_h_dt","nav_52w_l_dt","unrealized_app_dt","asset_dt","maturity_dt")
for (i in 1:length(Fund_summary_num_to_date_cols))
{
  # i <- 1
  Fund_summary[,Fund_summary_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Fund_summary[,Fund_summary_num_to_date_cols[i]])
  Fund_summary[,Fund_summary_num_to_date_cols[i]] <- as.numeric(Fund_summary[,Fund_summary_num_to_date_cols[i]])
  Fund_summary[,Fund_summary_num_to_date_cols[i]] <- as.integer(Fund_summary[,Fund_summary_num_to_date_cols[i]])
  Fund_summary[,Fund_summary_num_to_date_cols[i]] <- as.character(Fund_summary[,Fund_summary_num_to_date_cols[i]])
  Fund_summary[,Fund_summary_num_to_date_cols[i]] <- trim(Fund_summary[,Fund_summary_num_to_date_cols[i]])
  Fund_summary[,Fund_summary_num_to_date_cols[i]] <- ifelse(Fund_summary[,Fund_summary_num_to_date_cols[i]]=="0",NA, Fund_summary[,Fund_summary_num_to_date_cols[i]])
  Fund_summary[,Fund_summary_num_to_date_cols[i]] <- ifelse(Fund_summary[,Fund_summary_num_to_date_cols[i]]=="",NA, Fund_summary[,Fund_summary_num_to_date_cols[i]])
  Fund_summary[,Fund_summary_num_to_date_cols[i]] <- as.Date(Fund_summary[,Fund_summary_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
  
  #Fund_summary[,Fund_summary_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Fund_summary[,Fund_summary_num_to_date_cols[i]]), origin="1960-01-01"))
  #Fund_summary[,Fund_summary_num_to_date_cols[i]] <- ifelse(Fund_summary[,Fund_summary_num_to_date_cols[i]]=="0",NA, Fund_summary[,Fund_summary_num_to_date_cols[i]])
  #Fund_summary[,Fund_summary_num_to_date_cols[i]] <- trim(Fund_summary[,Fund_summary_num_to_date_cols[i]])
} 
rm(i)

#for (i in 1:ncol(Fund_summary))
for (i in which(sapply(Fund_summary,class)!="Date")) 
{
  Fund_summary[,i] <- unknownToNA(Fund_summary[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Fund_summary[,i] <- ifelse(is.na(Fund_summary[,i]),NA, Fund_summary[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Fund_summary",Fund_summary)
rm2(Fund_summary_num_to_pad_cols,Fund_summary_num_to_date_cols)
#rm2(Fund_summary)


####################################################
#Import Fund_summary2 Data
####################################################

Fund_summary2_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Fund_summary2",]
Fund_summary2 <- runsql("SELECT * FROM Fund_summary2",crsp_in_db)

Fund_summary2_num_to_pad_cols <- c("crsp_fundno")
for (i in 1:length(Fund_summary2_num_to_pad_cols))
{
  Fund_summary2[,Fund_summary2_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Fund_summary2[,Fund_summary2_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  Fund_summary2[,Fund_summary2_num_to_pad_cols[i]] <- trim(Fund_summary2[,Fund_summary2_num_to_pad_cols[i]])
} 
rm(i)

Fund_summary2_num_to_date_cols <- c("caldt","nav_latest_dt","tna_latest_dt","nav_52w_h_dt","nav_52w_l_dt","unrealized_app_dt","asset_dt","maturity_dt","mgr_dt","first_offer_dt","end_dt","fiscal_yearend")
for (i in 1:length(Fund_summary2_num_to_date_cols))
{
  # i <- 1
  Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Fund_summary2[,Fund_summary2_num_to_date_cols[i]])
  Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- as.numeric(Fund_summary2[,Fund_summary2_num_to_date_cols[i]])
  Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- as.integer(Fund_summary2[,Fund_summary2_num_to_date_cols[i]])
  Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- as.character(Fund_summary2[,Fund_summary2_num_to_date_cols[i]])
  Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- trim(Fund_summary2[,Fund_summary2_num_to_date_cols[i]])
  Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- ifelse(Fund_summary2[,Fund_summary2_num_to_date_cols[i]]=="0",NA, Fund_summary2[,Fund_summary2_num_to_date_cols[i]])
  Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- ifelse(Fund_summary2[,Fund_summary2_num_to_date_cols[i]]=="",NA, Fund_summary2[,Fund_summary2_num_to_date_cols[i]])
  Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- as.Date(Fund_summary2[,Fund_summary2_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
  
  #Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Fund_summary2[,Fund_summary2_num_to_date_cols[i]]), origin="1960-01-01"))
  #Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- ifelse(Fund_summary2[,Fund_summary2_num_to_date_cols[i]]=="0",NA, Fund_summary2[,Fund_summary2_num_to_date_cols[i]])
  #Fund_summary2[,Fund_summary2_num_to_date_cols[i]] <- trim(Fund_summary2[,Fund_summary2_num_to_date_cols[i]] )
} 
rm(i)

#for (i in 1:ncol(Fund_summary2))
for (i in which(sapply(Fund_summary2,class)!="Date")) 
{
  Fund_summary2[,i] <- unknownToNA(Fund_summary2[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Fund_summary2[,i] <- ifelse(is.na(Fund_summary2[,i]),NA, Fund_summary2[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Fund_summary2",Fund_summary2)
rm2(Fund_summary2_num_to_pad_cols,Fund_summary2_num_to_date_cols)
#rm2(Fund_summary2)


####################################################
#Import Monthly_tna_ret_nav Data
####################################################

Monthly_tna_ret_nav_fields <- CRSP_in_fields[CRSP_in_fields[,1]=="Monthly_tna_ret_nav",]
Monthly_tna_ret_nav <- runsql("SELECT * FROM Monthly_tna_ret_nav",crsp_in_db)

Monthly_tna_ret_nav_num_to_pad_cols <- c("crsp_fundno")
for (i in 1:length(Monthly_tna_ret_nav_num_to_pad_cols))
{
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_pad_cols[i]] <- trim(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_pad_cols[i]])
} 
rm(i)

Monthly_tna_ret_nav_num_to_date_cols <- c("caldt")
for (i in 1:length(Monthly_tna_ret_nav_num_to_date_cols))
{
  # i <- 1
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- gsub("[[:alpha:]]","",Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]])
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- as.numeric(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]])
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- as.integer(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]])
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- as.character(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]])
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- trim(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]])
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- ifelse(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]]=="0",NA, Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]])
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- ifelse(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]]=="",NA, Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]])
  Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- as.Date(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]],format="%Y%m%d",origin="1960-01-01")
  
  #Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]]), origin="1960-01-01"))
  #Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- ifelse(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]]=="0",NA, Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]])
  #Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] <- trim(Monthly_tna_ret_nav[,Monthly_tna_ret_nav_num_to_date_cols[i]] )
} 
rm(i)

#for (i in 1:ncol(Monthly_tna_ret_nav))
for (i in which(sapply(Monthly_tna_ret_nav,class)!="Date"))
{
  Monthly_tna_ret_nav[,i] <- unknownToNA(Monthly_tna_ret_nav[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Monthly_tna_ret_nav[,i] <- ifelse(is.na(Monthly_tna_ret_nav[,i]),NA, Monthly_tna_ret_nav[,i])
} 
rm(i)

ExportTable(crsp_out_db,"Monthly_tna_ret_nav",Monthly_tna_ret_nav)
rm2(Monthly_tna_ret_nav_num_to_pad_cols,Monthly_tna_ret_nav_num_to_date_cols)
#rm2(Monthly_tna_ret_nav)

 
# ##############################################################################
# cat("EXPAND DATES IN DAILY_RETURNS", "\n")
# ###############################################################################
# 
# Daily_returns <- runsql("SELECT * FROM Daily_returns",crsp_in_db)
# 
# 
# 
# 
# 
###############################################################################
cat("EXPAND DATES IN FUND_FEES", "\n")
###############################################################################

fund_fees <- runsql("SELECT * FROM Fund_fees",crsp_out_db)
fund_fees <- transform(fund_fees, begdt=as.IDate(begdt,origin="1970-01-01"), enddt=as.IDate(enddt,origin="1970-01-01"))

fund_fees_month_temp <- data.table(fund_fees)[,{s=seq(begdt,enddt,"days");list(yr=year(unlist(s)),month=month(unlist(s)))},
                                                by="crsp_fundno,begdt,enddt"]

rm2(fund_fees)

ExportTable(temp_db,"fund_fees_month_temp",fund_fees_month_temp)

rm2(fund_fees_month_temp)

fund_fees_month_temp <- runsql("SELECT DISTINCT * FROM fund_fees_month_temp",temp_db)

fund_fees_month_temp <- data.table(fund_fees_month_temp, key = c("crsp_fundno","yr","month"))

fund_fees_month_temp <- fund_fees_month_temp[unique(fund_fees_month_temp[,key(fund_fees_month_temp), with=FALSE]), mult='first']

fund_fees_month_temp <- fund_fees_month_temp[,list(crsp_fundno,
                                                   begdt=as.IDate(begdt,origin="1970-01-01"),
                                                   enddt=as.IDate(enddt,origin="1970-01-01"),yr,month),]
fund_fees_month_temp <- fund_fees_month_temp[,list(crsp_fundno,begdt=as.character(begdt),enddt=as.character(enddt),yr,month),]

fund_fees_month_temp <- as.data.frame(fund_fees_month_temp,stringsAsFactors=FALSE)

fund_fees <- runsql("SELECT * FROM Fund_fees",crsp_out_db)

fund_fees_month <- merge(fund_fees_month_temp, fund_fees, 
                         by.x=c("crsp_fundno","begdt","enddt"), by.y=c("crsp_fundno","begdt","enddt"), 
                         all.x=TRUE, all.y=FALSE, sort=FALSE, suffixes=c(".x",".y"))

rm2(fund_fees_month_temp,fund_fees)

fund_fees_month <- fund_fees_month[order(fund_fees_month[,"crsp_fundno"],fund_fees_month[,"yr"], fund_fees_month[,"month"]),] 

ExportTable(crsp_out_db,"Fund_fees_month",fund_fees_month)

rm2(fund_fees_month)

#DeleteTable(temp_db,"fund_fees_month_temp")

CRSP_out_tables <- ListTables(crsp_out_db)

###############################################################################
cat("EXPAND DATES IN FUND_STYLE", "\n")
###############################################################################

fund_style <- runsql("SELECT * FROM Fund_style",crsp_out_db)
fund_style <- transform(fund_style, begdt=as.IDate(begdt,origin="1970-01-01"), enddt=as.IDate(enddt,origin="1970-01-01"))

fund_style_month_temp <- data.table(fund_style)[,{s=seq(begdt,enddt,"days");list(yr=year(unlist(s)),month=month(unlist(s)))},
                                                by="crsp_fundno,begdt,enddt"]

rm2(fund_style)

ExportTable(temp_db,"fund_style_month_temp",fund_style_month_temp)

rm2(fund_style_month_temp)

fund_style_month_temp <- runsql("SELECT DISTINCT * FROM fund_style_month_temp",temp_db)

fund_style_month_temp <- data.table(fund_style_month_temp, key = c("crsp_fundno","yr","month"))

fund_style_month_temp <- fund_style_month_temp[unique(fund_style_month_temp[,key(fund_style_month_temp), with=FALSE]), mult='first']

fund_style_month_temp <- fund_style_month_temp[,list(crsp_fundno,
                                                     begdt=as.IDate(begdt,origin="1970-01-01"),
                                                     enddt=as.IDate(enddt,origin="1970-01-01"),yr,month),]
fund_style_month_temp <- fund_style_month_temp[,list(crsp_fundno,begdt=as.character(begdt),enddt=as.character(enddt),yr,month),]

fund_style_month_temp <- as.data.frame(fund_style_month_temp,stringsAsFactors=FALSE)

fund_style <- runsql("SELECT * FROM Fund_style",crsp_out_db)

fund_style_month <- merge(fund_style_month_temp, fund_style, 
                          by.x=c("crsp_fundno","begdt","enddt"), by.y=c("crsp_fundno","begdt","enddt"), 
                          all.x=TRUE, all.y=FALSE, sort=FALSE, suffixes=c(".x",".y"))

rm2(fund_style_month_temp,fund_style)

fund_style_month <- fund_style_month[order(fund_style_month[,"crsp_fundno"],fund_style_month[,"yr"], fund_style_month[,"month"]),] 

ExportTable(crsp_out_db,"Fund_style_month",fund_style_month)

rm2(fund_style_month)

#DeleteTable(temp_db,"fund_style_month_temp")

####################################################
#Check in output db
####################################################


CRSP_out_tables <- ListTables(crsp_out_db)
CRSP_out_fields <- ListFields(crsp_out_db)
