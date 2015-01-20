library(gdata)
library(sqldf)
library(RSQLite)

setwd("C:/Research_temp/")     
output_directory <- normalizePath("C:/Research_temp/",winslash = "\\", mustWork = NA)

#function_directory <- normalizePath("C:/Users/Brad/Dropbox/Research/R/",winslash = "\\", mustWork = NA)                     #HOME
function_directory <- normalizePath("C:/Users/bdaughdr/Dropbox/Research/R/",winslash = "\\", mustWork = NA)                 #WORK

source(file=paste(function_directory,"functions_db.R",sep=""),echo=FALSE)
source(file=paste(function_directory,"functions_utilities.R",sep=""),echo=FALSE)

in_db <- paste(output_directory,"MDMF.s3db",sep="")
out_db <- paste(output_directory,"MDMF_Formatted.s3db",sep="")

in_tables <- ListTables(in_db)
in_fields <- ListFields(in_db)

###Note this approach imports the .sas7bdat files that were obtained with sas.get from the Import_Mflinks.R file
### Important because of the formats used

###############################################################################
#Import Mdmf_data_raw Data
###############################################################################

Mdmf_data_raw_fields <- in_fields[in_fields[,1]=="Mdmf_data_raw",]
Mdmf_data_raw <- runsql("SELECT * FROM Mdmf_data_raw",in_db)

#Fund_hdr_num_to_pad_cols <- c("crsp_fundno","merge_fundno")
#for (i in 1:length(Fund_hdr_num_to_pad_cols))
#{
#  Fund_hdr[,Fund_hdr_num_to_pad_cols[i]] <- paste("", formatC(as.integer(Fund_hdr[,Fund_hdr_num_to_pad_cols[i]]), width=6, format="d", flag="0"), sep = "")
#  Fund_hdr[,Fund_hdr_num_to_pad_cols[i]] <- trim(Fund_hdr[,Fund_hdr_num_to_pad_cols[i]])
#} 

#Fund_hdr_num_to_char_cols <- c("crsp_portno","crsp_cl_grp")
#for (i in 1:length(Fund_hdr_num_to_char_cols))
#{
#  Fund_hdr[,Fund_hdr_num_to_char_cols[i]] <- as.character(as.integer(Fund_hdr[,Fund_hdr_num_to_char_cols[i]]))
#  Fund_hdr[,Fund_hdr_num_to_char_cols[i]] <- ifelse(Fund_hdr[,Fund_hdr_num_to_char_cols[i]]=="0",NA, Fund_hdr[,Fund_hdr_num_to_char_cols[i]])
#  Fund_hdr[,Fund_hdr_num_to_char_cols[i]] <- trim(Fund_hdr[,Fund_hdr_num_to_char_cols[i]])
#} 

Mdmf_data_raw_num_to_date_cols <- c("Global_Fund_Report_Anal_Date", "Inception_Date", "Prim_Prosp_Benchmark_Incep_Date",
                                    "Net_Assets_Date","Fund_Size_Date", "Shares_Outstanding_Date","_12_Mo_Yield_Date",
                                    "Latest_Div_Date","Latest_Cap_Gain_Date", "Latest_Cap_Gain_Date_LT","Latest_Cap_Gain_Date_MT",
                                    "Latest_Cap_Gain_Date_ST", "Latest_ROC_Date", "Portfolio_Date","Return_Date_Daily", "Morningstar_Page", 
                                    "Analysis_Date", "Asset_Change_Date", "MS_Category_Start_Date", "Target_Date_Report_Date")
for (i in 1:length(Mdmf_data_raw_num_to_date_cols))
{
  Mdmf_data_raw[,Mdmf_data_raw_num_to_date_cols[i]] <- as.character(as.Date(as.integer(Mdmf_data_raw[,Mdmf_data_raw_num_to_date_cols[i]]), origin="1960-01-01"))
  Mdmf_data_raw[,Mdmf_data_raw_num_to_date_cols[i]] <- ifelse(Mdmf_data_raw[,Mdmf_data_raw_num_to_date_cols[i]]=="0",NA, Mdmf_data_raw[,Mdmf_data_raw_num_to_date_cols[i]])
  Mdmf_data_raw[,Mdmf_data_raw_num_to_date_cols[i]] <- trim(Mdmf_data_raw[,Mdmf_data_raw_num_to_date_cols[i]])
} 

for (i in 1:ncol(Mdmf_data_raw))
{
  Mdmf_data_raw[,i] <- trim(Mdmf_data_raw[,i])
  Mdmf_data_raw[,i] <- unknownToNA(Mdmf_data_raw[,i], unknown=c("",".","NA_character_","NA_Real_","NA",NA),force=TRUE)
  Mdmf_data_raw[,i] <- ifelse(is.na(Mdmf_data_raw[,i]),NA, Mdmf_data_raw[,i])
} 

ExportTable(out_db,"Mdmf_data_raw",Mdmf_data_raw)
rm2(Mdmf_data_raw_num_to_date_cols,Mdmf_data_raw)

####################################################
#Check in output db
####################################################

out_tables <- ListTables(out_db)
out_fields <- ListFields(out_db)
