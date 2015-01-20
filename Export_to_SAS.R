library(foreign)

output_directory <- normalizePath("H:/Research/Mutual_Funds/Data/Text_Analysis/",winslash = "\\", mustWork = NA)
setwd("C:/Research_temp/")     

#Create data
############

ob1 <- data.frame(yr=c(1999),token=c("THE","BLACK","DOG","JUMPED","OVER","RED","FENCE"),multiple=c(4),stringsAsFactors=FALSE)
ob2 <- data.frame(yr=c(2000),token=c("I","WALKED","THE","BLACK","DOG"),multiple=c(3),stringsAsFactors=FALSE)
ob3 <- data.frame(yr=c(2001),token=c("SHE","PAINTED","THE","RED","FENCE"),multiple=c(1),stringsAsFactors=FALSE)
ob4 <- data.frame(yr=c(2002),token=c("THE","YELLOW","HOUSE","HAS","BLACK","DOG","AND","RED","FENCE"),multiple=c(2),stringsAsFactors=FALSE)
sample_data <-  rbind(ob1,ob2,ob3,ob4)

rm(ob1,ob2,ob3,ob4)

#Export data
############

#Method 1
sas_data_file <- paste(output_directory,"sample_data",".sasdata",sep="")
sas_program <- paste(output_directory,"sample_data_import",".sas",sep="")
write.foreign(sample_data, sas_data_file, sas_program, "SAS", dataname="WORK.sample_data", validvarname="V7")

#Method 2
write.dbf(sample_data,paste(output_directory,"sample_data",".dbf",sep=""))

