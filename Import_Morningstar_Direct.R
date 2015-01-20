#=====================================================================;
#PROGRAM DETAILS;
#=====================================================================;
#Program:   Import_Morningstar_Direct.R;
#Version: 	1.0;
#Author:    S. Brad Daughdrill;
#Date:		  05.14.2012;
#Purpose:	  Import data from Morningstar Direct;
#=====================================================================;

#Libraries used
library("gdata") #trim()

#Create local variables
columntitle="_"
columnyear="iyear"
paragraphcol="Investment_Strategy"
inputfile="MDMF_Data_Raw"
outputdatasetname="MDMF_final"

#Read in csv file and create data.frame names "Dataset"
#MDMFDataRaw <- read.csv("H:/Research/Mutual_Funds/Data/Original/MDMF_Data_Raw.csv", header=TRUE, sep=",", na.strings="NA", dec=".", strip.white=TRUE)
#MDMFDataRaw <- read.csv("H:/Research/Mutual_Funds/Data/Original/MDMF_Data_Raw.csv", header=TRUE, sep=",", na.strings=c(""), dec=".", strip.white=TRUE,as.is=TRUE)
MDMFDataRaw <- read.csv("H:/Research/Mutual_Funds/Data/Original/MDMF_Data_Raw.csv", header=TRUE, sep=",", na.strings="NA", dec=".", strip.white=TRUE)

MDMFData <- MDMFDataRaw  #Create data.frame named 'MDMFData'

#str(MDMFData)  #Display the internal *str*ucture of an R object 'MDMFData'

Inception_Date2 <- as.Date(MDMFData$Inception_Date, format="%m/%d/%Y")  #Convert "Inception_Date" to date format
Inception_Date_Year <- substr(as.POSIXct(Inception_Date2),  1,  4)  #Create variable to hold year

#Investment_Strategy_Only <- subset(MDMFData, select=c(Investment_Strategy))
temp_paragraph_col <- subset(MDMFData, select=c(get(paragraphcol)))
temp_paragraph_col2 <-as.character(temp_paragraph_col[,1])

assign(paste(as.character(paragraphcol),"_compress1XXX",sep=""),gsub(pattern="[[:punct:]]", replacement="", x=temp_paragraph_col2))
assign(paste(as.character(paragraphcol),"_compress2XXX",sep=""),gsub(pattern="[[:digit:]]", replacement="", x=get(paste(as.character(paragraphcol),"_compress1XXX",sep=""))))
assign(paste(as.character(paragraphcol),"_compress3XXX",sep=""),gsub(pattern="[[:cntrl:]]", replacement="", x=get(paste(as.character(paragraphcol),"_compress2XXX",sep=""))))
assign(paste(as.character(paragraphcol),"_compbl1XXX",sep=""),gsub(pattern=" {2,}", replacement="", x=get(paste(as.character(paragraphcol),"_compress3XXX",sep=""))))
assign(paste(as.character(paragraphcol),"_compbl2XXX",sep=""),gsub(pattern=" {2,}", replacement="", x=get(paste(as.character(paragraphcol),"_compbl1XXX",sep=""))))
assign(paste(as.character(paragraphcol),"_trimXXX",sep=""),trim(get(paste(as.character(paragraphcol),"_compbl2XXX",sep=""))))
assign(paste(as.character(paragraphcol),"_upcaseXXX",sep=""),toupper(get(paste(as.character(paragraphcol),"_trimXXX",sep=""))))

#Investment_Strategy_compress1 <- gsub(pattern="[[:punct:]]", replacement="", x=MDMFData$Investment_Strategy)   #Compressing punctuation characters
#Investment_Strategy_compress2 <- gsub(pattern="[[:digit:]]", replacement="", x=Investment_Strategy_compress1)  #Compressing digits characters
#Investment_Strategy_compress3 <- gsub(pattern="[[:cntrl:]]", replacement="", x=Investment_Strategy_compress2)  #Compressing control characters
#Investment_Strategy_compbl1   <- gsub(pattern=" {2,}", replacement=" ", x=Investment_Strategy_compress3)  #Remove multiple spaces
#Investment_Strategy_compbl2   <- gsub(pattern=" {2,}", replacement=" ", x=Investment_Strategy_compbl1)  #Remove multiple spaces
#Investment_Strategy_trim      <- trim(Investment_Strategy_compbl2)  #Trim strings
#Investment_Strategy_upcase    <- toupper(Investment_Strategy_trim)  #Upcase string

#print(identical(paste(as.character(paragraphcol),"_compress1XXX",sep=""),Investment_Strategy_compress1,false,))
#print(identical(paste(as.character(paragraphcol),"_compress2XXX",sep=""),Investment_Strategy_compress2))
#print(identical(paste(as.character(paragraphcol),"_compress3XXX",sep=""),Investment_Strategy_compress3))
#print(identical(paste(as.character(paragraphcol),"_compbl1XXX",sep=""),Investment_Strategy_compbl1))
#print(identical(paste(as.character(paragraphcol),"_compbl2XXX",sep=""),Investment_Strategy_compbl2))
#print(identical(paste(as.character(paragraphcol),"_trimXXX",sep=""),Investment_Strategy_trim))
#print(identical(paste(as.character(paragraphcol),"_upcaseXXX",sep=""),Investment_Strategy_upcase))



MDMFData2XXX <- cbind(MDMFData,Inception_Date2,Inception_Date_Year,get(paste(as.character(paragraphcol),"_upcaseXXX",sep=""))) #Add new columns to dataset
MDMFData2 <- cbind(MDMFData,Inception_Date2,Inception_Date_Year,Investment_Strategy_upcase) #Add new columns to dataset
#rm(MDMFData1) #Remove 'MDMFDATA1'
#stru(MDMFData2) #Display the internal *str*ucture of an R object 'MDMFData2'

MDMFData3XXX <- subset(MDMFData2XXX, select = -c(Inception_Date,get(paragraphcol)))
MDMFData3 <- subset(MDMFData2, select = -c(Inception_Date,Investment_Strategy))
#rm(MDMFData2) #Remove 'MDMFDATA2'
#str(MDMFData3) #Display the internal *str*ucture of an R object 'MDMFData3'

MDMFData4 <- MDMFData3[MDMFData3$Investment_Strategy!="",]  #Remove observations where investment strategy is blank
#rm(MDMFData3) #Remove 'MDMFDATA3'
#str(MDMFData4) #Display the internal *str*ucture of an R object 'MDMFData4'

names(MDMFData4)[names(MDMFData4)=="Investment_Strategy_upcase"]="Investment_Strategy"  #Rename "Investment_Strategy_upcase" to "Investment_Strategy"
names(MDMFData4)[names(MDMFData4)=="Inception_Date2"]="Inception_Date"  #Rename "Inception_Date2" to "Inception_Date"

ob <- which(MDMFData4$Name!="")  #Get observation number
tempnum1 <- formatC(ob, width = 7, format = "d", flag = "0") 
tempnum2 <- trim(tempnum1)
id0 <- paste(columntitle,tempnum2,sep="")
id <- trim(id0)

assign(as.character(outputdatasetname),cbind(id,MDMFData4))  #Create final data frame with "id" and 'MDMFData4' using macro name
#rm(MDMFData4) #Remove 'MDMFDATA4'
str(get(outputdatasetname))



#MDMFDataDate <- subset(MDMFData, select=c(Inception_Date))