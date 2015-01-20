# install.packages("R2wd")
# library(help=R2wd)
require(R2wd)


wdGet(T)  # If no word file is open, it will start a new one - can set if to have the file visiable or not
wdNewDoc("c:\\This.doc")	# this creates a new file with "this.doc" name

wdApplyTemplate("c:\\This.dot")	# this applies a template


wdTitle("Examples of R2wd (a package to write Word documents from R)")	# adds a title to the file

wdSection("Example 1 - adding text", newpage = T) # This can also create a header

wdHeading(level = 2, "Header 2")
wdBody("This is the first example we will show")
wdBody("(Notice how, by using two different lines in wdBody, we got two different paragraphs)")
wdBody("(Notice how I can use this: '\ n' (without the space), to  \n  go to the next 
       line)")
wdBody("?????? ???? ???????? ???????????? ?")
wdBody("It doesn't work with Hebrew...")
wdBody("O.k, let's move to the next page (and the next example)")

wdSection("Example 2 - adding tables", newpage = T)
wdBody("Table using 'format'")
wdTable(format(head(mtcars)))
wdBody("Table without using 'format'")
wdTable(head(mtcars))


wdSection("Example 3 - adding lm summary", newpage = T)

## Example from  ?lm 
ctl <- c(4.17,5.58,5.18,6.11,4.50,4.61,5.17,4.53,5.33,5.14)
trt <- c(4.81,4.17,4.41,3.59,5.87,3.83,6.03,4.89,4.32,4.69)
group <- gl(2,10,20, labels=c("Ctl","Trt"))
weight <- c(ctl, trt)

# This wouldn't work!
# temp <- summary(lm(weight ~ group))
# wdBody(temp)

# Here is a solution for how to implent the summary.lm output to word
wdBody.anything <- function(output)
{
  # This function takes the output of an object and prints it line by line into the word document
  # Notice that in many cases you will need to change the text font into courier new roman...
  a <- capture.output(output)
  for(i in seq_along(a))
  {
    wdBody(format(a[i]))
  }
}

temp <- summary(lm(weight ~ group))
wdBody.anything(temp)



wdSection("Example 4 - Inserting some plots", newpage = T)

wdPlot(rnorm(100), plotfun = plot, height = 10, width =20, pointsize = 20)
wdPlot(rnorm(100), plotfun = plot, height = 10, width =20, pointsize = 20)
wdPlot(rnorm(100), plotfun = plot, height = 10, width =20, pointsize = 50)

# wdPageBreak()


wdSave("c:\\This.doc") # save current file (can say what file name to use)
wdQuit() # close the word file



#############################################################################################


###########################################################
###########################################################
## Demo - Reporting Life in Transition Survey 2011 using
## R and MS Word - 30.10.2012 by Markus Kainu
###########################################################
###########################################################


###########################################################
###------------------ PART 1 ---------------------------###
#---- LOAD DATA FROM EBRD AND CLEAN IT FOR ANALYSIS ----- #
###########################################################
###########################################################
###########################################################


## Load Required Packages ##

library(car)
library(reshape2)
library(foreign)
library(stringr)

###########################################################
## Load Data ##
temp <- tempfile()
download.file("http://www.ebrd.com/downloads/research/surveys/lits2.dta", temp)
lits2 <- read.dta(temp)

###########################################################
## Clean data ##
# remove whitespaces from country
lits2$cntry <- str_trim(lits2$country_, side="right")
lits2$cntry <- as.factor(lits2$cntry)
# Recode "don't know"  -> "dont know"
lits2$crise <- as.factor(str_replace(lits2$q801, "Don't know", "Dont know"))
# Rename Sex and Age

lits2$sex <- lits2$q102_1 # Sex
lits2$age <- lits2$q104a_1 # Age
lits2$income <- lits2$q227 # income

## Merge classes
# Useless values into Not Applicaple
lits2$crise <- recode(lits2$crise, "c('Filtered','Not applicable','Not stated','Refused')=NA")

lits2$crise <- recode(lits2$crise, "'A GREAT DEAL'='a) A GREAT DEAL';
                      'A FAIR AMOUNT'='b) A FAIR AMOUNT';
                      'JUST A LITTLE'='c) JUST A LITTLE';
                      'NOT AT ALL'='d) NOT AT ALL';
                      'Dont know'='e) DONT KNOW'")

## Recode numerical values into factors

# Perceived income
lits2$income2 <- recode(lits2$income, "1:3='a) low income';
                        4:7='b) middle income';
                        8:10='c) high income';
                        else=NA")
lits2$income2 <- as.factor(lits2$income2)

#  Level of education
lits2$education <- recode(lits2$q515, "1:2='a) no or primary';
                          3:4='b) Secondary';
                          5:7='c) Tertiary or higher'")
lits2$education <- as.factor(lits2$education)

# Remove NA cases
lits2 <- lits2[ which(lits2$crise!= 'NA'), ]
lits2 <- lits2[ which(lits2$income2!= 'NA'), ]
lits2 <- lits2[ which(lits2$education!= 'NA'), ]

### Subset the final dataset ###
df2 <- subset(lits2, select=c("SerialID", "cntry",
                              "sex","age", # sex and age 
                              "weight","XCweight", # weights
                              "income","income2", # incomes
                              "crise","education"))

### Select 6 countries)

df <- subset(df2, cntry %in% c("Russia","Sweden","Italy",
                               "Mongolia","Turkey","Poland"))
df$cntry <- factor(df$cntry)

#####################################################
### Let's explore the data ###
#####################################################

head(df)
summary(df)
str(df)

########################################################
########################################################
### ------------- PART 2 ----------------------------###
#---------- Creation of the Word Document -------------#
########################################################
########################################################


## --------- begin preparations ------- ##

# In case you don't have "R2wd" and "rcom" -packages available, install them running following three rows. 
# Command "installstatconnDCOM()" will also install the current version of statconnDCOM that required for
# R to be able to interact with MS Word

## --- install packages "R2wd" and "rcom". --- ##

# install.packages("R2wd")
# install.packages("rcom")
# installstatconnDCOM()
# install.packages("statconnDCOM")

## --------- end preparations ------- ##



#####################################################
## --------- Creating the document ------- ##

## OPEN AN EMPTY DOCUMENT IN MS WORD !!!!! ###
##  THEN RUN THE CODE BELOW ##


library(R2wd)
library(rcom)
library(ggplot2)
library(reshape2)
library(car)
library(survey)
library(arm)

wdGet()
#####################################################
# Set page margins and orientation
wdPageSetup(orientation="portrait",
            margins=c(1,1,1,1),scope="all")
#####################################################
wdTitle("Reproducible reports using R and MS Word",
        label="R2wd")
#####################################################
#####################################################
#####################################################

#####################################################
#####################################################
wdSection("Introduction")
#####################################################
#####################################################

##
wdBody("This is an example on how to use the R2wd package for writing tables and graphs directly from microdata.")
##

#####################################################
wdSubsection("Plotting the age distribution using ggplot2")
#####################################################


plotfun<-function(t)
  print(ggplot(df, aes(x=age, color=sex)) + 
          geom_density() +
          theme(legend.position="top") +
          facet_wrap(~cntry))
wdPlot(t,plotfun=plotfun,
       caption="Unweighted distributions of age by sex",
       height=6,width=5)


#####################################################
#####################################################
wdSection("Statistical tables and graphs", newpage = TRUE)
#####################################################
#####################################################

##
wdBody("Then we made a section break and started from new page.")
##


#####################################################
wdSubsection("Mean age of respondents")
#####################################################

##
wdBody("First, we need a table presenting means and standard errors of age by sex and country in weighted scheme.")
##


#####################################################
### Survey design ###
#####################################################
library(survey)
d.df <- svydesign(id = ~SerialID, 
                  weights = ~weight, 
                  data = df)

# Means and standard errors of age by Sex and Country
t <- data.frame(svyby(~age, ~sex+cntry, design=d.df, svymean, na.rm=T))
names(t) <- c("Sex","Country","mean_age","SE")
t <- t[c(2,1,3,4)]

# lets round the numbers
t$mean_age <- round(t$mean_age, 1)
t$SE <- round(t$SE, 3)

##
wdBody("First, a table with no formatting.")
##

# table
wdTable(t)

##
wdBody("Then, on new page, a table with some formatting.")
##

# table
wdTable(t, caption = "Means and standard errors of age by Sex and Country", 
        # caption.pos="above", bookmark = NULL, 
        pointsize = 9, padding = 5, autoformat = 2, 
        row.names=FALSE)

##
wdBody("At last, we also want a graphical illustrations of the age with errorbars showing the standard errors" )
##

# errorbars for plot
errorbar <- aes(ymax = mean_age + SE, ymin=mean_age - SE)
# plot with errorbars
plotfun<-function(t)
  print(ggplot(t, aes(x=Country, y=mean_age, fill=Sex)) +
          geom_bar(position="dodge", stat="identity") +
          geom_errorbar(errorbar, position=(position_dodge(width=0.9)), width=0.25) +
          coord_cartesian(ylim=c(35,65)) +
          theme(legend.position="top"))
wdPlot(t,plotfun=plotfun,
       caption="Means and standard errors of age by Sex and Country",
       height=4,width=5)

#####################################################
wdSubsection("Perceived income by countries", newpage = T)
#####################################################

## quantities by country and gender
## as data.frame of relative shares
t <- prop.table(svytable(~income2+cntry, d.df),2)
t2 <- data.frame(t)
t2 <- t2[c(2,1,3)]

# for percentages and round at 2 decimal points
t2$Freq <- round((t2$Freq *100), 2)

wdTable(t2, caption.pos="above",bookmark = NULL, 
        pointsize = 9, padding = 5, autoformat = 3, 
        row.names=FALSE)

plotfun<-function(t2)
  print(ggplot(t2, aes(x=cntry, y=Freq, fill=income2, label=Freq)) + 
          geom_bar(stat="identity", position="dodge") + 
          theme(legend.position="top") +
          geom_text(position = position_dodge(width=1), vjust=-0.5, size=3))
wdPlot(t2,plotfun=plotfun,
       caption="Percentage distribution of income classes",
       height=3,width=6)

#####################################################
wdSectionBreak( continuous = TRUE, 
                bookmark = NULL,wdapp = .R2wd)

wdPageSetup(orientation="landscape" ,scope="section")
#####################################################

#####################################################
#####################################################
wdSubsection("Perceived effect of financial crisis by countries", 
             newpage = FALSE)
#####################################################

##
wdBody("And then one wide plot on a horizontal page" )
##

## quantities by country and crise and income
t <- prop.table(svytable(~crise+income2+cntry, d.df),3)
t2 <- data.frame(t)

# for percentages and round at 2 decimal points
t2$Freq <- round((t2$Freq *100), 2)

# Plot
plotfun<-function(t3)
  print(ggplot(t2, aes(x=cntry, y=Freq, fill=crise, label=Freq)) + 
          geom_bar(stat="identity", position="dodge") + coord_flip() + 
          facet_wrap(~income2) + theme(legend.position="top") +
          geom_text(position = position_dodge(width=1), hjust=-0.2, size=2))
wdPlot(t3,plotfun=plotfun,
       caption="Percentage distribution of household suffered a great deal of financial crisis",
       height=4,width=8)

#####################################################
wdSectionBreak( continuous = FALSE, 
                bookmark = NULL,wdapp = .R2wd)

wdPageSetup(orientation="portrait", scope="section")  
#####################################################

#####################################################
wdSubsection("Linear Regression", newpage = FALSE)
#####################################################

#####################################################
# Regression:
mod <- svyglm(income~crise+cntry+age, design=d.df)
regr_tab <- data.frame(summary(mod)$coefficients)
colnames(regr_tab) <- colnames(summary(mod)$coefficients)
regr_tab[ ,4] <- ifelse(regr_tab[ ,4] < .001, "< 0.001", 
                        ifelse(regr_tab[ ,4] < .01, "< 0.01", 
                               round(regr_tab[ ,4], 3)))
# print table to doc in word-default format:
wdTable(format(regr_tab), 
        caption = "Linear regression of perceived income", 
        caption.pos="above",bookmark = NULL, 
        pointsize = 9, padding = 5, autoformat = 3, 
        row.names=FALSE)
#####################################################
#####################################################
wdSubsection("Conclusions", newpage = FALSE)
#####################################################

##
wdBody("Works pretty well. For more information, google: R + R2wd!" )
##



wdSave("demo_report")
##
wdQuit()
#####################################################
#####################################################


lits2 <- read.csv("E:/workspace/lits/lits2/lits2.csv")