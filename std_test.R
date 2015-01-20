library(plyr)

x <- data.frame(id=1,value=c(1,2,3,4,5,6,7,8,9,10))
y <- data.frame(id=2,value=c(1,2,3,4,5,6,7,8,9,10)*5)
z <- data.frame(id=3,value=x[,2]*y[,2])

a <- rbind(x,y,z)

sd1 <- sd(x, na.rm = TRUE)
#sd2 <- sd1*sqrt((length(x)-1)/length(x))

sdg <- ddply(a, c("id"), summarize, sd = sd(value, na.rm = TRUE))