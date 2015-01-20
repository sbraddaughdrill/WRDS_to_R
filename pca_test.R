

library(psych)
library(graphics)

data_pca <- USArrests

## The variances of the variables in the
## USArrests data vary by orders of magnitude, so scaling is appropriate
(pc.cr <- princomp(data_pca))  # inappropriate
pc.cr <- princomp(data_pca, cor = TRUE) # =^= prcomp(USArrests, scale=TRUE)
## Similar, but different:
## The standard deviations differ by a factor of sqrt(49/50)

pc.cr <- princomp(data_pca, cor = TRUE)
summary(pc.cr)
loadings(pc.cr)  ## note that blank entries are small but not zero
plot(pc.cr) # shows a screeplot.
plot(pc.cr,type="lines")
barplot(pc.cr$sdev/pc.cr$sdev[1])
biplot(pc.cr)
pc.cr$scores

## Formula interface
princomp(~ ., data = data_pca, cor = TRUE)

## NA-handling
data_pca[1, 2] <- NA
pc.cr <- princomp(~ Murder + Assault + UrbanPop,data = data_pca, na.action = na.exclude, cor = TRUE)
pc.cr$scores[1:5, ]

## (Simple) Robust PCA:
## Classical:
(pc.cl  <- princomp(stackloss))
## Robust:
(pc.rob <- princomp(stackloss, covmat = MASS::cov.rob(stackloss)))

#####################################


USArrestspca <- principal(data_pca, nfactors=4, rotate="none")



#####################################

#http://psych.colorado.edu/wiki/lib/exe/fetch.php?media=labs:learnr:emily_-_principal_components_analysis_in_r:pca_how_to.pdf

recorders = data.frame("X"=c(0,0,1,1), "Y" = c(0,1,1,0),row.names=c("A", "B","C","D"))
locs = data.frame("X"=c(.3,.5),"Y"=c(.8,.2))
intensities = data.frame("sine"=sin(0:99*(pi/10))+1.2,"cosine"= .7*cos(0:99*(pi/15))+.9)

dists = matrix(nrow=dim(locs)[1], ncol=dim(recorders)[1],dimnames=list(NULL, row.names(recorders)))
for (i in 1:dim(dists)[2]){
  dists[,i]=sqrt((locs$X-recorders$X[i])^2+ (locs$Y-recorders$Y[i])^2)
}
set.seed(500)
recorded.data = data.frame(jitter(as.matrix(intensities)%*%as.matrix(exp(-2*dists)),amount=0))

pr=prcomp(recorded.data)
pr
plot(pr) #Plotting te variances
barplot(pr$sdev/pr$sdev[1])
pr2=prcomp(recorded.data, tol=.1)
pr2
plot.ts(pr2$x)
if(.Platform$OS.type=="windows") {
  quartz<-function() windows()
}
quartz(); plot.ts(intensities)
quartz(); plot.ts(recorded.data)
quartz(); plot.ts(cbind(-1*pr2$x[,1],pr2$x[,2]))

od=pr$x %*% t(pr$rotation)
od2=pr2$x %*% t(pr2$rotation)
quartz(); plot.ts(recorded.data)
quartz(); plot.ts(od)
quartz(); plot.ts(od2)

#######################################

######################################
install.packages("nFactors")
library(nFactors)


ev <- eigen(cor(data_pca)) # get eigenvalues
ap <- parallel(subject=nrow(data_pca),var=ncol(data_pca),rep=100,cent=.05)
nS <- nScree(ev$values, ap$eigen$qevpea)
plotnScree(nS)

######################################

#install.packages("FactoMineR")
library(FactoMineR)
source("http://factominer.free.fr/install-facto.r")
library(Rcmdr)

####################################
http://www.stat.cmu.edu/~cshalizi/350/2008/lectures/14/lecture-14.pdf

state.x77

plot.new() # Start up a new plot
# How big is the plotting window? Set ranges from the data we'll be graphing
plot.window(xlim=range(state.center$x),ylim=range(state.center$y))
# Put the name of each state at its center. Shrink text 25% so there's less
# overlap between the names.
text(state.center,state.name,cex=0.75)
# Add the horizontal axis at the bottom and the vertical at the left.
axis(1)
axis(2)
# Draw a box.
box()
# Add the titles
title(main="Where R Thinks the US States Are",xlab="Longitude",ylab="Latitude")

prcomp(state.x77)
plot(prcomp(state.x77),type="l")

apply(state.x77,2,sd)

biplot(prcomp(state.x77))
# Plot original data points and feature vectors against principal
# components; shrink data-point names by 25% for contrast/legibility

biplot(prcomp(state.x77,scale.=TRUE),cex=c(0.5,0.75))



state.x77.centers = cbind(state.x77,state.center$x,state.center$y)
colnames(state.x77.centers) = c(colnames(state.x77),"Longitude","Latitude")

plot(prcomp(state.x77, scale. = TRUE),type="l")

biplot(prcomp(state.x77.centers,scale.=TRUE),cex=c(0.5,0.8))






states.density = cbind(state.x77,state.x77[,"Population"]/state.x77[,"Area"],state.center$x,state.center$y)
colnames(states.density) = c(colnames(state.x77),"Density","Longitude", "Lattitude")
biplot(prcomp(states.density,scale.=TRUE),cex=c(0.5,0.75))

states.density.randfrost = states.density
states.density.randfrost[,"Frost"] = sample(states.density[,"Frost"])
biplot(prcomp(states.density.randfrost,scale.=TRUE),cex=c(0.5,0.8))

factanal(states.density,factors=1)

# Make a biplot from the output of factanal
# Presumes: fa.fit is a fitted factor model of the
# type returned by factanal
# fa.fit contains a scores object
# fa.fit has at least two factors!
# Inputs: fitted factor analysis model, additional
# parameters to pass to biplot()
# Side-effects: Makes biplot
# Outputs: None
biplot.factanal <- function (fa.fit,...)
{
  # Get the first two columns of scores, i.e.,
  # scores on first two factors
  x = fa.fit$scores[,1:2]
  # Get the loadings on the first two factors
  y = fa.fit$loadings[,1:2]
  biplot(x,y,...)
}

# Make scree (eigenvalue-magnitude) plots from
# the output of factanal()
# Input: fitted model of class factanal,
# x-axis label (default "factor"),
# y-axis label (default "eigenvalue")
# graphical parameters to pass to plot()
# Side-effects: Plots eigenvalues vs. factor number
# Output: None
screeplot.factanal <- function(fa.fit,xlab="factor",ylab="eigenvalue",...) {
  # sum-of-squares function for repeated application
  sosq <- function(v) {sum(v^2)}
  # Get the matrix of loadings
  my.loadings <- as.matrix(fa.fit$loadings)
  # Eigenvalues can be recovered as sum of
  # squares of each column
  evalues <- apply(my.loadings,2,sosq)
  plot(evalues,xlab=xlab,ylab=ylab,...)
}


library(lattice) # For more graphics commands!
state.g = cbind(state.center$x,
                state.center$y,
                factanal(states.density,factors=1,scores="regression")$scores)
colnames(state.g) = c("Longitude","Latitude","G")
levelplot(state.g[,"G"]~state.g[,"Longitude"]*state.g[,"Latitude"],
          xlab="Longitude",ylab="Latitude")
# See help(levelplot) for more

biplot.factanal(factanal(states.density,factors=2,scores="regression"),cex=c(0.5,0.8))

