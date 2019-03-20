#######################################
####         2018-07-25            ####
#######################################

library(tokenizers)
library(XLConnect)
library(arules)
library(dplyr)
library(tidyverse)
library(tm)
library(caret)
library(xgboost)

#set the directory
setwd(choose.dir())
getwd()

cust_jour<-read.csv(paste0(getwd(),"/Customer Journey for Month of December.csv"))
sampleset<-cust_jour[sample(size = 0.01*nrow(cust_jour),1:nrow(cust_jour)),]
head(sampleset)
gc()

############################################################
################### Load Naming Data #######################
############################################################

financials<- read.csv(paste0(getwd(),"/Financials.csv"))
distance<- read.csv(paste0(getwd(),"/Distance Matrix.csv"))
head(financials, n=5)

#Build a named distance matrix
distance<-data.frame(lapply(distance,gsub,pattern = "^0*",replacement =""))

lookup<-unique(financials[,c("ï..Tenant","Unit")])
lookup<-data.frame(lapply(lookup,gsub,pattern = "^0*",replacement =""))
lookup<-data.frame(lapply(lookup,trimws))
names(lookup)<-c("Tenant","Unit")
head(lookup)
named_dist<- inner_join(lookup, distance, by = c("Unit" = "Entrance"))
head(named_dist)
lookup<-rbind(lookup,updated)
named_dist<- inner_join(lookup, distance, by = c("Unit" = "Origin.Unit.or.Entrance"))
named_dist<- inner_join(lookup, named_dist, by = c("Unit" = "Destination.Unit"),suffix =c(".Destination",".Origin"))
named_dist<- data.frame(Origin = named_dist$Tenant.Origin, Destination = named_dist$Tenant.Destination, Distance = named_dist$Route_Distance..m.)

head(named_dist)
named_dist$Origin<-named_dist$Origin %>% tolower() %>% trimws()
head(named_dist$Origin)
named_dist$Destination<- named_dist$Destination %>% tolower() %>% trimws()
head(named_dist$Destination)

############################################
############ Match Naming Conv #############
############################################

clean_names<- function(set){
  clean_set <- set %>% unique() %>% tolower() %>% trimws() %>% unname() 
  return(clean_set)
}

#modified agrep
best_match <- function(pattern,x){
  y<-data.frame(x)
  tokens <- unlist(tokenizers::tokenize_words(pattern,strip_punct = FALSE))
  for(i in 1:length(tokens)){
    y<-cbind(y,as.numeric(grepl(pattern = tokens[i],x)))
    
  }
  if(sum(y[,-1])==0) return(NA)
  
  y$matches <- rowSums(cbind(y[,-1],rep(0,nrow(y))))
  
  m_val<-which(y$matches==max(y$matches))
  
  if(length(m_val>1)){
    delta<-rep(0,length(m_val))
    for(i in 1:length(m_val)){
      delta[i]<-abs(nchar(pattern) - nchar(x[m_val[i]]))
    }
    m_val<-m_val[which(delta == min(delta))]
  }
  
  ml_match<-as.character(y[m_val,1])
  
  return(ml_match)
}

#Function to create a lookup matrix
lookup_matx<- function(new_data,match_set){
  lmat<-data.frame(x = rep(NA,length(new_data)),x = rep(NA,length(new_data)))
  for(i in 1:length(new_data)){
    lmat[i,]<-c(as.character(new_data[i]),best_match(as.character(new_data[i]),match_set))
  }
  return(lmat)
}

#### Create nameing lookup table ####
dist_names<-clean_names(named_dist$Origin)
trans_names<-clean_names(sampleset$Space.Name)

lookup_tab<-lookup_matx(trans_names,dist_names) 
names(lookup_tab) <- c("Space.Name", "Dist.Name")

head(lookup_tab)
##############################################################
######### Format transactions into tidy for filtering ########
##############################################################

# Goal is to convert the data into tidy format.
detach("package:plyr", unload=TRUE)

head(sampleset)

#########################Sample set ######################################
tidy = sampleset %>% 
  mutate(ID = row_number()) %>%
  select(ID, everything()) %>% 
  select(-X) %>%
  rename(Space.Name.0 = Space.Name) %>%
  rename(Space.Type.0 = Space.Type) %>%
  rename(Space.Dwell.0 = Space.Dwell) %>%
  gather(key, value, -ID, -Date, -Encrypted.Mac) %>%
  mutate(space = gsub("Space.Name.|Space.Type.|Space.Dwell.", "", key)) %>%
  mutate(type = gsub("\\.\\d+", "", key)) %>%
  filter(!is.na(value)) %>%
  arrange(ID) %>%
  select(ID, Date, Encrypted.Mac, space, type, value) %>% 
  spread(type, value, convert=T) %>%
  filter(!is.na(Space.Dwell))

head(tidy)
tidy$Space.Name<-tidy$Space.Name %>% sapply(.,tolower)  %>% sapply(., trimws)

head(tidy)

write.csv(tidy, paste("December_No_connected_Customers_tidy.csv", sep=""))

tidy.filtered = tidy %>% filter(Space.Dwell >= 5) %>% filter(Space.Dwell <= 300) %>% filter(Space.Type == " STORE")
tidy.join<- inner_join(tidy.filtered, lookup_tab, by = c("Space.Name" = "Space.Name")) %>% select(ID,Dist.Name,Space.Dwell)

###############################################################
################## Analyze customer Journey ###################
###############################################################

total_spread<- function(set,named_dist){
  grid<-expand.grid(set,set) 
  distance_vec<-inner_join(named_dist,grid,by = c( "Origin"="Var1","Destination" = "Var2")) %>% select("Distance") %>% unlist() %>% unname()
  distance_vec<-as.numeric(levels(distance_vec))[distance_vec] %>% na.omit()
  spread<-sum(distance_vec)/length(distance_vec)
  return(spread)
}

seq_dist<- function(set,named_dist){
  org_len<-length(set)
  set<-c(set,NA)
  grid<-data.frame(store1=rep(0,org_len),store2=rep(0,org_len))
  for(i in 1:org_len){
    grid[i,]<-c(set[i],set[i+1])
  }
  distance_vec<-inner_join(named_dist,grid,by = c( "Origin"="store1","Destination" = "store2"))  %>% select("Distance") %>% unlist() %>% unname()
  distance_vec<-as.numeric(levels(distance_vec))[distance_vec] %>% na.omit()
  dist<-sum(distance_vec)
  return(dist)
}

get_stuff<-function(tidy.set,lookup_tab, named_dist){
  
  require(doParallel)
  gc()
  
  journey<- tidy.set %>% select(ID) %>% unique()
  
  no_cores<-parallel::detectCores()-1
  cl<-parallel::makeCluster(no_cores)
  doParallel::registerDoParallel(cl)
  
  par_calc<-foreach::foreach(i = 1:nrow(journey), .combine = 'rbind', .packages = c('dplyr','tm'), .export =  c('total_spread','seq_dist')) %dopar% {
    set<- tidy.set[which(tidy.set$ID==journey[i,]),] %>% select(Dist.Name,Space.Dwell)
    c(total_spread(set$Dist.Name,named_dist),seq_dist(set$Dist.Name,named_dist),mean(set$Space.Dwell))
  }
  
  parallel::stopCluster(cl)
  
  journey$spread<-par_calc[,1]
  journey$length<-par_calc[,2]
  journey$dwell<-par_calc[,3]
  
  return(journey)
}

head(tidy.join,n=5)
head(lookup_tab,n=5)
head(named_dist,n=5)

####### Aggregate and filter for journeys with more than one store visited
head(tidy.join)
agg_data<-get_stuff(tidy.join,lookup_tab,named_dist)%>% filter(length>0)
head(agg_data)

temp<-lm(dwell~spread,agg_data)
summary(temp)

temp<-lm(dwell~length,agg_data)
summary(temp)
cor(agg_data, method='spearman')

###################################################
################# Association Rules ################
####################################################

# load entrances
trans<-as(split(tidy.join[,"Dist.Name"],tidy.join[,"ID"]),"transactions")

# build hyper edge sets to reflect frequent trips
sets<-apriori(trans, 
              parameter = list(supp = 0.0005, conf = 0.2, target = "hyperedgesets", minlen = 3))

# Create a custom interestingness measre using lift x crossSupportRatio
lift<- interestMeasure(sets,measure = "lift",transactions = trans)
crossSupportRatio<- interestMeasure(sets,measure = "crossSupportRatio",transactions = trans)
quality(sets)$adjustedLift<- lift*crossSupportRatio 

#filter for best 20 itemsets                    
best_sets<-sort(sets,by="adjustedLift",decreasing = TRUE)[1:5]

arules::inspect(best_sets)

########################################################################################
########################################################################################

names(anchors) <- c("Lookup", "Lookup.Clean")
head(anchors)

anch_set<-subset(sets,subset = items %in% anchors$Lookup.Clean[1])

best_anch_sets<-sort(anch_set,by="adjustedLift",decreasing = TRUE) 
arules::inspect(best_anch_sets)

anch_set<-subset(sets,subset = items %in% anchors$Lookup.Clean[1])
arules::inspect(anch_set)
best_anch_sets<-sort(anch_set,by="adjustedLift",decreasing = TRUE)
arules::inspect(best_anch_sets)

best_anch_sets<-sort(anch_set,by="adjustedLift",decreasing = TRUE)[1:5] ####### Adjust number of rules to show
arules::inspect(best_anch_sets)
########################################################################################
########################################################################################

#put sets into list for analysis
best_sets<-as(items(best_sets),"list")

#export list  

write.table( data.frame(best_sets), file = paste0(getwd(),'/test.csv'), sep=',' )

capture.output(summary(best_sets),file = paste0(getwd(),'/test.txt'))

#############################################################
########### Calculate the spread for best sets ##############
#############################################################

total_spread(best_sets[[12]])















