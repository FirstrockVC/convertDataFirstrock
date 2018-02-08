library(plyr)
library(dplyr)
library(magrittr)
library(digest)
library(lubridate)
library(sqldf)
library(chron)
library(zoo)
library(xts)
library(ggplot2)
library(reshape)
library(reshape2)
library(survival)
library(DT)
library(jsonlite)
args = commandArgs(trailingOnly=TRUE)

 data_t <- function(dataset,period) {
      data_2<- read.csv(dataset, 
                header = FALSE,  
                quote = "\"", 
                dec = ",", 
                fill = TRUE, 
                strip.white = TRUE, 
                stringsAsFactors=FALSE)
      if(ncol(data_2) == 2) {
        colnames(data_2) <- c("user_id","date_confirmed")
      } else if (ncol(data_2) == 3) {
        colnames(data_2) <- c("user_id","date_confirmed","revenue")
      } else {
        return(ncol(data_2))
      }
      
      if(ncol(data_2) == 3) {
        data_2f<-data_2
      } else {
        data_2f<-data_2
        data_2f$revenue<-0
      }
      
      if((nchar(data_2$date_confirmed[1])<11)=='TRUE') {
        data_2f$date_confirmed<-as.numeric(data_2f$date_confirmed)
        data_2f$date_confirmed<-as.Date(as.POSIXct(data_2f$date_confirmed, origin="1970-01-01"))
      } else {
        data_2f$date_confirmed<-as.numeric(data_2f$date_confirmed)/1000
        data_2f$date_confirmed<-as.Date(as.POSIXct(data_2f$date_confirmed, origin="1970-01-01"))
      }
      
      data_2f$date_confirmed_w<-floor_date(ymd(data_2f$date_confirmed), period)
      data_3 <- sqldf("select date_confirmed_w, user_id, sum(revenue) as revenue from data_2f group by date_confirmed_w, user_id order by date_confirmed_w")
      data_3<-filter(data_3, !is.na(user_id))
      data_3<-filter(data_3, !is.na(date_confirmed_w))
      data_4<-sqldf("select cohort_w, date_confirmed_w, data_3.user_id, revenue FROM data_3, (select user_id, min(date_confirmed_w) AS cohort_w from data_3 group by user_id) AS B where data_3.user_id=B.user_id")
      data_4$cohort_w<-as.Date(as.numeric(data_4$cohort_w),origin = "1970-01-01")
      data_5 <- sqldf("select cohort_w as cohort_period, date_confirmed_w as activity_period, count(user_id) as users, sum(revenue) as revenue from data_4 group by cohort_period, activity_period order by cohort_period, activity_period")
      data_5$cohort_period<-as.Date(as.numeric(data_5$cohort_period),origin = "1970-01-01")
      data_5$activity_period<-as.Date(as.numeric(data_5$activity_period),origin = "1970-01-01")
      a<-seq(min(data_5$cohort_period), max(data_5$cohort_period), by = period)
      R<-rep(a, each=length(a))
      go<-cbind(R,a,0,0)
      colnames(go)<-c("cohort_period","activity_period","users","revenue")
      go<-as.data.frame(go)
      go$cohort_period<-as.Date(as.numeric(go$cohort_period),origin = "1970-01-01")
      go$activity_period<-as.Date(as.numeric(go$activity_period),origin = "1970-01-01")
      data_5<-rbind(data_5,go)
      data_5<-sqldf("select cohort_period, activity_period, sum(users) as users, sum(revenue) as revenue from data_5 group by cohort_period, activity_period order by cohort_period, activity_period")
      data_5$diffdays<-data_5$activity_period-data_5$cohort_period
      data_5<-data_5[data_5[, "diffdays"]>=0,]
      data_5<-data_5[,1:4]
      data_5[,1]<-format(data_5[,1],'%Y-%m-%d')
      data_5[,2]<-format(data_5[,2],'%Y-%m-%d')
      colnames(data_5)<-c(paste("cohort_",period,sep=""),paste("activity_",period,sep=""),"users","revenue")
      data_5<-toJSON(data_5, pretty = FALSE)
      prettify(data_5)
} 

data_t(args[2], args[3])