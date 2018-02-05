args = commandArgs(trailingOnly=TRUE)
library(jsonlite)
sayHello <- function(name, saludo){
name<-toJSON(name)
}

sayHello(args[1], args[1])