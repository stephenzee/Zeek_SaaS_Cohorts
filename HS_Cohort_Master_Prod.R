######Master Script for Help Scout cohort Analysis#####
######This can be run with VPN connection and access to the Marketing Pipeline Database
###### which Zeek can provide seperately

setwd("/Users/stephenzeek/Documents/HS Analytics/Cohorting/Cohorting_Data/HS_Cohort_Prod_Repo/SRC")

begin <- Sys.time()
options(java.parameters = "-Xmx8000m")
library(data.table)
library(plyr)
library(zoo)
library(rJava)
library(xlsx)
options(stringsAsFactors = FALSE)

source("HS_DB_CON.R")
source("HS_Cohort_DataPull_Prod.R")
source("HS_Cohort_Computation_Prod.R")
source("HS_Cohort_Output_Prod.R")

end <- Sys.time()
elp <- end - begin
print(elp)