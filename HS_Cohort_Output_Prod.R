###This Script writes the aggregation from "Cohort Computation" to the Results folder within the "Cohorting Data" directory
#### AND to output tables within the marketing pipeline DB - these tables will be the primary source of shared
#### visualizations. The excel was used for dev and will be used as a backup. 

####Output to Excel#######
write.xlsx2(deals_agg, paste(getwd(),"/Results/",as.character(Sys.Date()),"_HS_Cohorts.xlsx",sep=""), sheetName="Cohorts",append = TRUE,row.names = F)
write.xlsx2(month_agg,paste(getwd(),"/Results/",as.character(Sys.Date()),"_HS_Cohorts.xlsx",sep=""), sheetName="By_Month", append=TRUE,row.names = F)
write.xlsx2(chan_agg,paste(getwd(),"/Results/",as.character(Sys.Date()),"_HS_Cohorts.xlsx",sep=""), sheetName="By_Growth_Channel",append = TRUE,row.names = F)
# write.xlsx2(trial_st,paste(getwd(),"/Results/",as.character(Sys.Date()),"_HS_Cohorts.xlsx",sep=""), sheetName="Raw_Data", append=TRUE,row.names = F)
# write.xlsx2(scrub,paste(getwd(),"/Results/",as.character(Sys.Date()),"_HS_Cohorts.xlsx",sep=""), sheetName="Scrubs", append=TRUE,row.names = F)
write.xlsx2(ch,paste(getwd(),"/Results/",as.character(Sys.Date()),"_HS_Cohorts.xlsx",sep=""), sheetName="Churns", append=TRUE,row.names = F)
write.xlsx2(day_agg,paste(getwd(),"/Results/",as.character(Sys.Date()),"_HS_Cohorts.xlsx",sep=""), sheetName="By_Day", append=TRUE,row.names = F)


#####Create Output Tables#####
##### Could use an apply function or loop here too if needed####
dbWriteTable(con, name='HS_Cohort_Output', value=deals_agg,row.names = F,overwrite = T)
dbWriteTable(con, name='HS_TrialPlan', value=plan,row.names = F,overwrite = T)
dbWriteTable(con, name='HS_LT_Traffic', value=source,row.names = F,overwrite = T)
dbWriteTable(con, name='HS_Cohort_Channel', value=chan_agg,row.names = F,overwrite = T)
dbWriteTable(con, name='HS_Sign_by_Type', value=types,row.names = F,overwrite = T)
dbWriteTable(con, name='HS_by_Day', value=day_agg,row.names = F,overwrite = T)
dbWriteTable(con, name='HS_by_Month', value=month_agg,row.names = F,overwrite = T)
dbWriteTable(con, name='Exp_by_Day', value=expan,row.names = F,overwrite = T)


dbDisconnectAll <- function(){
  ile <- length(dbListConnections(MySQL())  )
  lapply( dbListConnections(MySQL()), function(x) dbDisconnect(x) )
  cat(sprintf("%s connection(s) closed.\n", ile))
}

dbDisconnectAll()