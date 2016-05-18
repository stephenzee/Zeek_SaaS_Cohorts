#setwd("/Users/stephenzeek/Documents/HS Analytics/Cohorting/Cohorting_Data")
####This Script picks up the data from "Cohort_Data_Pull" and does the actual computations####

####Make Cohorts - Week# and Year
dateRange <- tr$Created
x <- as.POSIXlt(dateRange) 
tr$Cohort <- as.numeric(strftime(x,format="%W")) + 1
tr$Cohort <- sprintf("%02d",tr$Cohort)
tr$year <- as.numeric(substr(tr$Created,3,4))
tr$Cohort <- paste(tr$Cohort,tr$year, sep = "")

#####create clean conversion dataset with only the variables we need at the moment#####
conv <- data.table(co_archive$Company_ID,co_archive$Company_Name,co_archive$Created_On,co_archive$Closed_On,
                   co_archive$Days_to_Close,co_archive$First_Payment_Amt,co_archive$isprepay,co_archive$PrePaid_Amount)
setnames(conv,names(conv),c("Company_ID","Company_Name","Created","Deal_Closed","Days_to_Close","First_Payment_Amt","isprepay",
                            "PrePaid_Amount"))
conv[,Company_ID := as.numeric(conv[,Company_ID])]
conv <-unique(conv)

####merge monthly and pre-pay conversions######
test <- merge(tr,conv, by = c("Company_ID","Company_Name","Created"), all.x = T)

####Flag Conversions#####
#### IF Deal_Closed != NA, treat as conversion (1), Else (0)###
test$Convert <- ifelse(!is.na(test$Deal_Closed),1,0)

###Compute Pre-Paid Applied and clean NAs
### Pre-Paid Applied = Annual Pre-paid amount/# weeks in year
### I should use a function (as below with na.zero) to clean NAs, will add it
test$pre_applied_wk <- round(test$PrePaid_Amount/53,2)
test$pre_applied_mo <- round(test$PrePaid_Amount/12,2)
test$pre_applied_wk[is.na(test$pre_applied_wk)] <- 0
test$pre_applied_mo[is.na(test$pre_applied_mo)] <- 0
test$PrePaid_Amount[is.na(test$PrePaid_Amount)] <- 0
test$isprepay[is.na(test$isprepay)] <- 0
test$First_Payment_Amt[is.na(test$First_Payment_Amt)] <- 0

### Clean scrubs from test object###
# test$scrub <- ifelse(test$Company.ID %in% scrub$Company.ID & test$Convert == 0,1,0)
# test <- test[scrub == 0]
# test <- unique(test)
# 
# rm(zen,hpy,frs,hot,ts,uv,dsk,tsup,grv,tlk,dsada,dsd,asdf,Ts,Eli,abc,nametest,nameTest,und,nameTEST,
#    aaa,fb,FB,dave,hs)

####Load LTV & merge with test#####
ltv_qry <- dbSendQuery(con,"select com_id as Company_ID, msb_lifetime_value as LTV
                       FROM helpscout_companies, members_subscriptions,helpscout_company_deal
                       WHERE com_owner_id = msb_usr_id AND cde_com_id = com_id
                       AND cde_closed_at BETWEEN '2015-01-01' AND curdate()")
ltv <- data.table(fetch(ltv_qry, n = -1))
test <- merge(test, ltv,by = "Company_ID", all.x = T )
test$LTV[is.na(test$LTV)] <- 0

####Read in Paid Search Data from GA Table####
ga_qry <- dbSendQuery(con,"select * from helpscout_google_analytics")
ga <- data.table(fetch(ga_qry, n = -1))

#####Merge with Trial Data
### This gives trial dataset w/First & Last Touch attribution#### 
trial_st <- merge(test,ga, by.x = "Company_ID",by.y = "hga_com_id", all.x = TRUE)
trial_st$hga_medium_first <-toupper(trial_st$hga_medium_first)
trial_st$hga_medium <-toupper(trial_st$hga_medium)
trial_st <- data.table(trial_st)
trial_st$hga_id <- NULL
trial_st$hga_content <- NULL
trial_st <- unique(trial_st)
trial_st$Month <- substr(trial_st$Created,6,7)
trial_st$Year <- substr(trial_st$Created,3,4)
trial_st$MO_YR <- paste(trial_st$Month,trial_st$Year,sep = "")
trial_st$PrePaid_Amount[is.na(trial_st$PrePaid_Amount)] <- 0
trial_st$First_Payment_Amt <- as.numeric(trial_st$First_Payment_Amt)

####Deal with first/last touch problem to get at paid channel###
####This takes out cannibalization by saying if first OR last is a growth channel, call the growth channel by name in $channel
trial_st$channel <- ifelse(trial_st$hga_source_first == "getapp" | trial_st$hga_source == "getapp","getapp",
                           ifelse(trial_st$hga_source_first == "capterra" | trial_st$hga_source == "capterra","capterra",
                                         ifelse(trial_st$hga_source_first == "ppc" | trial_st$hga_source == "ppc","ppc",
                                                ifelse(trial_st$hga_source_first == "google*" & trial_st$hga_medium_first == "CPC","ppc",
                                                       ifelse(trial_st$hga_source_first == "ppc-re" | trial_st$hga_source == "ppc-re","ppc-re",
                                                              ifelse(trial_st$hga_source_first == "ppc-tm" | trial_st$hga_source == "ppc-tm","ppc-tm",
                                                                     ifelse(trial_st$hga_source_first == "facebook" | trial_st$hga_source == "facebook","facebook",
                                                                            ifelse(trial_st$hga_source_first == "twitter" | trial_st$hga_source == "twitter","twitter",
                                                                                   ifelse(is.na(trial_st$hga_source_first) | is.na(trial_st$hga_source),"Other_Trials","Other_Trials")))))))))

trial_st$channel[is.na(trial_st$channel)] <- "Other_Trials"

###Active Trials####
trial_st$Active_TR <- ifelse(trial_st$Account_Status == 1,1,0)

# #Onboarded Trials - Acct Status = "Active Trial",15+ replies sent & closed date == NA
# trial_st$Onboard <- ifelse(trial_st$Account_Status == "Active Trial" & trial_st$X..Replies >= 15 & trial_st$Convert == 0,1,0)

####Make NAs in Days to Close 0 
# na.zero <- function (x) {
#   x[is.na(x)] <- 0
#   return(x)
# }

# trial_st$Days_to_Close <- na.zero(trial_st$Days_to_Close)
trial_st$Days_to_Close <- as.numeric(trial_st$Days_to_Close)
trial_st$LTV[is.na(trial_st$LTV)] <- 0
trial_st$First_Payment_Amt[is.na(trial_st$First_Payment_Amt)] <- 0

###Flag Churns
trial_st$Churn <- ifelse(trial_st$Company_ID %in% ch$Company_ID, 1,0)

####Flag Active Free Users####
trial_st$free_user <- ifelse(trial_st$Funnel_Stage == 6 & trial_st$Convert == 0 & trial_st$Account_Status != 4,1,0)

###Flag < 15 and >15 <30 day conversion#####
trial_st$fift <- ifelse(trial_st$Days_to_Close >= 0 & trial_st$Days_to_Close <= 15 & trial_st$Convert == 1,1,0)
trial_st$thirt <- ifelse(trial_st$Days_to_Close >= 0 & trial_st$Days_to_Close <= 30 & trial_st$Convert == 1,1,0)

#####Plus + Standard Conversions#######
trial_st$plusconv <- ifelse(trial_st$Plan_ID == 14 & trial_st$Convert == 1,1,0)
trial_st$st_conv <- ifelse(trial_st$Plan_ID == 13 & trial_st$Convert == 1 |trial_st$Plan_ID == 11 & trial_st$Convert == 1,1,0)
####Trial Counts####
trial_st$plus_tr <- ifelse(trial_st$Plan_Txt == "Plus",1,0)
trial_st$plus_tr[is.na(trial_st$plus_tr)] <- 0
trial_st$stan_tr <- ifelse(trial_st$Plan_Txt == "Standard"|trial_st$Plan_Txt == "Standard (Legacy)",1,0)
trial_st$stan_tr[is.na(trial_st$stan_tr)] <- 0
trial_st$free_tr <- ifelse(trial_st$Plan_Txt == "Free"|trial_st$Plan_Txt == "Free (Legacy)",1,0)
trial_st$free_tr[is.na(trial_st$free_tr)] <- 0
#####Paid Deals#######
trial_st$pd_deals <- ifelse(trial_st$channel != "Other_Trials" & trial_st$Convert ==1,1,0)
####Add Day of Week for Day Aggregation######
trial_st$wkday <- weekdays(as.Date(trial_st$Created))


####Aggregate by Cohort######
deals_agg <- trial_st[,list(Start_Date = min(Created),Trials = (sum(plus_tr) + sum(stan_tr)), Converts = sum(Convert),New_Revenue = sum(First_Payment_Amt)+sum(PrePaid_Amount),
                            CR = round(sum(Convert)/(sum(plus_tr) + sum(stan_tr)),3),
                            CloseDays = quantile(Days_to_Close, .50, na.rm=TRUE),Plus_Tr = sum(plus_tr), Stan_Tr = sum(stan_tr), Free_Tr = sum(free_tr),
                            ARPC = round((sum(First_Payment_Amt)+sum(pre_applied_wk))/sum(Convert),2),ActTrials = sum(Active_TR), 
                            Churns = sum(Churn),Plus_Deals = sum(plusconv),Stan_Deals = sum(st_conv),Paid_Deals = sum(pd_deals),Paid_Deals_per = round(sum(pd_deals)/sum(Convert),2),
                            FreeUsrs = sum(free_user),FreeCR = round(sum(free_user)/length(Created),3),New_MRR = sum(First_Payment_Amt),
                            LTV = sum(LTV),preappltv = (sum(pre_applied_wk)*52),AdjLTV = round(sum(LTV) - (sum(pre_applied_wk)*52))
                            ),by = Cohort]

#####Create Aggregation by Channel and Cohort
chan_agg <- trial_st[,list(Trials = (length(Created)),Converts = sum(Convert),New_Revenue = sum(First_Payment_Amt)+sum(PrePaid_Amount),CR = round(sum(Convert)/(sum(plus_tr) + sum(stan_tr)),3),
                           CloseDays = quantile(Days_to_Close, .50, na.rm=TRUE),
                           ARPC = round((sum(First_Payment_Amt)+sum(pre_applied_wk))/sum(Convert),2),ActTrials = sum(Active_TR),
                           Churns = sum(Churn),
                           AdjLTV = round(sum(LTV) - (sum(pre_applied_wk)*52))), by = c("Cohort","channel")]

#chan_agg[is.na(chan_agg)] <- 0

#######Aggregation by Month#####
month_agg <- trial_st[,list(Start_Date = min(Created),Trials = (sum(plus_tr) + sum(stan_tr)),Converts = sum(Convert),New_Revenue = sum(First_Payment_Amt)+sum(PrePaid_Amount),
                            CR = round(sum(Convert)/(sum(plus_tr) + sum(stan_tr)),3),New_MRR = sum(First_Payment_Amt),
                            CloseDays = quantile(Days_to_Close, .50, na.rm=TRUE),Plus_Tr = sum(plus_tr), Stan_Tr = sum(stan_tr), Free_Tr = sum(free_tr),
                            New_ARPC = round((sum(First_Payment_Amt)+sum(pre_applied_mo))/sum(Convert),2),ActTrials = sum(Active_TR), 
                            Churns = sum(Churn), 
                            FreeUsrs = sum(free_user),FreeCR = round(sum(free_user)/length(Created),3),
                            TrialConv = round(sum(fift)/sum(Convert),3),ExTrialConv = round(sum(thirt)/sum(Convert),3),
                            LTV = sum(LTV),preappltv = (sum(pre_applied_mo)*52),AdjLTV = round(sum(LTV) - (sum(pre_applied_mo)*52)),
                            Pay_Cust = (sum(Convert)-sum(Churn)),ChurnPer = round(sum(Churn)/sum(Convert),2)),by = MO_YR]


######By Day######
day_agg <- trial_st[,list(Trials = (length(Company_ID)),Converts = sum(Convert),
          Plus_Tr = sum(plus_tr), Stan_Tr = sum(stan_tr), Free_Tr = sum(free_tr)), by = Created]
day_agg$wkday <- weekdays(as.Date(day_agg$Created))

####Aggregate Traffic Data and Join with Deals Agg on Cohort####
# traffic_agg <- ga_archive[,list(Uniques = sum(Uniques),Evaluators = sum(Evaluators),Readers = sum(Readers)),
#                           by = Cohort]
# deals_agg <- merge(deals_agg,traffic_agg, by = "Cohort", all.x = T)

####Aggregate Traffic Data and Join with Month Agg on Cohort####
traffic_agg_mon <- ga_archive[,list(Uniques = sum(Uniques),Evaluators = sum(Evaluators),Readers = sum(Readers)),
                          by = MO_YR]
month_agg <- merge(month_agg,traffic_agg_mon, by = "MO_YR", all.x = T)

####Logic for Ordering by Cohort and by Year######
deals_agg$Year <- substr(deals_agg$Cohort,3,4)
deals_agg$Month <- substr(deals_agg$Start_Date,6,7)
deals_agg$Month_Year <- paste(deals_agg$Year,deals_agg$Month, sep = " ")
deals_agg <- deals_agg[order(Year),]

month_agg$Year <- substr(month_agg$MO_YR,3,4)
month_agg <- month_agg[order(Year),]
month_agg$MO_YR <- as.numeric(month_agg$MO_YR) 
month_agg$MO_YR <- sprintf("%04d",month_agg$MO_YR)

#########Create Objects for Additional Output Tables that will be picked up by 3rd party for visualization####
#### See HS_Cohort_Output_Prod.R#####
####Add Count of Traffic Source (Last Touch) and % of total####
#### .N gives counts by the unique strings in the by = clause####
source <- trial_st[, .N, by = hga_medium]
source <- source[order(-N)]
for(i in nrow(source)){
  source$perc_total <- round(source$N/sum(source$N),5)*100
}

#####Trials by Plan######
plan <- trial_st[, .N, by = Plan_Txt]
plan <- plan[order(-N)]
for(i in nrow(plan)){
  plan$perc_total <- round(plan$N/sum(plan$N),5)*100
}
#######Build Converts by Type Chart######
plus <- sum(deals_agg$Plus_Deals)
st <- sum(deals_agg$Stan_Deals)
fr <- sum(deals_agg$FreeUsrs)
tot <- sum(plus,st,fr)
plus_signup <- round(plus/tot * 100,2)
st_signup <- round(st/tot * 100,2)
fr_signup <- round(fr/tot* 100,2)
types <- data.frame(plus_signup,st_signup,fr_signup)

#######Remove any Closed Deals from Scrubs#####
# scrub$keep <- ifelse(scrub$Company.ID %in% trial_st$Company.ID, 1,0)
# scrub <- scrub[keep == 0]


#######calculate expansion revenue MRR only#####
lm <- data.table(lm$com_id,lm$mpy_adjusted_total)
setnames(lm, names(lm), c("com_id","LM_Payment"))
twomon <- data.table(twomon$com_id,twomon$mpy_adjusted_total)
setnames(twomon, names(twomon), c("com_id","Twomon_Payment"))
tm <- merge(tm,lm, by = "com_id", all.x = T)
tm <- merge(tm,twomon, by = "com_id", all.x = T)
tm <- merge(tm,pym, by = "mpy_msb_id", all.x = T)
tm$churn <- ifelse(tm$com_id %in% ch$Company_ID,1,0)
tm$prepay <- ifelse(tm$com_id %in% prep$Company_ID & is.na(tm$LM_Payment),1,0)
tm <- tm[Payments > 1]
tm <- tm[churn == 0]
tm <- tm[prepay == 0]
tm <- data.table(tm)
tm <- unique(tm)

tm$exprev <- ifelse(!is.na(tm$LM_Payment),tm$exprev <- tm$TM_Payment - tm$LM_Payment,
                    tm$exprev <- tm$TM_Payment - tm$Twomon_Payment)

tm$exprev[is.na(tm$exprev)] <- 0


#####aggregate#####
expan <- tm[,list(expansion_rev = sum(exprev)), by = mpy_authorized_at]
expan <- expan[order(mpy_authorized_at)]











