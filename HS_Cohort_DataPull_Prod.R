#setwd("/Users/stephenzeek/Documents/HS Analytics/Cohorting/Cohorting_Data")
####This script pulls Traffic, Closed Deal, Pre-Pay, First Payment, Plan, Trial, & Churn data####
#### and joins them into one data.table (excluding the churn data)####

#### This data comes from GA and will eventually be sent to a AMZN inbox and loaded automatically. 
#### For now, we just pull it and load the CSV
ga_archive <- read.csv("GA_by_Month_160415.csv") 
ga_archive <- data.table(ga_archive)
# ga_archive[,Date := as.Date(ga_archive[,Date],format = "%m/%d/%y")]

###Append Cohort to GA so it can be aggregated####
### Cohort = by week i.e. 0116 = week 1, 2016#####
# dateRange <- ga_archive$Date
# x <- as.POSIXlt(dateRange) 
# ga_archive$Cohort <- as.numeric(strftime(x,format="%W")) + 1
# ga_archive$Cohort <- sprintf("%02d",ga_archive$Cohort)
# ga_archive$year <- as.numeric(substr(ga_archive$Date,3,4))
# ga_archive$month <- as.numeric(substr(ga_archive$Date,6,7))
# ga_archive$Cohort <- paste(ga_archive$Cohort,ga_archive$year, sep = "")
ga_archive$MO_YR <- paste(ga_archive$Month,ga_archive$Year, sep = "")
ga_archive$MO_YR <- as.numeric(ga_archive$MO_YR)
ga_archive$MO_YR <- sprintf("%04d",ga_archive$MO_YR)

####Pull Together All Closed Deals
#### All Deals Closed after date in Query####
#### This will be left joined to Trial Data to create cohorts
#### i.e. How many trials that were created in week X eventually converted?
co_qry <- dbSendQuery(con, "SELECT com_id as Company_ID, com_name as Company_Name,DATE(cde_created_at) as Created_On, 
                      DATE(cde_closed_at) as Closed_On, msb_payment_type as Payment_Type,com_owner_id,com_deleted
                      FROM helpscout_companies, helpscout_company_deal,members_subscriptions,members_payments
                      WHERE com_id = cde_com_id AND msb_usr_id = com_owner_id AND msb_id = mpy_msb_id
                      AND com_id != 1
                      AND cde_closed_at BETWEEN '2015-01-01'AND curdate()
                      AND msb_payment_type = 1
                      AND mpy_status = 2")
co_archive <- data.table(fetch(co_qry, n = -1))
co_archive <- unique(co_archive)

###Data Type Transformation####
co_archive[,Company_ID := as.numeric(co_archive[,Company_ID])]
co_archive[,Created_On := as.Date(co_archive[,Created_On],format = "%Y-%m-%d")]
co_archive[,Closed_On := as.Date(co_archive[,Closed_On],format = "%Y-%m-%d")]
co_archive[,Days_to_Close := as.numeric(Closed_On - Created_On)]

#####Load Pre-Pays######
##### These Deals are in the closed deal table, but we need to treat their revenue differently
##### Joined Back to Deals with Pre-Paid Amount
####Query to ID Pre-Pays in Closed Deal Table######
prep_qry<- dbSendQuery(con,"select com_name as Company_Name, com_id as Company_ID, srp_msb_id, srp_amount as PrePaid_Amount, msb_usr_id, 
                        DATE(srp_created_at) as Prepay_Created_On, DATE(cde_closed_at) as Closed_On,srp_type
                      from helpscout_companies, helpscout_saas_report_prepays, members_subscriptions, helpscout_company_deal
                      WHERE com_id = cde_com_id AND msb_id = srp_msb_id AND com_owner_id = msb_usr_id 
                      AND cde_closed_at BETWEEN '2015-01-01' AND curdate() 
                      AND srp_type = 1
                      GROUP BY cde_closed_at")
prep <- data.table(fetch(prep_qry, n = -1))
prep <- unique(prep)

#####Merge Prepays Back to Closed Deals########
###### For folks new to R, "all.x = T" == LEFT JOIN
pre <- data.table(prep$Company_ID, prep$PrePaid_Amount)
setnames(pre, names(pre), c("Company_ID", "PrePaid_Amount"))
co_archive <- merge(co_archive, pre, by = "Company_ID", all.x = T)
co_archive$PrePaid_Amount[is.na(co_archive$PrePaid_Amount)] <- 0
co_archive$isprepay <- ifelse(co_archive$PrePaid_Amount > 0,1,0)

######First Payment#######
#### Looking for the first of a company's stream of payments by IDing min transaction date
#### Using group by to collapse into one row per company with min payment#####
pmt_qry <- dbSendQuery(con, "select com_id as Company_Name, com_name as Company_ID,DATE(cde_closed_at) as Closed_On,mpy_msb_id,
                       mpy_adjusted_total, msb_usr_id, DATE(min(mpy_created)) as First_Pay_Date, mpy_status
                       FROM helpscout_companies, members_payments, members_subscriptions, helpscout_company_deal
                       WHERE com_owner_id = msb_usr_id AND msb_id = mpy_msb_id AND cde_com_id = com_id AND mpy_adjusted_total > 0 
                       AND cde_closed_at BETWEEN '2015-01-01' AND curdate()
                       AND mpy_status = 2
                       GROUP BY com_id
                       ORDER BY First_Pay_Date desc")

pmt <- data.table(fetch(pmt_qry, n = -1))
pmt <- unique(pmt)
pmt <- data.table(pmt$Company_ID,pmt$Company_Name,pmt$mpy_adjusted_total,pmt$mpy_status)
setnames(pmt, names(pmt), c("Company_Name","Company_ID","First_Payment_Amt","Pymt_Status"))

######Bring First Payment Amt Back to co_archive, Make prepay first payment amount = 0#######
co_archive <- merge(co_archive, pmt, by = c("Company_Name","Company_ID"),all.x = T)
co_archive[isprepay == 1,First_Payment_Amt := 0]

#####Bring in Plan####
###This is current plan, and will change as companies up/downgrade####
### We will bring in initial plan and plan changes in V2
pln_qry <- dbSendQuery(con,"select com_id as Company_ID, com_name as Company_Name, 
                      msb_mpp_id as Plan_ID, mpp_label as Plan_Txt
                      FROM helpscout_companies, members_subscriptions, members_pricing_plans, helpscout_company_deal
                      WHERE com_owner_id = msb_usr_id AND msb_mpp_id = mpp_id AND cde_com_id = com_id
                      AND cde_created_at BETWEEN '2015-01-01' AND curdate()")
pln <- data.table(fetch(pln_qry, n = -1))

#####Trials#######
#####Build Temp Trials Query to Get to the rest of Development#####
##### We are blocked here because we need plaintext emails to properly scrub junk trials
##### See commented section below. Pulled temp trials object so I could continue to develop

tr_qry <- dbSendQuery(con, "select com_id as Company_ID, com_name as Company_Name,cmd_primary_vertical_id as Primary_Vertical, cde_account_status as Account_Status, 
              cde_funnel_stage as Funnel_Stage, com_user_count as Users, DATE(com_created_at) as Created
              FROM helpscout_companies 
              LEFT JOIN helpscout_company_marketing_data ON com_id = cmd_com_id 
              LEFT JOIN helpscout_company_deal ON com_id = cde_com_id
              WHERE com_created_at BETWEEN '2015-01-01' AND curdate()")
tr <- data.table(fetch(tr_qry, n = -1))
tr[,Created := as.Date(tr[,Created],format = "%Y-%m-%d")]

tr <- merge(tr,pln, by = c("Company_ID","Company_Name"),all.x = T) 

####Scrub Trial Email Addresses for "Test", "+","Zendesk","HappyFox","Freshdesk", etc
# zen <- tr_archive[like(Acct.Owner.Email,"zendesk")] 
# zen$key <- "Zendesk"
# hpy <- tr_archive[like(Acct.Owner.Email,"happyhox")]
# hpy$key <- "HappyFox"
# frs <- tr_archive[like(Acct.Owner.Email,"freshdesk")]
# frs$key <- "Freshdesk"
# ts <- tr_archive[like(Acct.Owner.Email,"test")]
# ts$key <- "test"
# Ts <- tr_archive[like(Acct.Owner.Email,"Test")]
# Ts$key <- "Test"
# nameTest <- tr_archive[like(Company.Name,"Test")]
# nameTest$key <- "Name_Test"
# nametest <- tr_archive[like(Company.Name,"test")]
# nametest$key <- "Name_test"
# nameTEST <- tr_archive[like(Company.Name,"TEST")]
# nameTEST$key <- "Name_TEST"
# asdf <- tr_archive[like(Company.Name,"asd")]
# asdf$key <- "asdf"
# dsada <- tr_archive[like(Company.Name,"dsada")]
# dsada$key <- "dsada"
# uv <- tr_archive[like(Acct.Owner.Email,"uservoice.com")]
# uv$key <- "uservoice"
# dsk <- tr_archive[like(Acct.Owner.Email,"desk.com")]
# dsk$key <- "desk.com"
# tsup <- tr_archive[like(Acct.Owner.Email,"teamsupport.com")]
# tsup$key <- "teamsupport"
# grv <- tr_archive[like(Acct.Owner.Email,"groovehq.com")]
# grv$key <- "Groove"
# Eli <- tr_archive[like(Acct.Owner.Email,"Eli*")]
# Eli$key <- "Eli"
# abc <- tr_archive[like(Company.Name,"abc")]
# abc$key <- "abc"
# tlk <- tr_archive[like(Acct.Owner.Email,"TalkDesk")]
# tlk$key <- "TalkDesk"
# dsd <- tr_archive[like(Company.Name,"dsd")]
# dsd$key <- "dsd"
# und <- tr_archive[like(Company.Name,"____*")]
# und$key <- "underscore"
# aaa <- tr_archive[like(Company.Name,"aaaa")]
# aaa$key <- "aaaa"
# fb <- tr_archive[like(Company.Name,"fb")]
# fb$key <- "fb"
# FB <- tr_archive[like(Company.Name,"FB")]
# FB$key <- "FB"
# dave <- tr_archive[like(Acct.Owner.Email,"designpro")]
# dave$key <- "dave"
# hs <- tr_archive[like(Acct.Owner.Email,"helpscout")]
# hs$key <- "helpscout"
# 
# scrub <- rbind(zen,hpy,frs,hot,ts,uv,dsk,tsup,grv,tlk,dsada,dsd,asdf,Ts,Eli,abc,nametest,nameTest,und,nameTEST,
#                aaa,fb,FB,dave,hs)

#####Churns######
ch_qry <- dbSendQuery(con,"SELECT src_com_id as Company_ID, com_name as Company_Name, DATE(cde_created_at) as Created_On, 
                           DATE(src_churned_at) as Churned_On,DATE(cde_closed_at) as Closed_On, src_churn_reason, src_churn_amount
                           from helpscout_saas_report_churn, helpscout_companies, helpscout_company_deal
                           WHERE src_com_id = com_id AND cde_com_id = com_id
                           AND cde_closed_at BETWEEN '2015-01-01' AND curdate()")
ch <- data.table(fetch(ch_qry, n = -1))
ch <- unique(ch)
ch[,Churned_On := as.Date(ch[,Churned_On],format = "%Y-%m-%d")]
ch[,Closed_On := as.Date(ch[,Closed_On],format = "%Y-%m-%d")]
ch[,Created_On := as.Date(ch[,Created_On],format = "%Y-%m-%d")]
ch[,Days_to_Churn := as.numeric(Churned_On - Closed_On)]

########Bring in Payment Data to Calculate Expansion Revenue######
######## Note that this does not account for pre-pays yet#########
#mpy_type == 1] ### is MRR payment i.e not pre-pay
#mpy_adjusted_total > 0] ### exclude refunds
#mpy_status == 2 & mpy_trans_id > 0 ] ###authorized and completed payments

tm_qry <- dbSendQuery(con,"select com_id, com_name, DATE(mpy_authorized_at) as mpy_authorized_at,
                      mpy_trans_id,mpy_msb_id,mpy_adjusted_total AS TM_Payment
                      FROM helpscout_companies, members_payments, members_subscriptions
                      WHERE com_owner_id = msb_usr_id AND mpy_msb_id = msb_id
                      AND com_id != 1
                      AND mpy_type = 1
                      AND mpy_adjusted_total > 0 
                      AND mpy_status = 2  
                      AND mpy_trans_id IS NOT NULL
                      AND MONTH(mpy_authorized_at) = (MONTH(NOW())) AND YEAR(mpy_authorized_at) = YEAR(NOW())")
tm <- data.table(fetch(tm_qry, n = -1))

lm_qry <- dbSendQuery(con,"select com_id, com_name, DATE(mpy_authorized_at) as mpy_authorized_at,mpy_adjusted_total, mpy_trans_id
                      FROM helpscout_companies, members_payments, members_subscriptions
                      WHERE com_owner_id = msb_usr_id AND mpy_msb_id = msb_id
                      AND com_id != 1
                      AND mpy_type = 1
                      AND mpy_adjusted_total > 0 
                      AND mpy_status = 2
                      AND mpy_trans_id IS NOT NULL 
                      AND MONTH(mpy_authorized_at) = (MONTH(NOW())-1) AND YEAR(mpy_authorized_at) = YEAR(NOW()) OR
                      MONTH(mpy_authorized_at) = 12 AND MONTH(NOW())=1 AND YEAR(mpy_authorized_at) = (YEAR(NOW()) - 1)")
lm <- data.table(fetch(lm_qry, n = -1))

twomon_qry <- dbSendQuery(con,"select com_id, com_name, DATE(mpy_authorized_at) as mpy_authorized_at,mpy_adjusted_total, mpy_trans_id
                          FROM helpscout_companies, members_payments, members_subscriptions
                          WHERE com_owner_id = msb_usr_id AND mpy_msb_id = msb_id
                          AND com_id != 1
                          AND mpy_type = 1
                          AND mpy_adjusted_total > 0 
                          AND mpy_status = 2
                          AND mpy_trans_id IS NOT NULL
                          AND MONTH(mpy_authorized_at) = (MONTH(NOW())-2) AND YEAR(mpy_authorized_at) = YEAR(NOW()) OR
                          MONTH(mpy_authorized_at) = 11 AND MONTH(NOW())=1 AND YEAR(mpy_authorized_at) = (YEAR(NOW()) - 1)")
twomon <- data.table(fetch(twomon_qry, n = -1))

#####Bring in count of payments to filter out new companies#####
pym_qry <- dbSendQuery(con, "select mpy_msb_id, count(`mpy_msb_id`) AS Payments
                       FROM members_payments, members_subscriptions
                       WHERE mpy_msb_id = msb_id
                       AND mpy_type = 1
                       AND mpy_adjusted_total > 0 
                       AND mpy_status = 2 
                       AND mpy_trans_id IS NOT NULL  
                       GROUP BY mpy_msb_id")
pym <- data.table(fetch(pym_qry, n = -1))

######Bring in Active Prepays######
exp_pre_qry <- dbSendQuery(con,"SELECT com_id, com_name, mpy_msb_id, DATE(mpy_authorized_at) as mpy_authorized_at, mpy_adjusted_total,mpy_type
                           from helpscout_companies, members_payments, members_subscriptions
                           WHERE com_owner_id = msb_usr_id AND mpy_msb_id = msb_id 
                           AND MONTH(mpy_authorized_at) = (MONTH(NOW())) AND YEAR(mpy_authorized_at) = YEAR(NOW())
                           AND mpy_adjusted_total > 0 
                           AND mpy_status = 2 
                           AND mpy_trans_id IS NOT NULL
                           AND com_id IN
                           (
                           select com_id from helpscout_companies, members_payments, members_subscriptions
                           WHERE com_owner_id = msb_usr_id AND mpy_msb_id = msb_id 
                           AND mpy_adjusted_total = 0 
                           AND MONTH(mpy_authorized_at) = (MONTH(NOW())-1) AND YEAR(mpy_authorized_at) = YEAR(NOW()) OR
                           MONTH(mpy_authorized_at) = 12 AND MONTH(NOW())=1 AND YEAR(mpy_authorized_at) = (YEAR(NOW()) - 1)
                           )")
exp_pre <- data.table(fetch(exp_pre_qry, n = -1))


