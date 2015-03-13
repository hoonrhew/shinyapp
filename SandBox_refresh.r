library(RODBC)
# ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) : this is to deal with errors. if no error, a==character(0), if errors, a is list of characters (a list, not vector.)
source("/srv/shiny-server/Global variables/DWH_access.r")


take_pause <- function(x)
{
  p1 <- proc.time()
  Sys.sleep(x)
  proc.time() - p1 # The cpu usage should be negligible
}


query <-"
USE sandbox
IF EXISTS (
  		SELECT *
			FROM sys.Tables
			where NAME = 'Items_line_item_level'
		)DROP TABLE Sandbox.dbo.Items_line_item_level

create table items_line_item_level (
item_id int null,
sku varchar(50) null,
taxon_id int null,
warehouse_id int null,
born_date smalldatetime null,
consgmt_born_date smalldatetime null,
has_discounts bit null,
designer_name varchar(255) null,
is_marquis smallint null,
CatLevel0 varchar(50) null,
CatLevel1 varchar(50) null,
FinanceCat varchar(50) null,
consignor_id int null,
consignorUserID int null,
commission_rule_id int null,
Consignor_Zipcode varchar(255) null,
Consignor_city varchar(255) null,
Consignor_StateInAddress varchar(255) null,
Consignor_Country varchar(255) null,
FstConsgOfTHECongr smalldatetime null,
NewConsgrMth int null,
BuyerOverlap varchar(255) null,
BoughtPriorThisConsgm int null,
BuyerFstPurchDate int null,
CurChannel varchar(255) null,
CurMM varchar(255) null,
CurMMCity varchar(255) null,
consignment_id int null,
merchandise_manager_id int null,
Channel varchar(255) null,
MM varchar(255) null,
MMCity varchar(255) null,
item_ordered_id int null,
order_id int null,
order_number varchar(255) null,
buyer_user_id int null,
buyer_cat varchar(255) null,
BuyerZipcode varchar(255) null,
BuyerCity int null,
BuyerStateInAddress varchar(255) null,
BuyerCountry varchar(255) null,
FstOrderOfTHEBuyer smalldatetime null,
NewBuyerMth int null,
BuyrMemshipCrted smalldatetime null,
consgrOverlap varchar(255) null,
ConsgedPriorThisSale int null,
sale_date smalldatetime null,
is_canceled int not null,
CancelIndicator varchar(255) null,
state varchar(255) null,
shipping_date smalldatetime null,
WaitingReturn varchar(255) null,
return_date smalldatetime null,
return_reason varchar(255) null,
price numeric null,
discount_order_amount numeric null,
discount_item_amount numeric null,
flash_sale_id int null,
SHIPPING_METHOD varchar(255) null,
consgrPmtDate smalldatetime null,
EstSalesPrUsingConsgrPmt decimal null,
consgrPmtAmt decimal(18,2) null,
consgrcommissp decimal null,
paid2consgr int null,
IdexPerItem int null,
Rankshipped int null,
Date_WH_IN int null,
root_taxonomy varchar(255) null,
RMA_expiration_date smalldatetime null,
parent_item_id int null
  -- constraint pkc_items_line_item_level primary key clustered (item_ordered_id, item_id)

)
"
query <-"use sandbox; truncate table  Sandbox.dbo.Items_line_item_level"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)

