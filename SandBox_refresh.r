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

## Items_line_item_level_V7_consignorPMT revision
query <-"
use dwh_report;
insert into Sandbox.dbo.Items_line_item_level
select
    item.item_id as item_id,
		item.sku,
		item.taxon_id,
		item.warehouse_id,
		item.born_date,
  	null as consgmt_born_date,
		item.has_discounts,
		NULL as designer_name,
		NULL is_marquis,
		NULL CatLevel0,
		NULL as CatLevel1,
		NULL as FinanceCat,
    item.consignor_id,
		NULL as consignorUserID,
		NULL as commission_rule_id,

		/** insert fake values. will be replaced with NULL. I don't want to deal with field type. I'm willing to change for any functional suggestion. */

		NULL as Consignor_Zipcode, -- will be updated in later part
		NULL as Consignor_city,         -- will be updated in later part
		NULL as Consignor_StateInAddress,   -- will be updated in later part
		NULL as Consignor_Country,           -- will be updated in later part

		NULL as FstConsgOfTHECongr,
		NULL as NewConsgrMth,
		NULL as BuyerOverlap,
		NULL as BoughtPriorThisConsgm, /* prior??  better naming? */
		NULL as BuyerFstPurchDate,

		NULL as CurChannel,
		NULL as CurMM,
		NULL as CurMMCity,

		item.consignment_id,
		NULL as merchandise_manager_id,
		NULL as Channel,
		NULL as MM,
		NULL as MMCity,


		/*************************************/
		/*** line_item and buyer information */
		/*************************************/
		/*-- item.item_id,											 */
		iod.item_ordered_id,
		-- iod.order_id as order_id_LI,
		od.order_id as order_id,
    od.number as order_number,
		iod.user_id as buyer_user_id,
		NULL as buyer_cat,

		od.zipcode as BuyerZipcode,
		NULL as BuyerCity,
		od.ship_state as BuyerStateInAddress,
		od.country_id as BuyerCountry,

		NULL as FstOrderOfTHEBuyer,
		NULL as NewBuyerMth,
		NULL BuyrMemshipCrted,
		NULL as consgrOverlap,
		NULL  as ConsgedPriorThisSale, /* -- prior??  better naming? */
		iod.sale_date,
		case when iod.is_canceled=1 or od.status='canceled' then 1 else 0 end as is_canceled,
		case when iod.is_canceled=1 then 'itemLevelCancel' when od.status='canceled' then 'orderLevelCancel' else NULL end as CancelIndicator,
		iod.state,
		iod.shipping_date,
		NULL as WaitingReturn,

		NULL as return_date,
		NULL as return_reason,
		iod.price,
		iod.discount_order_amount,
		iod.discount_item_amount,
		iod.flash_sale_id,
		NULL AS SHIPPING_METHOD,

		NULL as consgrPmtDate,
		null as EstSalesPrUsingConsgrPmt,
		null as consgrPmtAmt,
		null as consgrcommissp,
		null as paid2consgr,
		NULL as IdexPerItem,
		NULL as Rankshipped,
		NULL as Date_WH_IN,
    NULL as root_taxonomy,
    NULL as RMA_expiration_date,
    item.parent_id as parent_item_id

from
		/******************************/
		/*  about Item  ***************/
		/******************************/
		items item
		full outer join (select * from item_ordered where not (sale_date <'2013-07-01' and price=5)) iod -- needs to be here for 'full outer join'
		/*-- 1st look memebership subscription exclusion need to be here!!
    if this is where caluse below, all items which do not have sales record will not be included, bc sale_date is null
    */
		on item.item_id=iod.item_id
		full outer join DWH_Report.dbo.orders od        /*-- needs to be here for 'full outer join' */
		on od.order_id=iod.order_id

    /*
    where
		(item_type_id<>2 or item.item_id is null)  /*-- not gift card. */
		-- and item.sku is not null */
 order by item.born_date, iod.sale_date, iod.item_ordered_id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #

par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan) ;take_pause(5)

##########################################
### Update consignor address
##########################################

query <-"
		/** Look up Dior and fill up addresses */
		update Sandbox.dbo.Items_line_item_level
		set Consignor_Zipcode=coalesce(substring(zp.zipcode,1,5), substring(zp1.zipcode,1,5), substring(sSHads.zipcode,1,5), substring(sbiads.zipcode,1,5)) ,
			Consignor_city=case when zp.zipcode is not null then zp.city when zp1.zipcode is not null then  zp1.city else null end,
			Consignor_StateInAddress=case when zp.zipcode is not null then zp.state when zp1.zipcode is not null then  zp1.state else NULL end,
			Consignor_Country=	case
									when
									zp.zipcode is not null or  zp1.zipcode is not null then 'US'
									when
									case when sSHads.zipcode is not null then sSHads.country_id when sbiads.zipcode is not null then  sbiads.country_id else NULL end
									=214 then 'US'
									when
									case when sSHads.zipcode is not null then sSHads.country_id when sbiads.zipcode is not null then  sbiads.country_id else null end
									=35 then 'CANADA'
									else NULL end
		from Sandbox.dbo.Items_line_item_level ilil
		left join ODS_Report.Dior.consigners con on con.id=ilil.consignor_id
		left join ODS_Report.Dior.spree_users ur on con.user_id=ur.id
		left join ODS_Report.Dior.spree_addresses sSHads on sSHads.id=ur.ship_address_id
					left join (select zipcode, city, state from sandbox.dbo.zipcode_primary) zp
					on substring(sSHads.zipcode,1,5)=zp.zipcode
		left join ODS_Report.Dior.spree_addresses sbiads on sbiads.id=ur.bill_address_id
					left join (select zipcode, city, state from sandbox.dbo.zipcode_primary) zp1
					on substring(sbiads.zipcode,1,5)=zp1.zipcode
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)


# /***************************************************************/
# /*** RANK each SHIPPING per Item *******************************/
# /* Refer and modify!
# http://technet.microsoft.com/en-us/library/ms186734.aspx */
query <-"

	USE SANDBOX;
		IF EXISTS (
			SELECT *
			FROM sys.Tables
			where NAME = 'Items_line_item_level_t'
		)DROP TABLE Sandbox.dbo.Items_line_item_level_t

		select
		*,
		case when is_canceled=0 and shipping_date is not null then ROW_NUMBER() over( partition by item_id order by shipping_date) else NULL end as RankShipped_t
		into Sandbox.dbo.Items_line_item_level_t
		from Sandbox.dbo.Items_line_item_level
		where
		is_canceled=0 and shipping_date is not null

		update Sandbox.dbo.Items_line_item_level
		set Rankshipped= ililt.Rankshipped_t
		from Sandbox.dbo.Items_line_item_level ilil
		left join Sandbox.dbo.Items_line_item_level_t ililt
		on ilil.item_ordered_id=ililt.item_ordered_id

		drop table Sandbox.dbo.Items_line_item_level_t

/*** END fo 'RANK each SHIPPING per Item'***********************/
/***************************************************************/
"
#chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
#par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
#odbcClose(chan);take_pause(5)


query <-"
    		UPDATE ilil
    		SET  ilil.consignorUserID=consgr.user_id
    		FROM Sandbox.dbo.Items_line_item_level ilil
    		LEFT JOIN dwh_report.dbo.consignors consgr
    		ON consgr.consignor_id=ilil.consignor_id
"


chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)
query <-"
      		UPDATE ilil
      		SET  ilil.root_taxonomy=rtax.name
      		FROM
      		Sandbox.dbo.Items_line_item_level ilil
      		left join ods_report.dior.spree_variants svar
      		on ilil.sku=svar.sku and svar.is_master=0
      		left join ods_report.dior.spree_products prod
      		on svar.product_id=prod.id
      		left join ods_report.dior.spree_taxonomies rtax
      		on rtax.id=prod.taxonomy_id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)




query <-"
  -- GO
		update ilil
		set merchandise_manager_id =congment.merchandise_manager_id,
			MM =mmgr.first_name+ ' '+ mmgr.last_name,
			MMCity =mmgr.city ,
			ilil.Channel=congmty.name
		FROM Sandbox.dbo.Items_line_item_level ilil
		left join dwh_report.[dbo].consignments congment
			on ilil.consignment_id=congment.consignment_id
		left join dwh_report.[dbo].[consignor_types] congmty
			on congmty.consignor_type_id=congment.consignor_type_id
		left join dwh_report.[dbo].[merchandise_managers] mmgr  -- this is jointed to consignment.
			on congment.merchandise_manager_id=mmgr.merchandise_manager_id

"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)


query <-"
  -- GO
		update ilil
		set
    CurChannel =cty.name ,
		CurMM =mm.first_name+ ' '+ mm.last_name,
		CurMMCity =mm.city
		FROM Sandbox.dbo.Items_line_item_level ilil
		left join (select consignor_id, consignor_type_id, merchandise_manager_id from dwh_report.dbo.consignors) consgr
			on ilil.consignor_id=consgr.consignor_id
		left join dwh_report.[dbo].[consignor_types] cty
			on cty.consignor_type_id=consgr.consignor_type_id
		left join dwh_report.[dbo].merchandise_managers mm
			on mm.merchandise_manager_id=consgr.merchandise_manager_id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)





query <-"
		update ilil
		set BuyrMemshipCrted =ur.created_at
		FROM Sandbox.dbo.Items_line_item_level ilil
		left join ods_report.dior.spree_users ur
			on ilil.buyer_user_id=ur.id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)


query <-"
update ilil
set
 consgrPmtDate =de.created_at
,consgrPmtAmt=cast(de.amount as decimal)
,consgrcommissp=case when de.amount is not null and cast(de.amount as decimal)<>0 then cli.percentage else null end
,EstSalesPrUsingConsgrPmt=case when de.amount is not null and cast(de.amount as decimal)<>0 and cli.percentage<>0 then cast(de.amount as decimal)*100/cli.percentage else null end
,paid2consgr=case when de.amount is not null and cast(de.amount as decimal)<>0 then cli.has_been_payed_to_consigner else null end

FROM Sandbox.dbo.Items_line_item_level ilil
left join ods_report.dior.consigner_line_items cli
on ilil.item_ordered_id=cli.spree_line_item_id
LEFT JOIN ods_report.dior.sale_records sr
ON sr.spree_line_item_id=cli.spree_line_item_id -- and cli.has_been_payed_to_consigner=1
left join ods_report.dior.double_entries de
on de.chargeable_event_id=sr.id
where  sr.amount IS NOT NULL AND sr.amount <>0
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)


query <-"
		update Sandbox.dbo.Items_line_item_level
		set SHIPPING_METHOD=sm.name
		from Sandbox.dbo.Items_line_item_level ilil
			left join (select order_id, min(shipping_method_id) as shipping_method_id from ODS_Report.Dior.spree_shipments group by order_id) sh
			on ilil.order_id=sh.order_id
			left join dwh_Report.[dbo].[shipping_methods] sm
			on sh.shipping_method_id= sm.shipping_method_id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)

#######################################################

query <-"
		update Sandbox.dbo.Items_line_item_level
		set designer_name=dsgr.name ,
        is_marquis=dsgr.is_marquis
		from Sandbox.dbo.Items_line_item_level ilil
			left join dwh_report.dbo.items item
			on ilil.item_id=item.item_id
		left join ODS_Report.Dior.designers dsgr  /*-- DWH_Report.designers doesn't have is_marquis. */
		on item.designer_id=dsgr.id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)



query <-"
		update Sandbox.dbo.Items_line_item_level
		set CatLevel0=tx.level0 ,
        CatLevel1=tx.level1
		from Sandbox.dbo.Items_line_item_level ilil
			left join dwh_report.dbo.items item
			on ilil.item_id=item.item_id
  		left join dwh_report.dbo.taxons tx
  			on item.taxon_id=tx.taxon_id

"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)


query <-"
		update Sandbox.dbo.Items_line_item_level
		set commission_rule_id=sconsgr.consigner_commission_rule_id
		from Sandbox.dbo.Items_line_item_level ilil
		left join ODS_Report.Dior.consigners sconsgr
		on ilil.consignor_id=sconsgr.id

"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)



query <-"
				select
				consignor_id,
				min(born_date)  as MthNewConsgr
        into  #NewConsgrMth
    		from Sandbox.dbo.Items_line_item_level
				group by consignor_id

		update Sandbox.dbo.Items_line_item_level
		set FstConsgOfTHECongr=NewConsgrMth.MthNewConsgr
        ,NewConsgrMth=case when CONVERT(VARCHAR(7), ilil.born_date, 120)=CONVERT(VARCHAR(7), NewConsgrMth.MthNewConsgr, 120) then 1 else null end
		from Sandbox.dbo.Items_line_item_level ilil
		left join #NewConsgrMth  NewConsgrMth
		on ilil.consignor_id=NewConsgrMth.consignor_id

		drop table #NewConsgrMth
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)


#########################

query <-"
		select
  		buyer_user_id,
  		min(sale_date)  as MthNewBuyer
        into #NewBuyerMth
        from Sandbox.dbo.Items_line_item_level ilil
				group by buyer_user_id

    		update ilil
    		set FstOrderOfTHEBuyer=NewBuyerMth.MthNewBuyer
            ,NewBuyerMth =case when CONVERT(VARCHAR(7), ilil.sale_date, 120)=CONVERT(VARCHAR(7), NewBuyerMth.MthNewBuyer, 120) then 1 else null end
    		from Sandbox.dbo.Items_line_item_level ilil
        left join #NewBuyerMth NewBuyerMth
        		on ilil.buyer_user_id=NewBuyerMth.buyer_user_id

        drop table #NewBuyerMth

"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)


query <-"
 			select
							consgr.user_id as user_id,
							it.consignor_id,
							min(it.born_date) as FstConsgDate
        into #consgrOverlap
					from
							dwh_report.dbo.items it
							left join dwh_report.dbo.consignors consgr
							on consgr.consignor_id=it.consignor_id
					group by consgr.user_id, it.consignor_id

    		update ilil
    		set consgrOverlap=case when consgrOverlap.user_id is not null then 'Consgr' else null end
                ,ConsgedPriorThisSale = case when consgrOverlap.FstConsgDate <= ilil.sale_date  then 1  else null end
			from Sandbox.dbo.Items_line_item_level ilil
            left join #consgrOverlap  consgrOverlap
        		on ilil.buyer_user_id=consgrOverlap.user_id

        drop table #consgrOverlap
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)

######################################################################


query <-"
select
io1.consignor_id,
min(io2.FstOrderOfTHEBuyer) as FstPurchDate
into #BuyerOverlap
from
Sandbox.dbo.Items_line_item_level io1
left join (
          select buyer_user_id, min(FstOrderOfTHEBuyer) as FstOrderOfTHEBuyer
          from Sandbox.dbo.Items_line_item_level
          group by buyer_user_id
          )io2
on io1.consignorUserID=io2.buyer_user_id
where   -- io1.sale_date is not null and
io1.sku is not null
and io2.buyer_user_id is not null
group by io1.consignor_id


update Sandbox.dbo.Items_line_item_level
set BuyerOverlap=case when BuyerOverlap.consignor_id is not null then 'Buyer' else null end
,BoughtPriorThisConsgm = case when BuyerOverlap.FstPurchDate <= ilil.born_date then 1  else null end
from Sandbox.dbo.Items_line_item_level ilil
left join #BuyerOverlap BuyerOverlap
on ilil.consignor_id=BuyerOverlap.consignor_id
where BuyerOverlap.consignor_id is not null

drop table #BuyerOverlap
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)


######################################################################


query <-"
		update Sandbox.dbo.Items_line_item_level
		set buyer_cat=tsc.name
		from Sandbox.dbo.Items_line_item_level ilil
			left join (select id, traffic_source_id from ODS_Report.Dior.spree_users) ur  /*-- users in DWH doesn't have address_id */
			on ilil.buyer_user_id=ur.id
			left join ODS_Report.Dior.traffic_sources ts
			on ts.id=ur.traffic_source_id
			left join ODS_Report.Dior.traffic_source_categories tsc
			on tsc.id=ts.traffic_source_category_id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)





query <-"
		/* -----------------------------------------------------
		-- irt.status is the state in SIU. SIU.state='returned' when spree_RA.state='accepted'
		-- in few cases, SIU.state='received' could mean 'accepted' (it's a bug in the current system, as of 2014/01/06. Buyer return dept. cannot assign 'accepted' in some(?) cases.
		--     Anyway, it's very few cases, so I'm ignoring for now.
		----------------------------------------------------- */
		update ilil
		set RMA_expiration_date=rr.expiration_date ,
		    WaitingReturn =  case when irt.status='shipped' and  (rr.status not in ('deleted','rejected') or rr.status is null)  and rr.expiration_date >= CURRENT_TIMESTAMP then 'waiting_return' else null end,
		    return_date = case when irt.status='returned' then irt.return_date else null end    /* -- RAM issued is not considered here. only Return received! */
		from Sandbox.dbo.Items_line_item_level ilil
			left join (select item_ordered_id,return_request_id, status, return_date, reason, ROW_NUMBER() OVER (partition by item_ordered_id order by return_date desc) as rownum from dwh_report.dbo.item_returns) irt
			on ilil.item_ordered_id=irt.item_ordered_id and rownum=1
				left join dwh_report.dbo.return_requests rr
			on rr.return_request_id=irt.return_request_id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)

query <-"
		update ilil
		set
		    return_reason = case when irt.status='returned' or (rr.status not in ('canceled','rejected') and irt.status='shipped' and rr.expiration_date >= CURRENT_TIMESTAMP ) then irt.reason else null end
		from Sandbox.dbo.Items_line_item_level ilil
			left join (select item_ordered_id,return_request_id, status, return_date, reason, ROW_NUMBER() OVER (partition by item_ordered_id order by return_date desc) as rownum from dwh_report.dbo.item_returns) irt
			on ilil.item_ordered_id=irt.item_ordered_id and rownum=1
				left join dwh_report.dbo.return_requests rr
			on rr.return_request_id=irt.return_request_id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)



query <-"
          select
          item_id,
          item_ordered_id,
          sale_date,
          ROW_NUMBER() over( partition by ilil.item_id order by ilil.sale_date) as IdexPerItem
          into #temp
          from Sandbox.dbo.Items_line_item_level ilil
          where item_id is not null and sku is not null

          update ilil
          set IdexPerItem=t.IdexPerItem
          from Sandbox.dbo.Items_line_item_level ilil
          left join #temp t
          on t.item_id=ilil.item_id and t.item_ordered_id=ilil.item_ordered_id

          drop table #temp
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)

##########

query <-"
          update ilil
          set
          		BuyerCountry= country.name
          from
          Sandbox.dbo.Items_line_item_level ilil
          left join dwh_report.[dbo].[countries] country
          on country.country_id=ilil.BuyerCountry
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)



query <-"
update ilil
set
FinanceCat= case when CatLevel0='Art'  then 'Art'
                 when CatLevel1 like '%jewelry%'  then 'Jewelry'
                 when CatLevel1 like '%watches%' then 'Watches'
                 when CatLevel0='Men' then 'Men'
                 when CatLevel0='Home' then 'Home'
                else 'Women' end

from
Sandbox.dbo.Items_line_item_level ilil
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)





query <-"
    	UPDATE ilil
    	SET  consgmt_born_date= coalesce(b.published_at, ilil.born_date)
    	FROM Sandbox.dbo.Items_line_item_level ilil
    	LEFT JOIN
    	(select
      	i.consignment_id ,
      	min( i.born_date) as published_at
        from
        dwh_report.dbo.items i
        -- where ( i.rejected_at is null and( i.status is null or i.status  not in ('removed_by_fm','removed_part_of_set','deleted')))
        group by
        i.consignment_id
      	) b
    	ON b.consignment_id=ilil.consignment_id
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)










#######################################################################################################
#######################################################################################################
################ Items_item_level
#######################################################################################################
#######################################################################################################

query <-"
/* comments out from the very front place is done for
coco. removal. I need to work on it.
*/

/*USE sandbox
IF EXISTS (

SELECT *
FROM sys.Tables
where NAME = 'Items_item_level'
)DROP TABLE Sandbox.dbo.Items_item_level
*/
truncate table  Sandbox.dbo.Items_item_level

USE DWH_Report
insert into  Sandbox.dbo.Items_item_level
select
ilil.*


,CntSoldShipped.CntTimesSold
,CntSoldShipped.CntTimesCanceled
,CntSoldShipped.CntTimesShipped
,CntSoldShipped.WaitingReturn
,CntSoldShipped.CntTimesReturned
,CntSoldShipped.CntTimesConsgrPmt
,CntSoldShipped.GSAtCongmtPmt
,CntSoldShipped.ConsgPmt
,CntSoldShipped.EstSalesPrUsingConsgrPmt
,CntSoldShipped.minCongrPmtDate
,CntSoldShipped.maxCongrPmtDate

,FstSale.sale_date as FstSaleDate
,FstSale.state  as FstSaleState
,FstSale.shipping_date as FstSaleShipDate
,FstSale.WaitingReturn as FstSaleWaitingReturn
,FstSale.return_date as FstSaleReturnDate
,FstSale.price as FstSalePrice
,FstSale.GSamt as FstSaleGSAmt
,NULL as FstMerchandisedDate
,NULL as DaysFromMerchToFstSale
,lastSale.sale_date as LatestSaleDate
,lastSale.state  as LastSaleState
,lastSale.shipping_date as LatestSaleShipDate
,lastSale.WaitingReturn as LatestSaleWaitingReturn
,lastSale.return_date as LatestSaleReturnDate
,lastSale.price as LatestSalePrice
,lastSale.GSamt as LatestGSamt
/* -- may add KILLED later. */

from
/*  Sandbox.dbo.Items_line_item_level has multiple records for an item (as many as 'number of ordered') */
(
select
item_id as item_id,
parent_item_id,
sku,
taxon_id,
warehouse_id,
born_date,
consgmt_born_date,
has_discounts,

designer_name,
is_marquis,
CatLevel0,
CatLevel1,
FinanceCat,
consignor_id,
consignorUserID,
commission_rule_id,
Consignor_Zipcode,
Consignor_city,
Consignor_StateInAddress,
Consignor_Country,
FstConsgOfTHECongr,
NewConsgrMth,
BuyerOverlap,
BoughtPriorThisConsgm, /*-- prior??  better naming? */

CurChannel,
CurMM,
CurMMCity,
consignment_id,
merchandise_manager_id,

Channel,
MM,
MMCity,

sum(price+discount_order_amount+discount_item_amount) as GSgenerated

from  Sandbox.dbo.Items_line_item_level
group by
item_id,
parent_item_id,
sku,
taxon_id,
warehouse_id,
born_date,
consgmt_born_date,
has_discounts,
designer_name,
is_marquis,
CatLevel0,
CatLevel1,
FinanceCat,
consignor_id,
consignorUserID,
commission_rule_id,
Consignor_Zipcode,
Consignor_city,
Consignor_StateInAddress,
Consignor_Country,
FstConsgOfTHECongr,
NewConsgrMth,
BuyerOverlap,
BoughtPriorThisConsgm, /* -- prior??  better naming? */

CurChannel,
CurMM,
CurMMCity,
consignment_id,
merchandise_manager_id,

Channel,
MM,
MMCity
) ilil


left join (
select
A.item_id,
count(*) as CntTimesSold,
sum(A.is_canceled) as CntTimesCanceled,
sum(case when A.shipping_date is not null then 1 else 0 end) as CntTimesShipped,
-- sum(case when A.WaitingReturn= 'waiting_return' then 1 else 0 end) as WaitingReturn,
sum(case when A.WaitingReturn is not null then 1 else 0 end) as WaitingReturn,
sum(case when A.return_date is not null then 1 else 0 end) as CntTimesReturned,
sum(case when consgrPmtAmt>0 then 1 else 0 end) as CntTimesConsgrPmt,

min(case when consgrPmtAmt>0 then consgrPmtDate else null end) as minCongrPmtDate,
max(case when consgrPmtAmt>0 then consgrPmtDate else null end) as maxCongrPmtDate,
max( case when consgrPmtAmt is not null then price+discount_order_amount+discount_item_amount else null end) GSAtCongmtPmt,
max(consgrPmtAmt) as ConsgPmt,
max(case when consgrPmtAmt>0 then EstSalesPrUsingConsgrPmt  else 0 end) as EstSalesPrUsingConsgrPmt

from

Sandbox.dbo.Items_line_item_level





A
group by A.item_id
) CntSoldShipped
on CntSoldShipped.item_id=ilil.item_id
left join
(select
* from
(select
iod.item_id, sale_date,state,
shipping_date,
case when irt.status='shipped' and rr.expiration_date >= CURRENT_TIMESTAMP then 'waiting_return' else null end as WaitingReturn,
case when irt.status='returned' then irt.return_date else null end as return_date, /* RAM issued is not considered here. only Return received! */
price,
price+discount_order_amount+discount_item_amount as GSamt,
ROW_NUMBER() over( partition by iod.item_id order by sale_date ) as rank
from item_ordered iod
left join item_returns irt
on iod.item_ordered_id=irt.item_ordered_id
left join return_requests rr
on rr.return_request_id=irt.return_request_id
) t
where t.rank=1
) FstSale
on CntSoldShipped.item_id=FstSale.item_id
left join
(select
* from
(select
iod.item_id, sale_date,state,
iod.user_id as buyer_user_id,
shipping_date,
case when irt.status='shipped' and rr.expiration_date >= CURRENT_TIMESTAMP then 'waiting_return' else null end as WaitingReturn,
case when irt.status='returned' then irt.return_date else null end as return_date, /* RAM issued is not considered here. only Return received! */
price,
price+discount_order_amount+discount_item_amount as GSamt,
ROW_NUMBER() over( partition by iod.item_id order by sale_date desc) as rank
from item_ordered iod
left join item_returns irt
on iod.item_ordered_id=irt.item_ordered_id
left join return_requests rr
on rr.return_request_id=irt.return_request_id
) t1
where t1.rank=1
) lastSale
on CntSoldShipped.item_id=lastSale.item_id


where ilil.item_id is not null and ilil.sku is not null
order by
ilil.born_date, ilil.item_id
;

/* select top 1000 * from Sandbox.dbo.Items_item_level */
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)



##########
query <-"

/*************************************/
/*** save the lastest execution time */

use sandbox;
IF EXISTS (
SELECT *
FROM sys.Tables
where NAME = 'lastestSandboxRefresh'
)DROP TABLE sandbox.dbo.lastestSandboxRefresh
;
select current_timestamp as lastSandboxFresh into sandbox.dbo.lastestSandboxRefresh

"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)



##########
query <-"
use sandbox;
ALTER TABLE Sandbox.dbo.Items_item_level
ALTER COLUMN DaysFromMerchToFstSale int
ALTER TABLE Sandbox.dbo.Items_item_level
ALTER COLUMN FstMerchandisedDate smalldatetime
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)
query <-"
update iil
set
FstMerchandisedDate= fsx.FirstMerchandisedDate,
DaysFromMerchToFstSale=datediff(day,case when fsx.FirstMerchandisedDate is null and iil.born_date <= iil.FstSaleDate then iil.born_date
when fsx.FirstMerchandisedDate is null and iil.born_date > iil.FstSaleDate then iil.FstSaleDate -- daystosell set to zero
when iil.born_date>= fsx.FirstMerchandisedDate then iil.born_date -- daystosell set to zero
else fsx.FirstMerchandisedDate end, iil.FstSaleDate )
From
sandbox.dbo.items_item_level iil
Left join

(
Select
fsi.item_id, fsi.flash_sale_id as first_flash_sale_id,
coalesce(first_look_starts_at, start_date) as FirstMerchandisedDate
,row_number() over (partition by fsi.item_id order by coalesce(first_look_starts_at, start_date) ) as rk
-- -- need to correct. flash_sale_id is not necessarily in order by first_look_starts_at

FROM DWH_Report.dbo.flash_sales_items fsi
left join DWH_Report.dbo.flash_sales fs
on fs.flash_sale_id = fsi.flash_sale_id and fs.label not like '% TEST%'

-- group by fsi.item_id, fsi.flash_sale_id
) fsx
on fsx.item_id = iil.item_id and  fsx.rk=1
where iil.sku is not null and iil.born_date is not null


update iil
set DaysFromMerchToFstSale=case when DaysFromMerchToFstSale<0 then 0 else DaysFromMerchToFstSale end
From
sandbox.dbo.items_item_level iil
"
chan <- odbcConnect(dsn="AMAZON_DWH",uid=uid , pwd=pwd) #chan <- odbcConnect(dsn="T3600-01",uid="hoon" , pwd="RealReal1") #
par <- 0; a<-"ERROR: "; while( ifelse(length(a)==0,FALSE, max(grepl("ERROR: ", a))==1) & par<5 ){if(par!=1){take_pause(5)}; a <- sqlQuery(chan, query); par <- par+1; print(a); print(par)} ; rm("par", "a")
odbcClose(chan);take_pause(5)





query<- "
drop table sandbox.[dbo].[Items_item_level]
CREATE TABLE sandbox.[dbo].[Items_item_level](
[item_id] [int] NULL,
[parent_item_id] [int] NULL,
[sku] [varchar](50) NULL,
[taxon_id] [int] NULL,
[warehouse_id] [int] NULL,
[born_date] [smalldatetime] NULL,
[consgmt_born_date] [smalldatetime] NULL,
[has_discounts] [bit] NULL,
[designer_name] [varchar](255) NULL,
[is_marquis] [smallint] NULL,
[CatLevel0] [varchar](50) NULL,
[CatLevel1] [varchar](50) NULL,
[FinanceCat] [varchar](50) NULL,
[consignor_id] [int] NULL,
[consignorUserID] [int] NULL,
[commission_rule_id] [int] NULL,
[Consignor_Zipcode] [varchar](255) NULL,
[Consignor_city] [varchar](255) NULL,
[Consignor_StateInAddress] [varchar](255) NULL,
[Consignor_Country] [varchar](255) NULL,
[FstConsgOfTHECongr] [smalldatetime] NULL,
[NewConsgrMth] [int] NULL,
[BuyerOverlap] [varchar](255) NULL,
[BoughtPriorThisConsgm] [int] NULL,
[CurChannel] [varchar](255) NULL,
[CurMM] [varchar](255) NULL,
[CurMMCity] [varchar](255) NULL,
[consignment_id] [int] NULL,
[merchandise_manager_id] [int] NULL,
[Channel] [varchar](255) NULL,
[MM] [varchar](255) NULL,
[MMCity] [varchar](255) NULL,
[GSgenerated] [numeric](38, 0) NULL,
[CntTimesSold] [int] NULL,
[CntTimesCanceled] [int] NULL,
[CntTimesShipped] [int] NULL,
[WaitingReturn] [int] NULL,
[CntTimesReturned] [int] NULL,
[CntTimesConsgrPmt] [int] NULL,
[GSAtCongmtPmt] [numeric](20, 0) NULL,
[ConsgPmt] [decimal](18, 2) NULL,
[EstSalesPrUsingConsgrPmt] [decimal](18, 0) NULL,
[minCongrPmtDate] [smalldatetime] NULL,
[maxCongrPmtDate] [smalldatetime] NULL,
[FstSaleDate] [smalldatetime] NULL,
[FstSaleState] [varchar](25) NULL,
[FstSaleShipDate] [smalldatetime] NULL,
[FstSaleWaitingReturn] [varchar](14) NULL,
[FstSaleReturnDate] [smalldatetime] NULL,
[FstSalePrice] [numeric](16, 2) NULL,
[FstSaleGSAmt] [numeric](18, 2) NULL,
[FstMerchandisedDate] [smalldatetime] NULL,
[DaysFromMerchToFstSale] [int] NULL,
[LatestSaleDate] [smalldatetime] NULL,
[LastSaleState] [varchar](25) NULL,
[LatestSaleShipDate] [smalldatetime] NULL,
[LatestSaleWaitingReturn] [varchar](14) NULL,
[LatestSaleReturnDate] [smalldatetime] NULL,
[LatestSalePrice] [numeric](16, 2) NULL,
[LatestGSamt] [numeric](18, 2) NULL
)
"