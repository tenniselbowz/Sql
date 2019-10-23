USE [SFDCIntegrations]
GO
/****** Object:  StoredProcedure [dbo].[execute_daily_salesforce_etl]    Script Date: 10/22/2019 7:10:44 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Nathan Shinn
-- Create date: 1/9/2014
-- Description:	Loads Staging tables for Daily Salesforce.com Data Syncronization
-- =============================================
ALTER PROCEDURE [dbo].[execute_daily_salesforce_etl]
	@p_days_to_process int,
	@p_processing_status nvarchar(max) output
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	
	/********************************
      DCW Data
      *******************************/
	-- 
	-- DCW Accounts
	-- 
	--accounts
	drop table dcw_product
	select * into dcw_product
	  from openquery(BIZDATA1, 'Select pro_desc, dr_dc_key 
								  from dcw.dbo.dcw_Products
							   inner join dcw.dbo.dcw_DistRegions on dr_pro_code = pro_code')
	/*Account: TRC (012C0000000Hq9V)
	Contact: SFDC Integration (012C0000000HqOd)*/									
	drop table stg_dcw_Dist_Channel
	select distinct dc_co_name
									,dc_agent_type
									,dc_id_no
									,dc_parent_id_no
									,dc_location
									,dc_phone
									,dc_co_email
									,dc_co_website
									,dc_co_fax
									,dc_speed_no
									,dc_mail_qty
									,con_name
									,dc_dist_notes
									,dc_active_flag
									,dc_reg_office
									,record_type_id
									,dbo.get_dcw_products(dc_key) products 
				  into stg_dcw_Dist_Channel
			 from OPENQUERY(BIZDATA1,'Select distinct  dc_co_name
											,case dc_agent_type
												when ''DIS'' then	''Independent McQuay Rep''
												when ''SVC'' then	''MQ Factory Service''
												when ''PRT'' then	''Parts Distributor''
												when ''REP'' then	''Independent McQuay Rep''
												when ''OEM'' then	''OEM''
											 end dc_agent_type
											,dc_id_no
											,'''' as dc_parent_id_no
											,dc_location
											,dc_phone
											,dc_co_email
											,dc_co_website
											,dc_co_fax
											,dc_speed_no
											,dc_mail_qty
											,con_name
											,dc_dist_notes
											,dc_active_flag
											,dc_reg_office
											,dc_key
											,''012C0000000Hq9V'' as record_type_id
										from dcw.dbo.dcw_DistChannels
										left outer join dcw.dbo.dcw_Continents on dc_con_key = con_key
									   where DC_AGENT_TYPE  IN (''REP'', ''PRT'', ''SVC'', ''OEM'',''DIS'')
										 and dc_parent_id_no = dc_id_no');
										 
	alter table stg_dcw_Dist_Channel ALTER COLUMN dc_parent_id_no nvarchar(10)
	insert into stg_dcw_Dist_Channel(dc_co_name
									,dc_agent_type
									,dc_id_no
									,dc_parent_id_no
									,dc_location
									,dc_phone
									,dc_co_email
									,dc_co_website
									,dc_co_fax
									,dc_speed_no
									,dc_mail_qty
									,con_name
									,dc_dist_notes
									,dc_active_flag
									,dc_reg_office
									,record_type_id
									,products)
	select distinct 
		 dc_co_name
		,dc_agent_type
		,dc_id_no
		,dc_parent_id_no
		,dc_location
		,dc_phone
		,dc_co_email
		,dc_co_website
		,dc_co_fax
		,dc_speed_no
		,dc_mail_qty
		,con_name
		,dc_dist_notes
		,dc_active_flag
		,dc_reg_office
		,record_type_id 
		,dbo.get_dcw_products(dc_key) products 
		from OPENQUERY(BIZDATA1,'Select dc_co_name
											,case dc_agent_type
												when ''DIS'' then	''Independent McQuay Rep''
												when ''SVC'' then	''MQ Factory Service''
												when ''PRT'' then	''Parts Distributor''
												when ''REP'' then	''Independent McQuay Rep''
												when ''OEM'' then	''OEM''
											 end dc_agent_type
											,dc_id_no
											,dc_parent_id_no
											,dc_location
											,dc_phone
											,dc_co_email
											,dc_co_website
											,dc_co_fax
											,dc_speed_no
											,dc_mail_qty
											,con_name
											,dc_dist_notes
											,dc_active_flag
											,dc_reg_office
											,pro_desc
											, ''012C0000000Hq9V'' as record_type_id
											,dc_key
										from dcw.dbo.dcw_DistChannels
										left outer join dcw.dbo.dcw_Continents on dc_con_key = dcw.dbo.dcw_Continents.con_key
										left outer join (Select dr_dc_key, pro_desc 
														   from dcw.dbo.dcw_Products
														   inner join dcw.dbo.dcw_DistRegions on dr_pro_code = pro_code) dcw_Products
												 on dcw_Products.dr_dc_key = dc_key
									   where DC_AGENT_TYPE  IN (''REP'', ''PRT'', ''SVC'', ''OEM'',''DIS'')
								 and dc_parent_id_no <> dc_id_no');

	alter table stg_dcw_Dist_Channel ALTER COLUMN products text

	 
	--Addresses
	drop table stg_dcw_address
	Select * into stg_dcw_address
	from OPENQUERY(BIZDATA1,'select distinct  dc_id_no
											, dc_id_no dcw_id
											,  ''D-''+dc_ship_addr1 
												 +ISNULL(dc_ship_addr2, '''')
												 +ISNULL(dc_ship_addr3, '''') 
												 +ISNULL(dc_ship_city,'''') 
												 +ISNULL(dc_ship_state,'''') 
												 +ISNULL(dc_ship_zip,'''')
												 +ISNULL(dc_ship_country,'''') + dc_id_no warehouse_id
											, dc_ship_addr1 Address_Line_1	
											, dc_ship_addr2	Address_Line_2	
											, dc_ship_addr3	Address_Line_3
											, dc_ship_city Address_City
											, dc_ship_state Address_State_Province
											, dc_ship_zip Address_Postal_Code
											, dc_ship_country Address_Country
											, 1 Ship_to
											, 0 mail_to
											, dc_active_flag
									from dcw.dbo.dcw_DistChannels
								   where DC_AGENT_TYPE IN (''REP'', ''PRT'', ''SVC'', ''OEM'',''DIS'') 
								   union
								   select distinct  dc_id_no
											, dc_id_no dcw_id
											,  ''DCW_M_'' + dc_id_no warehouse_id
											, dc_ship_addr1 Address_Line_1	
											, dc_ship_addr2	Address_Line_2	
											, dc_ship_addr3	Address_Line_3
											, dc_ship_city Address_City
											, dc_ship_state Address_State_Province
											, dc_ship_zip Address_Postal_Code
											, dc_ship_country Address_Country
											, 0 ship_to
											, 1 mail_to
											, dc_active_flag
									from dcw.dbo.dcw_DistChannels
								   where DC_AGENT_TYPE IN (''REP'', ''PRT'', ''SVC'', ''OEM'',''DIS'') ')   
		  

		                           
	-- contacts
	drop table stg_dcw_Personnel
	Select * into stg_dcw_Personnel
	from OPENQUERY(BIZDATA1,'select distinct  convert(varchar,per_id_no) + ''-''+ convert(varchar,dc_id_no) warehouse_id
										,   per_first_name
										,	per_last_name
										,	per_mi
										,	per_email
										,	per_emp_title
										,	per_roletype
										,	per_phone
										,	per_principal
										,	per_lit_contact
										,	case per_active
											 when 0 then ''No''
											 else ''Yes''
											 end per_active
										,	case per_active
											 when 0 then ''Inactive''
											 else ''Active''
											 end  Status
										,	per_biz_record
										,	per_inactive_date
										,	per_id_no
										,   dc_id_no
										,	''012C0000000HqOd'' RecordTypeId
									from dcw.dbo.dcw_DistChannels dc
									inner join dcw.dbo.dcw_Personnel p on p.per_dc_key = dc.dc_key
								   where DC_AGENT_TYPE IN (''REP'', ''PRT'', ''SVC'', ''UPL'') ') 
                             
    
   	
	 /********************************
      Shipment Data
      *******************************/
    --
    -- 1. Load Account Staging
    --
    -- Archive previous load
    --load staging
    drop table dbo.stg_account;
	SELECT '012C0000000Hq9V' RecordTypeId, o.* INTO stg_account
	FROM OPENQUERY(PLYDATA2,
	'SELECT distinct 
			case when rtrim(ltrim(dw_cust_billto.dcb_customer_name)) = '''' then dw_cust_billto.dcb_oracle_customer_number else  rtrim(ltrim(dw_cust_billto.dcb_customer_name)) end As Name,
			dw_cust_billto.dcb_phone As Phone,
			dw_cust_billto.dcb_phone2 As Phone2,
			dw_cust_billto.dcb_email As Email,
			--dw_cust_billto.dcb_postal_code As Postal_code,
			dw_cust_billto.dcb_oracle_customer_number As oracle_customer_number,
			case dw_cust_billto.dcb_source_orig
			   when ''Oracle107'' then ''1'' when ''OracleR12'' then ''2'' ELSE ''0'' END As dw_Source_System,
			dw_cust_billto.dcb_oracle_customer_number As warehouse_id
	FROM DW_McQuayDataWarehouse.dbo.DW_FACT_SHIPMENTS dw_shipments
			inner join DW_McQuayDataWarehouse.dbo.DW_DIM_CUSTOMER_BILLTO dw_cust_billto 
			on dw_shipments.fs_customer_billto_key = dw_cust_billto.dcb_customer_billto_key
	  WHERE dw_shipments.fs_equipment = 1 
		and dw_shipments.fs_returned = 0
		and dw_shipments.fs_etl_load_date > DATEADD("day", -10, CONVERT(date,GETDATE()))
		and DCB_CURRENT_ROW = 1 ') o

	alter table stg_account ADD ID INT IDENTITY(1,1)
	alter table stg_account ALTER COLUMN warehouse_id varchar(244)
    
    -- 2. Load Address Staging
    
    --Get customer billto address information from PLYDATA2\DW_McQuayDataWarehouse.dbo.DW_DIM_CUSTOMER_BILLTO
	drop table stg_address;

	SELECT * INTO stg_address
	FROM OPENQUERY(PLYDATA2,
	'SELECT distinct 
			convert(varchar,dw_cust_billto.DCB_ORACLE_ADDRESS_KEY) As oracle_address_key,
			'''' + convert(varchar,dw_cust_billto.DCB_ORACLE_ADDRESS_KEY) As warehouse_id,
			case dw_cust_billto.dcb_source_orig
			   when ''Oracle107'' then ''1'' when ''OracleR12'' then ''2'' ELSE ''0'' END As dw_Source_System,
			replace(dw_cust_billto.dcb_address_line1,''"'',''""'') As Address_1,
			replace(dw_cust_billto.dcb_address_line2,''"'',''""'') As Address_2,
			replace(dw_cust_billto.dcb_address_line3,''"'',''""'') As Address_3,
			replace(dw_cust_billto.dcb_address_line4,''"'',''""'') As Address_4,
			replace(dw_cust_billto.dcb_postal_code,''"'',''""'') As Postal_code,
			replace(dw_cust_billto.dcb_city,''"'',''""'') As City,
			replace(dw_cust_billto.dcb_county,''"'',''""'') As County,
			replace(dw_cust_billto.dcb_state_province_code,''"'',''""'') As State,
			replace(dw_cust_billto.dcb_country_code,''"'',''""'') As Country,
			replace(dw_cust_billto.dcb_oracle_customer_number,''"'',''""'') As oracle_customer_number,
			replace(convert(varchar,dw_cust_billto.dcb_oracle_customer_number),''"'',''""'') oracle_entity_warehouse_key,
			replace(dw_cust_billto.DCB_IS_PRIMARY_BILLTO,''"'',''""'') As Is_Primary_Bill_To,
			''0'' as equipment_address
	FROM DW_McQuayDataWarehouse.dbo.DW_FACT_SHIPMENTS dw_shipments
			inner join DW_McQuayDataWarehouse.dbo.DW_DIM_CUSTOMER_BILLTO dw_cust_billto 
			on dw_shipments.fs_customer_billto_key = dw_cust_billto.dcb_customer_billto_key
	  WHERE dw_shipments.fs_equipment = 1 
		and dw_shipments.fs_returned = 0
		and dw_shipments.fs_etl_load_date > DATEADD("day", -10, CONVERT(date,GETDATE()))
		and DCB_CURRENT_ROW = 1')

	-- add the owner customer column
	  Alter Table stg_address add  owner_account_name varchar(255);
	  alter Table stg_address ADD ID INT IDENTITY(1,1) 
	  alter table stg_address alter column oracle_address_key varchar(32) null
	  alter table stg_address alter column warehouse_id varchar(255) null
	  alter table stg_address add  job_owner_key varchar(255) null
	  alter table stg_address add  job_contractor_owner_key varchar(255) null

	-- get the Job Addresses
	insert into stg_address(Address_1,Address_2,Postal_Code, City, State, County, Country
				 ,warehouse_id, dw_Source_System, equipment_address)
	select distinct replace(upper(Address_1),'"','""') Address_1
				  , replace(upper(Address_2),'"','""') Address_2
				  , replace(upper(Postal_Code),'"','""') Postal_Code
				  , replace(upper(City),'"','""') City
				  , replace(upper(State),'"','""') State
				  , replace(upper(County),'"','""') County
				  , replace(upper(Country),'"','""') Country
				  , replace(warehouse_id,'"','""') warehouse_id
				  , dw_Source_System
				  , '1' as x
	from openquery(PLYDATA2,
	'select distinct
			rtrim(ltrim(dw_job.dj_job_address_line1)) As Address_1, 
			rtrim(ltrim(dw_job.dj_job_address_line2)) As Address_2,
			rtrim(ltrim(dw_job.dj_job_city)) As City, 
			rtrim(ltrim(dw_job.dj_job_state_province_code)) As State,
			rtrim(ltrim(dw_job.dj_job_postal_code)) As Postal_Code, 
			rtrim(ltrim(dw_job.dj_job_county)) As County,
			rtrim(ltrim(dw_job.dj_job_country_code)) As Country,
			''J-''+ rtrim(ltrim(dw_job.dj_job_address_line1)) + rtrim(ltrim(dw_job.dj_job_address_line2)) + rtrim(ltrim(dw_job.dj_job_city)) + rtrim(ltrim(dw_job.dj_job_county)) + rtrim(ltrim(dw_job.dj_job_state_province_code)) + rtrim(ltrim(dw_job.dj_job_country_code)) + rtrim(ltrim(dw_job.dj_job_postal_code))  as warehouse_id,
			case dj_source
			   when ''Oracle107'' then ''1'' when ''OracleR12'' then ''2'' ELSE ''0'' END  as dw_Source_System
	   FROM DW_McQuayDataWarehouse.dbo.DW_FACT_SHIPMENTS dw_shipments
			inner join DW_McQuayDataWarehouse.dbo.DW_DIM_JOB dw_job 
			on dw_shipments.fs_job_key = dw_job.dj_job_key
	  WHERE DJ_CURRENT_ROW = 1 
		and dw_shipments.fs_returned = 0
		and dw_shipments.fs_equipment = 1
		and dw_shipments.fs_etl_load_date > DATEADD("day", -10, CONVERT(date,GETDATE()))' )
    
    -- 
    -- 3. Load Orders
    -- 
    drop table stg_order

SELECT * INTO stg_order
FROM OPENQUERY(PLYDATA2,
'SELECT distinct 
        dw_order_info.doi_go_number As GO_Number, 
        dw_order_info.doi_sales_order_number As SO_Number, 
        dw_order_info.doi_job_name As Job_Name,
        dw_order_info.doi_end_use As Vertical_Market, 
        rtrim(ltrim(dw_job.dj_owner_contact)) As Owner_Contact,
        rtrim(ltrim(dw_job.dj_owner_address_line1)) as job_owner_address_1 , 
        rtrim(ltrim(dw_job.dj_owner_address_line2)) as job_owner_address_2,
        rtrim(ltrim(dw_job.dj_owner_city))  as job_owner_city,
        rtrim(ltrim(dw_job.dj_owner_state_province_code)) as job_owner_state,
        rtrim(ltrim(dw_job.dj_owner_postal_code)) as job_owner_postal_code,
        ltrim(rtrim(dw_job.dj_owner_name)) As owner_Name, 
        ltrim(rtrim(dw_job.dj_owner_phone)) As owner_Phone,
        ltrim(rtrim(dw_job.dj_general_contractor_name)) As General_Contractor_Name, 
        ltrim(rtrim(dw_job.dj_general_contractor_phone)) As General_Contractor_Phone,
        rtrim(ltrim(dw_job.dj_general_contractor_address_line1)) dj_general_contractor_address_line1, 
        rtrim(ltrim(dw_job.dj_general_contractor_address_line2)) dj_general_contractor_address_line2,
        rtrim(ltrim(dw_job.dj_general_contractor_city)) dj_general_contractor_city,
        rtrim(ltrim(dw_job.dj_general_contractor_state_province_code)) dj_general_contractor_state_province_code, 
        rtrim(ltrim( dw_job.dj_general_contractor_postal_code)) dj_general_contractor_postal_code,
        dw_cust_billto.dcb_oracle_customer_number As billto_cust_warehouse_id,
        dw_cust_shipto.dcs_source_system_customer_number As shipto_cust_warehouse_id ,
        dw_sales_office.dsco_office_number As DCW_Sales_Office_Number,		
		convert(varchar(max),dw_sales_personnel.dscp_personnel_id) + ''-''+ convert(varchar(max),dw_sales_personnel.dscp_office_number) As DCW_Sales_Rep_Key,
		convert(varchar(max),dw_sales_personnel.dscp_personnel_id) As DCW_Sales_Rep_Number,
		dw_shipments.fs_order_info_key As dw_Order_Info_Key,
	    '''' + convert(varchar(max),dw_cust_billto.DCB_ORACLE_ADDRESS_KEY) As billto_addr_warehouse_id,
	    '''' + convert(varchar(max),DCS_ORIGINAL_SYSTEM_ADDRESS_ID) as shipto_addr_warehouse_id,
        ''J-''+ rtrim(ltrim(dw_job.dj_job_address_line1)) + rtrim(ltrim(dw_job.dj_job_address_line2)) + rtrim(ltrim(dw_job.dj_job_city)) + rtrim(ltrim(dw_job.dj_job_county)) + rtrim(ltrim(dw_job.dj_job_state_province_code)) + rtrim(ltrim(dw_job.dj_job_country_code)) + rtrim(ltrim(dw_job.dj_job_postal_code))  as Job_addr_warehouse_id
 FROM DW_McQuayDataWarehouse.dbo.DW_FACT_SHIPMENTS dw_shipments	
	inner join DW_McQuayDataWarehouse.dbo.DW_DIM_ORDER_INFO dw_order_info 
		on dw_shipments.fs_order_info_key = dw_order_info.doi_order_info_key
    left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_DATE dw_date 
		on dw_shipments.fs_shipdate_key = dw_date.dd_date_key	
	left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_SALES_CHANNEL_PERSONNEL dw_sales_personnel 
		on dw_shipments.fs_sales_channel_personnel_key = dw_sales_personnel.dscp_sales_channel_personnel_key	
	left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_SALES_CHANNEL_OFFICE dw_sales_office
		on dw_shipments.fs_sales_channel_office_key = dw_sales_office.dsco_sales_channel_office_key 
    left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_CUSTOMER_BILLTO dw_cust_billto 
		on dw_shipments.fs_customer_billto_key = dw_cust_billto.dcb_customer_billto_key and DCB_CURRENT_ROW = 1
	left join DW_McQuayDataWarehouse.dbo.DW_DIM_CUSTOMER_SHIPTO dw_cust_shipto 
		on dw_shipments.fs_customer_shipto_key = dw_cust_shipto.dcs_customer_shipto_key and DCS_CURRENT_ROW = 1
	left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_JOB dw_job 
		on dw_job.dj_job_key = dw_shipments.fs_job_key and DJ_CURRENT_ROW = 1
WHERE dw_shipments.fs_equipment = 1 
   and dw_shipments.fs_returned = 0 
   and dw_shipments.fs_etl_load_date > DATEADD("day", -10, CONVERT(date,GETDATE())) 
   ')
update stg_order
  set DCW_Sales_Office_Number = ''
 where DCW_Sales_Office_Number = 'Unknown'
 
update stg_order
  set DCW_Sales_Rep_Key = ''
 where DCW_Sales_Rep_Key like '0-%' or DCW_Sales_Rep_Key like '%Unknown'
 
drop table stg_shipment_order;

select distinct replace(convert(varchar(max),GO_Number),'"','""') GO_Number
               ,replace(convert(varchar(max),sh.SO_Number),'"','""') SO_Number 
               ,replace(convert(varchar(max),Job_Name),'"','""') Job_Name
               ,replace(Vertical_Market,'"','""') Vertical_Market
               ,replace(convert(varchar(max),sh.SO_Number),'"','""') Warehouse_ID
               ,replace(owner_Name,'"','""') owner_Name
               ,replace(owner_Phone,'"','""') owner_Phone 
               ,replace(Owner_Contact,'"','""') Owner_Contact
               ,replace(job_owner_address_1,'"','""') job_owner_address_1
               ,replace(job_owner_address_2,'"','""') job_owner_address_2
               ,replace(job_owner_city,'"','""') job_owner_city
               ,replace(job_owner_state,'"','""') job_owner_state
               ,replace(convert(varchar(max),job_owner_postal_code),'"','""') job_owner_postal_code
               ,replace(General_Contractor_Name,'"','""') General_Contractor_Name
               ,replace(General_Contractor_Phone,'"','""') General_Contractor_Phone
               ,replace(dj_general_contractor_address_line1,'"','""') dj_general_contractor_address_line1
               ,replace(dj_general_contractor_address_line2,'"','""') dj_general_contractor_address_line2
               ,replace(dj_general_contractor_city,'"','""') dj_general_contractor_city
               ,replace(dj_general_contractor_state_province_code,'"','""') dj_general_contractor_state_province_code
               ,replace(convert(varchar(max),dj_general_contractor_postal_code),'"','""') dj_general_contractor_postal_code
               ,stg_account.warehouse_id billto_cust_warehouse_id
               --,replace(replace(shipto_cust_warehouse_id,'"','""'), 'Unknown','') shipto_cust_warehouse_id
               ,replace(replace(convert(varchar(max),billto_addr.warehouse_id),'"','""'), 'Unknown','') billto_addr_warehouse_id
               --,replace(replace(shipto_addr_warehouse_id,'"','""'),'Unknown','') shipto_addr_warehouse_id
               ,replace(job_addr.warehouse_id,'"','""') job_addr_warehouse_id 
               --
               ,replace(convert(varchar(max),isnull(soh.soh_cust_po_number, ord.Customer_PO )),'"','""') Customer_PO 
			   ,replace(convert(varchar(max),isnull(soh.soh_rep_po,ord.Rep_PO )),'"','""') Rep_PO 
		       ,replace(convert(varchar(max),isnull(soh.soh_job_number, ord.Daikin_Tools_Job_Number )),'"','""') Daikin_Tools_Job_Number
		       -- 30,920 4 LEASE PAYMENTS. 1800-872-2657
		       ,replace(convert(varchar(max),oh_Header_Key),'"','""') oh_Header_Key 
		       ,replace(convert(varchar(max),oh_orig_system_header_id),'"','""') oh_orig_system_header_id 
		       ,replace(isnull(CONVERT(date,soh.soh_entered_date),ord.Order_Entry_Date),'"','""') Order_Entry_Date
		       ,replace(convert(varchar(max),DCW_Sales_Office_Number)	,'"','""') DCW_Sales_Office_Number	
		       ,replace(convert(varchar(max),DCW_Sales_Rep_Key),'"','""') DCW_Sales_Rep_Key	
		       ,replace(convert(varchar(max),DCW_Sales_Rep_Number),'"','""') DCW_Sales_Rep_Number
		       ,isnull(ord.dw_Source_System, '2') dw_Source_System
		      into stg_shipment_order
    from SFDCIntegrations.dbo.stg_order sh
    left outer join stg_account on sh.billto_cust_warehouse_id = stg_account.warehouse_id
    left outer join stg_address job_addr on job_addr.warehouse_id = sh.Job_addr_warehouse_id
    left outer join stg_address billto_addr on billto_addr.warehouse_id = sh.billto_addr_warehouse_id
    left outer join DW_McQuaySales.ODS.SALES_ORDER_HEADER soh
	  on convert(varchar(max),soh.soh_order_number) = convert(varchar(max),sh.SO_Number) and soh.SOH_ACTIVE_RECORD = 1
    left outer join
OPENQUERY(PLYDATA2,
'SELECT convert(varchar(max),oh_order_header.soh_sales_order_number) As SO_Number,
		--oh_order_header.soh_go_number As GO_Number,
		CONVERT(date,oh_order_header.soh_order_entry_date) As Order_Entry_Date,
		convert(varchar(max),oh_order_header.soh_customer_po_number) As Customer_PO,
		convert(varchar(max),oh_order_header.soh_sales_office_po_number) As Rep_PO,
		convert(varchar(max),oh_order_header.soh_job_number) As Daikin_Tools_Job_Number,
		--oh_order_header.soh_job_name As Job_Name,
		--oh_order_header.soh_end_use_code As Vertical_Market,
		convert(varchar(max),oh_order_header.soh_sales_office_number) As Rep_Office_Number,
		convert(varchar(max),oh_order_header.soh_sales_office_name) As Rep_Office_Name,
		convert(varchar(max),oh_order_header.soh_salesperson_name) As Sales_Person_Name,	
		convert(varchar(max),oh_order_header.soh_header_key) As oh_Header_Key,
		''1'' As dw_Source_System,
		convert(varchar(max),oh_order_header.soh_orig_system_header_id) As oh_Orig_System_Header_Id,
		convert(varchar(max),oh_order_header.soh_orig_system_cust_id) As oh_Orig_System_Cust_Id
FROM DW_McQuayOrderHistory.dbo.DWO_SALES_ORDER_HEADER oh_order_header
--where oh_order_header.SOH_ACTIVE_RECORD = 1
') ord
on sh.SO_Number = ord.SO_Number

drop table stg_order
select distinct GO_Number
               ,SO_Number 
               ,Warehouse_ID
               ,owner_Name
               ,owner_Phone 
               ,Owner_Contact
               ,job_owner_address_1
               ,job_owner_address_2
               ,job_owner_city
               ,job_owner_state
               ,job_owner_postal_code
               ,General_Contractor_Name
               ,General_Contractor_Phone
               ,dj_general_contractor_address_line1
               ,dj_general_contractor_address_line2
               ,dj_general_contractor_city
               ,dj_general_contractor_state_province_code
               ,dj_general_contractor_postal_code
               ,Customer_PO 
			   ,Rep_PO  
		       ,Order_Entry_Date
		       ,DCW_Sales_Office_Number	
		       ,max(dw_Source_System) dw_Source_System
		       ,max(DCW_Sales_Rep_Key) DCW_Sales_Rep_Key
		       ,max(DCW_Sales_Rep_Number) DCW_Sales_Rep_Number
               ,max(Job_Name) Job_Name
               ,max(billto_cust_warehouse_id) billto_cust_warehouse_id
               ,max(billto_addr_warehouse_id) billto_addr_warehouse_id
               ,max(Vertical_Market) Vertical_Market
		       ,max(Daikin_Tools_Job_Number) Daikin_Tools_Job_Number
               ,max(job_addr_warehouse_id) job_addr_warehouse_id
 into stg_order 
 from stg_shipment_order
group by GO_Number
       ,SO_Number 
       ,Warehouse_ID
       ,owner_Name
       ,owner_Phone 
       ,Owner_Contact
       ,job_owner_address_1
       ,job_owner_address_2
       ,job_owner_city
       ,job_owner_state
       ,job_owner_postal_code
       ,General_Contractor_Name
       ,General_Contractor_Phone
       ,dj_general_contractor_address_line1
       ,dj_general_contractor_address_line2
       ,dj_general_contractor_city
       ,dj_general_contractor_state_province_code
       ,dj_general_contractor_postal_code
       ,Customer_PO 
	   ,Rep_PO  
       ,Order_Entry_Date
       ,DCW_Sales_Office_Number	
    
    --
    -- 4. Load Products
    --
    drop table stg_product
    select * into stg_product
	from openquery(PLYDATA2,
	'select DISTINCT
		   dep_equipment_description	Name
		 , prod.DEP_EQUIPMENT_MODEL	ProductCode
		 , prod.DEP_EQUIPMENT_PRODUCT_FAMILY	DW_Product_Family__c
		 , prod.DEP_EQUIPMENT_PRODUCT_LINE	DW_Product_Line__c
		 , prod.DEP_EQUIPMENT_BUSINESS_UNIT	DW_Product_Business_Unit__c
		 , prod.DEP_EQUIPMENT_PRODUCT_CLASS	DW_Product_Class__c
		 , prod.DEP_EQUIPMENT_PCL_DESCRIPTION	DW_PCL_Description__c
		 , prod.DEP_ACTIVE_DATE	DW_Active_Date__c
		 , prod.DEP_INACTIVE_DATE	DW_Inactive_Date__c
		 , prod.DEP_EQUIPMENT_PRODUCT_KEY	Warehouse_ID__c
	  from DW_McQuayDataWarehouse.dbo.DW_FACT_SHIPMENTS ship
		   inner join DW_McQuayDataWarehouse.dbo.DW_DIM_EQUIPMENT_PRODUCT prod
			 on prod.DEP_EQUIPMENT_PRODUCT_KEY = ship.FS_PRODUCT_KEY
	  where ship.fs_etl_load_date > DATEADD("day", -10, CONVERT(date,GETDATE()))')
    
    -- 
    -- 5. Load Assets
    -- 
	drop table stg_order_asset
	drop table stg_asset
	-- get the base asset data from the shipment
	SELECT distinct REPLACE(Serial_Number,'"','""') Serial_Number,
			Ship_Date,
			'001C0000019yujW' AccountId,
			REPLACE(Code_String,'"','""') Code_String,
			REPLACE(sh.DCW_Sales_Office_Number,'"','""') DCW_Sales_Office_Number,		
			REPLACE(sh.DCW_Sales_Rep_Number,'"','""') DCW_Sales_Rep_Number,
			REPLACE(Equipment_Model,'"','""') Equipment_Model,		
			REPLACE(Equipment_Description,'"','""') Equipment_Description,		
			REPLACE(Equipment_Description,'"','""') Asset_Name,	
			REPLACE(Equipment_Product_Line,'"','""') Equipment_Product_Line,			
			REPLACE(Equipment_Product_Family,'"','""') Equipment_Product_Family,
			REPLACE(GO_Line_Number,'"','""') GO_Line_Number,
			REPLACE(SO_Line_Number,'"','""') SO_Line_Number,
			REPLACE(sh.SO_Number,'"','""') Order_Warehouse_id,
			REPLACE(Quantity,'"','""') Quantity,  
			REPLACE(dw_Equipment_Key,'"','""') dw_Equipment_Key,				
			REPLACE(dw_Shipments_Key,'"','""') dw_Shipments_Key, 
			REPLACE(dw_Product_Key,'"','""') dw_Product_Key,
			REPLACE(dw_Code_String_Key,'"','""') dw_Code_String_Key, 
			REPLACE(dw_Sales_Channel_Office_Key,'"','""') dw_Sales_Channel_Office_Key,        
			REPLACE(dw_Sales_Channel_Personnel_Key,'"','""') dw_Sales_Channel_Personnel_Key, 
			REPLACE(dw_Customer_BillTo_Key,'"','""') dw_Customer_BillTo_Key,  
			REPLACE(dw_Customer_ShipTo_Key,'"','""') dw_Customer_ShipTo_Key, 
			--REPLACE(dw_Geography_Key,'"','""') dw_Geography_Key,          
			--REPLACE(dw_Job_Key,'"','""') dw_Job_Key,
			--REPLACE(dw_Order_Info_Key,'"','""') dw_Order_Info_Key, 
			REPLACE(dw_Source_System,'"','""') dw_Source_System, 
			--dw_ETL_Date,
			REPLACE(shipto_customer_name,'"','""') shipto_customer_name,
			REPLACE(ShipTo_Customer_Address_1,'"','""') ShipTo_Customer_Address_1,
			REPLACE(ShipTo_Customer_Address_2,'"','""') ShipTo_Customer_Address_2,
			REPLACE(ShipTo_Customer_Address_3,'"','""') ShipTo_Customer_Address_3,
			REPLACE(ShipTo_Customer_Address_4,'"','""') ShipTo_Customer_Address_4,
			REPLACE(ShipTo_Customer_Postal,'"','""') ShipTo_Customer_Postal,
			REPLACE(ShipTo_Customer_City,'"','""') ShipTo_Customer_City,
			REPLACE(ShipTo_Customer_County,'"','""') ShipTo_Customer_County,
			REPLACE(ShipTo_Customer_State_Province,'"','""') ShipTo_Customer_State_Province,
			REPLACE(ShipTo_Customer_Country,'"','""') ShipTo_Customer_Country,
			REPLACE(job_address_warehouse_id,'"','""') job_address_warehouse_id,
			--REPLACE(Daikin_Tools_Job_Number,'"','""') Daikin_Tools_Job_Number,
			REPLACE(owner_contact,'"','""') owner_contact,
			REPLACE(owner_phone,'"','""') owner_phone,
			REPLACE(owner_Name,'"','""') owner_name, ---
			REPLACE(General_Contractor_Name,'"','""') General_Contractor_Name,
			REPLACE(sh.SO_Number,'"','""') SO_Number,
			REPLACE(ohl.Tag,'"','""') Tag
			into stg_order_asset
	FROM OPENQUERY(PLYDATA2,
	'SELECT dw_shipments.fs_serial_number As Serial_Number, 
			CONVERT(date,dw_date.DD_DATE_SHORT) As Ship_Date,
			dw_code_string.dcst_complete_code_string As Code_String,
			dw_sales_office.dsco_office_number As DCW_Sales_Office_Number,		
			dw_sales_personnel.dscp_personnel_id As DCW_Sales_Rep_Number,
			dw_shipments.fs_model As Equipment_Model,	
			dw_product.dep_equipment_description As Equipment_Description,	
			dw_product.dep_equipment_product_line As Equipment_Product_Line,			
			dw_product.dep_equipment_product_family As Equipment_Product_Family,
			dw_order_info.doi_go_line_number As GO_Line_Number,
			dw_order_info.doi_sales_order_line_number As SO_Line_Number,
			dw_order_info.doi_sales_order_number As SO_Number,
			dw_shipments.fs_quantity As Quantity,  
			dw_product.dep_equipment_product_key As dw_Equipment_Key,				
			dw_shipments.fs_shipments_key As dw_Shipments_Key, 
			dw_shipments.fs_product_key As dw_Product_Key,
			dw_shipments.fs_code_string_key As dw_Code_String_Key, 
			dw_shipments.fs_sales_channel_office_key As dw_Sales_Channel_Office_Key,        
			dw_shipments.fs_sales_channel_personnel_key As dw_Sales_Channel_Personnel_Key, 
			dw_shipments.fs_customer_billto_key As dw_Customer_BillTo_Key,  
			dw_shipments.fs_customer_shipto_key As dw_Customer_ShipTo_Key, 
			dw_shipments.fs_geography_key As dw_Geography_Key,          
			dw_shipments.fs_job_key As dw_Job_Key,
			dw_shipments.fs_order_info_key As dw_Order_Info_Key, 
			dw_shipments.fs_fact_source As dw_Source_System, 
			CONVERT(date,dw_shipments.fs_etl_load_date) As dw_ETL_Date,
			ltrim(rtrim(dw_job.dj_owner_name)) As owner_Name, 
			ltrim(rtrim(dw_job.dj_owner_phone)) As owner_Phone,
			rtrim(ltrim(dw_job.dj_owner_contact)) As Owner_Contact,
			dw_cust_shipto.dcs_customer_name shipto_customer_name,
			dw_cust_shipto.dcs_address_line1 As ShipTo_Customer_Address_1,
			dw_cust_shipto.dcs_address_line2 As ShipTo_Customer_Address_2,
			dw_cust_shipto.dcs_address_line3 As ShipTo_Customer_Address_3,
			dw_cust_shipto.dcs_address_line4 As ShipTo_Customer_Address_4,
			dw_cust_shipto.dcs_postal_code As ShipTo_Customer_Postal,
			dw_cust_shipto.dcs_city As ShipTo_Customer_City,
			dw_cust_shipto.dcs_county As ShipTo_Customer_County,
			dw_cust_shipto.dcs_state_province_code As ShipTo_Customer_State_Province,
			dw_cust_shipto.dcs_country_code As ShipTo_Customer_Country,
			dw_job.dj_general_contractor_name As General_Contractor_Name,
			replace(''J-''+ rtrim(ltrim(dw_job.dj_job_address_line1)) + rtrim(ltrim(dw_job.dj_job_address_line2)) + rtrim(ltrim(dw_job.dj_job_city)) + rtrim(ltrim(dw_job.dj_job_county)) + rtrim(ltrim(dw_job.dj_job_state_province_code)) + rtrim(ltrim(dw_job.dj_job_country_code)) + rtrim(ltrim(dw_job.dj_job_postal_code)) 
					,''"'','''')  as job_address_warehouse_id 
	FROM DW_McQuayDataWarehouse.dbo.DW_FACT_SHIPMENTS dw_shipments
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_DATE dw_date 
			  on dw_shipments.fs_shipdate_key = dw_date.dd_date_key
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_CODE_STRING dw_code_string 
			  on dw_shipments.fs_code_string_key = dw_code_string.dcst_code_string_key	
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_SALES_CHANNEL_PERSONNEL dw_sales_personnel 
			  on dw_shipments.fs_sales_channel_personnel_key = dw_sales_personnel.dscp_sales_channel_personnel_key	
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_SALES_CHANNEL_OFFICE dw_sales_office
			  on dw_shipments.fs_sales_channel_office_key = dw_sales_office.dsco_sales_channel_office_key	
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_EQUIPMENT_PRODUCT dw_product
			  on dw_shipments.fs_product_key = dw_product.dep_equipment_product_key	
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_ORDER_INFO dw_order_info 
			  on dw_shipments.fs_order_info_key = dw_order_info.doi_order_info_key
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_JOB dw_job 
			  on dw_shipments.fs_job_key = dw_job.dj_job_key	 
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_CUSTOMER_SHIPTO dw_cust_shipto 
			  on dw_shipments.fs_customer_shipto_key = dw_cust_shipto.dcs_customer_shipto_key
			left outer join DW_McQuayDataWarehouse.dbo.DW_DIM_CUSTOMER_BILLTO dw_cust_billto 
			  on dw_shipments.fs_customer_billto_key = dw_cust_billto.dcb_customer_billto_key		
	WHERE   dw_shipments.fs_equipment = 1 
		and dw_shipments.fs_returned = 0
		and dw_shipments.fs_etl_load_date > DATEADD("day", -10, CONVERT(date,GETDATE()))
			') sh
	left outer join
	OPENQUERY(PLYDATA2,
	'SELECT distinct oh_order_line.sol_sales_order_number,
			oh_order_line.sol_sales_order_line_number As Sales_Order_Line_Number,	
			oh_order_line.sol_tagging As Tag
	FROM DW_McQuayOrderHistory.dbo.DWO_SALES_ORDER_LINE oh_order_line
	WHERE oh_order_line.sol_unit_of_measure = ''EA''
	  and oh_order_line.sol_subtype is null
	--and len(oh_order_line.sol_sales_order_line_number) = 9
	') ohl
	on sh.SO_Number = ohl.sol_sales_order_number 
	   and (ohl.Sales_Order_Line_Number is null OR substring(ohl.Sales_Order_Line_Number,1,4) = replicate('0',4-len(sh.SO_Line_Number)) + sh.SO_Line_Number)


	-- Get the Data from McQuay Sales HARD CODE SOURCE SYSTEM to 2. HAVE TO USE ACTIVE RECORD = 1


	select Serial_Number,
			Ship_Date,
			AccountId,
			Code_String,
			sa.DCW_Sales_Office_Number,		
			sa.DCW_Sales_Rep_Number,
			Equipment_Model,		
			Equipment_Description,		
			Asset_Name,	
			Equipment_Product_Line,			
			Equipment_Product_Family,
			GO_Line_Number,
			SO_Line_Number,
			so.warehouse_id Order_Warehouse_id,
			Quantity,  
			replace(dw_Equipment_Key, '-1', '') dw_Equipment_Key,				
			dw_Shipments_Key, 
			replace(dw_Product_Key, '-1', '') DW_PRODUCT_KEY,
			dw_Code_String_Key, 
			dw_Sales_Channel_Office_Key,        
			dw_Sales_Channel_Personnel_Key, 
			--sa.warehouse_id dw_Customer_BillTo_Key,  
			--dw_Customer_ShipTo_Key, 
			isnull(sa.dw_Source_System, '2') dw_Source_System,
			shipto_customer_name,
			ShipTo_Customer_Address_1,
			ShipTo_Customer_Address_2,
			ShipTo_Customer_Address_3,
			ShipTo_Customer_Address_4,
			ShipTo_Customer_Postal,
			ShipTo_Customer_City,
			ShipTo_Customer_County,
			ShipTo_Customer_State_Province,
			ShipTo_Customer_Country,
			sadr.warehouse_id job_address_warehouse_id,
			sa.owner_contact,
			sa.owner_phone,
			sa.owner_Name,
			sa.General_Contractor_Name,
			sa.SO_Number,
			max(isnull(replace(ms_order_line.sol_line_tag, '"','""'), sa.Tag)) Tag
			into stg_asset
		from stg_order_asset sa
		left outer join stg_order so
		  on so.warehouse_id = Order_Warehouse_id
		left outer join stg_address sadr 
		  on sadr.warehouse_id = job_address_warehouse_id
		left outer join DW_McQuaySales.ODS.SALES_ORDER_HEADER ms_order_header
			on convert(varchar(max),sa.SO_Number) = convert(varchar(max),ms_order_header.SOH_ORDER_NUMBER)
			and ms_order_header.SOH_ACTIVE_RECORD = 1
		left outer join DW_McQuaySales.ODS.SALES_ORDER_LINE ms_order_line
			on convert(varchar(max),sa.SO_Line_Number) = convert(varchar(max),ms_order_line.SOL_LINE_NUMBER)
			and ms_order_header.SOH_SOURCE_HEADER_ID = ms_order_line.SOL_SOURCE_HEADER_ID
			and ms_order_line.SOL_ACTIVE_RECORD = 1
			and ms_order_line.SOL_ORDER_QUANTITY_UOM = 'EA'
     group by
     Serial_Number,
			Ship_Date,
			AccountId,
			Code_String,
			sa.DCW_Sales_Office_Number,		
			sa.DCW_Sales_Rep_Number,
			Equipment_Model,		
			Equipment_Description,		
			Asset_Name,	
			Equipment_Product_Line,			
			Equipment_Product_Family,
			GO_Line_Number,
			SO_Line_Number,
			so.warehouse_id ,
			Quantity,  
			replace(dw_Equipment_Key, '-1', '') ,				
			dw_Shipments_Key, 
			replace(dw_Product_Key, '-1', '') ,
			dw_Code_String_Key, 
			dw_Sales_Channel_Office_Key,        
			dw_Sales_Channel_Personnel_Key, 
			--sa.warehouse_id dw_Customer_BillTo_Key,  
			--dw_Customer_ShipTo_Key, 
			isnull(sa.dw_Source_System, '2') ,
			shipto_customer_name,
			ShipTo_Customer_Address_1,
			ShipTo_Customer_Address_2,
			ShipTo_Customer_Address_3,
			ShipTo_Customer_Address_4,
			ShipTo_Customer_Postal,
			ShipTo_Customer_City,
			ShipTo_Customer_County,
			ShipTo_Customer_State_Province,
			ShipTo_Customer_Country,
			sadr.warehouse_id ,
			sa.owner_contact,
			sa.owner_phone,
			sa.owner_Name,
			sa.General_Contractor_Name,
			sa.SO_Number

	update stg_asset
	  set DCW_Sales_Office_Number = ''
	where DCW_Sales_Office_Number = 'Unknown'
    
    
    
	
END
