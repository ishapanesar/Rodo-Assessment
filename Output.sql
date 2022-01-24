---Provider1

---------------------------------------------------------------------
-- Creating a staging table for Provider Feed 1 to insert raw values 
---------------------------------------------------------------------

--drop table bi_userdb.stg_test_isha_drop_1;
Create table bi_userdb.stg_test_isha_drop_1(
DealerID varchar,	
Stock varchar,	
Type varchar,
VIN varchar,
Year varchar,
Make varchar,
Model varchar,
"Trim" varchar,
ModelNumber varchar ,
ExteriorColor varchar,
ExteriorColorCode varchar,
InteriorColor varchar,
InteriorColorCode varchar,
Seats varchar,
Navigation varchar,
WheelSize varchar,
MoonRoof varchar,
DriverSeatControl varchar,
PassengerSeatControl varchar,
Engine varchar,
Transmission varchar,
Miles numeric,
DateInStock date,
AdditionalSpecs varchar(10000),
Body varchar,
Series varchar,
EngineCylinders varchar ,
EngineDisplacement varchar,
Drivetrain varchar,
DealersNotes varchar(10000),
MSRP numeric,
BaseMSRP numeric,
Invoice varchar,
ImageList varchar(10000),
Video_URL varchar(10000),
OtherMedia varchar,
DamageWaiverValue varchar,
OptionCode varchar,
OptionDescription varchar,
Certified varchar,
DMSStatus varchar,
ListPrice numeric 
);

---------------------------------------------------------------------
-- Copying values from the s3 bucket assuming the files are delivered
-- to the s3 bucket using a 'copy command' 
---------------------------------------------------------------------

copy bi_userdb.stg_test_isha_drop_1 
from 'Bucket Path' access_key_id ''
secret_access_key '' BLANKSASNULL csv ignoreheader 1  DATEFORMAT AS 'MM/DD/YYYY' FILLRECORD;

---------------------------------------------------------------------
-- Creating a table to insert consistent values
---------------------------------------------------------------------
create table bi_userdb.test_isha_drop_1 (
dealership_id varchar,	 
vin	varchar,
mileage	varchar,	 
is_new varchar,
stock_number varchar,
dealer_year	varchar, 
dealer_make	varchar,	 
dealer_model varchar,	 
dealer_trim	 varchar, 
dealer_model_number	 varchar, 
dealer_msrp	varchar,
dealer_invoice	varchar,
dealer_body varchar, 
dealer_inventory_entry_date date,
dealer_exterior_color_description	varchar,
dealer_interior_color_description varchar,	 
dealer_exterior_color_code varchar, 
dealer_interior_color_code varchar, 
dealer_transmission_name varchar, 
dealer_transmission_type varchar,
dealer_installed_option_codes varchar, 
dealer_installed_option_descriptions varchar, 
dealer_additional_specs	 varchar(10000),
dealer_doors varchar,
dealer_drive_type varchar,
dealer_images varchar(10000),
dealer_certified varchar
);

---------------------------------------------------------------------
-- deleting values from the table using the staging table to avoid 
-- any duplicates
---------------------------------------------------------------------
delete
from
	bi_userdb.test_isha_drop_1
		using bi_userdb.stg_test_isha_drop_1
where
	bi_userdb.stg_test_isha_drop_1.DealerID = bi_userdb.test_isha_drop_1.dealership_id
	and upper(bi_userdb.stg_test_isha_drop_1.vin)= upper(bi_userdb.test_isha_drop_1.vin)
	and upper(bi_userdb.stg_test_isha_drop_1.Stock)= upper(bi_userdb.test_isha_drop_1.stock_number)

---------------------------------------------------------------------
-- Inserting values from staging table into the production table 
---------------------------------------------------------------------

insert into bi_userdb.test_isha_drop_1(select 
DealerID,
vin,
Miles,
CASE
	WHEN type = 'New' then 'True'
	WHEN type = 'Used' then 'False'
END AS is_new,
stock,
year,
make,
model,
"trim",
modelnumber,
CASE
	WHEN type = 'New' then msrp::varchar
	WHEN type = 'Used' then listprice::varchar
END AS dealer_msrp,
CASE
	WHEN type = 'New' then invoice::varchar
	WHEN type = 'Used' then listprice::varchar
END AS dealer_invoice,	
body,
dateinstock,
exteriorcolor,
interiorcolor,
exteriorcolorcode,
interiorcolorcode,
transmission,
Null as dealer_transmission_type,
optioncode,
optiondescription,
Additionalspecs,
Null as dealer_doors,
drivetrain,
imagelist,
certified from bi_userdb.stg_test_isha_drop_1 );

---------------------------------------------------------------------
---------------------------------------------------------------------

drop table bi_userdb.stg_test_isha_drop_1

---------------------------------------------------------------------
---------------------------------------------------------------------

-- Provider2 

---------------------------------------------------------------------
-- Creating a staging table for Provider Feed 2 to insert raw values 
---------------------------------------------------------------------
--drop table bi_userdb.stg_test_isha_drop_2;
create table bi_userdb.stg_test_isha_drop_2(
DealerId varchar,
Year varchar,
Make varchar,
Model varchar,
VIN varchar,
Stock varchar,
Mileage varchar,
InventoryDate date,
MSRP varchar,
Invoice varchar,
ExteriorColor varchar,
InteriorColor varchar,
Options varchar(10000),
Photos varchar(10000),
ExteriorColorCode varchar,
OptionCodes varchar,
InteriorColorCode varchar,
ModelCode varchar,
"New/Used" varchar,
Trim varchar,
Transmission varchar)

--------------------------------------------------------------------- 
---------------------------------------------------------------------

copy bi_userdb.stg_test_isha_drop_2
from 'Bucket Path' access_key_id ''
secret_access_key '' Delimiter as ',' BLANKSASNULL csv ignoreheader 1  DATEFORMAT AS 'auto' FILLRECORD;

---------------------------------------------------------------------
---------------------------------------------------------------------

-- drop table bi_userdb.test_isha_drop_2;
create table bi_userdb.test_isha_drop_2(
dealership_id varchar,	 
vin	varchar,
mileage varchar,	 
is_new	varchar,	
stock_number varchar,	
dealer_year varchar,		 
dealer_make	varchar,	
dealer_model varchar,	 
dealer_trim	varchar,	 
dealer_model_number varchar,	 
dealer_msrp varchar,	
dealer_invoice varchar,	
dealer_body varchar,	
dealer_inventory_entry_date	date,
dealer_exterior_color_description varchar,		 
dealer_interior_color_description varchar,	 
dealer_exterior_color_code varchar,	
dealer_interior_color_code varchar,	 
dealer_transmission_name varchar,	 
dealer_transmission_type varchar,	
dealer_installed_option_codes varchar,	 
dealer_installed_option_descriptions varchar(10000),	
dealer_additional_specs varchar,	
dealer_doors varchar,	
dealer_drive_type varchar,	
dealer_images varchar(10000),	
dealer_certified varchar)

---------------------------------------------------------------------
---------------------------------------------------------------------

delete
from
	bi_userdb.test_isha_drop_2
		using bi_userdb.stg_test_isha_drop_2
where
	bi_userdb.stg_test_isha_drop_2.DealerID = bi_userdb.test_isha_drop_2.dealership_id
	--and upper(bi_userdb.stg_test_isha_drop_2.vin)= upper(bi_userdb.test_isha_drop_2.vin)
	and upper(bi_userdb.stg_test_isha_drop_2.Stock)= upper(bi_userdb.test_isha_drop_2.stock_number)
	
---------------------------------------------------------------------
---------------------------------------------------------------------

insert into bi_userdb.test_isha_drop_2 (
select 
dealerid,
VIN,
mileage,
CASE
	WHEN "new/used" = 'N' then 'True'
	WHEN "new/used" = 'U' then 'False'
END AS is_new,
stock,
year,
make,
model,
"trim",
modelcode,
msrp,--listprice not provided 
invoice,--listprice not provided
Null as dealer_body,
inventorydate ::date ,
exteriorcolor,
interiorcolor,
exteriorcolorcode,
interiorcolorcode,
transmission,
Null as dealer_transmission_type,
optioncodes, 
"options",
Null as dealer_additional_specs,
Null as dealer_doors,
Null as dealer_drive_type,
replace(photos,'|',','), -- making the image field consistent by replacing '|' with ','
null as dealercertified -- Certified values are not provided in the feed
from bi_userdb.stg_test_isha_drop_2
);
 ---------------------------------------------------------------------
---------------------------------------------------------------------

drop table bi_userdb.stg_test_isha_drop_2

---------------------------------------------------------------------
---------------------------------------------------------------------


--===================================================================================================================
-- Created On : 2022-23-01             By: Isha Panesar Chandna        JIRA: Ticket_No
-- This table stores the dealer data provided by diff users
--===================================================================================================================

-- drop table bi_userdb.isha_dealer_data;
CREATE TABLE bi_userdb.isha_dealer_data (
    hash varchar ENCODE RAW,
    dealership_id varchar NOT NULL ENCODE ZSTD,
    vin varchar ENCODE ZSTD,
    mileage int4 ENCODE ZSTD,
    is_new varchar ENCODE ZSTD,
    stock_number varchar ENCODE ZSTD,
    dealer_year int4 ENCODE ZSTD,
    dealer_make varchar ENCODE ZSTD,
    dealer_model varchar ENCODE ZSTD,
    dealer_trim varchar ENCODE ZSTD,
    dealer_model_number varchar ENCODE ZSTD,
    dealer_msrp int4 ENCODE ZSTD,
    dealer_invoice int4 ENCODE ZSTD,
    dealer_body varchar ENCODE ZSTD,
    dealer_inventory_entry_date date ENCODE LZO,
    dealer_exterior_color_description varchar ENCODE ZSTD,
    dealer_interior_color_description varchar ENCODE ZSTD,
    dealer_exterior_color_code varchar ENCODE ZSTD,
    dealer_interior_color_code varchar ENCODE ZSTD,
    dealer_transmission_name varchar ENCODE ZSTD,
    dealer_installed_option_codes varchar(10000) ENCODE ZSTD,
    dealer_installed_option_descriptions varchar(10000) ENCODE ZSTD,
    dealer_additional_specs varchar(10000) ENCODE ZSTD,
    dealer_doors  varchar ENCODE ZSTD,
    dealer_drive_type varchar ENCODE ZSTD,
    updated_at timestamp  ENCODE LZO,
    dealer_images varchar(10000) ENCODE ZSTD,
    dealer_certified varchar ENCODE ZSTD
)
DISTSTYLE AUTO
SORTKEY AUTO;

---------------------------------------------------------------------
---------------------------------------------------------------------

CREATE temp TABLE temp_1 AS (
select
	MD5(coalesce(vin,'null') + mileage + coalesce(dealer_msrp,'null')),
	dealership_id,
	vin,
	mileage:: integer ,
	is_new ,
	stock_number,
	dealer_year :: integer ,
	dealer_make,
	dealer_model,
	dealer_trim,
	dealer_model_number,
	dealer_msrp :: integer ,
	dealer_invoice :: integer, 
	dealer_body,
	dealer_inventory_entry_date,
	dealer_exterior_color_description,
	dealer_interior_color_description,
	dealer_exterior_color_code,
	dealer_interior_color_code,
	dealer_transmission_name,
	dealer_installed_option_codes,
	dealer_installed_option_descriptions,
	dealer_additional_specs,
	dealer_doors,
	dealer_drive_type,
	getdate(),
	dealer_images,
	dealer_certified  from bi_userdb.test_isha_drop_1
Union
select
	MD5(coalesce(vin,'null') + mileage + coalesce(dealer_msrp,'null')),
	dealership_id,
	vin,
	mileage :: integer ,
	is_new ,
	stock_number,
	dealer_year :: integer,
	dealer_make,
	dealer_model,
	dealer_trim,
	dealer_model_number,
	dealer_msrp :: integer,
	dealer_invoice :: integer,
	dealer_body,
	dealer_inventory_entry_date,
	dealer_exterior_color_description,
	dealer_interior_color_description,
	dealer_exterior_color_code,
	dealer_interior_color_code,
	dealer_transmission_name,
	dealer_installed_option_codes,
	dealer_installed_option_descriptions,
	dealer_additional_specs,
	dealer_doors,
	dealer_drive_type,
	getdate(),
	dealer_images,
	dealer_certified
	 from bi_userdb.test_isha_drop_2
);

--------------------------------------------------------------------
---------------------------------------------------------------------

delete
from
	bi_userdb.isha_dealer_data
		using temp_1
where
	temp_1.dealership_id = bi_userdb.isha_dealer_data.dealership_id
	and upper(temp_1.vin)= upper((bi_userdb.isha_dealer_data.vin)
	and lower(temp_1.hash)= lower(bi_userdb.isha_dealer_data.hash)

---------------------------------------------------------------------
---------------------------------------------------------------------

Insert into bi_userdb.isha_dealer_data (
SELECT * FROM temp_1
);
 ---------------------------------------------------------------------
---------------------------------------------------------------------





