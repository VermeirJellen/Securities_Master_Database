-- drop database securities_master;
-- SET SQL_SAFE_UPDATES = 0;
CREATE DATABASE securities_master;
use securities_master;

SET SQL_SAFE_UPDATES = 0;

-- create timezone table
CREATE TABLE timezone(
	id_timezone int NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    name varchar(32) NOT NULL,
    utc_offset int NOT NULL
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DELIMITER $$
CREATE PROCEDURE getTimeZoneIdByName(
	IN tzone_name varchar(32), 
	OUT tzone_id int)
BEGIN
	SELECT id_timezone INTO tzone_id 
	FROM timezone
	WHERE name = tzone_name;
END$$
DELIMITER ;

insert into timezone(name,utc_offset)
values 
("EDT",-5), 
("CET",+1);

-- create locale table
CREATE TABLE locale(
	id_locale int NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    id_timezone int NOT NULL REFERENCES timezone(id_timezone),
    country varchar(255) NULL,
	city varchar(255) NULL,
	currency varchar(64) NULL
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DELIMITER $$
CREATE PROCEDURE getLocaleIdByLocation(
	IN locale_country varchar(255),
	IN locale_city varchar(255),
    OUT locale_id int)
BEGIN
	SELECT id_locale INTO locale_id
    FROM locale
    WHERE country = locale_country and city = locale_city;
END$$
DELIMITER ;

call getTimeZoneIdByName("EDT",@id_timezone_EDT);
call getTimeZoneIdByName("CET",@id_timezone_CET);

insert into locale(id_timezone,country,city,currency)
values
(@id_timezone_EDT,"USA", "New York City", "USD"),
(@id_timezone_CET,"Belgium", "Brussels", "EUR");

-- create dst_info table
CREATE TABLE dst_info(
	id_dst_info int NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    id_locale int NOT NULL REFERENCES locale(id_locale),
    dst_year int NOT NULL,
    dst_start datetime NOT NULL,
    dst_end datetime NOT NULL
)  ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

call getLocaleIdByLocation("USA","New York City",@id_locale_NYC);
call getLocaleIdByLocation("Belgium","Brussels",@id_locale_Brussels);

insert into dst_info(id_locale,dst_year,dst_start,dst_end)
values
(@id_locale_NYC,2015,"2015-03-08 02:00:00","2015-11-01 02:00:00"),
(@id_locale_NYC,2016,"2016-03-13 02:00:00","2016-11-06 02:00:00"),
(@id_locale_Brussels,2015,"2015-03-29 02:00:00","2015-10-25 02:00:00"),
(@id_locale_Brussels,2016,"2016-03-27 02:00:00","2016-10-30 02:00:00");


-- create exchange table
CREATE TABLE exchange (
  id_exchange int NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
  id_locale int NOT NULL REFERENCES locale(id_locale),
  abbrev varchar(32) NOT NULL,
  name varchar(255) NOT NULL,
  exchange_open time NOT NULL,
  exchange_close time NOT NULL,
  da_insert datetime NULL, -- Triggers avoid NULL values
  da_last_update datetime NULL -- Triggers avoid NULL values
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

select * from datapoint

DELIMITER $
CREATE TRIGGER before_exchange_insert
	BEFORE INSERT ON exchange
    FOR EACH ROW BEGIN
    SET new.da_insert = UTC_TIMESTAMP();
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
CREATE TRIGGER before_exchange_update
	BEFORE UPDATE ON exchange
    FOR EACH ROW BEGIN
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
DELIMITER ;

insert into exchange(id_locale,abbrev,name,exchange_open,exchange_close)
values
(@id_locale_NYC,"NYSE","New York Stock Exchange","09:30:00","16:00:00"),
(@id_locale_NYC,"NASDAQ","NASDAQ","09:30:00","16:00:00"),
(@id_locale_Brussels,"EURONEXT","EURONEXT","09:00:00","17:30:00");


-- create datsource table
CREATE TABLE datasource (
  id_datasource int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name varchar(64) NOT NULL,
  website varchar(255) NULL,
  email varchar(255) NULL,
  da_insert datetime NULL,
  da_last_update datetime NULL
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DELIMITER $
CREATE TRIGGER before_datasource_insert
	BEFORE INSERT ON datasource
    FOR EACH ROW BEGIN
    SET new.da_insert = UTC_TIMESTAMP();
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
CREATE TRIGGER before_datasource_update
	BEFORE UPDATE ON datasource
    FOR EACH ROW BEGIN
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
DELIMITER ;

insert into datasource(name,website)
values 
("DTN IQFeed", "www.iqfeed.net"),
("Quandl - WIKI","https://www.quandl.com"),
("Quandl - GOOGLE","https://www.quandl.com"),
("Quandl - YAHOO","https://www.quandl.com"),
("Quandl - EODData","eoddata.com"),
("Quandl - QuoteMedia","http://www.quotemedia.com/"),
("Yahoo","finance.yahoo.com/"),
("Interactive Brokers","https://www.interactivebrokers.com/");


-- create assetclass lookup table
CREATE TABLE assetclass (
  id_assetclass int NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,
  name varchar(255) NOT NULL
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

insert into assetclass(name) 
values
("Equities"),
("Indices"),
("Foreign Exchange"),
("Futures"),
("Commodities"),
("Bonds"),
("Equity Options"),
("Derivatives - Caps, Floors, Swaps"),
("Interest Rates");

-- create sector_gics lookup table
CREATE TABLE sector_gics(
	id_sector_gics int NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,
    name varchar(255) NOT NULL,
    code smallint NOT NULL  
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

insert into sector_gics(name,code)
values
("Energy",10),
("Materials",15),
("Industrials",20),
("Consumer Discretionary",25),
("Consumer Staples",30),
("Health Care",35),
("Financials",40),
("Information Technology",45),
("Telecommunications Services",50),
("Utilities",55);

-- create sector_idb lookup table
CREATE TABLE sector_icb(
	id_sector_icb int NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,
    name varchar(255) NOT NULL,
    code smallint NOT NULL  
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

insert into sector_icb(name,code)
values
("Oil & Gas",0001),
("Basic Materials",1000),
("Industrials",2000),
("Consumer Goods",3000),
("Health Care",4000),
("Consumer Services",5000),
("Telecommunications",6000),
("Utilities",7000),
("Financials",8000),
("Technology",9000);


-- Create symbol table
CREATE TABLE symbol (
  id_symbol int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  id_exchange int NULL REFERENCES exchange (id_exchange),
  id_assetclass int NOT NULL REFERENCES assetclass (id_assetclass),
  id_sector_gics int NULL REFERENCES sector_gics(id_sector_gics),
  id_sector_icb int NULL REFERENCES sector_icb(id_sector_icb),
  ticker varchar(32) NOT NULL,
  name varchar(255) NULL,
  currency varchar(32) NULL,
  comment varchar(255) NULL,
  da_insert datetime NULL, -- Triggers avoid NULL values
  da_last_update datetime NULL -- Triggers avoid NULL values
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DELIMITER $
CREATE TRIGGER before_symbol_insert
	BEFORE INSERT ON symbol
    FOR EACH ROW BEGIN
    SET new.da_insert = UTC_TIMESTAMP();
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
CREATE TRIGGER before_symbol_update
	BEFORE UPDATE ON symbol
    FOR EACH ROW BEGIN
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
DELIMITER ;


-- Create corporate action_dividend table
CREATE TABLE corporate_action_dividend(
	id_corporate_action_dividend int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_symbol int NOT NULL REFERENCES symbol(id_symbol),
    value decimal(19,5) NOT NULL,
	da_ex_dividend date NULL,
    da_payment date NULL,
    da_announcement date NULL,
    da_record date NULL,
    da_insert datetime NULL,
    da_last_update datetime NULL
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DELIMITER $
CREATE TRIGGER before_corporate_action_dividend_insert
	BEFORE INSERT ON corporate_action_dividend
    FOR EACH ROW BEGIN
    SET new.da_insert = UTC_TIMESTAMP();
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
CREATE TRIGGER before_corporate_action_dividend_update
	BEFORE UPDATE ON corporate_action_dividend
    FOR EACH ROW BEGIN
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
DELIMITER ;


-- Create corporate_action_merger_split table
CREATE TABLE corporate_action_merger_split(
	id_corporate_action_merger_split int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_symbol int NOT NULL REFERENCES symbol(id_symbol),
    merge_split_ratio decimal(19,5) NOT NULL,
	da_ex_merge_split date NULL,
    da_announcement date NULL,
    da_record date NULL,
    da_insert datetime NULL,
    da_last_update datetime NULL
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DELIMITER $
CREATE TRIGGER before_corporate_action_merger_split_insert
	BEFORE INSERT ON corporate_action_merger_split
    FOR EACH ROW BEGIN
    SET new.da_insert = UTC_TIMESTAMP();
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
CREATE TRIGGER before_corporate_action_merger_split_update
	BEFORE UPDATE ON corporate_action_merger_split
    FOR EACH ROW BEGIN
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
DELIMITER ;


-- Create trading frequency lookup table
CREATE TABLE tradingfrequency (
  id_tradingfrequency int NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,
  name varchar(255) NOT NULL
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

insert into tradingfrequency(name)
values
("TICK"),
("SECOND_01"),
("SECOND_05"),
("MINUTE_01"),
("MINUTE_05"),
("MINUTE_10"),
("MINUTE_15"),
("MINUTE_30"),
("HOURLY_01"),
("HOURLY_04"),
("DAILY"),
("WEEKLY"),
("MONTHLY");


-- Create timeseries table
CREATE TABLE timeseries(
  id_timeseries int NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,
  id_symbol int NOT NULL REFERENCES symbol (id_symbol),
  id_datasource int NOT NULL REFERENCES datasource (id_datasource),
  id_tradingfrequency int NOT NULL REFERENCES tradingfrequency (id_tradingfrequency),
  da_begin datetime(6) NOT NULL,
  da_end datetime(6) NOT NULL,
  isactive boolean NOT NULL DEFAULT FALSE,
  isblocked boolean NOT NULL DEFAULT FALSE,
  reasonblocked varchar(255) NULL,
  da_insert datetime NULL, -- Triggers avoid NULL values
  da_last_update datetime NULL -- Triggers avoid NULL values
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DELIMITER $
CREATE TRIGGER before_timeseries_insert
	BEFORE INSERT ON timeseries
    FOR EACH ROW BEGIN
    SET new.da_insert = UTC_TIMESTAMP();
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
CREATE TRIGGER before_timeseries_update
	BEFORE UPDATE ON timeseries
    FOR EACH ROW BEGIN
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
DELIMITER ;


-- Create datapoint table
CREATE TABLE datapoint (
  id_datapoint int NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,
  id_timeseries int NOT NULL REFERENCES timeseries(id_timeseries),
  timestamp datetime(6) NOT NULL, -- Ctimesurrent time (ticks) or Closing bar
  price decimal(19,5) NOT NULL, -- Mid Price
  bid decimal(19,5) NULL,
  ask decimal(19,5) NULL,
  open decimal(19,5) NULL,
  high decimal(19,5) NULL,
  low decimal(19,5) NULL,
  close decimal(19,5) NULL,
  volume bigint NULL,
  adjbid decimal(19,5) NULL,
  adjask decimal(19,5) NULL,
  adjopen decimal(19,5) NULL,
  adjhigh decimal(19,5) NULL,
  adjlow decimal(19,5) NULL,
  adjclose decimal(19,5) NULL,
  adjvolume bigint NULL,
  isshortable boolean NOT NULL DEFAULT TRUE,
  transactioncost decimal(19,5) NULL,
  commission decimal(19,5) NULL,
  tradingcost decimal(19,5) NULL,
  min_tradesize decimal(19,5) NULL,
  max_tradesize decimal(19,5) NULL,
  isinterpolated boolean NOT NULL DEFAULT FALSE,
  isoutlier boolean NOT NULL DEFAULT FALSE,
  isoutlierextreme boolean NOT NULL DEFAULT FALSE,
  da_insert datetime NULL, -- Triggers avoid NULL values
  da_last_update datetime NULL -- Triggers avoid NULL values
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

DELIMITER $
CREATE TRIGGER before_datapoint_insert
	BEFORE INSERT ON datapoint
    FOR EACH ROW BEGIN
    SET new.da_insert = UTC_TIMESTAMP();
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
CREATE TRIGGER before_datapoint_update
	BEFORE UPDATE ON datapoint
    FOR EACH ROW BEGIN
    SET new.da_last_update = UTC_TIMESTAMP();
    END$
DELIMITER ;

-- select * from exchange;
-- select * from locale;
-- select * from dst_info;
-- select * from timezone;