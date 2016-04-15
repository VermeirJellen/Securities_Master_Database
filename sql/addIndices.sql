-- SET SQL_SAFE_UPDATES = 0;
-- Add NYSE ARCA exchange info
call getLocaleIdByLocation("USA","New York City",@id_locale_NYC);
insert into exchange(id_locale,abbrev,name,exchange_open,exchange_close)
values
(@id_locale_NYC,"ARCA","Archipelago","09:30:00","16:00:00");

DELIMITER $$
CREATE PROCEDURE getAssetclassIdByName(
	IN asset_type_name varchar(255),
    OUT assetclass_id int)
BEGIN
	SELECT id_assetclass into assetclass_id
    FROM assetclass
    where name = asset_type_name;
END$$
DELIMITER ;

DELIMITER $$
CREATE PROCEDURE updateAssetclassIdBySymbolList(
	IN ticker_list varchar(255),
	IN assetclass varchar(255))
BEGIN
	UPDATE symbol
    SET id_assetclass =
    (
		SELECT id_assetclass
		FROM assetclass
		WHERE name = assetclass
    )
    WHERE id_symbol IN
    (
		SELECT id_symbol
		FROM 
        ( 	select id_symbol from symbol
			WHERE FIND_IN_SET(ticker,ticker_list)
		) AS symbol_alt
    );
END$$
DELIMITER ;

-- drop procedure getAssetclassIdByName;
-- drop procedure updateAssetclassIdBySymbolList;

call getAssetclassIdByName("Indices",@id_assetclass_indices);

insert into symbol(ticker,id_assetclass,name,currency,comment)
values
("SPY",@id_assetclass_indices,"SPDR S&P500 ETF Trust","USD","INDEX"),
("XLE",@id_assetclass_indices,"Energy Select Sector SPDR Fund","USD","INDEX"),
("XLU",@id_assetclass_indices,"Utilities Select Sector SPDR Fund","USD","INDEX"),
("XLK",@id_assetclass_indices,"Technology Select Sector SPDR Fund","USD","INDEX"),
("XLB",@id_assetclass_indices,"Materials Select Sector SPDR Fund","USD","INDEX"),
("XLP",@id_assetclass_indices,"Consumer Staples Select Sector SPDR Fund","USD","INDEX"),
("XLY",@id_assetclass_indices,"Consumer Discretionary Select Sector SPDR Fund","USD","INDEX"),
("XLI",@id_assetclass_indices,"Industrial Select Sector SPDR Fund","USD","INDEX"),
("XLV",@id_assetclass_indices,"Health Care Select Sector SPDR Fund","USD","INDEX"),
("XLF",@id_assetclass_indices,"Financial Select Sector SPDR Fund","USD","INDEX"),
("BEL",@id_assetclass_indices,"LYXOR UCITS ETF BEL 20 TR","EUR","INDEX");


-- set @Indices = 'SPY,XLE,XLU,XLK,XLB,XLP,XLY,XLI,XlV,XLF,BEL';
-- call updateAssetClassInfo(@Indices,"Indices");

DELIMITER $$
CREATE PROCEDURE updateSectorGICS(
	IN ticker_list varchar(32),
	IN industry varchar(255))
BEGIN
	UPDATE symbol
    SET id_sector_GICS =
    (
		SELECT id_sector_GICS
		FROM sector_gics
		WHERE name = industry
    )
    WHERE id_symbol IN
    (
		SELECT id_symbol
		FROM 
        ( 	select id_symbol from symbol
			WHERE FIND_IN_SET(ticker,ticker_list)
		) AS symbol_alt
    );
END$$
DELIMITER ;

call updateSectorGICS("XLE","Energy");
call updateSectorGICS("XLB","Materials");
call updateSectorGICS("XLI","Industrials");
call updateSectorGICS("XLY","Consumer Discretionary");
call updateSectorGICS("XLP","Consumer Staples");
call updateSectorGICS("XLV","Health Care");
call updateSectorGICS("XLF","Financials");
call updateSectorGICS("XLK","Information Technology");
call updateSectorGICS("XLU","Utilities");

select * from datasource;