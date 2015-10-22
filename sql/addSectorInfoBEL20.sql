DELIMITER $$
CREATE PROCEDURE updateSectorICB(
	IN ticker_list varchar(32),
	IN industry varchar(255))
BEGIN
	UPDATE symbol
    SET id_sector_icb =
    (
		SELECT id_sector_icb
		FROM sector_icb
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

set @BEL20Industrials = 'ACKB,BEFB,BEKB,COFB,ELI';
set @BEL20Financials = 'AGS,DL,GBLB,KBC';
set @BEL20ConsumerGoods = 'ABI';
set @BEL20Telecommunications = 'BELG';
set @BEL20ConsumerServices = 'COLR,DELB,DIE,TNET';
set @BEL20OilGas = 'GSZ';
set @BEL20BasicMaterials = 'SOLB,UMI';
set @BEL20HealthCare = 'THR,UCB';

call updateSectorICB(@BEL20Industrials,"Industrials");
call updateSectorICB(@BEL20Financials,"Financials");
call updateSectorICB(@BEL20ConsumerGoods,"Consumer Goods");
call updateSectorICB(@BEL20Telecommunications,"Telecommunications");
call updateSectorICB(@BEL20ConsumerServices,"Consumer Services");
call updateSectorICB(@BEL20OilGas,"Oil & Gas");
call updateSectorICB(@BEL20BasicMaterials,"Basic Materials");
call updateSectorICB(@BEL20HealthCare,"Health Care");