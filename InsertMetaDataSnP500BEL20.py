# -*- coding: utf-8 -*-
"""

@author: Jellen Vermeir
"""
import os
import sys
import MySQLdb as mdb
from math import ceil
from lxml import html
from urllib2 import urlopen

# Fetch the assetclass table foreign key value from the securities_master database
# assetType: AssetType name for which the foreign key value should be fetched
def getAssetId(assetClass):
    # Absolute Path to DB settings and credentials configuration
    MYSQL_CNF = os.path.join(os.path.abspath('.'),'config','credentials.cnf') # Fetch local credentials file
    # Connect to DB
    DB = mdb.connect(read_default_file = MYSQL_CNF,
                     read_default_group="algotrading_securities_master")
    cursor = DB.cursor()
    
    idAssetQuery= "select id_assetclass from assetclass where name = %s"
    cursor.execute(idAssetQuery,(assetClass))
    assetId = cursor.fetchone()[0] # Returns one row with secondary key    
    
    DB.close()
    return assetId
    
# Return the name-foreign key dictionary mapping for the sector lookup table
def getSectorMappingGICS():
    # Absolute Path to DB settings and credentials configuration
    MYSQL_CNF = os.path.join(os.path.abspath('.'),'config','credentials.cnf') # Fetch local credentials file
    # Connect to database
    DB = mdb.connect(read_default_file = MYSQL_CNF,
                     read_default_group="algotrading_securities_master")
    cursor = DB.cursor()
    
    # Create name-key mapping for the sector_gics table foreign key values
    sectorQuery = "SELECT name, id_sector_gics from sector_gics"
    cursor.execute(sectorQuery)
    sectorKeyMap = dict(cursor.fetchall())
    
    DB.close()
    return sectorKeyMap
    
# Download and parse the Wikipedia list of BEL20
def getBel20MetaData():
    # Download the list of SnP500 companies and obtain the rows in the table
    wikipage = html.parse(urlopen('https://en.wikipedia.org/wiki/BEL20'))
    symbolstable = wikipage.xpath('//table[2]/tr')[1:21]

    idEquity = getAssetId('Equities')
    #sectorKeyMap = getSectorMappingICB() todo
    
    # Obtain the symbol information for each row in the Bel20 constituent table
    # Rowstructure: Company name | ICB Sector | Ticker Symbol | ...
    symbols = []
    for i, symbol in enumerate(symbolstable):
        tds = symbol.getchildren()
        symbolTicker = tds[2].getchildren()[0].text # Note: Entry contains child <a> tag
        symbolName = tds[0].getchildren()[0].text # Note: Entry contains child <a> tag
        idSector = None # View updateBel20ICB.sql
        
        # Create a DB-tuple and append to the list
        symbols.append( (idEquity, idSector, symbolTicker, symbolName, 'EUR', "BEL20") )

    return symbols # Return the tuples containing the metadata
    
# Download and parse the Wikipedia list of SnP500
def getSnp500MetaData():
    # Download the list of SnP500 companies and obtain the rows in the table
    wikipage = html.parse(urlopen('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'))
    symbolstable = wikipage.xpath('//table[1]/tr')[1:503] #Note: 502 common stocks in snp500

    idEquity = getAssetId('Equities') # get Equities assetclass foreign key 
    sectorKeyMap = getSectorMappingGICS() # get sector foreign key mappings
    
    # Obtain the symbol information for each row in the SnP500 constituent table
    # Rowstructure: Ticker Symbol | Security name | SEC Filings | GICS Sector | GICS Sub Industry | ...
    symbols = []
    for i, symbol in enumerate(symbolstable):
        tds = symbol.getchildren()
        symbolTicker = tds[0].getchildren()[0].text # Note: Entry contains child <a> tag
        symbolName = tds[1].getchildren()[0].text # Note: Entry contains child <a> tag
        idSector = sectorKeyMap[tds[3].text] # get id_sector_gics foreign key value
        
        # Create a DB-tuple and append to the list
        symbols.append( (idEquity, idSector, symbolTicker, symbolName, 'USD',"S&P500") )

    return symbols # Return the tuples containing the metadata


"""Insert the symbols into the MySQL database."""
def insertSymbolMetaData(symbols,sector_gics):
    MYSQL_CNF = os.path.join(os.path.abspath('.'),'config','credentials.cnf') # Fetch local credentials file
    DB = mdb.connect(read_default_file = MYSQL_CNF,
                     read_default_group="algotrading_securities_master")
    
    # Create the insert strings
    strSector = "id_sector_gics" if sector_gics else "id_sector_icb"
    strColumn = "id_assetclass, %s, ticker, name, currency, comment" % (strSector)
    strInsert = ("%s, " * 6)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (strColumn, strInsert)
    print final_str, len(symbols)
    
    # Carry out an INSERT INTO for every symbol
    with DB: 
        cur = DB.cursor()
        # Split up the inserts: This line avoids the MySQL MAX_PACKET_SIZE
        for i in range(0, int(ceil(len(symbols) / 100.0))):
            cur.executemany(final_str, symbols[i*100:(i+1)*100])


if __name__ == "__main__":
    symbolsSNP500 = getSnp500MetaData()
    insertSymbolMetaData(symbolsSNP500,True)
    
    symbolsBEL20 = getBel20MetaData()
    insertSymbolMetaData(symbolsBEL20,False)