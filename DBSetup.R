# 1 - Run createSecuritiesMaster.sql + update ./config/credentials.cnf.
# 2 - Run InsertMetaDataS&P500BEL20.py
# 3 - Run addSectorInfoBEL20.sql
# 4 - Run addIndices.sql
# 4 - Run THIS script

# if(!require(installr)) {install.packages("installr"); require(installr)}
# updateR()
# packages = c("RMySQL","RCURL","bizdays","XML","Quandl","xts")
# install.packages(packages)

# setwd(this/directory)
# getwd()

source("facade/DBFacade.R")
source("scripts/addExchangeInfoSnP500BEL20.R")
source("scripts/scrapeInsertCorporateActionDB.R")
source("scripts/processEODDataQuandl.R")

mydb = connectionSecuritiesMaster()

addExchangeInfoSnP500BEL20(conn=mydb)
scrapeInsertCorporateActionDB(conn=mydb)
ProcessEODDataQuandl(conn=mydb)

dbDisconnect(mydb)