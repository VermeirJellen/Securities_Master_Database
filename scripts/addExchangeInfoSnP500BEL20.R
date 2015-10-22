source("facade/DBFacade.R")

addExchangeInfoSnP500BEL20 <- function(conn=NULL)
{
  # Open DB connection
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  # 1. Update Bel 20 exchange Id
  symbolsIdBel20Stocks <- queryDB(c("id_symbol"),"symbol",data.frame(comment="BEL20"),conn=mydb)
  symbolsIdBel20Index <- queryDB(c("id_symbol"),"symbol",data.frame(comment="INDEX",ticker="BEL"),conn=mydb)
  symbolsIdBel20 <- rbind(symbolsIdBel20Stocks,symbolsIdBel20Index)
  
  euroNextExchangeId <- queryDB(select="id_exchange",tableName="exchange",
                                         where=data.frame(abbrev="EURONEXT"))
  # Perform the update
  tableName <- "symbol"
  set <- data.frame(id_exchange = rep(as.numeric(euroNextExchangeId),nrow(symbolsIdBel20)))
  where <- data.frame(id_symbol = as.vector(symbolsIdBel20))
  updateDB(tableName,set,where,conn=mydb,printQuery=TRUE)
  
  
  # 2. Update SPDR indices exchange id's
  symbolsIdIndices <- queryDB(c("id_symbol"),"symbol",data.frame(comment="INDEX"),conn=mydb)
  symbolsIdSnP500Indices <- symbolsIdIndices[!(symbolsIdIndices$id_symbol %in% symbolsIdBel20Index$id_symbol),]
  arcaExchangeId <- queryDB(select="id_exchange",tableName="exchange",
                           where=data.frame(abbrev="ARCA"),conn=mydb)
  
  # Perform the update
  tableName <- "symbol"
  set <- data.frame(id_exchange = rep(as.numeric(arcaExchangeId),length(symbolsIdSnP500Indices)))
  where <- data.frame(id_symbol = as.vector(symbolsIdSnP500Indices))
  updateDB(tableName,set,where,conn=mydb,printQuery=TRUE)
  
  
  # 3. UPDATE S&P500 stocks exchange Id's
  csvFile <- read.csv(file.path(getwd(),"quandl","QuandlCodesSnP500.csv"))
  symbolIdSnP500 <- queryDB(c("id_symbol","ticker"),"symbol",data.frame(comment="S&P500"))
  nyseExchangeId <- queryDB(select="id_exchange",tableName="exchange",
                            where=data.frame(abbrev="NYSE"))
  nasdaqExchangeId <- queryDB(select="id_exchange",tableName="exchange",
                                      where=data.frame(abbrev="NASDAQ"))
  
  tickerCodes <- csvFile$Code
  prefixNYSE <- "GOOG/NYSE_"
  prefixNASDAQ <- "GOOG/NASDAQ_"
  
  tickers <- symbolIdSnP500$ticker
  codesNYSE <- paste(prefixNYSE,tickers,sep="")
  codesNASDAQ <- paste(prefixNASDAQ,tickers,sep="")
  
  # Extra tickers that do not occur in the quandl csv (manual lookup)
  nasdaqExtra <- c("ATVI","AAL","HSIC","DISCK","ENDP","EQIX","GOOGL","HCA",
                   "JBHT","KHC","LVLT","PYPL","QRVO","O","RCL","SIG",
                   "SWKS","TGNA","WBA","ANTM","YHOO","ZBH","BXLT")
  nyseExtra <- c("AAP","NYSE","HRB","CPGX","ES","HBI","JOY","WRK","SLG","TYC","UAL")
  
  symbolIdNYSECSV <- symbolIdSnP500$id_symbol[which(codesNYSE %in% tickerCodes)]
  symbolIdNYSEExtra <- symbolIdSnP500$id_symbol[which(symbolIdSnP500$ticker %in% nasdaqExtra)]
  symbolIdNYSE <- c(symbolIdNYSECSV,symbolIdNYSEExtra)
  
  symbolIdNASDAQCSV <- symbolIdSnP500$id_symbol[which(codesNASDAQ %in% tickerCodes)]
  symbolIdNASDAQExtra <- symbolIdSnP500$id_symbol[which(symbolIdSnP500$ticker %in% nyseExtra)]
  symbolIdNASDAQ <- c(symbolIdNASDAQCSV,symbolIdNASDAQExtra)
  
  nrNyse <- length(symbolIdNYSE)
  nrNasdaq <- length(symbolIdNASDAQ)
  
  # Perform the update
  tableName <- "symbol"
  set <- data.frame(id_exchange = c(rep(nyseExchangeId[1,],nrNyse),rep(nasdaqExchangeId[1,],nrNasdaq)))
  where <- data.frame(id_symbol = c(as.vector(symbolIdNYSE),as.vector(symbolIdNASDAQ)))
  updateDB(tableName,set,where,conn=mydb,printQuery=TRUE)
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
}