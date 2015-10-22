source("facade/DBFacade.R")

# Todo: function was developed before existance of DBFacade
# --> some functionality can be replaced with the generic funcions in DBFacade.R
insertUpdateTimeSeriesDB <- function(symbol,datasource,tradingFrequency,seriesData,
                                     isActive=FALSE,isBlocked=FALSE,reasonBlocked=NULL,conn=NULL)
{
  # Avoid timezone issues: use UTC as default convertor
  Sys.setenv(TZ='UTC')
  
  # Open DB connection
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  #################################################
  ## Writing or Updating the timeseries metadata ##
  #################################################
  
  # Get timeseries foreign keys
  idSymbol <- ifelse(is.numeric(symbol),symbol,getIdSymbol(symbol,conn=mydb))
  idDataSource <- ifelse(is.numeric(datasource),datasource,getIdDataSource(datasource,conn=mydb))
  idTradingFrequency <- ifelse(is.numeric(tradingFrequency),
                              tradingFrequency,getIdTradingFrequency(tradingFrequency,conn=mydb))
  
  daBegin <- index(seriesData[1,])
  daEnd <- index(seriesData[nrow(seriesData),])
  
  #strSelect = "SELECT id_timeseries from timeseries"
  #strCondition = paste("WHERE id_symbol = ",idSymbol," and id_datasource = ",idDataSource,
  #                        " and id_tradingFrequency = ",idTradingFrequency,";",sep="")
  idTimeSeries <- getIdTimeSeries(idSymbol,idDataSource,idTradingFrequency,conn=mydb)
  if(is.na(idTimeSeries)) # Timeseries data not found, perform DB insertion
  {
    strReasonBlocked <- ifelse(is.null(reasonBlocked),"NULL",paste("'",reasonBlocked,"'",sep=""))
    strInsert = paste("insert into timeseries(id_symbol,id_datasource,id_tradingfrequency,",
                      "da_begin,da_end,isactive,isblocked,reasonblocked)",sep="")
    strValues = paste("values(",idSymbol,",",idDataSource,",",idTradingFrequency,
                      ",'",daBegin,"','",daEnd,"',",isActive,",",isBlocked,",",strReasonBlocked,");",sep="")
    insertionQuery <- paste(strInsert,strValues)
    print(insertionQuery); dbGetQuery(mydb,insertionQuery)
  }
  else # timeseries already in DB. Perform DB update if necessary
  {
    strSelect = "SELECT da_begin, da_end, isactive, isblocked, reasonblocked from timeseries"
    strCondition = paste("WHERE id_timeseries = ",idTimeSeries,";",sep="")
    oldResults = dbGetQuery(mydb,paste(strSelect,strCondition))
    
    oldDaBegin = oldResults$da_begin; oldDaEnd = oldResults$da_end
    oldIsActive = oldResults$isactive; oldIsBlocked = oldResults$isblocked
    oldReasonBlocked = oldResults$reasonblocked
    
    # Check for modifications.
    if(as.POSIXlt(oldDaBegin) != as.POSIXlt(daBegin) ||
       as.POSIXlt(oldDaEnd) != as.POSIXlt(daEnd) ||
       oldIsActive != isActive || oldIsBlocked != isBlocked ||
       ifelse(is.na(oldReasonBlocked),!is.null(reasonBlocked),oldReasonBlocked!=reasonBlocked))
    { # Modifications where made to timeseries tuple. Execute row update
      strReasonBlocked <- ifelse(is.null(reasonBlocked),"NULL",paste("'",reasonBlocked,"'",sep=""))
      strUpdate = "update timeseries"
      strModification = paste("set da_begin = '",daBegin,"', da_end = '",daEnd,"', isactive = ",
                              isActive,", isblocked = ", isBlocked, ", reasonblocked = ",strReasonBlocked,sep="")
      strCondition = paste("WHERE id_timeseries = ",idTimeSeries,";",sep="")
      updateQuery <- paste(strUpdate,strModification,strCondition)
      print(updateQuery); dbGetQuery(mydb,updateQuery)
    }
  }
  
  ########################################
  ## Writing or updating the datapoints ##
  ########################################
  
  # toInsert <- seriesData$price # timestamp (index) and price are mandatory fields
  # strValues <- paste("('",as.data.frame(index(seriesData)),"','",seriesData[,"price"],"'", sep="", collapse=",")
  nonMandatoryFields <- c("bid","ask","open","high","low","close","volume","adjbid","adjask","adjopen",
                          "adjhigh","adjlow","adjclose","adjvolume","transactioncost","commission","
                          min_tradesize","max_tradesize","isinterpolated","isoutlier","isoutlierextreme")
  # Paste column name information to string (for usage in DB querys)
  strFields <- "timestamp,price"
  # save column name information in vector (for usage in xts/data.frame subsetting)
  colNames <- c("timestamp","price")
  # strValueIndexExpression <- "paste(\"('\",eval(parse(text=\"as.character(index(seriesData[i,]))\")),\"'\",sep=\"\")"
  # strValuePriceExpression <- "paste(\",'\",eval(parse(text=\"as.character(seriesData[i,'open'])\")),\"'\",sep=\"\")"
  # strValuesExpression <- paste("paste(eval(",strValueIndexExpression,"), eval(",strValuePriceExpression,"),sep=\"\")",sep="")
  for(field in nonMandatoryFields)
  {
    if(field %in% names(seriesData))
    {
      strFields <- paste(strFields,",",field,sep="")
      # toInsert <- merge(toInsert,seriesData[,field])
      # strValues <- paste(strValues,",'",seriesData[,field],"'", sep="", collapse=",")
      
      # strValueFieldExpression <- paste("paste(\",'\",eval(parse(text=\"as.character(seriesData[i,'",field,"'])\")),\"'\",sep=\"\")",sep="")
      # strValuesExpression <- paste("paste(eval(",strValuesExpression,"), eval(",strValueFieldExpression,"),sep=\"\")",sep="")
      
      colNames <- c(colNames,field)
    }
  }
  
  ###################################################
  # Writing or updating the datapoints              #
  # Handle timestamps that are already present in DB#
  ###################################################
  idTimeSeries <- getIdTimeSeries(idSymbol,idDataSource,idTradingFrequency,conn=mydb)
  timestamps <- dbGetQuery(mydb,paste("SELECT id_datapoint, timestamp from datapoint WHERE id_timeseries = ",
                                      idTimeSeries,";",sep=""))
  newTimeStamps <- as.POSIXlt(index(seriesData))
  oldTimeStamps <- as.POSIXlt(timestamps$timestamp)
  
  # Obtain vector with indices of timestamps that are already in db
  duplicateVectorNew <- as.POSIXct(newTimeStamps) %in% as.POSIXct(oldTimeStamps)
  # Extract rows from the new information for which the timestamps are already in DB
  duplicateSeries <- seriesData[duplicateVectorNew,]
  
  # Handle timestamps that are already in db. Update rows if modifications detected.
  if(nrow(duplicateSeries) > 0)
  {
    # Fetch the indices of timestamps already in DB
    duplicateVectorOld <- as.POSIXct(oldTimeStamps) %in% as.POSIXct(newTimeStamps)
    # Fetch the relevant id_datapoints and convert to string
    duplicateDatapointIdStr <- paste(timestamps$id_datapoint[duplicateVectorOld],collapse=",")
    # Fetch the rowinformation for the duplicate entrys
    selectionQuery <- paste("SELECT",strFields,"FROM datapoint WHERE id_datapoint in (",duplicateDatapointIdStr,");")
    selectionResult <- dbGetQuery(mydb,selectionQuery)
    
    # Put revlaant columns in correct order, for comparison reasons
    oldResult <- selectionResult[,colNames]
    # convert timestamp to POSIXlt for comparison reasons
    oldResult$timestamp <- as.POSIXlt(oldResult$timestamp)
    
    # convert the new series to a dataframe.
    newResult <- data.frame(timestamp=index(duplicateSeries),duplicateSeries[,colNames[-1]])
    # convert timestamp to POSIXlt, for comparison reasons
    newResult$timestamp <- as.POSIXlt(newResult$timestamp)
    # round all numerics (or booleans) to 5 digits maximum, for comparison reasons
    newResult[,-1] <- round(newResult[,-1],digits=5)
    # remove rownames
    rownames(newResult) <- NULL
    
    # Add unique entrys in 1 dataframe.
    # Note: all rows in oldResult and newResult are independently unique
    uniqueCombined <- unique(rbind(oldResult,newResult))
    # Obtain unique "modified" rows that are in newResult but not in oldResult
    uniqueAdded <- uniqueCombined[-seq(nrow(oldResult)),]
    
    if(nrow(uniqueAdded) > 0) # Modifed information detected
    {
      # Obtain the datapoint id's for the modified rows
      toUpdateDatapoints <- timestamps$id_datapoint[as.POSIXlt(timestamps$timestamp) %in% as.POSIXlt(uniqueAdded$timestamp)]
      # Create dataframe
      writableFrame <- data.frame(id_datapoint=toUpdateDatapoints,uniqueAdded)
      writableFrame$timestamp <- as.character(writableFrame$timestamp)
      rownames(writableFrame) <- seq(1,nrow(writableFrame))
      # Update information in DB
      print(paste("Symbol ", symbol,", Timeseries id ", idTimeSeries,
                  ": Updating ", nrow(writableFrame)," already inserted datapoint(s) in DB", sep=""))
      # print(writableFrame)
      # dbWriteTable does not overwrite rows..
      # dbWriteTable(mydb,"datapoint",writableFrame,append=TRUE,row.names=FALSE)
      
      insertUpdateDB(tableName="datapoint",seriesData=writableFrame,conn=mydb,printQuery=TRUE)
    }
    else
    {
      print(paste("Symbol ",symbol,", Timeseries id ", idTimeSeries,
                  ": No modifications were detected for already inserted datapoint(s)",sep=""))
    }
  }
  
  ###################################################
  # Writing or updating the datapoints              #
  # Handle new timestamps                           #
  ###################################################
  
  newSeries <- seriesData[!duplicateVectorNew,]
  if(nrow(newSeries > 0))
  {
    # Convert newSeries data to data.frame 
    writableFrame <- data.frame(id_timeseries=rep(idTimeSeries,nrow(newSeries)),
                                timestamp=index(newSeries),coredata(newSeries[,colNames[-1]]))
    print(paste("Writing",nrow(writableFrame),"new datapoints to DB for symbol"
                                        ,symbol,"and timeseries id",idTimeSeries))
    # Write the data.frame to the DB
    dbWriteTable(mydb,"datapoint",writableFrame[,c("id_timeseries",colNames)],append=TRUE,row.names=FALSE)
  }
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
}