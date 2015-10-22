source("scripts/insertUpdateTimeSeriesDB.R")
source("scripts/backAdjustTimeseries.R")
library(Quandl)
library(bizdays)

ProcessEODDataQuandl <- function(startDate=as.Date("2001-01-10"),endDate=Sys.Date(),conn=NULL)
{
  # Open DB connection
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  # Set quandl API token
  authPath = file.path(getwd(),"quandl","authentication","quandl.cnf")
  authToken = read.csv(authPath,header=FALSE)
  Quandl.auth(as.character(authToken[1,]))
  
  # Read file containing the WIKI codes
  filePathWIKI = file.path(getwd(),"quandl","WIKI_tickers.csv")
  # filePathSnP = file.path(getwd(),"quandl","QuandlCodesSnP500.csv")
  filePathBel20 = file.path(getwd(),"quandl","QuandlCodesBel20Yahoo.csv")
  filePathIndices = file.path(getwd(),"quandl","QuandlCodesIndices.csv")
  
  fileWIKI = read.csv(filePathWIKI,header=TRUE,sep=",")
  # fileSnP = read.csv(filePathSnP,header=TRUE,sep=",")
  fileBel20 = read.csv(filePathBel20,header=TRUE,sep=",")
  fileIndices = read.csv(filePathIndices,header=TRUE,sep=",")
  
  symbolsList <- dbGetQuery(mydb,"SELECT ticker from symbol;")$ticker
  
  for(symbol in symbolsList) # !! tickers not unique
  {
    cat(paste("\nPROCESS TIMESERIES FOR SYMBOL ",symbol,":\n",sep=""))
    nextSymbol <- FALSE # hack: R can not process next statement during error handling
    timeSeries <- NULL
    tryCatch(
    {
      if(symbol %in% fileBel20[,1]) # Yahoo data for Bel20 (includes stocks and the BEL ETF index)
      { 
        src <- "Quandl - Yahoo"
        bel20Code = as.character(fileBel20[fileBel20$Ticker==symbol,'Code'])
        timeSeries <- Quandl(bel20Code,start_date=startDate,end_date=endDate)
       # match db column names
        names(timeSeries) <- c("timestamp","open","high","low","close","volume","adjclose")
      }
      else if (symbol %in% fileIndices[,1])
      {
        src <- "Quandl - QuoteMedia"
        indexCode <- as.character(fileIndices[fileIndices$Ticker==symbol,'Code'])
        timeSeries <- Quandl(indexCode,start_date=startDate,end_date=endDate)
        # We don't need the dividend and split ratio info here
        timeSeries <- timeSeries[,c(1:6,9:13)]
        # match db columns names
        names(timeSeries) <- c("timestamp","open","high","low","close","volume",
                                  "adjopen","adjhigh","adjlow","adjclose","adjvolume")
      }
      else # All SnP500 stocks are present in quandl WIKI
      {
        wikiCode = paste("WIKI/",symbol,sep="")
        if(wikiCode %in% fileWIKI[,1]) # WIKI code was found
        {
          src <- "Quandl - WIKI"
          timeSeries <- Quandl(wikiCode,start_date=startDate,end_date=endDate)
          # We don't need the dividend and split ratio info here
          timeSeries <- timeSeries[,c(1:6,9:13)]
          names(timeSeries) <- c("timestamp","open","high","low","close","volume",
                                    "adjopen","adjhigh","adjlow","adjclose","adjvolume")
        }
        else
          print("Quandl symbol code could not be retrieved.. aborting..")
      }
    },
    error=function(cond){
      print(paste("Encountered error condition.. aborting"))
      message(cond)
      cat("Start processing next symbol immediately..")
      # next;
      nextSymbol <- TRUE# hack: R can not process next statement during error handling
    })
    if(nextSymbol)
      next # hack: R can not process next statement during error handling
    
    # Process the timeseries (cleaning / outlierdetction / backadjustment)
    # .. and Write to DB
    if(!is.null(timeSeries))
    {
      # Convert dataframe to XTS object
      timeSeriesXTS <- as.xts(timeSeries[,-1],order.by=timeSeries[,1])
    
      # Clean the timeseries
      cleanedSeriesXTS <- cleanData(timeSeriesXTS) 
      
      # Copy adjclose as 'price' column and add the column name
      colNames <- names(cleanedSeriesXTS)
      cleanedSeriesXTS <- cbind(cleanedSeriesXTS,cleanedSeriesXTS[,'adjopen'])
      names(cleanedSeriesXTS) <- c(colNames,"price")
    
      # Insert the timeseries into the DB
      insertUpdateTimeSeriesDB(symbol,datasource = src,
                             tradingFrequency = "DAILY", seriesData = cleanedSeriesXTS, conn=mydb)
    }
  }
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
}

cleanData <- function(timeSeries)
{
  print("Cleaning data:")
  # Insert / Interpolate missing values and/or dates
  timeSeries <- interpolateMissingValuesTimestamps(timeSeries)
  
  cNames <- names(timeSeries)
  # if(length(seriesAdjustedPricesFields)==0)
  # {
  #   dividendInfo <- getDividends("M",from=as.Date("2001-02-10"),to=Sys.Date())
  #   splitInfo <- getSplits("M",from=as.Date("2001-02-10"),to=Sys.Date())
  #   splitInfo <- 1/splitInfo
  #   timeSeries <- backAdjustTimeSeries(seriesData=timeSeries,dividendInfo=dividendInfo,
  #                              mergeSplitInfo=splitInfo,useAdjClose=FALSE,adjustVolume=FALSE)
  #}
  #else 
  if(!("adjopen" %in% cNames) & ("adjclose" %in% cNames))
    timeSeries <- backAdjustTimeSeries(seriesData=timeSeries,useAdjClose=TRUE)
  
  #Vector of possible adjusted prices fields that require outlier detection
  allAdjustedPricesFields = c("adjopen","adjhigh","adjlow","adjclose")
  # Get available ajusted prices columns for the input timeSeries
  seriesAdjustedPricesFields = intersect(allAdjustedPricesFields,names(timeSeries))
  # outlier detection for adjusted prices
  timeSeries <- outlierDetection(timeSeries,colNames=seriesAdjustedPricesFields,
                                              reasonOutlier="Outlier detected - Adjusted Prices")
  # Outlier detection for adjusted volume
  # timeSeries <- outlierDetection(timeSeries,colNames=c("adjvolume"),maxAllowedDeviation=7,
  #                       extremeDeviation=15,reasonOutlier="Outlier detected - Adjusted Volume")
  
  # Return the timeSeries object
  return(timeSeries)
}


interpolateMissingValuesTimestamps <- function(timeSeries)
{
  # Fetch businessDays corresponding to timestamp interval of this timeseries
  businessDays <- getBusinessDaysTimeseries(timeSeries)
  # Add missing businessDays to the timeseries
  timeSeriesComplete <- merge(timeSeries,businessDays)
  # Get indices of rows for which at least one column contains NA values
  missingValuesIndices <- which(!complete.cases(timeSeriesComplete))
  
  if(length(missingValuesIndices > 0)) # NA values detected
  {
    print(paste("Missing values detection: interpolated",length(missingValuesIndices),"timestamps containing missing values"))
    # Fill NA's, last observation carried forward
    cleanedSeries <- na.locf(timeSeriesComplete,fromLast=FALSE)
    # Fill NA's ,next observation carried backward
    cleanedSeries <- na.locf(cleanedSeries,fromLast=TRUE)
    
    cleanedSeries <- addUpdateColumn(cleanedSeries,colName="isinterpolated",
                                                indices=missingValuesIndices)
    return(cleanedSeries)
  }
  
  # No cleaning required
  return(timeSeries)
}


outlierDetection <- function(timeSeries,colNames=c("adjclose"),rollingWidth=30,
                             maxAllowedDeviation=4,extremeDeviation=8,actionMaxAllowed="flag",
                             actionExtreme="flag",reasonOutlier="Outlier Detected")
{
  minRequiredDataPoints <- ceiling(rollingWidth*1.5)
  if(nrow(timeSeries) >= minRequiredDataPoints)
  {
    # Create a dataframe containing the relevant columns
    adjSeriesDF <- data.frame(coredata(timeSeries[,colNames]))
    
    # Perform centered rolling median calculation
    rollingMedian <- as.data.frame(rollapply(adjSeriesDF,width=rollingWidth,FUN=median,by.column=TRUE,fill=NA,align="center"))
    # Obtain the number of NA occurences at the borders that occur after applying the centered rolling calculation
    nrNABorder <- floor(rollingWidth/2); nRows <- nrow(adjSeriesDF)
    # Calculate indices of the NA values
    naIndicesStart <- seq(1,nrNABorder); naIndicesEnd <- seq(nRows,nRows-nrNABorder+1)
    # Calculate statistics for NA values at border by performing left or right rolling calculation
    rollingMedian[naIndicesStart,] <- head(as.data.frame(rollapply(adjSeriesDF[1:ceiling(1.5*rollingWidth),],
                                                                   width=rollingWidth,FUN=median,by.column=TRUE,fill=NA,align="left")),nrNABorder)
    rollingMedian[naIndicesEnd,] <- tail(as.data.frame(rollapply(adjSeriesDF[(nRows-ceiling(1.5*rollingWidth)):nRows,],
                                                                 width=rollingWidth,FUN=median,by.column=TRUE,fill=NA,align="right")),nrNABorder)
    
    # Perform the rolling mad calculation
    rollingMad <- as.data.frame(rollapply(adjSeriesDF,width=rollingWidth,FUN=mad,by.column=TRUE,fill=NA,align="center"))
    rollingMad[naIndicesStart,] <- head(as.data.frame(rollapply(adjSeriesDF[1:ceiling(1.5*rollingWidth),],
                                                                width=rollingWidth,FUN=mad,by.column=TRUE,fill=NA,align="left")),nrNABorder)
    rollingMad[naIndicesEnd,] <- tail(as.data.frame(rollapply(adjSeriesDF[(nRows-ceiling(1.5*rollingWidth)):nRows,],
                                                              width=rollingWidth,FUN=mad,by.column=TRUE,fill=NA,align="right")),nrNABorder)
    
    # Calculate min/max allowed outlier borders
    lowBorder <- as.data.frame(rollingMedian-rollingMad*maxAllowedDeviation)
    highBorder <- as.data.frame(rollingMedian+rollingMad*maxAllowedDeviation)
    # Calculate min/max extreme outlier borders
    lowBorderExtreme <- as.data.frame(rollingMedian-rollingMad*extremeDeviation)
    highBorderExtreme <- as.data.frame(rollingMedian+rollingMad*extremeDeviation)
    
    logicalMatrix <- ((adjSeriesDF < lowBorder) | (adjSeriesDF > highBorder))
    # Get rows containing outliers
    outlierRows <- which(apply(logicalMatrix,1,function(x) any(x)))
    
    logicalMatrixExtreme <- ((adjSeriesDF < lowBorderExtreme) | (adjSeriesDF > highBorder))
    # Get rows containing extreme outliers
    outlierRowsExtreme <- which(apply(logicalMatrixExtreme,1,function(x) any(x)))
    
    nrOutliers <- length(outlierRows)
    nrOutliersExtreme <- length(outlierRowsExtreme)
    if(nrOutliers > 0)
    {
      timeSeries <- addUpdateColumn(timeSeries,colName="isoutlier",indices=outlierRows)
      
      # Note: no characters possible in xts object
      # timeSeries <- addUpdateColumn(timeSeries,colName="isoutliercomment",indices=outlierRows,
      #                              value=reasonOutlier,default="NULL") 
    }
    if(nrOutliersExtreme > 0)
      timeSeries <- addUpdateColumn(timeSeries,colName="isoutlierextreme",indices=outlierRowsExtreme)
    
    # Todo: Clean / Interpolate / Remove .. depending on flags
    # For now: We just flag the datapoints in DB. Manual intervention needed.
    print(paste("Outlier detection: flagged",nrOutliers,"outliers of which",
                        nrOutliersExtreme,"can be considered extreme outliers"))
  }
  else
    print(paste("Outlier detection: Warning - Requiring at least",minRequiredDataPoints,
            "timestamps to perform outlier detection when using a rolling window of",rollingWidth))
  
  return(timeSeries)
}

# This function adds or updates a column with name 'colName' to the input timeseries
# When the column is added, the default column value is first set to 'default' value for all rows
# Column values for particular row 'indices' are set to the given 'value'
addUpdateColumn <- function(timeSeries,colName,indices,value=1,default=0)
{
  colNames <- names(timeSeries)
  if(colName %in% colNames) # column already present in timeSeries: Modify the indices.
  {
    # Create string expression that sets the row indices to true for a column with name "colName"
    expression <- paste("timeSeries$",eval(colName),"[c(",paste(eval(indices),collapse=","),")]"," <- ",eval(value),sep="")
    # Evaluate the expression
    eval(parse(text=expression))
  }
  else # Column not yet present in timeSeries: Add it.
  {
    # init the column with default values
    newColumn <- rep(eval(default),nrow(timeSeries))
    # Set specific rowindices to the given value
    newColumn[indices] <- eval(value)
    # Add the column to the original timeseries
    timeSeries <- cbind(timeSeries,newColumn)
    # Set the name of the new column
    names(timeSeries) <- c(colNames,eval(colName))
  }
  # Return the timeseries with the updated or added column
  return(timeSeries)
}


# This function returns a vector of business days that lie inside the timeseries' timestamp interval
getBusinessDaysTimeseries <- function(timeseries)
{
  # Get first day of the timeseries
  daBegin <- index(timeseries[1,])
  # Get last day of the timeseries
  daEnd <- index(timeseries[nrow(timeseries),])
  
  # Fetch calendar with holiday information
  businessCalendar <- Calendar(holidaysANBIMA, weekdays=c("saturday", "sunday"))
  # Get business days sequence for input timeseries
  businessDays <- bizseq(daBegin,daEnd,businessCalendar)
  
  return(businessDays) # return the businessDays
}


###
# Todo
# Below db query's should be accessed through DB facade
###
getIdSymbol <- function(ticker,conn=NULL)
{
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  idSymbol = dbGetQuery(mydb,paste("SELECT id_symbol from symbol where ticker = '",ticker,"';",sep=""))
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
  
  return(as.numeric(idSymbol))
}

getIdTradingFrequency <- function(tradingFrequency,conn=NULL)
{
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  idTradingFrequency = dbGetQuery(mydb,paste("SELECT id_tradingfrequency from tradingfrequency where name = '",tradingFrequency,"';",sep=""))
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
  
  return(as.numeric(idTradingFrequency))
}

getIdDataSource <- function(datasource,conn=NULL)
{
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  idDatasource = dbGetQuery(mydb,paste("SELECT id_datasource from datasource where name = '",datasource,"';",sep=""))
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
  
  return(as.numeric(idDatasource))
}

getIdTimeSeries <- function(idSymbol,idDataSource,idTradingFrequency,conn=NULL)
{
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  idTimeSeries= dbGetQuery(mydb,paste("SELECT id_timeseries from timeseries WHERE ",
                                      "id_symbol = ", idSymbol, " and id_datasource = ",idDataSource," and id_tradingfrequency = ",
                                      idTradingFrequency,";",sep=""))
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
  
  return(as.numeric(idTimeSeries))
}