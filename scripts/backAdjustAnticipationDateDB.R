source("scripts/insertUpdateTimeSeriesDB.R")
source("facade/DBFacade.R")
library(Quandl)
library(bizdays)

# Perform back adjustment algorithm for active timeseries in DB
# Back adjustment must be performed when there is an ex date the following day
# Functionality Should be sheduled after market hours (on the day before the ex date)
#
# Inputs / outputs: none
backAdjustAnticipationDateDB <- function(anticipationDate=Sys.Date()+1)
{
  mydb <- connectionSecuritiesMaster()
  
  backAdjustDividendAnticipationDateDB(anticipationDate,conn=mydb)
  backAdjustMergerSplitAnticipationDateDB(anticipationDate,conn=mydb)
  
  dbDisconnect(mydb)
}

# Perform backadjustment for dividends - Active timeseries for which
# the dividend ex-date corresponds to the anticipationdate will be
# backadjusted
#
backAdjustDividendAnticipationDateDB <- function(anticipationDate=Sys.Date()+1, conn=NULL)
{
  # Open DB connection
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  # Fetch dividend information for companies that have their div ex date on the ancitipationDate
  querySelectStr <- "SELECT id_symbol, value from corporate_action_dividend"
  queryConditionStr <- paste("WHERE da_ex_dividend = '",anticipationDate,"';")
  divInfo <- dbGetQuery(mydb,paste(querySelectStr,queryConditionStr))
  
  nrDiv <- nrow(divInfo)
  if(nrDiv > 0)
  {
    for(i in seq(1,nrDiv))
    {
      idSymbol <- divInfo[i,1]
      
      # Fetch active timeseries to be adjusted
      seriesInfo <- getActiveTimeseriesInfo(idSymbol, conn=mydb)
      
      # There might be more than one active timseries for a given symbol..
      nrActiveSeries <- nrow(seriesInfo)
      if(nrActiveSeries > 0)
      {
        for(j in seq(1,nrActiveSeries))
        {
          idTimeSeries = as.numeric(seriesInfo[j,1])
          cNames = c("timestamp","price","close","adjopen","adjhigh","adjlow","adjclose")
          
          # Fetch the timeseries' datapoints
          datapoints <- queryDB(select=cNames,tableName="datapoint",
                                where=data.frame(id_timeseries=idTimeSeries),con=mydb,printQuery=TRUE)
          # Remove columns for which there is no data
          datapoints <- Filter(function(x)!all(is.na(x)), datapoints)

          # Convert to timeseries object
          datapointsXTS <- as.xts(datapoints[,-1],order.by=as.POSIXlt(datapoints$timestamp))
          names(datapointsXTS) <- cNames[-1]
          
          # Obtain tomorrows dividend payout and the last closing price of the active timeseries
          divValue <- divInfo[i,2]; lastClose <- as.numeric(datapointsXTS$close[nrow(datapointsXTS)])
          # calculate adjustment ratio
          adjustmentRatio <- 1 - (divValue/lastClose);
          
          # Adjust price columns
          adjPriceCols <- c("price","adjopen","adjhigh","adjlow","adjclose")
          datapointsXTS[,adjPriceCols] <- datapointsXTS[,adjPriceCols]*adjustmentRatio
          
          # Cleaning optional but not necessary
          # datapointsXTS <- cleanData(datapointsXTS)
          
          # update the timeseries data in DB
          idDataSource <- as.numeric(seriesInfo[j,2])
          idTradingFrequency <- as.numeric(seriesInfo[j,3])
          insertUpdateTimeSeriesDB(idSymbol,idDataSource,idTradingFrequency,datapointsXTS,conn=mydb)
        }
      }
    }
  }
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
}

backAdjustMergerSplitAnticipationDateDB <- function(anticipationDate=Sys.Date()+1, conn=NULL)
{
  # Open DB connection
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  # Fetch dividend information for companies that have an ex date on the ancitipationDate
  querySelectStr <- "SELECT id_symbol, merge_split_ratio from corporate_action_merger_split"
  queryConditionStr <- paste("WHERE da_ex_merge_split = '",as.POSIXct(anticipationDate),"';")
  splitInfo <- dbGetQuery(mydb,paste(querySelectStr,queryConditionStr))
  
  nrSplit <- nrow(splitInfo)
  if(nrSplit > 0)
  {
    for(i in seq(1,nrSplit))
    {
      idSymbol <- splitInfo[i,1]
      
      # Fetch active timeseries to be adjusted
      seriesInfo <- getActiveTimeseriesInfo(idSymbol, conn=mydb)
      
      # There might be more than one active timseries for each symbol..
      nrSeriesInfo <- nrow(seriesInfo)
      if(nrSeriesInfo > 0)
      {
        for(j in seq(1,nrow(seriesInfo)))
        {
          idTimeSeries = as.numeric(seriesInfo[j,1])
          cNames = c("timestamp","price","close","adjopen","adjhigh",
                                                "adjlow","adjclose","adjvolume")
          # Fetch the timeseries' datapoints
          datapoints <- queryDB(select=cNames,tableName="datapoint",
                                where=data.frame(id_timeseries=idTimeSeries),conn=mydb,printQuery=TRUE)
          # Remove columns for which there is no data
          datapoints <- Filter(function(x)!all(is.na(x)), datapoints)
          
          datapointsXTS <- as.xts(datapoints[,-1],order.by=as.POSIXlt(datapoints$timestamp))
          names(datapointsXTS) <- cNames[-1]
          
          # Obtain tomorrows dividend payout and the last closing price of the active timeseries
          splitRatio <- splitInfo[i,2];
          
          # Adjust price columns
          adjPriceCols <- c("price","adjopen","adjhigh","adjlow","adjclose")
          datapointsXTS[,adjPriceCols] <- datapointsXTS[,adjPriceCols]/splitRatio
          
          # adjust volume columns
          adjVolumeCol <- c("adjvolume")
          datapointsXTS[,adjVolumeCol] <- datapointsXTS[,adjVolumeCol]*splitRatio
          
          # Cleaning is not necessary
          # datapointsXTS <- cleanData(datapointsXTS)
          
          # update the timeseries data in DB
          idDataSource <- as.numeric(seriesInfo[j,2])
          idTradingFrequency <- as.numeric(seriesInfo[j,3])
          insertUpdateTimeSeriesDB(idSymbol,idDataSource,idTradingFrequency,datapointsXTS,conn=mydb)
        }
      }
    }
  }
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
}

# This function returns a dataframe that contains information about the
# timeseries table rows in DB that are currently active (isactive = TRUE)
#
# Inputs: 
# 1 -'idSymbol': The id_symbol key of the requested time series
# 1 -'idSymbol': Scalar
#
# Outputs: 
# A dataframe with rows of the following structure:
# (id_timeseries, id_datasource, id_tradingfrequency) 
# (scalar,        scalar          scalar)
getActiveTimeseriesInfo <- function(idSymbol,conn=NULL)
{
  # Open DB connection
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn

  querySelectStr <- "SELECT id_timeseries, id_datasource, id_tradingfrequency from timeseries"
  queryConditionStr <- paste("WHERE id_symbol =",idSymbol,"and isactive = 1;")
  seriesInfo <- dbGetQuery(mydb,paste(querySelectStr,queryConditionStr))
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
  
  # Return the timeseries datapoints
  return(seriesInfo)
}