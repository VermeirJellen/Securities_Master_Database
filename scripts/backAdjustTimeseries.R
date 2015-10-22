library(xts)

# Perform back adjustment algorithm for active timeseries in DB
# Back adjustment must be performed when there is an ex date the following day
# Functionality Should be sheduled after market hours (on the day before the ex date)
#
# Inputs / outputs: none
backAdjustTimeSeries <- function(seriesData,dividendInfo=NULL,
                                 mergeSplitInfo=NULL,useAdjClose=FALSE,adjustVolume=FALSE)
{
  priceCols <- c("price","open","high","low","close"); 
  adjPriceCols <- c("adjprice","adjopen","adjhigh","adjlow","adjclose")
  volumeCols <- c("volume"); adjVolumeCols <- c("adjvolume")
  
  # We can only adjust columns that are present in the series
  seriesCols <- names(seriesData)
  availablePriceCols <- which(priceCols %in% seriesCols)
  avaiableVolumeCols <- which(volumeCols %in% seriesCols)
  
  # Use adjclose to Close ratio to adjust other price columns
  if(useAdjClose & ("close" %in% seriesCols) & ("adjclose" %in% seriesCols))
  {
    # Calculate the adjustment ratios
    adjustmentRatios <- seriesData$adjclose/seriesData$close
    
    # Adjust the prices with the adjustment ratios
    adjCols <- xts(order.by=index(seriesData),
                   apply(seriesData[,priceCols[availablePriceCols]],2,function(x) x*coredata(adjustmentRatios)))
    
    # Bind the calculated adjusted prices columns together with the original series
    excludeAdjPriceCols <- which(!(seriesCols %in% adjPriceCols))
    adjSeriesData <- cbind(seriesData[,excludeAdjPriceCols],adjCols)
    
    # Set column names of adjusted series data
    names(adjSeriesData) <- c(seriesCols[excludeAdjPriceCols],adjPriceCols[availablePriceCols])
  }
  else
  {
    # Do we adjust for dividends?
    adjustForDividends <- ifelse(is.null(dividendInfo),FALSE,TRUE)
    # Do we adjust for splits?
    adjustForSplits <- ifelse(is.null(mergeSplitInfo),FALSE,TRUE)
    
    if(adjustForDividends & adjustForSplits)
    {
      adjustmentRatiosDividend <- getDividendAdjustmentRatios(seriesData,dividendInfo)
      adjustmentRatiosSplits <- getSplitAdjustmentRatios(seriesData,mergeSplitInfo)
      adjustmentRatiosCombined <- adjustmentRatiosDividend*adjustmentRatiosSplits
      
      # Calculate adjusted prices with combined adjustmentRatios
      adjSeriesData <- calculateAdjustedPrices(seriesData,adjustmentRatiosCombined)
      
      if(adjustVolume) # Calculate adjusted volume with splitratios only
        adjSeriesData <- calculateAdjustedVolume(adjSeriesData,adjustmentRatiosSplits)
    }
    else if(adjustForDividends) # Adjust prices for dividends. Not required for volume
    {
      adjustmentRatiosDividend <- getDividendAdjustmentRatios(seriesData,dividendInfo)
      adjSeriesData <- calculateAdjustedPrices(seriesData,adjustmentRatiosDividend)
    }
    else if(adjustForSplits) # Adjust both volume and prices for splits
    {
      adjustmentRatiosSplits = getSplitAdjustmentRatios(seriesData,mergeSplitInfo)
      adjSeriesData <- calculateAdjustedPrices(seriesData,adjustmentRatiosSplits)
      if(adjustVolume) # Caclulate adjusted Volume
        adjSeriesData <- calculateAdjustedVolume(adjSeriesData,adjustmentRatiosSplits)
    }
    else
    {
      print("Backadjustment warning: Adjustment was not executed")
      # Adjusted data is equal to original data
      adjSeriesData <- seriesData
    }
  }
  return(adjSeriesData)
}

getDividendAdjustmentRatios <- function(seriesData,dividends)
{
  # Change column name corresponding to DB field name
  names(dividends) <- c("value")
  divClose <- merge(dividends,seriesData[,"close"])
  # Lag corporate action date one day backward (before the ex date)
  divClose$value <- lag.xts(divClose$value,-1)
  # Calculate temporal dividend adjustment ratio (for the closing price)
  temporalRatioDiv <- 1 - (divClose$value/divClose$close)
  # Set ratio for the lastest date to 1
  temporalRatioDiv[nrow(temporalRatioDiv),1] <- 1
  
  divAdjRatio <- temporalRatioDiv
  # Perform reverse cumulative product on non-na temporal adjustment ratios
  divAdjRatio[!is.na(temporalRatioDiv),1] <- rev(cumprod(rev(coredata(temporalRatioDiv[!is.na(temporalRatioDiv),]))))
  # next observation carried backward
  divAdjRatio <- na.locf(divAdjRatio,fromLast=TRUE)
  
  return(divAdjRatio)
}

getSplitAdjustmentRatios <- function(seriesData,splits)
{
  # Change column name, corresponds to DB field name
  names(splits) <- c("merge_split_ratio")
  splitsDates <- merge(splits,index(seriesData))
  # Lag corporate action date one day backward (before the ex date)
  splitsDates$merge_split_ratio <- lag.xts(splitsDates$merge_split_ratio,-1)
  
  # Calculate temporal split adjustment ratio
  temporalRatioSplits <- 1/splitsDates$merge_split_ratio
  # Set ratio for the lastest date to 1
  temporalRatioSplits[nrow(temporalRatioSplits),1] <- 1
  
  splitAdjRatio <- temporalRatioSplits
  splitAdjRatio[!is.na(temporalRatioSplits),1] <- rev(cumprod(rev(coredata(temporalRatioSplits[!is.na(temporalRatioSplits),]))))
  splitAdjRatio <- na.locf(splitAdjRatio,fromLast=TRUE)
  
  return(splitAdjRatio)
}

calculateAdjustedPrices <- function(seriesData,adjustmentRatios)
{
  # Name of original data fields
  priceCols <- c("price","open","high","low","close"); 
  # Names of corresponding adjusted data fields
  adjPriceCols <- c("adjprice","adjopen","adjhigh","adjlow","adjclose")
  
  # We can only adjust columns that are present in the series
  seriesCols <- names(seriesData)
  
  # Todo: can be simplified by using a single multiplication
  if("close" %in% seriesCols)
  {
    rawClose <- seriesData[,"close"]
    # Calculate adjusted close
    adjustedClose <- rawClose * adjustmentRatios
    availablePriceCols <- priceCols[which(priceCols %in% seriesCols)]
  
    # calculate non-close adjusted prices by the same ratio
    adjPriceSeries <- xts(order.by=index(seriesData),
                     apply(seriesData[,availablePriceCols],2,function(x) x-coredata(rawClose)))
    adjPriceSeries[,] <- apply(adjPriceSeries,2,function(x) x*coredata(adjustmentRatios))
    adjPriceSeries[,] <- apply(adjPriceSeries,2,function(x) x+coredata(adjustedClose))
    
    # Add adjusted prices to series, potentially removing old adjusted prices columns
    adjSeries <- cbind(seriesData[,!(seriesCols %in% adjPriceCols)],adjPriceSeries)
    # Set column names of the series
    names(adjSeries) <- c(seriesCols[!(seriesCols %in% adjPriceCols)],
                          adjPriceCols[which(priceCols %in% seriesCols)])
  }
  else
  {
    return(seriesData)
  }
  
  return(adjSeries)
}

calculateAdjustedVolume <- function(seriesData,adjustmentRatios)
{
  # We can only adjust columns that are present in the series
  seriesCols <- names(seriesData)
  availableVolumeCols <- which(volumeCols %in% seriesCols)
  
  if("volume" %in% seriesCols)
  {
    # calculate adjusted volume
    adjVolumeSeries <- seriesData$volume / adjustmentRatios
    # Add adjusted volume to series, potentially removing old adjusted volume column.
    adjSeries <- cbind(seriesData[,!(seriesCols %in% "adjvolume")],adjVolumeSeries)
    # Set column names of the series
    names(adjSeries) <- c(seriesCols[!(seriesCols %in% "adjvolume")],"adjvolume")
  }
  else # volume can not be adjusted
  {
    return(seriesData)
  }
  
  return(adjSeries)
}