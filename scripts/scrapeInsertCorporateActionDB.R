source("facade/DBFacade.R")
library(RCurl)
library(RMySQL) 
library(bizdays)
library(XML)

# Scrape dividend and split-merger information from the fidelity webpage
# Add info to DB
# Todo: replace DB functions with DBFacade functionality
scrapeInsertCorporateActionDB <- function(startDate=Sys.Date(),
                                          endDate=Sys.Date()+30,conn=NULL)
{
  # Open DB connection
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn

  symbols = dbGetQuery(mydb, 'select ticker from symbol')

  # Fetch calendar with holiday information
  businessCalendar <- Calendar(holidaysANBIMA, weekdays=c("saturday", "sunday"))
  # We only parse dividend and split/merge information up to two months in the future
  businessDays <- bizseq(startDate,endDate,businessCalendar)

  # Create base URL strings for the dividend and merge/split info
  baseURLDividends = "https://eresearch.fidelity.com/eresearch/conferenceCalls.jhtml?tab=dividends&begindate="
  baseURLMergeSplit = "https://eresearch.fidelity.com/eresearch/conferenceCalls.jhtml?tab=splits&begindate="

  # Create xpath expression that is used to scrape the relevant info from the pages
  baseXPATH = "//*[./td/a[contains(@href,'symbols=ticker') and text()='ticker']]/td[not(a)]/text()"

  # Last processed flag for mergers and splits: process each monthly page only one time
  msLastProcessedMonth = 0

  # Process every dividend date on the fidelity page separately
  # Proces the merge split information on the first of each month
  for(i in seq(1,length(businessDays)))
  {
    # Convert dates to POSIXlt for easier processing
    businessDay = as.POSIXlt(businessDays[[i]])
    year = businessDay[[6]]+1900
    month = businessDay[[5]]+1
    day = businessDay[[4]]
  
    # Generate the dividend URL string for this particular date
    dateString = paste(month,"/",day,"/",year,sep="")
    urlStringDividend = paste(baseURLDividends,dateString,sep="")
    # Parse the webpage
    dividendPage = htmlParse(getURL(urlStringDividend))
  
    # Every first business day of a month, process merger/split page
    if(msLastProcessedMonth!=month)
    {
      urlStringMergeSplit = paste(baseURLMergeSplit,dateString,sep="")
      mergeSplitPage = htmlParse(getURL(urlStringMergeSplit))
    }
  
    # For each symbol in our DB we check if there is dividend info present on the dividend page
    # We also check the monthly merge split page on the first business day of each month
    # If we find info for a symbol then we insert or update the relevant entry in the DB
    for(j in seq(1,nrow(symbols)))
    {
      symbolStr = symbols[j,1]
    
      # Obtain the correct xpath string for this symbol
      xpathString = gsub("ticker",symbolStr,baseXPATH)
      # Fetch the dividend information from the page
      dividendInfo = getNodeSet(dividendPage,xpathString)
    
      if(length(dividendInfo) != 0) # Dividend information for symbol was found
      {
        # Extract the dividend characteristics
        dividendProperties = sapply(dividendInfo,xmlValue)
      
        # Fetch the dividend value
        divValue = as.numeric(dividendProperties[[1]])
      
        # Process the relevant dates
        divAnnouncementDate = as.Date(dividendProperties[[2]], "%m/%d/%Y")
        divRecordDate = as.Date(dividendProperties[[3]], "%m/%d/%Y")
        divExDate = as.Date(dividendProperties[[4]], "%m/%d/%Y")
        divPayDate = as.Date(dividendProperties[[5]], "%m/%d/%Y")
        
        insertUpdateDividendInfoDB(symbolStr,divValue,divExDate,divAnnouncementDate,divRecordDate,divPayDate,mydb)
      }
    
      if(msLastProcessedMonth!=month) # Process the monthly merger/split page
      {
        mergeSplitInfo = getNodeSet(mergeSplitPage,xpathString)
        
        if(length(mergeSplitInfo) != 0) # Merge split information for symbol was found
        {
          # Extract the split/merge characteristics
          mergeSplitProperties = sapply(mergeSplitInfo,xmlValue)
      
          # Manipulate split ratio string into decimal value
          msSplitRatioList = strsplit(mergeSplitProperties[[1]],':')
          msNumerator = as.numeric(msSplitRatioList[[1]][1]) 
          msDenominator = as.numeric(msSplitRatioList[[1]][2]) 
          msSplitRatio = msNumerator/msDenominator
      
          # Process the relevant dates
          msAnnouncementDate = as.Date(mergeSplitProperties[[2]], "%m/%d/%Y")
          msRecordDate = as.Date(mergeSplitProperties[[3]], "%m/%d/%Y")
          
          if(length(mergeSplitProperties) == 4)
            msExDate = as.Date(mergeSplitProperties[[4]], "%m/%d/%Y")
          else
            msExDate = NULL
          
          insertUpdateSplitMergerInfoDB(symbolStr,msSplitRatio,msExDate,msAnnouncementDate,msRecordDate,mydb)
        }
      }
    }
    # Set flag: make sure that we don't reprocess the monthly merger/split page
    msLastProcessedMonth = month
  }
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
}

insertUpdateDividendInfoDB <- function(ticker,value,exDate,announcementDate,recordDate,payDate,conn=NULL)
{
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  # Fetch the symbol_id for this ticker symbol
  idSymbol = dbGetQuery(mydb,paste("select id_symbol from symbol where ticker = '",ticker,"'",sep=""))[1,1]
  
  # Fetch ex dividend dates and dividend values
  qResult = dbGetQuery(mydb,paste("select da_ex_dividend, value from corporate_action_dividend where id_symbol = ",idSymbol))
  exDividendDates = qResult$da_ex_dividend
  dividendValues = qResult$value
  
  # We first check if the ex-date was already inserted in the DB
  # If this is the case then we check for modified information and update the tuple if necessary
  if(length(exDividendDates) > 0 && !is.null(exDate))
  {
    # Check for existing DB-tuple for the current ex-dividend date
    if(is.element(as.character(exDate),exDividendDates))
    {
      # Fetch all information for the already inserted dividend date
      selectionStr = "select id_corporate_action_dividend, value, da_payment, da_announcement, da_record from corporate_action_dividend "
      conditionStr = paste("where id_symbol = ",idSymbol," and da_ex_dividend = '", exDate,"';",sep="")
      tuple = dbGetQuery(mydb,paste(selectionStr,conditionStr,sep=""))
      
      oldValue = tuple[1,2]
      old_da_payment = tuple[1,3]
      old_da_announcement = tuple[1,4]
      old_da_record = tuple[1,5]
      
      # Update the table row when there was a recent modification to the values
      if(!(oldValue==value && old_da_payment == payDate  # Update the row if modifications where made
           && old_da_announcement == announcementDate && old_da_record == recordDate)) 
      {
        updateStr = "update corporate_action_dividend "
        modificationStr = paste("set value = ",value,", da_payment = '",payDate,
                                "', da_announcement = '", announcementDate,"', da_record = '",recordDate,"' ",sep="")
        conditionStr = paste("WHERE id_corporate_action_dividend = ",tuple[1,1],";",sep="")
        updateQuery = paste(updateStr,modificationStr,conditionStr,sep="")
        # Perform the update
        print(updateQuery); dbGetQuery(mydb,updateQuery)
      }
      
      # Close the connection if it was created locally
      if(is.null(conn))
        dbDisconnect(mydb)
      
      return(); # return, avoid inserting new tuple
    }
  }
  
  # We account for the possbility of reanouncement and movement of ex dividend dates
  # We check if the dividend value is already in DB for another future ex dividend date
  futureValues = dividendValues[exDividendDates>Sys.Date()]
  futureValues = futureValues[!is.na(futureValues)] # Ignore potential NA div dates
  matchingValues = which(futureValues == value)
  if(length(matchingValues != 0)) # Matching values found
  {
    # Block timeseries - Manual checking / reactivation is required
    reasonBlockedStr = paste("Possible duplicate entry for dividend value ",value,sep="")
    blockActivateTimeseriesDB(ticker,block=TRUE,reason=reasonBlockedStr,conn=mydb)
  }
  
  # Account for posibility of NULL exDate
  if(is.null(exDate)) 
  {
    if(is.element(value,dividendValues[is.na(exDividendDates)]))
      return(); # dividendValue with null ex date already inserted.. return
    
    # Block timeseries - Manual checking / reactivation is required
    reasonBlockedStr = "NULL ex dividend date inserted"
    blockActivateTimeseriesDB(ticker,block=TRUE,reason=reasonBlockedStr,conn=mydb)
    exDateStr = "NULL" 
  }  
  else 
    exDateStr = paste("'",exDate,"'",sep="")
  
  insertString = "insert into corporate_action_dividend(id_symbol,value,da_ex_dividend,da_payment,da_announcement,da_record) "
  valueString = paste("values(",idSymbol,",",value,",",exDateStr,",'",payDate,"','",announcementDate,"','",recordDate,"');",sep="")
  insertionQuery = paste(insertString,valueString,sep="")
  
  # Perform the insertion
  print(insertionQuery); dbGetQuery(mydb,insertionQuery)
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
}

insertUpdateSplitMergerInfoDB <- function(ticker,splitRatio,exDate,announcementDate,recordDate,conn=NULL)
{
  if(is.null(conn))
    mydb=connectionSecuritiesMaster()
  else
    mydb=conn
  
  # Fetch the symbol_id for this ticker symbol
  idSymbol = dbGetQuery(mydb,paste("select id_symbol from symbol where ticker = '",ticker,"'",sep=""))[1,1]
  
  # Fetch ex_merge_split dates and splitratios
  qResult = dbGetQuery(mydb,paste("select da_ex_merge_split, merge_split_ratio from corporate_action_merger_split where id_symbol = ",idSymbol))
  exMergeSplitDates = qResult$da_ex_merge_split
  mergeSplitRatios = qResult$merge_split_ratio
  
  # We first check if the ex-date was already inserted in the DB
  # If this is the case then we check for modified information and update the tuple if necessary
  if(length(exMergeSplitDates) > 0 && !is.null(exDate))
  {
    # Check for existing DB-tuple for the current ex-dividend date
    if(is.element(as.character(exDate),exMergeSplitDates))
    {
      # Fetch all information for the already inserted dividend date
      selectionStr = "select id_corporate_action_merger_split, merge_split_ratio, da_announcement, da_record from corporate_action_merger_split "
      conditionStr = paste("where id_symbol = ",idSymbol," and da_ex_merge_split = '", exDate,"';",sep="")
      tuple = dbGetQuery(mydb,paste(selectionStr,conditionStr,sep=""))
      
      oldRatio = tuple[1,2]
      old_da_announcement = tuple[1,3]
      old_da_record = tuple[1,4]
      
      # Update the table row when there was a recent modification to the values
      if(!(oldRatio==splitRatio && old_da_announcement == announcementDate && old_da_record == recordDate)) 
      {
        updateStr = "update corporate_action_merger_split "
        modificationStr = paste("set merge_split_ratio = ",splitRatio,"', da_announcement = '", announcementDate,
                                "', da_record = '",recordDate,"' ",sep="")
        conditionStr = paste("WHERE id_corporate_action_merger_split = ",tuple[1,1],";",sep="")
        updateQuery = paste(updateStr,modificationStr,conditionStr,sep="")
        # Perform the update
        print(updateQuery); dbGetQuery(mydb,updateQuery)
      }
      
      # Close the connection if it was created locally
      if(is.null(conn))
        dbDisconnect(mydb)
      return; # return, avoid inserting new tuple
    }
  }
  
  # We account for the possbility of reanouncement and movement of ex dates
  # We check if the splitratio value is already in DB for another future ex date
  futureValues = mergeSplitRatios[exMergeSplitDates>Sys.Date()]
  futureValues = futureValues[!is.na(futureValues)] # Ignore potential NA div dates
  matchingValues = which(futureValues == splitRatio)
  if(length(matchingValues != 0)) # Matching values found
  {
    # Block timeseries - Manual checking / reactivation is required
    reasonBlockedStr = paste("Possible duplicate entry for splitratio ",splitRatio,sep="")
    blockActivateTimeseriesDB(ticker,block=TRUE,reason=reasonBlockedStr,conn=mydb)
  }
  
  # Account for posibility of NULL exDate
  if(is.null(exDate)) 
  {
    if(is.element(splitRatio,mergeSplitRatios[is.na(exMergeSplitDates)]))
      return(); # splitRatio with null ex date already inserted.. return
    
    # Block timeseries - Manual checking / reactivation is required
    reasonBlockedStr = "NULL ex_merge_split date inserted"
    blockActivateTimeseriesDB(ticker,block=TRUE,reason=reasonBlockedStr,conn=mydb)
    exDateStr = "NULL" 
  }  
  else 
    exDateStr = paste("'",exDate,"'",sep="")
  
  insertString = "insert into corporate_action_merger_split(id_symbol,merge_split_ratio,da_ex_merge_split,da_announcement,da_record) "
  valueString = paste("values(",idSymbol,",",splitRatio,",",exDateStr,",'",announcementDate,"','",recordDate,"');",sep="")
  insertionQuery = paste(insertString,valueString,sep="")
  
  # Perform the insertion
  print(insertionQuery); dbGetQuery(mydb,insertionQuery)
  
  # Close the connection if it was created locally
  if(is.null(conn))
    dbDisconnect(mydb)
}