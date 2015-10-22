SECURITIES MASTER DATABASE

This project includes the scripts that are necessary to set up a basic securities master database. The database structure is fairly generic in the sense that it can provide data for any assetclass type and/or datafrequency: The table layout can be viewed in "Securities_Master.jpg". 
Additionally, the project includes scripts for the downloading, parsing and inserting of timeseries data for the S&P500 stocks, SPDR ETF funds, the BEL20 stocks and its corresponding ETF tracker. Simple scripts for data cleaning, outlier detection and corporate action backadjustment are provided as well. The user can follow the instructions below to set up the database.

1 - Connect to your preferred MySQL DB server and run the ./sql/createSecuritiesMaster.sql script to create the "securities_master" database. The script creates the relevant tables and inserts some basic information regarding symbol metadata (sector codes, asset types, exchange data, locales and timezones). A few additional triggers and procedures are added as well during the process.
2 - Add or update the credentials.cnf file in the ./config directory. View additional info in the ./config folder readme file for further instructions.
3 - Set the python working directory to the current "base" directory and execute the InsertMetaDataSnP500BEL20.py script. Note that the base directory is "this" directory where the .py script is located. The script will scrape the SnP500 stock symbol and GICS sector information from wikipedia and insert it in the DB. Additionally, BEL20 symbol info will also be parsed and inserted.
4 - Execute the addSectorInfoBEL20.sql file to manually add the ICB sector information for the BEL20 stocks (this information was not parsed and inserted via the above .py script)
5 - Execute the addIndices.sql file to manually add SPDR ETF and Lyxor BEL20 ETF symbol and asset class information to the DB.

5) - Create a Quandl user account: https://www.quandl.com/. Add your Quandl API authentication key code to ./quandl/authentication/quandl.cnf. Additional instructions are provided in the ./quandl/authentication readme file.
6) - Open DBSetup.R. If you are missing packages or are running an out of date version of R then you should run lines 7-10 to perform the necessary updates / installations. When ready, set the R working directory to the current "base" directory and execute lines 14-25 of the script.
6.1) - Lines 14-17 will source the necessary R scripts that are located in the ./facade and ./scripts subfolders. 
6.2) - Line 19 creates a DB connection to your database
6.3) - Line 21 executes a simple script that will add relevant exchange key metadata to the symbols.
6.4) - Line 22 executes a script that scrapes dividend and merge-split data from the fidelity corporate action calendar and inserts the information in the DB. The script will parse and insert data for the next 30 days, starting at the current date. Previously inserted data will be modified or overwritten if changes are detected.
6.5) - Line 23 executes the script that downloads and processes the actual timeseries data for the symbols.
6.5.1) - Perform simple interpolation for missing datapoints, taking the ANBIMA holiday calendar into account.
6.5.2) - If adjusted data is not provided, perform a backadjustment algorithm to calculate the adjusted prices (open, high, low close) and adjusted volume. For the BEL20 Yahoo timeseries data, the adjusted prices are calculated by simply taking the given adjusted close prices into account. All the other timeseries already contain precomputed dividend and merge-split adjusted data, hence no additional action is required. (Note that the backadjustTimeseries.R script contains the necessary functionality to perform the CRSP backadjustment algorithm. The script can be fed with corporate action data from Yahoo or the corporate action information in DB. View lines 116-124 in ProcessEODDataQuandl.R for an example on how to call the script).
6.5.3) - Perform outlier detection for the adjusted price data, based upon a rolling median absolute deviation criterion. Currently, datapoints are flagged as outliers or extreme outliers when the 4 and 8 MAD levels are breached in a 30 day rolling window. The relevant datapoints are NOT modified.
6.5.4) - Transaction costs, shorting constraints and trade size data information is not inserted in the DB.
6.6) - Line 26 closes the DB connection.

7.1 - Shedule ProcessEODDataQuandl.R to run each day after market close. This will add the new daily datapoints to the active timeseries (and potentially update or recalculate certain datapoints).
7.2 - Run scrapeInsertCorporateActionDB.R on a periodic basis to keep the future corporate action data up to date at all times. For example, it can be run one time each week while using the default forward looking window of 30 days.
7.3.1 - When using the data for live trading, shedule the backadjustAntipicationDateDB.R script to run each day after market close. By default, this script will perform the necessary backadjustment for the active timeseries that have their corporate action ex dates the following day. Note that this anticipation step is unnecessary when the data is not used for trading signal generation the next day (because step 7.1 will reupdate the datapoints during the backadjustment process the next evening, or whenever you choose to rerun it).
7.3.2 - Timeseries are inserted as "non-active". If you wish to modify this then you can add the isActive=TRUE flag as a parameter to the "insertUpdateTimeSeriesDB" call in ProcessEODDataQuandl.R, at line 100-101.

8 - The DBFacade provides functions that can be used to fetch or update DB information in a generic way: View ./facade/DBFacadeExample.R for simple examples. For more advanced queries, SQL statements can be executed via the respective RMySQL functionality.

IMPORTANT NOTICE REGARDING STEP 7.3: The fidelity corporate action calendar does not contain corporate action data for BEL20 stocks, SPY, and the ETF funds. Updates for this issue will be provided in the near future.
(FYI, the next ex dividend date for the SPDR ETF's is the third friday of december 2015).

Project code is copyrighted by Jellen Vermeir and available for distribution under the FreeBSD license conditions. View license.txt. Feel free to contact me about this project via jellenvermeir@gmail.com