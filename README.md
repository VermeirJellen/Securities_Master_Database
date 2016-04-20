##SECURITIES MASTER DATABASE

This project includes scripts that can be used to set up a basic securities master database. The database structure is fairly generic in the sense that it can contain data for any assetclass type and/or datafrequency: The table layout can be viewed in `./Securities_Master.jpg`. Additionally, the project includes scripts for the downloading, parsing and inserting of timeseries data for the S&P500 stocks, SPDR ETF funds, the BEL20 stocks and its corresponding ETF tracker. Simple scripts for data cleaning, outlier detection and corporate action backadjustment are provided as well. The user can follow the instructions below to set up the database and insert the data.

###Database Setup Procedure
1.  Connect to your preferred MySQL DB server and run the `./sql/createSecuritiesMaster.sql` script to create the "securities_master" database. The script creates the relevant tables and inserts some basic information regarding symbol metadata (sector codes, asset types, exchange data, locales and timezones). A few additional triggers and procedures are added as well during the process.
- Add or update `./config/credentials.cnf`. View `./config/readme.txt` for additional instructions.
- Set the python working directory to the current "base" directory and execute the `./InsertMetaDataSnP500BEL20.py` script. Note that the base directory is "this" directory where the `.py` script is located. The script will scrape the SnP500 stock symbol and GICS sector information from wikipedia and insert it in the Database. Additionally, BEL20 symbol info will also be parsed and inserted.
- Execute the `./sql/addSectorInfoBEL20.sql` file to manually add the ICB sector information for the BEL20 stocks (this information was not parsed and inserted via the above `.py` script)
- Execute the `./sql/addIndices.sql` file to manually add SPDR ETF and Lyxor BEL20 ETF symbol and asset class information to the DB.

### Downloading and Inserting Timeseries Data
First, create a free Quandl user account: <https://www.quandl.com/>. Add your Quandl API authentication key code to `./quandl/authentication/quandl.cnf`. Additional instructions are provided in `./quandl/authentication/readme.txt`.
Next, open the `./DBSetup.R` script. If you are missing packages or are running an out of date version of R then you should uncomment and run lines 7-10 of the script to perform the necessary updates and installations. When ready, set the R working directory to the current "base" directory and execute lines 15-26 of the script. Following functionality is performed:

1. Lines 15-18 will source the necessary R scripts that are located in the `./facade` and `./scripts` subfolders. 
- Line 20 creates a DB connection to your database
- Line 22 launches a simple script that will add relevant exchange key metadata to the symbols.
- Line 23 launches a script that scrapes dividend and merge-split data from the fidelity corporate action   calendar and inserts the information in the Database. The script will parse and insert data for the next 30 days, starting at the current date. Previously inserted data will be modified or overwritten if changes are detected.
- Line 24 launches a script that downloads and processes the actual timeseries data for the symbols:
     - Perform simple interpolation for missing datapoints, taking the ANBIMA holiday calendar into account.
     - If adjusted data is not provided, perform a backadjustment algorithm to calculate the adjusted prices (open, high, low close) and adjusted volume. For the BEL20 Yahoo timeseries data, the adjusted prices are calculated by simply taking the given adjusted close prices into account. All the other timeseries already contain precomputed dividend and merge-split adjusted data, hence no additional action is required. (Note that the `./scripts/backadjustTimeseries.R` script contains the necessary functionality to perform the CRSP backadjustment algorithm. The script can be fed with corporate action data from Yahoo or the corporate action information in DB. View lines 116-124 in `./scripts/ProcessEODDataQuandl.R` for an example on how to call the script).
     - Perform outlier detection for the adjusted price data, based upon a rolling median absolute deviation criterion. Currently, datapoints are flagged as outliers or extreme outliers when the 4 and 8 MAD levels are breached in a 30 day rolling window. The relevant datapoints are NOT modified.
     - Transaction costs, shorting constraints and trade size data information is not inserted in the DB.
- Line 26 closes the Database connection.


### Continuous Updates and Live Trading
- Schedule `./scripts/ProcessEODDataQuandl.R` to run each day after market close. This script adds new daily datapoints to the active timeseries and will potentially update or recalculate certain datapoints.
- When using the data for live trading, schedule the `./scripts/backadjustAntipicationDateDB.R` script to run each day after market close. By default, this script performs the necessary backadjustment for the active timeseries that have their corporate action ex dates the following day. Note that this anticipation step is unnecessary when the data is not used for trading signal generation the next trading day. (`./scripts/ProcessEODDataQuandl` will reupdate the datapoints during the backadjustment process whenever you choose to rerun it).
- Run `./scripts/scrapeInsertCorporateActionDB.R` on a periodic basis to keep the future corporate action data up to date at all times. For example, it can be run one time each week while using the default forward looking window of 30 days. **Important:** The fidelity corporate action calendar does not contain corporate action data for BEL20 stocks, SPY, and the ETF funds: Future corporate action data will need to be added manually in order to anticipate backadjustments.
- Timeseries are inserted into the database as "non-active", by default. If you wish to modify this then you can add the isActive=TRUE flag as a parameter to the "insertUpdateTimeSeriesDB" call in `./scripts/ProcessEODDataQuandl.R`, at line 100-101.

### Fetching data from Database
`./facade/DBFacade.R` provides functions that can be used to fetch or update the database information in a generic way: View `./facade/DBFacadeExample.R` for simple examples. For more advanced queries, SQL statements can be executed via the respective RMySQL functionality, or other similar packages.

### Licensing
Copyright 2016 Jellen Vermeir. <jellenvermeir@gmail.com>

Securities Master Database is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. Securities Master Database is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with Securities Master Database. If not, see <http://www.gnu.org/licenses/>.