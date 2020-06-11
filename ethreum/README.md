## ethereumAnalyzer info

### Modules
- (default) requests  
- (default) BeautifulSoup  
- (default) datetime  
- (default) time  
- (default) webdriver

### Class
- ethereumAnalyzer  

### Class info
- ethereumAnalyzer  
    * def setDriverPath(driverPath)  
        > --- role ---  
        > webdriver settings for selenium  
        > -----~-----

    * def formTime(timeString)  
        > --- role ---  
        > convert time string to datetime form  
        > -----~-----  

    * def generateTimeTermByDay(targetTime, termDays)  
        > --- role ---  
        > generate date for investigate  
        > -----~-----  

    * def TxInfoByTxid(txid)  
        > --- role ---  
        > get transaction information in etherscan.io  
        > -----~-----  

    * def getNormalTransactionByAddress(addr, targetTime, termDays)  
        > --- role ---  
        > get transaction information by address with time filter  
        > -----~-----  

    * def getBlockTimeByParentTxid(parentTxid)  
        > --- role ---  
        > get block time by Parent Txid(only apply at internal transaction)  
        > -----~-----

    * def getInternalTransactionByAddress(addr, targetTime, termDays)  
        > --- role ---  
        > get internal transaction by address with time filter  
        > -----~-----  

    * def getAddressInfo(address)  
        > --- role ---  
        > get address information  
        > -----~-----   

    * def getContractOwnerAddressByContractAddress(contractAddr, APIKEY)  
        > --- role ---  
        > get owner of contract address in ethplorer.io website
        > -----~-----  
        