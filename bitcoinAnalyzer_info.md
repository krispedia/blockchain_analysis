## bitcoinAnalyzer info

### Modules
- (default) datetime : filter date
- (custom) mongoDB : mongoDB connector

### class

- bitcoinAnalyzer  
    bitcoin analyz tools

### class info
- bitcoinAnalyzer  
    * def setDB(blockdbCollectionName, tdbCollectionName, addrdbCollectionName, abusedbCollectionName)  
        --- role ---  
        set DB connection  
        -----~-----
        
        block DB/ transaction DB/ address DB/ abuse address DB  

    * def getAbuseAddress()  
        --- role ---  
        get address data from mongoDB which collected by `bitcoinAbuseCrawler`  
        -----~-----

    * def getBlockByTime(targetDate=None, termDays=None)  
        --- role ---  
        get block index from mongoDB with time filter  
        -----~-----

        from date = targetDate - termDays  
        to date = targetDate - termDays  

    * def getInOutAddress(targetDate=None, termDays=None)  
        --- role ---  
        get transaction in/out address data with time filter 
        -----~-----

    * def getAbuseInOutAddress(abuseAddress, inOrOuts)  
        --- role ---  
        get abuse in/out address  
        -----~-----

    * def filterAbuseAddress(targetDate, termDays)  
        --- role ---  
        get abuse address in transaction data with time filter  
        -----~-----
        
        1.getAbuseAddress()  
        2.getInOutAddress(targetDate, termDays)  
        3.getAbuseInOutAddress(abuseAddress, inOrOuts)  

    * def getAbuseTxInfo(abuseAddress, inOrOut)  
        --- role ---  
        get transaction data from mongoDB   
        -----~-----  

    * def getAddressTx(addreess)  
        --- role ---  
        get transaction data of addresses  
        -----~-----  

        sorted by time(old)  

    * def getNextTransaction(address, time, value)  
        --- role ---  
        get next transaction by address, time, value  
        -----~-----

        1.get transaction of address by `getAddressTX()`  
        2.compare time(near prev time & after prev time)  
        3.compare value(same as prev value)  

    * def generateNextChainByTxid(txid, curLevel, lastLevel, res)  
        --- role ---  
        make next chain of txid which implemented by recursion  
        -----~-----  

    * def generateNextTxChain(abuseInputsTxInfo, DEPTH)  
        --- role ---  
        make multiple next chain of txid  
        -----~-----

    * def getPrevTransaction(address, time value)  
        --- role ---  
        get prev transaction by address, time, value    
        -----~-----  

        1.get transaction of address by `getAddressTX()`  
        2.compare time(near next time & before next time)  
        3.compare value(same as next value)  

    * def generatePrevChainByTxid(txid, curLevel, lastLevel, res)  
        --- role ---  
        make prev chain of txid which implemented by recursion  
        -----~-----   

    * def generatePrevTxChain(abuseOutputsTxInfo, DEPTH)  
        --- role ---  
        make multiple prev chain of txid  
        -----~-----  