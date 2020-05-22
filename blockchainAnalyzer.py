#import mongoDB
import mongoDB14 as mongoDB

import datetime

class bitcoinAnalyzer:

    def setDB(self):
        """
        DB 연결
        """
        self.tdbName = 'ws_datas'
        tcollectionName = 'wallet_transaction'
        tkeyField = 'txid'

        self.adbName = 'ws_datas'
        acollectionName = 'wallet_address'
        akeyField = 'address'

        self.addrdbName = 'ws_datas'
        addrcollectionName = 'bitcoinAbuse_address'
        addrkeyField = 'address'

        # transactionDB connect
        self.tdb = mongoDB.MongoConnector(self.tdbName, tcollectionName)
        self.tdb.connect()
        self.tdb.setKeyField(tkeyField)
        
        # addressDB connect
        self.adb = mongoDB.MongoConnector(self.adbName, acollectionName)
        self.adb.connect()
        self.adb.setKeyField(akeyField)

        # abuse addressDB connect
        self.addrdb = mongoDB.MongoConnector(self.addrdbName, addrcollectionName)
        self.addrdb.connect()
        self.addrdb.setKeyField(addrkeyField)
        

        return self.tdb, self.adb, self.addrdb

    def getAbuseAddress(self):
        """
        abuse_address DB에 있는 address 가져오기
        """
        aea = list(self.addrdb.find())
        abuseAddress = []

        for each in aea:
            #print(each['address'])
            abuseAddress.append(each['address'])
        return abuseAddress

    def getInOutAddress(self):
        """
        전체 트랜잭션 내 in/out 주소 가져오기
        """
        tea = self.tdb.find()

        inputAddrs = []
        outputAddrs = []

        for each in tea:
            for item in each['inputs']:
                inputAddrs.append(item['address'])
            for item in each['outputs']:
                outputAddrs.append(item['address'])
            #print(len(each['inputs']))
            #print(len(each['outputs']))

        inputs = list(set(inputAddrs))
        outputs = list(set(outputAddrs))

        return inputs, outputs

    def getAbuseInOutAddress(self, abuseAddress, inOrOuts):
        """
        in/out address 중 abuse에 속하는 데이터 가져오기
        """
        abuse = []

        for each in inOrOuts:
            if each in abuseAddress:
                abuse.append(each)
                #print(each)
        return abuse

    def filterAbuseAddress(self):
        """
        트랜잭션 내 abuse address 가져오기
        
        1. abuse address 가져오기 - getAbuseAddress()
        2. 트랜잭션에서 in/out address 가져오기 - getInOutAddress()
        3. in/out address 중에서 abuse인 데이터 가져오기 - getAbuseInOutAddress()
        """

        abuseAddress = self.getAbuseAddress()
        inputs, outputs = self.getInOutAddress()

        abuseInputs = self.getAbuseInOutAddress(abuseAddress, inputs)
        abuseOutputs = self.getAbuseInOutAddress(abuseAddress, outputs)

        return abuseAddress, inputs, outputs, abuseInputs, abuseOutputs
    
    def getAbuseTxInfo(self, abuseAddress, inOrOut):
        """
        abuse와 연관된 트랜잭션 가져오기
        """
        if inOrOut == 'input':
            target = 'inputs.address'
        elif inOrOut == 'output':
            target = 'outputs.address'
        abuseTxInfo = []
        for each in abuseAddress:
            for item in list(self.tdb.find(target,each)):
                infor = {'abuseAddress':each,
                         'abuseTX':None
                        }
                del item['_id']
                del item['dateScraped']
                #print(item)
                infor['abuseTX'] = item
                abuseTxInfo.append(infor)

        return abuseTxInfo
    
    def getAddressTx(self, address):
        """
        address의 input/ output 트랜잭션 가져오기
        """
        # 오래된 순서대로 
        inputTX = list(self.tdb.find('inputs.address',address).sort('time',1))
        outputTX = list(self.tdb.find('outputs.address',address).sort('time',1))

        return inputTX, outputTX        
    
    
    def getNextTransaction(self, address, time, value):
        """
         output으로 받은 address의 다음 transaction을 확인하는 함수

         1. address의 input 트랜잭션 가져오기 - getAddressTX()
         2. time이 큰것/ value 가 같은 것 비교해서 가져오기
        """
        print('[ - ] address = ',address)
        # address 트랜잭션 오래된 순서대로 정렬
        inputTX, _ = self.getAddressTx(address)

        # address의 abuse 연관된 transaction 이후 데이터만 수집
        d = []
        for each in inputTX:
            d.append(each['time'])       
        rIndex = len(list(filter(lambda x: x > time, d)))

        # 없으면 0
        if rIndex == 0:
            return [], address, time, value

        aea = inputTX[-1*rIndex:]
        print("[ - ] 기간 내 transaction 개수 = ", len(aea))

        # time을 비교해서 0번에 있는 데이터가 다음 트랜잭션인지는 확인해봐야함
        # 만약 0번에 있는 데이터가 다음 트랜잭션이면, nextTxid = None 으로 두고 nexTxid가 나오면 break 하면 됨
        # 0번 데이터가 다음 트랜잭션이 아니라면, nextTxid = []로 두고 다음 트랜잭션을 찾는 방안이 필요할 것
        nextTxid = []
        #nextTxid = None
        for each in aea:
            # input에 같은 value 가 있을 경우
            for item in each['inputs']:  
                if item['value'] == value:
                    nextTxid.append(each['txid'])
                    #nextTxid = each['txid']

        return nextTxid, address, time, value
    # 재귀 version
    def generateNextChainByTxid(self, txid, curLevel, lastLevel, res):
        """
        현재 tx -> 다음 tx
        """
        if curLevel > lastLevel:
            return res
        elif curLevel == 0:
            each = list(self.tdb.find('txid',txid))[0]

            node = {'level':curLevel,
                    'txid':txid
                   }        
            res.append(node)
            return self.generateNextChainByTxid(txid, curLevel+1, lastLevel, res)
        else:
            each = list(self.tdb.find('txid',txid))[0]

            print('[ - ] 확인하는 txid = ', each['txid'])
            for item in each['outputs']:
                if item['spent'] == True:
                    node = {'lavel':curLevel,
                            'txid_prev':txid,
                            'txid_next':None,
                            'address':None,
                            'time':None,
                            'value':None
                           }
                    # getNextTransaction(address, time, value)
                    nextTxid, address, time, value = self.getNextTransaction(item['address'], each['time'], item['value'])
                    print(nextTxid)  
                    if len(nextTxid) > 0:
                        node['txid_next'] = nextTxid[0]
                        node['address'] = address
                        node['time'] = time
                        node['value'] = value
                        res.append(node)
                        res = self.generateNextChainByTxid(node['txid_next'], curLevel+1, lastLevel, res)
            return res
        
    def generateNextTxChain(self, abuseInputsTxInfo, DEPTH):
        """
        현재 tx -> 다음 tx

        abuseInputs로 나온 tx 체인만들기
        """
        res = []
        for each in abuseInputsTxInfo:
            infor = {'abuseAddress': each['abuseAddress'],
                     'abuseChain':None
                    }
            #print(each)
            chain = self.generateNextChainByTxid(each['abuseTX']['txid'],0, DEPTH, [])
            infor['abuseChain'] = chain
            res.append(infor)

        return res

    def getPrevTransaction(self, address, time, value):
        """
         output으로 받은 address의 다음 transaction을 확인하는 함수

         1. address의 output 트랜잭션 가져오기 - getAddressTX()
         2. time이 작은것/ value 가 같은 것 비교해서 가져오기
        """
        print('[ - ] address = ',address)
        # address 트랜잭션 오래된 순서대로 정렬
        _, outputTX = self.getAddressTx(address)
        # 최근 순서대로 정렬 #? 오래된 코인부터 쓰는지 최근 코인부터 쓰는지 확인해야함.
        outputTX.reverse()

        # address의 abuse 연관된 transaction 이후 데이터만 수집
        d = []
        for each in outputTX:
            d.append(each['time'])       
        rIndex = len(list(filter(lambda x: x < time, d)))

        # 없으면 0
        if rIndex == 0:
            return [], address, time, value

        aea = outputTX[-1*rIndex:]
        print("[ - ] 기간 내 transaction 개수 = ", len(aea))

        # time을 비교해서 0번에 있는 데이터가 다음 트랜잭션인지는 확인해봐야함
        # 만약 0번에 있는 데이터가 다음 트랜잭션이면, prevTxid = None 으로 두고 prevTxid가 나오면 break 하면 됨
        # 0번 데이터가 다음 트랜잭션이 아니라면, prevTxid = []로 두고 다음 트랜잭션을 찾는 방안이 필요할 것
        prevTxid = []
        #prevTxid = None
        for each in aea:
            # output에 같은 value 가 있을 경우
            for item in each['outputs']:  
                if item['value'] == value:
                    prevTxid.append(each['txid'])
                    #prevTxid = each['txid']

        return prevTxid, address, time, value
    # 재귀 version
    def generatePrevChainByTxid(self, txid, curLevel, lastLevel, res):
        """
        이전 tx <- 현재 tx
        """
        if curLevel > lastLevel:
            return res
        elif curLevel == 0:
            each = list(self.tdb.find('txid',txid))[0]

            node = {'level':curLevel,
                    'txid':txid
                   }        
            res.append(node)
            return self.generatePrevChainByTxid(txid, curLevel+1, lastLevel, res)
        else:
            each = list(self.tdb.find('txid',txid))[0]

            print('[ - ] 확인하는 txid = ', each['txid'])
            for item in each['inputs']:
                node = {'lavel':-1*curLevel,
                        'txid_prev':None,
                        'txid_next':txid,
                        'address':None,
                        'time':None,
                        'value':None
                       }
                # getNextTransaction(address, time, value)
                prevTxid, address, time, value = self.getPrevTransaction(item['address'], each['time'], item['value'])
                #print(prevTxid)  
                if len(prevTxid) > 0:
                    node['txid_prev'] = prevTxid[0]
                    node['address'] = address
                    node['time'] = time
                    node['value'] = value
                    res.append(node)
                    res = self.generatePrevChainByTxid(node['txid_prev'], curLevel+1, lastLevel, res)
            return res
    
    def generatePrevTxChain(self, abuseoutputsTxInfo, DEPTH):
        """
        이전 tx <- 현재 tx

        abuseInputs로 나온 tx 체인만들기
        """
        res = []
        for each in abuseoutputsTxInfo:
            infor = {'abuseAddress':each['abuseAddress'],
                     'abuseChain':None
                    }
            #print(each)
            chain = self.generatePrevChainByTxid(each['abuseTX']['txid'],0, DEPTH, [])
            infor['abuseChain'] = chain
            res.append(infor)
            #print(infor)

        return res

if __name__ =="__main__":
    a = bitcoinAnalyzer()
    tdb, adb, addrdb = a.setDB()
    abuseAddress, inputs, outputs, abuseInputs, abuseOutputs = a.filterAbuseAddress()
    abuseInputsTxInfo = a.getAbuseTxInfo(abuseInputs, 'input')
    abuseOutputsTxInfo = a.getAbuseTxInfo(abuseOutputs,'output')

    DEPTH = 2
    # generatePrevTxChain(abuseInputsTX, DEPTH)
    prevRes = a.generatePrevTxChain(abuseOutputsTxInfo[3:4], DEPTH)
    print(prevRes)
    
    nextRes = a.generateNextTxChain(abuseInputsTxInfo[:1], DEPTH)
    print(nextRes)
