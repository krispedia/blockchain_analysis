import warnings
warnings.filterwarnings(action='ignore')

import requests
from bs4 import BeautifulSoup
import time
import datetime
import re
from proxy import Proxy

import mongoDB14 as mongoDB

##### timeout error handling
import backoff
import os
REQUESTS_MAX_RETRIES = int(os.getenv("REQUESTS_MAX_RETRIES", 100))

class ServerError(requests.exceptions.HTTPError):
    pass

# Re-usable decorator with exponential wait.
retry_timeout = backoff.on_exception(
    wait_gen=backoff.expo,
    exception=(
        ServerError,
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError
    ),
    max_tries=REQUESTS_MAX_RETRIES,
)

class bitcoinUserConnectionAnalyzer:
    def __init__(self):
        self.dbName = None
        self.keyField = None
        self.proxies = self.setProxy()
        
    def setDBName(self, dbName):
        """
        | collection을 모아둘 db 이름 설정
        """
        self.dbName = dbName
        
    def setKeyField(self, keyField):
        """
        | collection의 keyField 설정
        """
        self.keyField = keyField
        
    def setCollectionPrefix(self, prefix):
        """
        | collection 명 prefix 설정
        """
        self.collectionPrefix = prefix
    
    def setUserTxDB(self, userID):
        """
        | userID 기반 트랜잭션 데이터 DB 설정
        |
        | **코드 특징**
        | **여러 userID를 하나의 분석 도구로 살펴봐야 할 수 있기 때문에 
        | **setDBName(), setKeyField(), setCollectionPrefix()를 통일 시켜 놓고
        | **userID로 DB를 생성, 접근할 수 있도록 분리해놓았음. 
        """
        collectionName = self.collectionPrefix+userID
        db = mongoDB.MongoConnector(self.dbName, collectionName)
        db.connect()
        db.setKeyField(self.keyField) 
        
        return db
        
    def setProxy(self):
        """
        | 프록시 설정
        """
        self.p = Proxy()
        self.proxies = self.p.getRandomProxies('yes')
        
        return self.proxies
    
    @retry_timeout    
    def getSoup(self, url):
        """
        | URL 의 soup 가져오기
        """
        # ssl 에러 제거하려고 verify=False 
        req = requests.get(url, proxies=self.proxies, verify=False)

        # 페이지를 정상적으로 가져오지 않았다면
        if req.status_code != 200:
            # 페이지 정상적으로 가져올 때 까지 proxy 변경
            while(req.status_code != 200):
                self.proxies = self.setProxy()
                req = requests.get(url, proxies=self.proxies)

        return BeautifulSoup(req.text, 'html.parser')
    
    def getCurPage(self, userID):
        """
        | 새로 가져올 user 트랜잭션 페이지 시작점 가져오기
        |
        | **코드 특징**
        | **데이터를 가져오다가 중간에 끊긴 경우 이전에 가져온 이후 데이터부터 가져올 수 있도록
        | **DB에서 확인함. 
        | **계속해서 새로운 데이터가 추가되므로 데이터 수집이 중단되었던 시기의 페이지의 내용이 
        | **새로 가져오기 위해 확인하는 페이지의 내용과 다를 수 있다는 점을 유의해야함. 
        | **만약 이전에 가져온 트랜잭션 이후 새로 생긴 트랜잭션만 가져와 데이터를 업데이트 하고 싶은 경우
        | **코드를 조금 수정하면 될것임. 
        """
        db = self.setUserTxDB(userID)
        
        # db에 데이터가 있는 경우
        if db.collection.count_documents({}) != 0:
            curPage =  int(db.collection.count_documents({})/100) +1
        # db에 데이터 없는 경우
        else:
            curPage = 1

        return curPage
    
    def getLastPage(self, userID):
        """
        | 새로 가져올 user 트랜잭션 마지막 페이지 가져오기
        """
        url = f'https://www.walletexplorer.com/wallet/{userID}'
        soup = b.getSoup(url)
        if len(soup.find('div',{'class':'paging'}).findAll('a'))>1:
            lastPage = int(soup.find('div',{'class':'paging'}).findAll('a')[-1]['href'].split('page=')[1])
        else:
            lastPage = 1

        return lastPage
    
    # To. 20200616의 나에게
    # db 넣는 조건 변경할것!!!  -> 변경했음 
    # userID 길이 통일해야함. 링크에 있는 URL 에서 user ID 가져오도록 변경할 -> 변경함
    def getUserTx(self, userID, fromTime=None, toTime=None):
        """
        | user 트랜잭션 데이터 가져오기
        |
        | Parameter
        |    userID: str
        |        확인하려는 소유자 ID(walletExplorer 자체 지정 ID)
        |    fromTime: datetime
        |        필수는 아님. 기간 필터로 사용할 수 있음 
        |    toTime: datetime
        |        필수는 아님. 기간 필터로 사용할 수 있음 
        """
        filterOutR = False
        filterOutS = False
        
        if fromTime == None: fromTime = datetime.datetime(1970,1,1,0,0,0)
        if toTime == None: toTime = datetime.datetime.now()
        
        db = self.setUserTxDB(userID)
        
        for page in range(self.getCurPage(userID), self.getLastPage(userID)+1):
            url = f'https://www.walletexplorer.com/wallet/{userID}?page={page}'
            print('[ * ] req -> ', url)
            soup = self.getSoup(url)
            
            # soup.find('table',{'class':'txs'}).findAll('tf') 하면 하나의 컬럼 외에도 다른 데이터들이 들어와서 
            # received/ sent 따로 받아야함.
            if filterOutR == False:
                # 받은 데이터 
                for each in soup.find('table',{'class':'txs'}).findAll('tr',{'class':'received'}):
                    #print(each)
                    try:
                        infor = {'time':datetime.datetime.strptime(each.find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S'),
                                 'received':True,
                                 'userID': each.find('td',{'class':'walletid'}).find('a')['href'].split('/')[-1].strip(), 
                                 'value':float(each.find('td',{'class':'diff'}).text.strip()),
                                 'balance':float(each.findAll('td',{'class':'amount'})[-1].text.strip()),
                                 'txid':each.find('td',{'class':'txid'}).find('a')['href'].split('/')[-1]
                                }
                    except:
                        # userID 부분에 '(multiple) 이라 적혀있는 것도 있음'
                        infor = {'time':datetime.datetime.strptime(each.find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S'),
                                 'received':True,
                                 'userID': each.find('td',{'class':'walletid'}).text.replace('(','').replace(')','').strip(), 
                                 'value':float(each.find('td',{'class':'diff'}).text.strip()),
                                 'balance':float(each.findAll('td',{'class':'amount'})[-1].text.strip()),
                                 'txid':each.find('td',{'class':'txid'}).find('a')['href'].split('/')[-1]
                                }
                        
                    print(infor)
                    if infor['time']<fromTime:
                        filterOutR = True
                        break
                    if infor['time']>=fromTime and infor['time']<=toTime:
                        db.insertOne(infor)
                    
            if filterOutS == False:
                # 보낸 데이터
                for each in soup.find('table',{'class':'txs'}).findAll('tr',{'class':'sent'}):
                    #print(each)
                    infor = {'time':datetime.datetime.strptime(each.find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S'),
                             'received':False,
                             'userID': each.findAll('td',{'class':'walletid'})[1].find('a')['href'].split('/')[-1].strip(),
                             'value':float(each.find('td',{'class':'diff'}).text.strip()),
                             'balance':float(each.findAll('td',{'class':'amount'})[-1].text.strip()),
                             'txid':each.find('td',{'class':'txid'}).find('a')['href'].split('/')[-1]
                            }

                    print(infor)
                    if infor['time']<fromTime:
                        filterOutS = True
                        break
                    if infor['time']>=fromTime and infor['time']<=toTime:
                        db.insertOne(infor)
                    
            if filterOutR == True and filterOutS == True:
                break
            time.sleep(5)

    def getReceivedUserIDs(self, userID, fromTime=None, toTime=None):
        """
        | userID 가 코인을 송금한 대상 리스트 확인
        """
        db = self.setUserTxDB(userID)
        
        q = {'received':False}
        if fromTime != None and toTime != None:
            q['time'] = {'$gte':fromTime,
                         '$lte':toTime
                        }
        elif fromTime != None:
            q['time'] = {'$gte':fromTime}
        elif toTime != None:
            q['time'] = {'$lte':toTime}
        
        # 최근 순으로 가져옴
        ea = list(db.collection.find(q))
        
        receiverUserIDs = set()
        for each in ea:
            receiverUserIDs.add(each['userID'])
            
        print('[ - ] receiverIDs LEN = ', len(list(receiverUserIDs)))
        
        return list(receiverUserIDs)
    
    def getTxList(self, userID, receiverID):
        """
        | 특정 송금 대상과 연계된 트랜잭션 DB에서 추출
        |
        | Parameter
        |    userID: str
        |        확인하는 userID
        |    receiverID: str
        |        송금 대상 userID
        | Return
        |    txidList: list
        |        해당 트랜잭션 리스트
        """
        db = self.setUserTxDB(userID)
        # receiverID로 송금한 트랜잭션만 가져옴
        ea = list(db.collection.find({'received':False, 'userID':receiverID}))

        txidList = []
        for each in ea:
            #print(each['txid'])
            txidList.append(each['txid'])

        return txidList

    @retry_timeout
    def getTxInfo(self, txid): 
        """
        | 트랜잭션 정보 walletExplorer.com에서 확인
        |
        | Parameter
        |    txid: str
        |        확인할 트랜잭션 ID
        | Return
        |    infor: dict
        |        트랜잭션 정보
        """
        url = 'https://www.walletexplorer.com/txid/' + txid
        soup = self.getSoup(url)
        #print(soup)

        info_continer = soup.find('table', {'class': 'info'}).findAll('tr')
        tx_continer = soup.find('table', {'class': 'tx'}).findAll('table',{'class': 'empty'})
        infor = {
            'dateScraped':datetime.datetime.now(),
            'txid':None,
            'block':0,
            'block_pos':0,
            'time':None,
            'sender':None,
            'fee_BTC':None,
            'fee_satoshis/byte':None,
            'size':0,
            'inputs':[],
            'outputs':[],
        }
        infor['txid'] = info_continer[0].find('td').getText()
        txt = info_continer[1].find('td').getText()
        infor['block'] = int(re.findall('(\d*)\s?\(',txt)[0])
        infor['block_pos'] = int(re.findall('pos\s?(\d*)',txt)[0])
        infor['time'] = datetime.datetime.strptime(re.findall('(\d*-\d*-\d*\s*\d*:\d*:\d*)', info_continer[2].find('td').getText())[0], '%Y-%m-%d %H:%M:%S') 

        # 일반적인 거래 transaction인 경우 info_container에 txid/ included in block/ time/ sender/ fee/ size 정보 있음.
        if len(info_continer) == 6:
            if info_continer[3].find('td').find('a'):
                infor['sender'] = info_continer[3].find('td').find('a').getText().replace('[','').replace(']','')
            else:
                infor['sender'] = info_continer[3].find('td').getText().replace('[','').replace(']','')
            txt = info_continer[4].find('td').getText()
            infor['fee_BTC'] = re.findall('(\S*)\s?B', txt)[0]
            infor['fee_satoshis/byte'] =re.findall('\((\S*)\s?sato', txt)[0] 
            infor['size'] = int(re.findall('(\d*)', info_continer[5].find('td').getText())[0])

            for inp in tx_continer[0].findAll('tr'):
                input_infor = {
                    'address':None,
                    'sender': None,
                    'value':None
                }
                tds = inp.findAll('td')
                input_infor['address'] = tds[0].find('a').getText()
                input_infor['sender'] = infor['sender']
                input_infor['value'] = re.findall('(\S*)\s*?B',tds[1].getText())[0]
                #print(input_infor)
                infor['inputs'].append(input_infor)

        # 채굴한 코인의 transaction인 경우 info_container에 txid/included in block/ time/ size 정보만 있음.
        # input 에 address/ value값이 없으므로 누가 채굴했는지(sender)만 가져오면 됨. 
        # 예)https://www.walletexplorer.com/txid/3216115b6e90bc1f72201f95bda8e89cf2ce8ba7323520ff022a5200478d6774 
        elif len(info_continer) == 4:
            infor['sender'] = tx_continer[0].find('em').getText() # sender 정보가 없어서 tx input 에서 가져와야함.
            infor['size'] = int(re.findall('(\d*)', info_continer[3].find('td').getText())[0]) 

        #print(infor)

        for outp in tx_continer[1].findAll('tr'):

            oput_infor = {
                'address':None,
                'reciever':None,
                'value':None,
                'spent':True 
            }
            tds = outp.findAll('td')
            try:oput_infor['address'] = tds[0].find('a').getText()
            except: oput_infor['address'] = re.findall('\d*.\s?(\D*)', tds[0].getText())[0] 
            try:oput_infor['reciever'] = tds[1].find('a').getText().replace('[','').replace(']','')
            except:oput_infor['reciever'] = tds[1].getText().replace('[','').replace(']','')
            oput_infor['value'] = re.findall('(\S*)\s*?B',tds[2].getText())[0]
            if tds[3].getText() == 'unspent':
                oput_infor['spent'] = False
            #print(oput_infor)
            infor['outputs'].append(oput_infor)

            #db.updateMany('txid',txid, infor)
        return infor
    
    
    def getNextTxid(self, address, time, value):
        """
        | 다음 트랜잭션 ID 찾기
        | DB 데이터 저장없이 walletExplorer 데이터를 바로 사용함
        |
        | 방법1. 트랜잭션 페이지 1번 부터 확인하는 코드. 코드 확인 완료
        | 
        | Parameter
        |    address: str
        |        getTxInfo() 리턴값의 outputs 각각의 address
        |        ex) infor = getTxInfo(txid)
        |            for eachOutput in infor['outputs']:
        |                address = eachOutput['address']
        |    time: datetime
        |        현재 트랜잭션 일어난 시간
        |        ex) infor = getTxInfo(txid)
        |            time = infor['time']
        |    value: float
        |        다음 트랜잭션을 확인할 output 대상 value
        |        ex) infor = getTxInfo(txid)
        |            for eachOutput in infor['outputs']:
        |                address = eachOutput['value']
        | Return 
        |    txid: str
        |        다음 트랜잭션 ID
        """
        url = f'https://www.walletexplorer.com/address/{address}'
        soup = self.getSoup(url)

        timeLimitOver = False

        # address의 트랜잭션 마지막 페이지 번호 확인
        if len(soup.find('div',{'class':'paging'}).findAll('a')) == 3:
            lastPage = int(soup.find('div',{'class':'paging'}).findAll('a')[2]['href'].split('page=')[-1])
        else:
            lastPage = 1

        # 페이지 순서대로 입력으로 들어온 time 보다 이후에 일어난 트랜잭션 txList에 수집
        txList=[]
        for page in range(1, lastPage+1):
            url = f'https://www.walletexplorer.com/address/{address}?page={page}'
            soup = b.getSoup(url)

            # 페이지에 나타나는 테이블의 첫번째 줄은 인덱스이므로 필요 없음
            for each in soup.find('table',{'class':'txs'}).findAll('tr')[1:]:
                infor = {'time':datetime.datetime.strptime(each.find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S'),
                         'value':each.find('td',{'class':'amount'}).text.strip(),
                         'received':True,
                         'txid':each.find('td',{'class':'txid'}).text.strip()
                        }
                if infor['value'].startswith('-'):
                    infor['received'] = False
                infor['value'] = float(infor['value'][1:])

                # 살펴보는 트랜잭션 시간 이전이면 상관없음
                if infor['time'] < time:
                    timeLimitOver = True
                    break
                txList.append(infor)

            # 살펴보는 트랜잭션 시간 이전이 확인되면 다음 페이지를 살펴볼 필요 없음
            if timeLimitOver: break

        # 최근 순서대로 들어가있으니 오래된 순서로 변경
        txList.reverse()

        # 트랜잭션 모음 살펴보면서 주소와 코인 값이 같은 트랜잭션 확인
        for each in txList:
            txInfo = getTxInfo(each['txid'])
            for eachInputs in txInfo['inputs']:
                # 송금한 내역이고 살펴 보는 트랜잭션에서 받은 value 와 동일한 value 가 사용되면 이게 다음 트랜잭션임
                if each['received'] == False and eachInputs['address'] == address and eachInputs['value'] == value:
                    return each['txid']

        # 사용된 이력이 없으면 None 임    
        return None
    
    def getStartPage(self, address, time):
        """
        | 이진 탐색으로 time이 시작되는 페이지 찾기
        """
        url = f'https://www.walletexplorer.com/address/{address}'
        soup = b.getSoup(url)

        # address의 트랜잭션 마지막 페이지 번호 확인
        if len(soup.find('div',{'class':'paging'}).findAll('a')) == 3:
            lastPage = int(soup.find('div',{'class':'paging'}).findAll('a')[2]['href'].split('page=')[-1])
        else:
            lastPage = 1

        time1 = datetime.datetime.strptime(soup.find('table',{'class':'txs'}).findAll('tr')[2].find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S')
        time2 = datetime.datetime.strptime(soup.find('table',{'class':'txs'}).findAll('tr')[-1].find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S')

        # 첫번째 페이지가 해당 페이지인 경우
        if time2<time and time1>=time:
            return 1
        # 첫번째 페이지가 time 보다 이후 인 경우
        elif time2 > time:    
            begin = 1
            end = lastPage
            while 1:
                page = int((begin+end)/2)

                url = f'https://www.walletexplorer.com/address/{address}?page={page}'
                soup = b.getSoup(url)   
                print('[ - ] req -> ', url)

                time1 = datetime.datetime.strptime(soup.find('table',{'class':'txs'}).findAll('tr')[2].find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S')
                time2 = datetime.datetime.strptime(soup.find('table',{'class':'txs'}).findAll('tr')[-1].find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S')

                # 해당 페이지를 찾은것임
                if time2<=time and time1>=time:
                    break
                # 살펴본 페이지 날짜가 찾는 날짜보다 이후라면 2등분에서 오른쪽 
                elif time2 > time:
                    begin = page+1
                # 살펴본 페이지 날짜가 찾는 날짜보다 이전이라면 2등분에서 왼쪽
                elif time1 < time:
                    end = page-1
                else:
                    if page == lastPage: page = page-1
                    else: page = page+1
            return page
        # 첫번째 페이지가 time보다 이전인 경우 - 오류임. 
            return None
        
    def getNextTxid_binarySearch(self, address, time, value):
        """
        | 다음 트랜잭션 ID 찾기
        | DB 데이터 저장없이 walletExplorer 데이터를 바로 사용함
        | 
        | 방법2. 2진 탐색 방법 적용해서 해당 페이지 추출 후 그 페이지부터 확인하는 방법
        |
        | Parameter
        |    address: str
        |        getTxInfo() 리턴값의 outputs 각각의 address
        |        ex) infor = getTxInfo(txid)
        |            for eachOutput in infor['outputs']:
        |                address = eachOutput['address']
        |    time: datetime
        |        현재 트랜잭션 일어난 시간
        |        ex) infor = getTxInfo(txid)
        |            time = infor['time']
        |    value: float
        |        다음 트랜잭션을 확인할 output 대상 value
        |        ex) infor = getTxInfo(txid)
        |            for eachOutput in infor['outputs']:
        |                address = eachOutput['value']
        | Return 
        |    txid: str
        |        다음 트랜잭션 ID
        """
        url = f'https://www.walletexplorer.com/address/{address}'
        soup = self.getSoup(url)

        timeLimitOver = False

        # address의 트랜잭션 마지막 페이지 번호 확인
        if len(soup.find('div',{'class':'paging'}).findAll('a')) == 3:
            lastPage = int(soup.find('div',{'class':'paging'}).findAll('a')[2]['href'].split('page=')[-1])
        else:
            lastPage = 1

        startPage = self.getStartPage(address, time)

        # 페이지 순서대로 입력으로 들어온 time 보다 이후에 일어난 트랜잭션 txList에 수집
        txList=[]
        for page in range(startPage, 0, -1):
            url = f'https://www.walletexplorer.com/address/{address}?page={page}'
            soup = self.getSoup(url)
            print('[ - ] req -> ',url)

            # 페이지에 나타나는 테이블의 첫번째 줄은 인덱스이므로 필요 없음
            for each in soup.find('table',{'class':'txs'}).findAll('tr')[1:]:
                infor = {'time':datetime.datetime.strptime(each.find('td',{'class':'date'}).text, '%Y-%m-%d %H:%M:%S'),
                         'value':each.find('td',{'class':'amount'}).text.strip(),
                         'received':True,
                         'txid':each.find('td',{'class':'txid'}).text.strip()
                        }
                if infor['value'].startswith('-'):
                    infor['received'] = False
                infor['value'] = float(infor['value'][1:])

                # 살펴보는 트랜잭션 시간 이전이면 상관없음
                if infor['time'] < time:
                    timeLimitOver = True
                    break
                txList.append(infor)

            # 살펴보는 트랜잭션 시간 이전이 확인되면 다음 페이지를 살펴볼 필요 없음
            if timeLimitOver: break

        # 트랜잭션 모음 살펴보면서 주소와 코인 값이 같은 트랜잭션 확인
        for each in txList:
            txInfo = getTxInfo(each['txid'])
            for eachInputs in txInfo['inputs']:
                # 송금한 내역이고 살펴 보는 트랜잭션에서 받은 value 와 동일한 value 가 사용되면 이게 다음 트랜잭션임
                if each['received'] == False and eachInputs['address'] == address and eachInputs['value'] == value:
                    return each['txid']

        # 사용된 이력이 없으면 None 임    
        return None

if __name__ =="__main__":
    b = bitcoinUserConnectionAnalyzer()
    b.setDBName('ws_datas')
    b.setKeyField('txid')
    b.setCollectionPrefix('bitcoin_user_')

    # 날짜 필터링 없음
    #b.getUserTx('00001daa90d8b9ec')

    # 날짜 필터링 사용
    b.getUserTx('00001daa90d8b9ec',datetime.datetime(2020,1,1),datetime.datetime(2020,6,15,23,59,59))

    receivers = b.getReceivedUserIDs('00001daa90d8b9ec')

    for eachReceiver in receivers:  
        txidList = getTxList(userID, eachReceiver)
        for eachTxid in txidList:
            print('====')
            print(eachTxid)
            infor = getTxInfo(eachTxid)
            for each in infor['outputs']:
                print('----')
                print(each['address'])
                nextTxid = getNextTxid(each['address'], infor['time'], each['value'])
                print(nextTxid)