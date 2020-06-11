##-*- coding:utf-8 -*-

from bs4 import BeautifulSoup
import requests
import re
import time
import datetime

import mongoDB14 as mongoDB
import proxy

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

class walletexplorerCrawler:
    
    def __init__(self):
        self.postdb = None
        self.logdb = None
        self.p = proxy.Proxy()
        self.proxies = None
        
    def setDB(self, exchangeCollectionName, walletCollectionName, transactionCollectionName)-> None:
        dbName =  'ws_datas'
        walletKeyField = 'address'
        transactionKeyField = 'txid'
        
        self.exchangedb = mongoDB.MongoConnector(dbName, exchangeCollectionName)
        self.exchangedb.connect()
        self.exchangedb.setKeyField('url')
        
        # walletDB connect
        self.walletdb = mongoDB.MongoConnector(dbName, walletCollectionName)
        self.walletdb.connect()
        self.walletdb.setKeyField(walletKeyField)
        
        # walletexplorerNumdb connect
        #self.walletNumdb= MongoConnector(dbName, walletNumCollectionName)
        #self.walletNumdb.connect()
        #self.walletNumdb.setKeyField(walletNumKeyField)
    
        # walletTransactionDB connect
        self.walletTransactiondb = mongoDB.MongoConnector(dbName, transactionCollectionName)
        self.walletTransactiondb.connect()
        self.walletTransactiondb.setKeyField(transactionKeyField)
    
    def setProxy(self):
        print('[ - ] === FIND PROXY ===')
        self.proxies = self.p.getRandomProxies('yes')
        
        print('[ - ] proxy set done! ' + self.proxies['https'])
    
    ##### db에서 찾는 함수들
    def getCategoryUrls(self):    
        categoryUrls = []
        print('[ - ] === FIND CATEGORY URL ===')
        for each in list(self.exchangedb.find()):
            categoryUrls.append(each['url'])
            #print(each['url'])
            
        return categoryUrls
    
    def getCategory(self, url)->list:    
        res=[]
        if self.proxies == None:
            req = requests.get(url)
        else:
            req = requests.get(url, proxies = self.proxies)
        print('[ * ] req -> ',req.url)
        soup = BeautifulSoup(req.text, 'html.parser')
        
        while soup.text.startswith('Too many requests.'):
            self.setProxy()
            req = requests.get(url, proxies = self.proxies)
            print('[ * ] req -> ',req.url)
            soup = BeautifulSoup(req.text, 'html.parser')            
            
        try:
            for each in soup.find('table',{'class':'serviceslist'}).findAll('li'):
                for category in each.findAll('a'):
                    siteName = category['href'].split('/')[-1]
                    category_url = 'https://www.walletexplorer.com' + category['href'] + '/addresses?page={page}'
                    #print(category['href'].split('/')[-1])
                    #print(category_url)
                    if category_url not in res:
                        res.append(category_url) 
                        infor = {'siteName': siteName,
                                 'url':category_url,
                                 'wallet_currentPage':0
                                }
                        print('[ - ] === INSERT SITENAME ===')
                        self.exchangedb.insertOne(infor)
        except Exception as e:
                print(e)
            
        return res    
                             
    @retry_timeout
    def getWallet(self, siteUrl)-> list:
        
        res = []
        res_wallet_num = []
        
        req = requests.get(siteUrl.format(page=1), proxies=self.proxies)
        print('[ * ] siteUrl req -> ',req.url)
        soup = BeautifulSoup(req.text, 'html.parser')
        
        #print(soup)

        # request 많이 보내서 사이트에서 접근을 막은 경우 proxy 새로 지정
        while soup.text.startswith('Too many requests.'):
            self.setProxy()
            req = requests.get(siteUrl.format(page=1), proxies = self.proxies)
            print('[ * ] req -> ',req.url)
            soup = BeautifulSoup(req.text, 'html.parser') 

        # 페이지를 못가져 오는 경우 대비
        try:
            soup.find('h2').getText()
        except:
            print(soup)
            while soup.find('h2') == None:
                self.setProxy()
                req = requests.get(siteUrl.format(page=1), proxies=self.proxies)
                print('[ * ] siteUrl req -> ',req.url)
                soup = BeautifulSoup(req.text, 'html.parser')            
        
        try:
            lastPage  = int(re.findall('/\s?(\d*)', soup.find('div',{'class':'paging'}).getText())[0])
        except:
            lastPage = 1

        # 처음 들어온 데이터의 경우에만 lastPage 업데이트하기
        print('[ - ] === UPDATE WALLET_LASTPAGE ===')
        self.exchangedb.updateOne('url',siteUrl,'wallet_lastPage',lastPage)
        
        print('[ - ] lastPage = ', lastPage)
        wallet_name = re.findall('Wallet\s?(\S*)', soup.find('h2').getText())[0]
        res_wallet_num.append(wallet_name)
        print('[ - ] walletName = ', wallet_name)
        
        print('[ - ] === FIND WALLET_CURRENTPAGE ===')
        donePage = list(self.exchangedb.find('url',siteUrl))[0]['wallet_currentPage']

        for page in range(donePage+1, lastPage+1):

            req = requests.get(siteUrl.format(page=page), proxies=self.proxies)
            print('[ * ] siteUrl req -> ',req.url)
            soup = BeautifulSoup(req.text, 'html.parser')

            # request 많이 보내서 사이트에서 접근을 막은 경우 proxy 새로 지정
            while soup.text.startswith('Too many requests.'):
                self.setProxy()
                req = requests.get(siteUrl.format(page=page), proxies = self.proxies)
                print('[ * ] req -> ',req.url)
                soup = BeautifulSoup(req.text, 'html.parser')             
            
            # 페이지를 못 가져올 경우를 대비
            try:
                soup.find('table').findAll('tr')
            except:
                print(soup)
                while soup.find('table') == None:
                    self.setProxy()
                    req = requests.get(siteUrl.format(page=page), proxies=self.proxies)
                    print('[ * ] siteUrl page req -> ',req.url)
                    soup = BeautifulSoup(req.text, 'html.parser')
                
            for each in soup.find('table').findAll('tr')[1:]:
                infor = {
                         'dateScraped':datetime.datetime.now(),
                         'address': None,
                         'balance': None,
                         'incoming_txs': None,
                         'last_used_in_block': None,
                         'wallet_name': wallet_name,
                         'tx_currentPage':0
                }
                tds = each.findAll('td')
                infor['address'] = tds[0].find('a').getText()
                infor['balance'] = tds[1].getText().strip()
                infor['incoming_txs'] = int(tds[2].getText())
                infor['last_used_in_block'] = int(tds[3].getText())

                #print(infor)
                res.append(infor)
                print('[ - ] === INSERT WALLET ADDRESS ===')
                self.walletdb.insertOne(infor)
                
                #self.getWalletTx(infor['address'])
            print('[ - ] === UPDATE WALLET_CURRENTPAGE ===')    
            self.exchangedb.updateOne('url',siteUrl,'wallet_currentPage',page)
                
        return res, res_wallet_num
        
    @retry_timeout
    def getWalletTx(self, walletAddress):
        transactionUrl = f'https://www.walletexplorer.com/address/{walletAddress}'

        req = requests.get(transactionUrl, proxies=self.proxies)
        print('[ * ] tx list req -> ',req.url)
        soup = BeautifulSoup(req.text,'html.parser')

        # request 많이 보내서 사이트에서 접근을 막은 경우 proxy 새로 지정
        while soup.text.startswith('Too many requests.'):
            self.setProxy()
            req = requests.get(transactionUrl, proxies = self.proxies)
            print('[ * ] req -> ',req.url)
            soup = BeautifulSoup(req.text, 'html.parser') 

        #print(soup.find('div',{'class':'paging'}))
        if soup.find('div',{'class':'paging'}) and len(soup.find('div',{'class':'paging'}).findAll('a')) > 1:
            #print(soup.find('div',{'class':'paging'}).findAll('a')[-1])
            lastPage = int(soup.find('div',{'class':'paging'}).findAll('a')[-1]['href'].split('page=')[1])
        else:
            #print(1)
            lastPage = 1

        # 처음 들어온 경우에만 lastPage 업데이트하기
        print('[ - ] === UPDATE TX_LASTPAGE ===')
        self.walletdb.updateOne('address',walletAddress,'tx_lastPage',lastPage)
        #print(lastPage)
        
        print('[ - ] === FIND TX_CURRENTPAGE ===')
        donePage = list(self.walletdb.find('address',walletAddress))[0]['tx_currentPage']
        
        for page in range(donePage+1, lastPage+1):
            req = requests.get(transactionUrl+'?page='+str(page), proxies=self.proxies)
            print('[ * ] tx page req -> ',req.url)
            soup = BeautifulSoup(req.text, 'html.parser')
            
            # request 많이 보내서 사이트에서 접근을 막은 경우 proxy 새로 지정
            while soup.text.startswith('Too many requests.'):
                self.setProxy()
                req = requests.get(transactionUrl+'?page='+str(page), proxies = self.proxies)
                print('[ * ] req -> ',req.url)
                soup = BeautifulSoup(req.text, 'html.parser') 

            # 페이지 가져오지 못할 경우 대비
            try:
                soup.find('table').findAll('tr')
            except:
                print(soup)
                while soup.find('table') == None:
                    self.setProxy()
                    req = requests.get(transactionUrl+'?page='+str(page), proxies=self.proxies)
                    print('[ * ] tx page req -> ',req.url)
                    soup = BeautifulSoup(req.text, 'html.parser')    

            # 첫번째 줄은 필드명이므로 1부터 시작
            for each in soup.find('table').findAll('tr')[1:]:
                #print(each)
                txid = each.find('td',{'class': 'txid'}).find('a')['href'].split('/')[-1]
                infor = self.getTxData(txid)
                print('[ - ] === INSERT TX ===')
                self.walletTransactiondb.insertOne(infor)     
            print('[ - ] === UPDATE TX_CURRENTPAGE ===')
            self.walletdb.updateOne('address',walletAddress,'tx_currentPage',page)
            
    def getTxData(self, txid):
        
        transaction_url = 'https://www.walletexplorer.com/txid/' + txid
        #print(transaction_url)

        req = requests.get(transaction_url, proxies=self.proxies)
        print('[ * ] tx req -> ',req.url)
        soup = BeautifulSoup(req.text, 'html.parser')
        
        # request 많이 보내서 사이트에서 접근을 막은 경우 proxy 새로 지정
        while soup.text.startswith('Too many requests.'):
            self.setProxy()
            req = requests.get(transaction_url, proxies = self.proxies)
            print('[ * ] req -> ',req.url)
            soup = BeautifulSoup(req.text, 'html.parser') 

        # 페이지 못가져 오는 경우 대비
        try:
            soup.find('table',{'class':'info'}).findAll('tr')
        except:
            print(soup)
            while soup.find('table',{'class':'info'}) == None:
                self.setProxy()
                req = requests.get(transaction_url, proxies=self.proxies)
                print('[ * ] tx req -> ',req.url)
                soup = BeautifulSoup(req.text, 'html.parser')            

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

        #print(infor)
        return infor

import argparse

def getArgs():
    parser = argparse.ArgumentParser(description='desc')

    parser.add_argument('--ifrom', required=True, help= 'start INDEX')
    parser.add_argument('--ito', required=True, help='end INDEX')

    args = parser.parse_args()

    #print(args.ifrom)
    #print(args.ito)

    return int(args.ifrom), int(args.ito)

if __name__ == "__main__":

    ifrom, ito = getArgs()

    print('[ - ] from= ',ifrom)
    print('[ - ] to= ',ito)

    c = walletexplorerCrawler()
    # setDB(exchangeCollectionName, walletCollectionName, transactionCollectionName)-> None
    c.setDB('wallet_exchange','wallet_address','wallet_transaction')
    c.setProxy()
    # 거래소 사이트 url 가져오기
    # getCategory(url)-> list
    #categoryUrls = c.getCategory('https://www.walletexplorer.com/') # 한번만 돌리면 됨.
    categoryUrls = c.getCategoryUrls()
    
    # 거래소별 wallet 가져오기 (address & transaction)
    for url in categoryUrls[ifrom:ito]:
        res, res_wallet_num = c.getWallet(url)