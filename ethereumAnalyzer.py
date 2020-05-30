import requests
from bs4 import BeautifulSoup

import datetime
import time

from selenium import webdriver

class ethereumAnalyzer:
    def __init__(self):
        pass
    def setDriverPath(self, driverPath):
        self.driverPath = driverPath

    def setDriver(self):
        options = webdriver.ChromeOptions()
        #self.options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        self.driver = webdriver.Chrome(self.driverPath, options=options)
        
    def formTime(self, timeString):
        """
         시간 포맷 맞춰주기
        """
        # May-25-2020 05:54:26 AM
        monthString = timeString.split('-')[0]

        if monthString == 'Jan': month = 1
        elif monthString == 'Feb': month = 2
        elif monthString == 'Mar': month = 3
        elif monthString == 'Apr': month = 4
        elif monthString == 'May': month = 5        
        elif monthString == 'Jun': month = 6   
        elif monthString == 'Jul': month = 7   
        elif monthString == 'Aug': month = 8   
        elif monthString == 'Sep': month = 9   
        elif monthString == 'Oct': month = 10   
        elif monthString == 'Nov': month = 11  
        elif monthString == 'Dec': month = 12

        timeString = timeString.replace(monthString,str(month))

        t = datetime.datetime.strptime(timeString[:-3], '%m-%d-%Y %H:%M:%S')

        if timeString.split(' ')[-1] == 'PM':
            t += datetime.timedelta(hours=12)

        return t
    
    def generateTimeTermByDay(self, targetTime, termDays):
        """
         확인할 시간 텀 생성 
         
         ex) 2020.01.01 ~ 2020.01.13
        """
        
        return targetTime - datetime.timedelta(days=termDays), targetTime + datetime.timedelta(days=termDays+1)

    ######### etherscan.io 사이트의 데이터 사용 #####
    
    def getTxInfoByTxid(self, txid):  
        """
         트랜잭션 정보 가져오기
        """
        url = f'https://etherscan.io/tx/{txid}'
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        infor = {'block':None,
                 'txid':None,
                 'time':None,
                 'fromAddress': None,
                 'fromAddress_tag': None,
                 'fromContract': False,
                 'to':[],
                 #'toAddress': [],
                 #'toAddress_tag': ,
                 #'toContract': False,
                 'contractAddress':None,
                 'contractAddress_tag':None,
                 #'contractValue_ETH':None,
                 'value_ETH': None,
                 'value_USD': None,
                 'fee_ETH':None,
                 'fee_USD':None,
                 'gasLimit':None,
                 'gasUsed':None,
                 'gasUsed_percentage':None,
                 'gasPrice_ETH':None,
                 'gasPrice_Gwei':None,
                 'nonce':None,
                 'inputData':None
                }

        for each in soup.find('div',{'id':'ContentPlaceHolder1_maintable'}).findAll('div',{'class':'row'}):
            data = each.text.replace('\n','').split(':')
            #print(data)
            if data[0].startswith('Nonce'):
                infor['nonce'] = int(data[0].replace('Position','').split(' ')[1].strip())
            if len(data) > 1:
                if data[0].startswith('Block'):
                    infor['block'] = data[1].split(' ')[0]
                elif data[0].startswith('Transaction Hash'):
                    infor['txid'] = data[1]
                elif data[0].startswith('Timestamp'):
                    infor['time'] = a.formTime(each.text.replace('Timestamp','').split('(')[1].split(' +UTC')[0]) 
                elif data[0].startswith('From'):
                    if len(data[1].split('(')) > 1:
                        infor['fromAddress'] = data[1].split('(')[0].strip()
                        infor['fromAddress_tag'] = data[1].split('(')[1].replace(')','').strip()
                    else:
                        infor['fromAddress'] = data[1].strip()

                elif data[0].startswith('To'):
                    # contract를 통한 거래인 경우
                    if data[1].strip().startswith('Contract'):
                        # contract 데이터 추가
                        infor['contractAddress'] = each.find('a',{'id':'contractCopy'}).text
                        if each.find('span',{'class':'mr-1'}):
                            infor['contractAddress_tag'] = each.find('span',{'class':'mr-1'}).text.replace('(','').replace(')','')
                        #infor['contractValue_ETH'] = float(data[1].split('TRANSFER')[1].split('Ether')[0].strip())

                        # transfer 데이터 추가
                        for item in each.find('div',{'class':'col-md-9'}).find('ul').findAll('li'):
                            toInfor = {'toAddress':None,
                                       'toAddress_tag':None,
                                       'toContract':False,
                                       'toValue_ETH':None
                                      }                   
                            # address_tag 있는 경우
                            if len(item.find('span',{'class':'hash-tag'})['data-original-title'].split('(')) > 1:
                                toInfor['toAddress'] = item.findAll('span',{'class':'hash-tag'})[-1]['data-original-title'].split('(')[1].replace(')','').strip()
                                toInfor['toAddress_tag'] = item.findAll('span',{'class':'hash-tag'})[-1].text.strip()
                            # address_tag 없는 경우
                            else:
                                toInfor['toAddress'] = item.text.split('To')[1].strip()
                            toInfor['toValue_ETH'] = float(item.text.split('Ether')[0].replace('TRANSFER','').strip())
                            infor['to'].append(toInfor)

                    # 일반 거래인 경우
                    else:
                        toInfor = {'toAddress':None,
                                   'toAddress_tag':None,
                                   'toContract':False,
                                   'toValue_ETH':None
                                  }
                        # address_tag 있는 경우 
                        if len(data[1].split('(')) > 1:
                            toInfor['toAddress'] = data[1].split('(')[0].strip()
                            toInfor['toAddress_tag'] = data[1].split('(')[1].replace(')','').strip()
                        # address_tag 없는 경우
                        else:
                            toInfor['toAddress'] = data[1].strip()  
                        infor['to'].append(toInfor)

                elif data[0].startswith('Value'):
                    infor['value_ETH'] = float(data[1].split('Ether')[0].replace(',','').strip())
                    infor['value_USD'] = float(data[1].split('($')[1].replace(')','').replace(',',''))
                elif data[0].startswith('Transaction Fee'):
                    infor['fee_ETH'] = float(data[1].split('Ether')[0].replace(',','').strip())
                    infor['fee_USD'] = float(data[1].split('($')[1].replace(')','').replace(',','').strip())
                elif data[0].startswith('Gas Limit'):
                    infor['gasLimit'] = float(data[1].replace(',',''))
                elif data[0].startswith('Gas Used'):
                    infor['gasUsed'] = float(data[1].split('(')[0].replace(',',''))
                    infor['gasUsed_percentage'] = float(data[1].split('(')[1].replace('%)','').strip())
                elif data[0].startswith('Gas Price'):
                    infor['gasPrice_ETH'] = float(data[1].split('Ether')[0].replace(',','').strip())
                    infor['gasPrice_Gwei'] = float(data[1].split('(')[1].replace('Gwei)','').replace(',','').strip())
                elif data[0].startswith('Input Data'):
                    infor['inputData'] = each.text.replace('Input Data:','').replace('Switch Back','').replace('\n',' ').split('View Input As')[0].strip()

        if infor['contractAddress'] == None and infor['to'][0]['toValue_ETH'] == None:
            infor['to'][0]['toValue_ETH'] = infor['value_ETH']

        return infor

    def getNormalTransactionByAddress(self, addr, targetTime, termDays):
        """
         address 해당 normal transaction 찾기
        """
        prevTimeLimit, nextTimeLimit = self.generateTimeTermByDay(targetTime, termDays)
        print('[ - ] ', prevTimeLimit)
        print('[ - ] ', targetTime)
        
        url = f'https://etherscan.io/txs?a={addr}&p=1'
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')    

        try:
            lastPage = int(soup.find('nav',{'aria-label':'page navigation'}).findAll('li')[-1].find('a')['href'].split('p=')[1])
        except:
            lastPage = 1

        res = []
        for page in range(1, lastPage+1):
            time.sleep(3)
            url = f'https://etherscan.io/txs?a={addr}&p={page}'
            self.driver.get(url)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            for each in soup.find('tbody').findAll('tr'):
                if each.text.startswith('There are no matching entries'):
                    return res
                infor = {'txid': each.findAll('td')[1].text,
                         'block': each.findAll('td')[2].text,
                         'time': datetime.datetime.strptime(each.findAll('td')[3].text, '%Y-%m-%d %H:%M:%S'),
                         'fromAddress':None,
                         'fromAddress_tag':None,
                         'fromContract':False,
                         'toAddress':None,
                         'toAddress_tag':None,
                         'toContract':False,
                         'value': float(each.findAll('td')[8].text.replace('Ether','').replace(',','').rstrip()),
                         'fee': float(each.findAll('td')[9].text)
                        }
                if each.findAll('td')[6].text.strip() == 'IN': 
                    if each.findAll('td')[5].text.startswith('0x'): infor['fromAddress'] = each.findAll('td')[5].text
                    else: 
                        infor['fromAddress'] = each.findAll('td')[5].find('a')['href'].split('/')[-1]
                        infor['fromAddress_tag'] = each.findAll('td')[5].text
                    infor['toAddress'] = addr
                    if each.findAll('td')[5].find('i'): infor['fromContract'] = True
                    if each.findAll('td')[7].find('i'): infor['toContract'] = True
                else:
                    infor['fromAddress'] = addr
                    if each.findAll('td')[7].text.startswith('0x'): infor['toAddress'] = each.findAll('td')[7].text
                    else:
                        infor['toAddress'] = each.findAll('td')[7].find('a')['href'].split('/')[-1]
                        infor['toAddress_tag'] = each.findAll('td')[7].text    
                    if each.findAll('td')[5].find('i'): infor['fromContract'] = True
                    if each.findAll('td')[7].find('i'): infor['toContract'] = True

                #print(infor)
                if infor['time'] < prevTimeLimit:
                    return res
                if infor['time'] < targetTime:
                    res.append(infor)
        return res
    
    def getBlockTimeByParentTxid(self, parentTxid):
        """
         Internal transaction 시간 가져오기 
        """
        info = self.getTxInfoByTxid(parentTxid)
        return info['block'], info['time']
    
    def getInternalTransactionByAddress(self, addr, targetTime, termDays):
        """
         address로 transaction Internal 찾기
        """
        prevTimeLimit, nextTimeLimit = self.generateTimeTermByDay(targetTime, termDays)
        print('[ - ] ', prevTimeLimit)
        print('[ - ] ', targetTime)

        url = f'https://etherscan.io/txsInternal?a={addr}&p=1'
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')    

        try:
            lastPage = int(soup.find('nav',{'aria-label':'page navigation'}).findAll('li')[-1].find('a')['href'].split('p=')[1])
        except:
            lastPage = 1

        res = []
        for page in range(1, lastPage+1):
            time.sleep(3)
            url = f'https://etherscan.io/txsInternal?a={addr}&p={page}'
            self.driver.get(url)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            for each in soup.find('tbody').findAll('tr'):
                infor = {'block':None,
                         'time':None,
                         'parent_txid': None,
                         'type':None,
                         'fromAddress':None,
                         'fromAddress_tag':None,
                         'fromContract':False,
                         'toAddress':None,
                         'toAddress_tag':None,
                         'toContract':False,
                         'value_ETH':None,
                         'value_wei':None
                        }
                if each.findAll('td')[0].find('a'): infor['block'] = each.findAll('td')[0].text
                if each.findAll('td')[1].find('span'): infor['time'] = datetime.datetime.strptime(each.findAll('td')[1].text, '%Y-%m-%d %H:%M:%S')

                # block 기록이 없는 경우
                if len(each.findAll('td')) == 8:
                    infor['parent_txid'] = each.findAll('td')[2].text.strip()
                    infor['block'], infor['time'] = self.getBlockTimeByParentTxid(infor['parent_txid'])
                    infor['type'] = each.findAll('td')[3].text.strip()
                    if each.findAll('td')[4].text.strip().startswith('0x'): infor['fromAddress'] = each.findAll('td')[4].text.strip()
                    else: 
                        try:
                            infor['fromAddress'] = each.findAll('td')[4].find('a')['data-original-title'].split('(')[1].replace(')','').strip()
                        except:
                            infor['fromAddress'] = each.findAll('td')[5].find('span')['data-original-title'].split('(')[1].replace(')','').strip()
                        infor['fromAddress_tag'] = each.findAll('td')[4].text.strip()
                    if each.findAll('td')[6].text.strip().startswith('0x'): infor['toAddress'] = each.findAll('td')[6].text.strip()
                    else:
                        infor['toAddress'] = each.findAll('td')[6].find('a')['href'].split('/')[-1].strip()
                        infor['toAddress_tag'] = each.findAll('td')[6].text.strip()
                    if each.findAll('td')[4].find('i'): infor['fromContract'] = True
                    if each.findAll('td')[6].find('i'): infor['toContract'] = True
                    # 정상 거래 완료된 경우
                    try:
                        infor['value_ETH'] = float(each.findAll('td')[7].text.replace('Ether','').replace(',','').rstrip())    
                    # 거래 오류난 경우
                    except:
                        infor['value_wei'] = float(each.findAll('td')[7].text.replace('wei','').replace(',','').rstrip()) 
                # block 기록이 있는 경우
                else:
                    infor['parent_txid'] = each.findAll('td')[3].text
                    infor['type'] = each.findAll('td')[4].text
                    # address_tag 업는 경우
                    if each.findAll('td')[5].text.strip().startswith('0x'): infor['fromAddress'] = each.findAll('td')[5].text.strip()
                    # address_tag 있는 경우
                    else: 
                        #print(each.findAll('td')[5].text)
                        try:
                            infor['fromAddress'] = each.findAll('td')[5].find('a')['data-original-title'].split('(')[1].replace(')','').strip()
                        except:
                            infor['fromAddress'] = each.findAll('td')[5].find('span')['data-original-title'].split('(')[1].replace(')','').strip()
                        infor['fromAddress_tag'] = each.findAll('td')[5].text.strip()
                    # address_tag 업는 경우
                    if each.findAll('td')[7].text.strip().startswith('0x'): infor['toAddress'] = each.findAll('td')[7].text.strip()
                    # address_tag 있는 경우
                    else:
                        infor['toAddress'] = each.findAll('td')[7].find('a')['href'].split('/')[-1]
                        infor['toAddress_tag'] = each.findAll('td')[7].text.strip()
                    if each.findAll('td')[5].find('i'): infor['fromContract'] = True
                    if each.findAll('td')[7].find('i'): infor['toContract'] = True
                    # 정상 거래 완려된 경우
                    try:
                        infor['value_ETH'] = float(each.findAll('td')[8].text.replace('Ether','').replace(',','').rstrip())
                    except:
                        infor['value_wei'] = float(each.findAll('td')[8].text.replace('wei','').replace(',','').rstrip())

                #print(infor)
                if infor['time'] != None and infor['time'] < prevTimeLimit:
                    return res
                
                res.append(infor)

        return res
    
    def getAddressInfo(self, address):
        """
         주소 정보 확인
        """
        url = f'https://etherscan.io/address/{address}'
        self.driver.get(url)

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        infor = {'address':address,
                 'address_tag': None,
                 'dateScraped':datetime.datetime.now(),
                 'balance_ETH':None,
                 'balance_USD':None,
                 'token_USD':0,
                 'token':[]
                }

        if soup.find('div',{'class':'card'}).find('span',{'class':'u-label'}):      
            infor['address_tag'] = soup.find('div',{'class':'card'}).find('span',{'class':'u-label'}).text.strip()

        infor['balance_ETH'] = float(soup.find('div',{'class':'card-body'}).findAll('div','col-md-8')[0].text.replace('Ether','').replace(',','').strip())
        infor['balance_USD'] = float(soup.find('div',{'class':'card-body'}).findAll('div',{'col-md-8'})[1].text.split('(')[0].replace('$','').replace(',','').strip())
        for item in soup.find('ul', {'class':'list'}).findAll('li'):
            tokenInfor = {'name':None,
                          'unit':None,
                          'amount':None,
                          'amount_USD':None,
                          'amount_USD_rate':None
                         }
            if item.find('span',{'class':'list-name'}):
                tokenInfor['name'] = item.find('span',{'class':'list-name'}).text.split('(')[0].strip()
                if item.find('span',{'class':'list-name'}).text.split('(')[0].strip().endswith('...'):
                    tokenInfor['name'] = item.find('span',{'class':'list-name'}).find('span')['title']
                tokenInfor['unit'] = item.find('span',{'class':'list-name'}).text.split('(')[1].replace(')','').strip()
                if tokenInfor['unit'].endswith('...'):
                    try:
                        tokenInfor['unit'] = item.find('span',{'class':'list-name'}).findAll('span')[1]['title']
                    except:
                        tokenInfor['unit'] = item.find('span',{'class':'list-name'}).findAll('span')[0]['title']
                tokenInfor['amount'] = float(item.find('span',{'class':'list-amount'}).text.replace('Token','').replace(tokenInfor['unit'],'').replace(',','').strip())

                if item.find('div',{'class':'text-right'}).text.startswith('$'):
                    tokenInfor['amount_USD'] = float(item.find('div',{'class':'text-right'}).text.split('@')[0].replace('$','').replace(',','').strip())
                    tokenInfor['amount_USD_rate'] = float(item.find('div',{'class':'text-right'}).text.split('@')[1].replace(',','').strip())
                    infor['token_USD'] += tokenInfor['amount_USD']
                #print(tokenInfor)
                infor['token'].append(tokenInfor)

        #print(infor)
        return infor    
    
    ######### ethplorer.io 사이트의 데이터 사용 #####

    def getContractOwnerAddressByContractAddress(self, contractAddr, APIKEY):
        """
         contract 생성자 주소 확인
        """
        url = f'http://api.ethplorer.io/getAddressInfo/{contractAddr}?apiKey={APIKEY}'
        req = requests.get(url)
        soup = req.json()

        info = soup['contractInfo']

        return info['creatorAddress']

if __name__=="__main__":
    driverPath = '/Users/Sujin/Desktop/chromedriver.exe'
    txid = '0x63c79e9c38835d75ecfe7d44c85b9f15be3d1bab073374dfbd177347dcd46b4a'
    addr = '0xabbb6bebfa05aa13e908eaa492bd7a8343760477'
    targetTime = datetime.datetime(2019,6,4,0,0,0)
    termDays = 10
    contractAddr = '0xfd9785b1148c6550e065a189d702560c67950d54'
    APIKEY = 'freekey'

    a = ethereumAnalyzer()
    a.setDriverPath(driverPath)
    a.setDriver()

    txInfo = a.getTxInfoByTxid(txid)

    nTx = a.getNormalTransactionByAddress(addr, targetTime, termDays)
    iTx = a.getInternalTransactionByAddress(addr, targetTime, termDays)

    a.getContractOwnerAddressByContractAddress(contractAddr, APIKEY)

    a.driver.close()