import requests
from bs4 import BeautifulSoup
import json
import datetime
import time

import mongoDB14 as mongoDB

class bitcoinAbuseCrawler:
    
    def __init__(self):
        pass
    def setApiKey(self, key):
        self.api_token = key
    
    def setDB(self, dbName, addressCollectionName, addressKeyField, reportCollectionName, reportKeyField)-> None:
        # addressDB connect
        self.addressdb = mongoDB.MongoConnector(dbName, addressCollectionName)
        self.addressdb.connect()
        self.addressdb.setKeyField(addressKeyField)
        
        # addressDB connect
        self.reportdb = mongoDB.MongoConnector(dbName, reportCollectionName)
        self.reportdb.connect()
        self.reportdb.setKeyField(reportKeyField)

    def getBitcoinAbuseAddress(self):
        page = 1
        lookupUrl = r'https://www.bitcoinabuse.com/api/reports/distinct?api_token={api_token}&page={page}'
        req = requests.get(lookupUrl.format(api_token = self.api_token, page = page))
        print('[ * ] address req -> ', req.url)
        soup = req.json()     

        while len(soup['data']) > 0:
            for item in soup['data']:
                infor = {'address':item['address'],
                         'nAbuseReport':item['count'],
                         'dateScraped':datetime.datetime.now()
                        }
                print(item['address'])
                self.addressdb.insertOne(infor)
            
            time.sleep(3)
            page += 1
            req = requests.get(lookupUrl.format(api_token = self.api_token, page = page))
            print('[ * ] address req -> ', req.url)
            soup = req.json()   
            
    def getAddressList(self):
        ea = list(self.addressdb.find())

        address = []
        for each in ea:
            address.append(each['address'])
            
        return address
            
    def formDate(self, dateString):
        dateList = dateString.split(' ')
        #May 12, 2020
        if len(dateList) == 3:
            year = int(dateList[-1])
            month = dateList[0]
            day = int(dateList[1].replace(',',''))
            
            if month == 'Jan':
                month = 1
            elif month == 'Feb':
                month = 2
            elif month == 'Mar':
                month = 3
            elif month == 'Apr':
                month = 4
            elif month == 'May':
                month = 5
            elif month == 'Jun':
                month = 6
            elif month == 'Jul':
                month = 7
            elif month == 'Aug':
                month = 8
            elif month == 'Sep':
                month = 9
            elif month == 'Oct':
                month = 10
            elif month == 'Nov':
                month = 11
            elif month == 'Dec':
                month = 12
                
            date = datetime.datetime(year, month, day, 0, 0, 0)
            
        return date
                  
    def getAbuseReport(self, address):

        dbNum = len(list(self.reportdb.find('address',address)))

        reportUrl = f'http://159.203.157.246/reports/{address}'
        req = requests.get(reportUrl)
        print('[ * ] report req -> ', req.url)
        soup = BeautifulSoup(req.text, 'html.parser')  
        
        # 오래된 순서대로 가져오려고 reverse()
        reports = soup.findAll('table')[1].find('tbody').findAll('tr')
        reports.reverse()
        
        for i, report in enumerate(reports):
            # 이미 가져온 오래된 report는 가져올 필요 없음
            if i < dbNum:
                continue
            infor = {'reportID':address.lstrip().rstrip()+'_'+str(i),
                     'address': address.lstrip().rstrip(),
                     'date': self.formDate(report.findAll('td')[0].text.lstrip().rstrip()),
                     'abuseType': report.findAll('td')[1].text.lstrip().rstrip(),
                     'abuser': report.findAll('td')[2].text.lstrip().rstrip(),
                     'description': report.findAll('td')[3].text.replace('\"','').replace('\r','').replace('\n','').replace('\t','').lstrip().rstrip()
                    }
            print(infor)
            #print(json.dumps(infor, indent=4, ensure_ascii=False))
            self.reportdb.insertOne(infor)

if __name__=="__main__":
    API_KEY = 'vyOShxZAsrN707CfiaUeR80laaxhcdT2UeZX9j0QPl2qy7BLw0oCsCRZx8Nw'

    c = bitcoinAbuseCrawler()
    c.setApiKey(API_KEY)
    c.setDB('ws_datas','bitcoinAbuse_address','address', 'bitcoinAbuse_report','reportID')
    #c.getBitcoinAbuseAddress()   

    addressList = c.getAddressList()

    for address in addressList:
        c.getAbuseReport(address)    
        time.sleep(3)