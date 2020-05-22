import requests
from bs4 import BeautifulSoup

import datetime

import mongoDB14 as mongoDB

class etherscamdbCrawler:
    def setDB(self):
        dbName = 'ws_datas'
        collectionName = 'etherscamdb_address'
        keyField = 'address'        
        
        self.db = mongoDB.MongoConnector(dbName, collectionName)
        self.db.connect()
        self.db.setKeyField(keyField)
    
    def crawl(self):
        url = 'https://etherscamdb.info/api/scams/'
        req = requests.get(url)
        soup = req.json()
        
        for each in soup['result']:
            if 'addresses' in list(each.keys()):
                #print(each['addresses'])
                for item in each['addresses']:
                    infor = {'address':item,
                             'dateScraped':datetime.datetime.now()
                            }
                    print(infor)
                    self.db.insertOne(infor)

if __name__=="__main__":
    c = etherscamdbCrawler()
    c.setDB()   
    c.crawl()