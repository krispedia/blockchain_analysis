from bs4 import BeautifulSoup
import requests
import re
import time
import datetime
import argparse

import mongoDB14 as mongoDB

class blockchainCrawler:
    
    def __init__(self):
        self.blockdb = None
        
    def setDB(self)-> None:
        sdbNameB = 'ws_datas'
        scollectionNameB='_block_m'
        skeyFieldB='block'

        sdbNameT = 'ws_datas'
        scollectionNameT='_transaction_m'
        skeyFieldT='txid'
        
        self.bdb = mongoDB.MongoConnector(sdbNameB, scollectionNameB)
        self.bdb.connect()
        self.bdb.setKeyField(skeyFieldB)

        self.tdb = mongoDB.MongoConnector(sdbNameT, scollectionNameT)
        self.tdb.connect()
        self.tdb.setKeyField(skeyFieldT)
        
    def getLastBlock(self):
        req = requests.get('https://www.blockchain.com/explorer?view=btc_blocks')
        soup = BeautifulSoup(req.text, 'html.parser')

        lastBlock = soup.find('div',{'class':'sc-1kj8up-0 jMNnbe'}).find('div',{'class':'sc-1g6z4xm-0 kfVgSf'}).find('a').text

        return int(lastBlock)
    
    def getLastBlockNumInDB(self):
        ea = list(self.bdb.find().sort('block',1))

        #print(ea[0]['block'])
        return ea[0]['block']

    def getBlock(self, ifrom, ito):  
        
        #lastBlockInDBNum = self.getLastBlockNumInDB()
        #print('[ - ] lastPage = ', lastBlockInDBNum)
        
        next_page = 'https://www.blockchain.com/btc/blocks?page=1/'
        human_time = datetime.datetime.strptime('2020-05-07 11:05:40', '%Y-%m-%d %H:%M:%S') 
        systemtime = 1588817142
        done = False
        first = True

        for blockNum in range(ifrom,ito):
            postUrl = 'https://www.blockchain.com/btc/block/'+ str(blockNum)
            try:
                #postUrl = 'https://www.blockchain.com/btc/block/'+ each_post.find('a')['href']
                #postUrl = 'https://www.blockchain.com/btc/block/'+ blockNum
                while True:
                    req = requests.get(postUrl)
                    time.sleep(2)
                    if req.status_code == 200:
                        break
                    time.sleep(10)
                print('[ * ] req -> ',req.url)
                soup = BeautifulSoup(req.text, 'html.parser')


                info_continer = soup.findAll('div',{'class': 'sc-8sty72-0 kcFwUU'})   
                hashData = info_continer[1].getText()

                if first:
                    #if self.blockdb.collection.count_documents({'hash':hashData}) > 0:
                    if self.bdb.collection.count_documents({'hash':hashData}) > 0:
                        print('[ - ] Find  documents')
                        continue
                    else:
                        first = False


                transUrl = 'https://blockchain.info/rawblock/' + hashData
                while True:
                    req = requests.get(transUrl)
                    if req.status_code == 200:
                        break
                    time.sleep(10)
                jsonData = req.json()
                print('[ * ] req json -> ',transUrl)

                infor = {
                        'dateScraped':datetime.datetime.now(),
                        'block':None,
                        'hash':None,
                        'confirmation':0,
                        'time':None,
                        'height':0,
                        'Miner':None,
                        'transactionNum':0,
                        'difficulty':None,
                        'merkleRoot':None,
                        'version':None,
                        'bits':0,
                        'weight':0,
                        'size':0,
                        'nonce':0,
                        'transactionVolume':None,
                        'blockReward':None,
                        'fee': None,
                         # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ wallet_transaction db 비슷~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
                        #'transaction':[]

                }
                infor['block'] = int(info_continer[7].getText().replace(',', ''))
                infor['hash'] = hashData
                infor['confirmation'] = int(info_continer[3].getText().replace(',', ''))
                temp_time = systemtime - jsonData['time']
                infor['time'] = human_time  -   datetime.timedelta(seconds = temp_time)
                infor['height'] = int(info_continer[7].getText().replace(',', ''))
                infor['Miner'] = info_continer[9].getText()
                infor['transactionNum'] = int(info_continer[11].getText().replace(',', ''))
                infor['difficulty'] = info_continer[13].getText().replace(',', '')
                infor['merkleRoot'] = info_continer[15].getText()
                infor['version'] = info_continer[17].getText()
                infor['bits'] = int(info_continer[19].getText().replace(',', ''))
                infor['weight'] = int(re.findall('\d*\.?\d+', info_continer[21].getText().replace(',', ''))[0])
                infor['size'] = int(re.findall('\d*\.?\d+', info_continer[23].getText().replace(',', ''))[0])
                infor['nonce'] = int(info_continer[25].getText().replace(',', ''))               
                infor['transactionVolume'] = re.findall('\d*\.?\d+', info_continer[27].getText().replace(',', ''))[0]
                infor['blockReward'] = re.findall('\d*\.?\d+', info_continer[29].getText().replace(',', ''))[0]
                infor['fee'] = re.findall('\d*\.?\d+', info_continer[31].getText().replace(',', ''))[0]
                print(infor)
                #print('\n')

                self.bdb.insertOne(infor)
                # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ wallet_transaction db 비슷~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
                transdDatas = jsonData['tx']
                for i, transData in  enumerate(transdDatas):
                    trans_infor = {
                                    'block': infor['block'],
                                    'block_pos': i,
                                    'txid':None,
                                    'time':None,
                                    'fee_BTC':None,
                                    'fee_satoshis/byte':None,
                                    'fee_satoshis/WU':None,
                                    'size':0,
                                    'inputs':[],
                                    'outputs':[]
                    }
                    trans_infor['txid'] = transData['hash']
                    temp_time = systemtime - transData['time']
                    trans_infor['time'] = human_time  -   datetime.timedelta(seconds = temp_time)
                    trans_infor['fee_BTC'] = '{0:.8f}'.format(transData['fee'] * 0.00000001)
                    trans_infor['size'] = transData['size']
                    trans_infor['fee_satoshis/byte'] = '{0:.3f}'.format(round(transData['fee'] / trans_infor['size'] , 3))
                    trans_infor['fee_satoshis/WU'] = '{0:.3f}'.format(round(transData['fee'] / transData['weight'] , 3))

                    for inputData in transData['inputs']:
                        input_infor = {
                                       "address":None,
                                       "value":None
                        }
                        try:
                            inputs_container = inputData['prev_out']
                            input_infor['address'] = inputData['prev_out']['addr']
                            input_infor['value'] = '{0:.8f}'.format(inputData['prev_out']['value'] * 0.00000001)
                        except Exception as e:
                            pass

                        trans_infor['inputs'].append(input_infor)

                    for outputData in transData['out']:
                        output_infor = {
                                        "address" : None,
                                        "value": None,
                                        "spent": False
                         }
                        try:
                            output_infor['address'] = outputData['addr']
                        except Exception as e:
                            pass

                        output_infor['value'] = '{0:.8f}'.format(outputData['value'] * 0.00000001)
                        output_infor['spent'] = outputData['spent']
                        trans_infor['outputs'].append(output_infor)

                    #print('transaction')
                    #print(trans_infor['time'])
                    print(trans_infor)
                    #print('\n')

                    #infor['transaction'].append(trans_infor)
                    self.tdb.insertOne(trans_infor)
                #self.blockdb.insertOne(infor) 

            except Exception as e:
                print(postUrl)
                print(next_page)
                print(e)
                return next_page
        return None

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

    c = blockchainCrawler()
    c.setDB()
    c.getLastBlock()

    next_page = c.getBlock(ifrom, ito)
