import requests
from web3 import Web3
from web3._utils.events import get_event_data
import web3
import json
import multiprocessing
import datetime
from datetime import timedelta
from tqdm import tqdm
import pandas as pd
import numpy as np
# from tqdm._tqdm import trange
from multiprocessing import Pool
import datetime
import time


# import mplfinance as mpf

def get_trans_info(i, j):
    try:
        contract_address = token_contract_set[j]
        event_template = contract_address.events.Transfer
        if i != iteration_num:
            filter = event_template.createFilter(fromBlock=hex(int(i * Block_internal)),
                                                 toBlock=hex(int((i + 1) * Block_internal - 1)))
        else:
            filter = event_template.createFilter(fromBlock=hex(int(i * Block_internal)),
                                                 toBlock='latest')
        events = filter.get_all_entries()
        print('events number', len(events))
        if len(events) == 0:
            return False
        else:
            for event in events:
                transactionHash = event.transactionHash
                transfer_info = w3.eth.get_transaction(transactionHash)
                Tx_Fee = transfer_info.value
                Tx_Fee = float(Web3.fromWei(Tx_Fee, 'ether'))
                block_num = event.blockNumber
                block_timestamp = w3.eth.getBlock(block_num).timestamp
                block_date_time = datetime.datetime.fromtimestamp(block_timestamp)
                datatimestr = datetime.datetime.strftime(block_date_time, '%Y-%m-%d %H:%M:%S')
                print('交易时间为：', datatimestr)

                nft_tr_info = pd.DataFrame({
                    'Datetime': datatimestr,
                    'ContractAddress': contract_address.address,
                    'TokenId': event.args.tokenId,
                    'From Address': transfer_info['from'],
                    'To Address': transfer_info.to,
                    'Value': Tx_Fee,
                    'BlockHash': transfer_info.blockHash.hex(),
                    'Blocknumber': block_num,
                    'TransactionHash': transactionHash.hex(),
                    'Gas': float(Web3.fromWei(transfer_info.gas, 'ether')),
                    'Gasprice': float(Web3.fromWei(transfer_info.gasPrice, 'ether'))}, index=[block_timestamp])
                nft_tr_info.to_csv('date/df_Transaction_history.csv', mode='a', index=True, header=False)
            return True
    except Exception as e:
        print(e)


'''
def func_call_back(res):
    global date_fee_dict
    date_fee_dict.update(res)#lst[index]=y
    print('use call_back function') #添加输出

def err_call_back(err):
    print(f'出错啦~ error：{str(err)}')
'''


def main():
    nft_collection_number = 0
    ERC_Standard = '721'
    # processes=12
    # pool = multiprocessing.Pool(processes=4)
    # 想办法使用多个线程，每个alchemy节点都可以开一个单独的线程，然后每个线程负责一个nft合约的扫描
    for j in range(len(token_contract_set)):
        print('合约地址为：', token_contract_set[j].address)
        # contract_totalId_number = contract_address.functions.totalSupply().call()
        for i in range(iteration_num, -1, -1):
            print("====================================================")
            print(i)

            x = get_trans_info(i, j)
            if not x:
                print('该交易历史回溯结束，进入下一个合约...')
                break
    # pool.close()
    # pool.join()



if __name__ == '__main__':
    # Alchemy_Key_Api = 'hPu-tophIgLWV-UFgJlZife49rmePmtS'
    # alchemy_url = "https://eth-mainnet.g.alchemy.com/v2/" + Alchemy_Key_Api
    # alchemy_wss_url = 'wss://eth-mainnet.g.alchemy.com/v2/' + Alchemy_Key_Api
    # element_api = '815dc346035a9565ff11ba6379146dbe'

    # Alchemy_Key_Api = 'BtKriOSkJwXY4JjVExxW8dar28ZBeY1m'
    # alchemy_url = "https://eth-mainnet.g.alchemy.com/v2/" + Alchemy_Key_Api
    # alchemy_wss_url = 'wss://eth-mainnet.g.alchemy.com/v2/' + Alchemy_Key_Api
    # # element_api = '815dc346035a9565ff11ba6379146dbe'

    alchemy_url_set = ['https://eth-mainnet.g.alchemy.com/v2/BtKriOSkJwXY4JjVExxW8dar28ZBeY1m',
                       'https://eth-mainnet.g.alchemy.com/v2/gw3OcPT1SboUT2dOKauzxrIOjC6DzJkj',
                       'https://eth-mainnet.g.alchemy.com/v2/hPu-tophIgLWV-UFgJlZife49rmePmtS']

    ERC721InterfaceId = '0x80ac58cd'
    ERC1155InterfaceId = '0xd9b67a26'

    w3 = Web3(Web3.HTTPProvider(alchemy_url_set[1]))
    # w3 = Web3(Web3.WebsocketProvider(alchemy_wss_url))
    print("节点是否可连接：", w3.isConnected())

    now_block_number = w3.eth.get_block('latest').number
    Block_internal = 1e4
    iteration_num = int(now_block_number // Block_internal)

    token_address_set = ['0x2438a0eeffa36cb738727953d35047fb89c81417',
                         '0xeb4e856f69158052ac0aaf7dc26f63dcb1ee067f',
                         '0xba627f3d081cc97ac0edc40591eda7053ac63532',
                         '0xbd5fb504d4482ef4366dfa0c0edfb85ed50a9bbb',
                         '0x08abed322775731d7b75dbdfe6151dc39ad83800']          # 对应的一些NFT合约地址
    token_address_set = [w3.toChecksumAddress(i) for i in token_address_set]

    with open('abi/ERC_721.json', 'r', encoding='utf-8') as f:
        abi_721 = json.load(f)
    with open('abi/ERC_1155.json', 'r', encoding='utf-8') as f:
        abi_1155 = json.load(f)

    token_contract_set = []
    for token_address in token_address_set:
        contract = w3.eth.contract(address=token_address, abi=abi_721)
        if contract.functions.supportsInterface(ERC721InterfaceId).call():
            token_contract_set.append(contract)

    print(token_contract_set)       # token_address_set里面5个地址，token_contract_set里面只有四个，是因为第一个地址"0x2438a0eeffa36cb738727953d35047fb89c81417"是erc1155的协议

    df_Transaction_history = pd.DataFrame(columns=['Datetime', 'ContractAddress', 'TokenId',
                                                   'From Address', 'To Address', 'Value', 'BlockHash',
                                                   'Blocknumber', 'TransactionHash', 'Gas', 'Gasprice'])
    df_Transaction_history.to_csv('date/df_Transaction_history.csv')

    main()


