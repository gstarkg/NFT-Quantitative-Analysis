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
import os, time, random
import datetime
import time


# import mplfinance as mpf

# 子进程-----使用不同的alchemy节点扫描不同的nft合约
# 多进程 https://www.liaoxuefeng.com/wiki/1016959663602400/1017628290184064
def childProcessScan(name, alchemy_url_set, token_address_set, iteration_num, Block_internal):
    w3 = Web3(Web3.HTTPProvider(alchemy_url_set[name]))     # 不同进程使用不同的alchemy节点
    print(name)
    print("节点是否可连接：", w3.isConnected())

    token_address_set = [w3.toChecksumAddress(i) for i in token_address_set]
    with open('abi/ERC_721.json', 'r', encoding='utf-8') as f:
        abi_721 = json.load(f)
    with open('abi/ERC_1155.json', 'r', encoding='utf-8') as f:
        abi_1155 = json.load(f)
    token_contract_set = []
    # 检查合约是否属于ERC721
    ERC721InterfaceId = '0x80ac58cd'
    ERC1155InterfaceId = '0xd9b67a26'
    for token_address in token_address_set:
        contract = w3.eth.contract(address=token_address, abi=abi_721)
        if contract.functions.supportsInterface(ERC721InterfaceId).call():
            token_contract_set.append(contract)

    for i in range(iteration_num, -1, -1):
        print("====================================================")
        print("第 %d 个进程--------第 %d 个区块", name, i*Block_internal)

        # 每次开的新的线程，选择一个新的alchemy节点和一个新的nft721合约。
        x = get_trans_info(i, w3, token_contract_set[name], iteration_num, Block_internal)
        if not x:
            print('该交易历史回溯结束，进入下一个合约...')
            break

    print("!!!!!!!!!!!!!!!!!!   第 %d 个进程结束", name)

def get_trans_info(i, w3, token_contract, iteration_num, Block_internal):
    try:
        contract_address = token_contract
        event_template = contract_address.events.Transfer       #
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

if __name__ == '__main__':
    token_address_set = ['0x2438a0eeffa36cb738727953d35047fb89c81417',
                         '0xeb4e856f69158052ac0aaf7dc26f63dcb1ee067f',
                         '0xba627f3d081cc97ac0edc40591eda7053ac63532',
                         '0xbd5fb504d4482ef4366dfa0c0edfb85ed50a9bbb',
                         '0x08abed322775731d7b75dbdfe6151dc39ad83800']  # 对应的一些NFT合约地址

    alchemy_url_set = ['https://eth-mainnet.g.alchemy.com/v2/BtKriOSkJwXY4JjVExxW8dar28ZBeY1m',
                       'https://eth-mainnet.g.alchemy.com/v2/gw3OcPT1SboUT2dOKauzxrIOjC6DzJkj',
                       'https://eth-mainnet.g.alchemy.com/v2/hPu-tophIgLWV-UFgJlZife49rmePmtS',
                       'https://eth-mainnet.g.alchemy.com/v2/NMRxu6oBULkj1QBjHYM6rQRDD1sZwx1E']

    w3 = Web3(Web3.HTTPProvider(alchemy_url_set[0]))
    now_block_number = w3.eth.get_block('latest').number        # 当前区块高度
    Block_internal = 1e4
    iteration_num = int(now_block_number // Block_internal)

    df_Transaction_history = pd.DataFrame(columns=['Datetime', 'ContractAddress', 'TokenId',
                                                   'From Address', 'To Address', 'Value', 'BlockHash',
                                                   'Blocknumber', 'TransactionHash', 'Gas', 'Gasprice'])
    df_Transaction_history.to_csv('date/df_Transaction_history.csv')

    p = Pool(10)
    for i in range(4):
        # 无法直接传入w3 = Web3(Web3.HTTPProvider('https://<your-provider-url>'))，这个很麻烦
        p.apply_async(childProcessScan, args=(i, alchemy_url_set, token_address_set, iteration_num, Block_internal))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
    # 使用两个线程一起确实比之前更快，但需要查看是否正确
    # 现在还有一个问题是，当某个进程完成后，即这个alchemy节点扫描完对应的nft合约地址后，就直接退出了。并不会扫描接下来还未被扫描的地址
    # 有段时间四个进程不能同时运行，只有两个进程在工作
    # 如果之后有更多的合约，需要一个进程结束后，这个进程还要去继续扫描没有开始的合约

    # 出现次这个， 'code': -32602, 'message': 'Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range and no limit on the response size, or you can request any block range with a cap of 10K logs in the response. Based on your parameters and the response size limit, this block range should work: [0xeea5c0, 0xeeb042]
    # 然后 该交易历史回溯结束，进入下一个合约...   随后进程结束
    # 不知道这个出现是不是就说明该合约的交易已经全部扫描完成

    # 有没有可能不需要每个合约单独扫描链，只需要扫描一次区块链，然后从中找对应的nft合约对应的事件  w3.eth.filter





