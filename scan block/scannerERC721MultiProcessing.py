# 请使用scannerERC721MultiProcessingV2版本

from web3 import Web3
import json
import pandas as pd
import datetime
from multiprocessing import Pool

def getEvent(num, i):
    # num 区块号
    # i alchemy节点号

    alchemy_url_set = ['https://eth-mainnet.g.alchemy.com/v2/BtKriOSkJwXY4JjVExxW8dar28ZBeY1m',
                       'https://eth-mainnet.g.alchemy.com/v2/gw3OcPT1SboUT2dOKauzxrIOjC6DzJkj',
                       'https://eth-mainnet.g.alchemy.com/v2/hPu-tophIgLWV-UFgJlZife49rmePmtS',
                       'https://eth-mainnet.g.alchemy.com/v2/NMRxu6oBULkj1QBjHYM6rQRDD1sZwx1E']

    # i = num % 4
    w3 = Web3(Web3.HTTPProvider(alchemy_url_set[i]))

    try:
        with open('abi/ERC_721.json', 'r', encoding='utf-8') as f:
            abi_721 = json.load(f)
        ERC721InterfaceId = '0x80ac58cd'
        with open('abi/ERC_1155.json', 'r', encoding='utf-8') as f:
            abi_1155 = json.load(f)

        print("===================================  ", num)
        print("进程 ", i)
        # 首先 扫描区块的交易哈希
        block = w3.eth.get_block(num)
        haveCheckTransferEventsContractAddressSet = []  # 存储该区块中已经扫描过的ERC721合约的地址，防止每个交易都扫描整个区块中的事件，导致重复
        # 然后 根据交易哈希得到contract address
        for tx in block.transactions:
            transactionReceipt = w3.eth.get_transaction_receipt(tx.hex())
            # 判断to地址是否为合约地址，普通地址get code结果是0x，合约地址不是0x
            if w3.eth.get_code(w3.toChecksumAddress(transactionReceipt['to'])).hex() != "0x":
                # print("是合约地址, contract address: ", transactionReceipt['to'])

                # 然后 检查contract address是否属于ERC721
                try:
                    contract = w3.eth.contract(address=w3.toChecksumAddress(transactionReceipt['to']), abi=abi_721)
                    if (contract.functions.supportsInterface(ERC721InterfaceId).call()) and (
                            transactionReceipt['to'] not in haveCheckTransferEventsContractAddressSet):
                        contractAddressErc721 = w3.toChecksumAddress(transactionReceipt['to'])
                        print("是 ERC721 合约，合约地址为 ", transactionReceipt['to'])

                        # 然后 如果属于就把Transfer event记录下来，否则就检查下一个
                        event_template = contract.events.Transfer
                        # !!!! 这里会导致出现重复的，因为现在的逻辑是，确定是Transfer交易就会扫描整个区块的该合约所有的Transfer交易，
                        # 所以增加了haveCheckTransferEventsContractAddressSet防止出现重复
                        filter = event_template.createFilter(fromBlock=hex(block['number']),
                                                             toBlock=hex(block['number']))
                        events = filter.get_all_entries()

                        if len(events) > 0:
                            haveCheckTransferEventsContractAddressSet.append(transactionReceipt['to'])
                            for event in events:
                                block_num = event.blockNumber
                                block_timestamp = w3.eth.getBlock(block_num).timestamp
                                block_date_time = datetime.datetime.fromtimestamp(block_timestamp)
                                datatimestr = datetime.datetime.strftime(block_date_time, '%Y-%m-%d %H:%M:%S')
                                print('event Transfer 交易时间为：', datatimestr)
                                transactionHash = event.transactionHash
                                transfer_info = w3.eth.get_transaction(transactionHash)
                                Tx_Fee = transfer_info.value
                                Tx_Fee = float(Web3.fromWei(Tx_Fee, 'ether'))
                                nft_tr_info = pd.DataFrame({
                                    'Datetime': datatimestr,
                                    'ContractAddress': transactionReceipt['to'],
                                    'TokenId': event['args']['tokenId'],
                                    'From Address': event['args']['from'],
                                    'To Address': event['args']['to'],
                                    'Value': Tx_Fee,
                                    'BlockHash': event['blockHash'].hex(),
                                    'Blocknumber': event['blockNumber'],
                                    'TransactionHash': tx.hex(),
                                    'Gas': float(Web3.fromWei(transfer_info.gas, 'ether')),
                                    'Gasprice': float(Web3.fromWei(transfer_info.gasPrice, 'ether'))},
                                    index=[block_timestamp])
                                nft_tr_info.to_csv('date/df_Transaction_history__multi.csv', mode='a', index=True,
                                                   header=False)

                except:
                    continue

    except:
        print("except")


if __name__ == "__main__":
    alchemy_url_set = ['https://eth-mainnet.g.alchemy.com/v2/BtKriOSkJwXY4JjVExxW8dar28ZBeY1m',
                       'https://eth-mainnet.g.alchemy.com/v2/gw3OcPT1SboUT2dOKauzxrIOjC6DzJkj',
                       'https://eth-mainnet.g.alchemy.com/v2/hPu-tophIgLWV-UFgJlZife49rmePmtS',
                       'https://eth-mainnet.g.alchemy.com/v2/NMRxu6oBULkj1QBjHYM6rQRDD1sZwx1E']
    w3 = Web3(Web3.HTTPProvider(alchemy_url_set[0]))

    df_Transaction_history = pd.DataFrame(columns=['Datetime', 'ContractAddress', 'TokenId',
                                                   'From Address', 'To Address', 'Value', 'BlockHash',
                                                   'Blocknumber', 'TransactionHash', 'Gas', 'Gasprice'])
    df_Transaction_history.to_csv('date/df_Transaction_history__multi.csv')

    p = Pool(4)
    for num in range(w3.eth.get_block('latest')['number'], 13916166, -4):
        for i in range(4):
            p.apply_async(getEvent, args=(num - i, i))
    p.close()
    p.join()


