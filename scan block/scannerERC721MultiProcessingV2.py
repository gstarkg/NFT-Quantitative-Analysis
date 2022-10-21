from web3 import Web3
import json
import pandas as pd
import datetime
from multiprocessing import Pool

haveCheckTransferEventsContractAddressSet = []  # 存储该区块中已经扫描过的ERC721合约的地址

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
        ERC1155InterfaceId = '0xd9b67a26'

        print("=================================================")
        print("目前扫描过的合约数量", len(haveCheckTransferEventsContractAddressSet))
        # 首先 扫描区块的交易哈希
        block = w3.eth.get_block(num)
        # 然后 根据交易哈希得到contract address
        for tx in block.transactions:
            transactionReceipt = w3.eth.get_transaction_receipt(tx.hex())
            # 判断to地址是否为合约地址，普通地址get code结果是0x，合约地址不是0x
            if w3.eth.get_code(w3.toChecksumAddress(transactionReceipt['to'])).hex() != "0x":
                # print("是合约地址, contract address: ", transactionReceipt['to'])

                # 然后 检查contract address是否属于ERC721
                # 如果是ERC721地址，还需要检查是否已经对该合约地址扫描过对应的Transfer事件，如果扫描过就不要对该合约进行扫描
                try:
                    contract_721 = w3.eth.contract(address=w3.toChecksumAddress(transactionReceipt['to']), abi=abi_721)
                    # contract_1155 = w3.eth.contract(address=w3.toChecksumAddress(transactionReceipt['to']), abi=abi_1155)
                    if (contract_721.functions.supportsInterface(ERC721InterfaceId).call()) and (transactionReceipt['to'] not in haveCheckTransferEventsContractAddressSet):
                        contractAddressErc721 = w3.toChecksumAddress(transactionReceipt['to'])
                        print("是新的 ERC721 合约，合约地址为 ", transactionReceipt['to'])

                        # 然后 如果属于就把Transfer event记录下来，否则就检查下一个
                        event_template = contract_721.events.Transfer
                        # 直接扫描该合约地址从2022.01.01到最新区块中的全部Transfer事件
                        filter = event_template.createFilter(fromBlock=hex(15053226),
                                                             toBlock=hex(w3.eth.get_block('latest')['number']))
                        events = filter.get_all_entries()

                        if len(events) > 0:
                            event_i = 0     # 记录扫描到第i个event
                            print("第" + repr(i) + "个进程, 区块号" + repr(num) + ", num events: " + repr(len(events)))
                            # 扫描过的合约地址要加入haveCheckTransferEventsContractAddressSet列表中，后续即使在其他区块中遇到也不需要重新扫描
                            haveCheckTransferEventsContractAddressSet.append(transactionReceipt['to'])
                            for event in events:
                                if event_i == 5:
                                    break
                                transactionHash = event.transactionHash
                                transfer_info = w3.eth.get_transaction(transactionHash)
                                Tx_Fee = transfer_info.value
                                Tx_Fee = float(Web3.fromWei(Tx_Fee, 'ether'))
                                if Tx_Fee > 0:
                                    block_num = event.blockNumber
                                    block_timestamp = w3.eth.getBlock(block_num).timestamp
                                    block_date_time = datetime.datetime.fromtimestamp(block_timestamp)
                                    datatimestr = datetime.datetime.strftime(block_date_time, '%Y-%m-%d %H:%M:%S')
                                    print("区块号 " + repr(num) + " 第" + repr(i) + "个进程, event Transfer 交易时间为：" + repr(
                                        datatimestr) + " " + repr(event_i) + "/" + repr(len(events)) + " 已检查" + repr(
                                        len(haveCheckTransferEventsContractAddressSet)))
                                    # # 调用合约的函数获取返回值的方法
                                    # print("&&&&&&  ", contract_721.functions.balanceOf(event['args']['to']).call())
                                    nft_tr_info = pd.DataFrame({
                                        'Datetime': datatimestr,
                                        'ContractAddress': transactionReceipt['to'],
                                        'Name': contract_721.functions.name().call(),
                                        'Symbol': contract_721.functions.symbol().call(),
                                        'TokenId': event['args']['tokenId'],
                                        'TokenURI': contract_721.functions.tokenURI(event['args']['tokenId']).call(),
                                        'From Address': event['args']['from'],
                                        'From ens': w3.ens.name(event['args']['from']),
                                        'To Address': event['args']['to'],
                                        'To ens': w3.ens.name(event['args']['to']),
                                        'To Address balanceOf': contract_721.functions.balanceOf(event['args']['to']).call(),
                                        'Value': Tx_Fee,
                                        'BlockHash': event['blockHash'].hex(),
                                        'Blocknumber': event['blockNumber'],
                                        'TransactionHash': transactionHash.hex(),
                                        'Gas': float(Web3.fromWei(transfer_info.gas, 'gwei')),
                                        'Gasprice': float(Web3.fromWei(transfer_info.gasPrice, 'gwei')),
                                        'Protocol': "ERC 721"},
                                        index=[block_timestamp])
                                    nft_tr_info.to_csv('date/df_Transaction_history__multi.csv', mode='a', index=True,
                                                       header=False)
                                    event_i = event_i + 1
                                # else:
                                #     event_i = event_i + 1

                    # elif (contract_1155.functions.supportsInterface(ERC1155InterfaceId).call()) and (transactionReceipt['to'] not in haveCheckTransferEventsContractAddressSet):
                    #     print("是新的 ERC1155 合约，合约地址为 ", transactionReceipt['to'])
                    #
                    #     # 然后 如果属于就把TransferSingle event记录下来，否则就检查下一个
                    #     event_template = contract_1155.events.TransferSingle
                    #     # 直接扫描该合约地址从2022.01.01到最新区块中的全部Transfer事件
                    #     filter = event_template.createFilter(fromBlock=hex(13916166),
                    #                                          toBlock=hex(w3.eth.get_block('latest')['number']))
                    #     events = filter.get_all_entries()
                    #
                    #     if len(events) > 0:
                    #         event_i = 0  # 记录扫描到第i个event
                    #         print("第" + repr(i) + "个进程, 区块号" + repr(num) + ", num events: " + repr(len(events)))
                    #         # 扫描过的合约地址要加入haveCheckTransferEventsContractAddressSet列表中，后续即使在其他区块中遇到也不需要重新扫描
                    #         haveCheckTransferEventsContractAddressSet.append(transactionReceipt['to'])
                    #         for event in events:
                    #             block_num = event.blockNumber
                    #             block_timestamp = w3.eth.getBlock(block_num).timestamp
                    #             block_date_time = datetime.datetime.fromtimestamp(block_timestamp)
                    #             datatimestr = datetime.datetime.strftime(block_date_time, '%Y-%m-%d %H:%M:%S')
                    #             print("区块号 " + repr(num) + " 第" + repr(i) + "个进程, event TransferSingle 交易时间为：" + repr(datatimestr) + " "  + repr(event_i) + "/" + repr(len(events)) + " 已检查" + repr(len(haveCheckTransferEventsContractAddressSet)))
                    #             transactionHash = event.transactionHash
                    #             transfer_info = w3.eth.get_transaction(transactionHash)
                    #             Tx_Fee = transfer_info.value
                    #             Tx_Fee = float(Web3.fromWei(Tx_Fee, 'gwei'))
                    #             nft_tr_info = pd.DataFrame({
                    #                 'Datetime': datatimestr,
                    #                 'ContractAddress': transactionReceipt['to'],
                    #                 'Name': ' ',
                    #                 'Symbol': ' ',
                    #                 'TokenId': event['args']['id'],
                    #                 'From Address': event['args']['from'],
                    #                 'To Address': event['args']['to'],
                    #                 'Value': Tx_Fee,
                    #                 'BlockHash': event['blockHash'].hex(),
                    #                 'Blocknumber': event['blockNumber'],
                    #                 'TransactionHash': tx.hex(),
                    #                 'Gas': float(Web3.fromWei(transfer_info.gas, 'gwei')),
                    #                 'Gasprice': float(Web3.fromWei(transfer_info.gasPrice, 'gwei')),
                    #                 'Protocol': "ERC 1155"},
                    #                 index=[block_timestamp])
                    #             nft_tr_info.to_csv('date/df_Transaction_history__multi.csv', mode='a', index=True,
                    #                                header=False)
                    #             event_i = event_i + 1

                except:
                    continue
        print("第" + repr(i) + "个进程结束 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    except:
        print("except")


if __name__ == "__main__":
    alchemy_url_set = ['https://eth-mainnet.g.alchemy.com/v2/BtKriOSkJwXY4JjVExxW8dar28ZBeY1m',
                       'https://eth-mainnet.g.alchemy.com/v2/gw3OcPT1SboUT2dOKauzxrIOjC6DzJkj',
                       'https://eth-mainnet.g.alchemy.com/v2/hPu-tophIgLWV-UFgJlZife49rmePmtS',
                       'https://eth-mainnet.g.alchemy.com/v2/NMRxu6oBULkj1QBjHYM6rQRDD1sZwx1E']
    w3 = Web3(Web3.HTTPProvider(alchemy_url_set[0]))

    df_Transaction_history = pd.DataFrame(columns=['Datetime', 'ContractAddress', 'Name', 'Symbol', 'TokenId', 'TokenURI',
                                                   'From Address', 'From ens', 'To Address', 'To ens', 'To Address balanceOf', 'Value', 'BlockHash',
                                                   'Blocknumber', 'TransactionHash', 'Gas', 'Gasprice', 'Protocol'])
    df_Transaction_history.to_csv('date/df_Transaction_history__multi.csv')

    # 多进程扫描从2022.01.01到当前区块的ERC721合约对应的Transfer事件
    # 4个alchemy节点对应4个进程，每次对4个区块进行扫描
    # 2022.01.01 13916166        2022.09.01 15449618      2022.07.01 15053226

    p = Pool(12)
    for num in range(w3.eth.get_block('latest')['number'], 15053226, -4):
        for i in range(4):
            p.apply_async(getEvent, args=(num - i, i))
    p.close()
    p.join()        # 主进程会卡在这里，等所有进程结束再继续执行

    # for num in range(w3.eth.get_block('latest')['number'], 13916166, -4):
    #     p = Pool(12)
    #     for i in range(4):
    #         p.apply_async(getEvent, args=(num - i, i))
    #     p.close()
    #     p.join()


