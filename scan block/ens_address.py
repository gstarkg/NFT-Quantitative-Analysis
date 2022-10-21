from web3 import Web3
from ens import ENS
# from ens.auto import ns

if __name__ == "__main__":
    api_url = "https://eth-mainnet.g.alchemy.com/v2/gw3OcPT1SboUT2dOKauzxrIOjC6DzJkj"
    provider = Web3.HTTPProvider(api_url)
    w3 = Web3(provider)

    eth_address = w3.ens.address('jasoncarver.eth')
    print(eth_address)

    ens_name = w3.ens.name('0x5B2063246F2191f18F2675ceDB8b28102e957458')
    print(ens_name)