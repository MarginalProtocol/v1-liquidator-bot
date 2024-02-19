# v1-liquidator-bot

Liquidator bot that liquidates unsafe [Marginal v1](https://github.com/MarginalProtocol/v1-core) leverage positions.

## Installation

The repo uses [ApeWorX](https://github.com/apeworx/ape) and [Silverback](https://github.com/apeworx/silverback) for development.

Set up a virtual environment

```sh
python -m venv .venv
source .venv/bin/activate
```

Install requirements and Ape plugins

```sh
pip install -r requirements.txt
ape plugins install .
```

## Usage

Include environment variables for the address of the [`NonfungiblePositionManager`](https://github.com/MarginalProtocol/book/blob/main/src/v1/periphery/contracts/NonfungiblePositionManager.sol/contract.NonfungiblePositionManager.md) contract
and the address of an example [`MarginalV1Pool`](https://github.com/MarginalProtocol/book/blob/main/src/v1/core/contracts/MarginalV1Pool.sol/contract.MarginalV1Pool.md) contract verified on the network

```sh
export CONTRACT_ADDRESS_MRGLV1_NFT_MANAGER=<address of nft manager contract on network>
export CONTRACT_ADDRESS_MRGLV1_POOL_EXAMPLE=<address of mrgl v1 pool example contract on network>
```

The latter is only used to more easily construct transaction calldata with `ape`, so can be any pool.

Then run silverback


```sh
silverback run "main:app" --network :mainnet:alchemy --account acct-name
```
