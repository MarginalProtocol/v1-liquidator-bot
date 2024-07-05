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

Include environment variables for the address of the [`PositionViewer`](https://github.com/MarginalProtocol/book/blob/main/src/v1/periphery/contracts/lens/PositionViewer.sol/contract.PositionViewer.md)
and the address of the [`MarginalV1Pool`](https://github.com/MarginalProtocol/book/blob/main/src/v1/core/contracts/MarginalV1Pool.sol/contract.MarginalV1Pool.md) contract verified on the network

```sh
export CONTRACT_ADDRESS_POSITION_VIEWER=<address of the position viewer contract on network>
export CONTRACT_ADDRESS_MARGV1_POOL=<address of marginal v1 pool contract on network>
```

Then run silverback

```sh
silverback run "main:app" --network :mainnet:alchemy --account acct-name
```
