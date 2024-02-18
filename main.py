import click
import os
from typing import Annotated  # NOTE: Only Python 3.9+

from ape import chain, Contract
from ape.api import BlockAPI
from ape.exceptions import ContractLogicError
from ape.types import ContractLog
from taskiq import Context, TaskiqDepends, TaskiqState

from silverback import SilverbackApp, SilverbackStartupState

# TODO: Remove once add DB with process history
START_BLOCK = os.environ.get("START_BLOCK", None)
if START_BLOCK is not None:
    START_BLOCK = int(START_BLOCK)

# Do this to initialize your app
app = SilverbackApp()

# Nonfungible position manager contract
manager = Contract(os.environ["CONTRACT_ADDRESS_MRGLV1_NFT_MANAGER"])

# In-memory db for now
# TODO: Use actual DB
DB = {}


@app.on_startup()
def app_startup(startup_state: SilverbackStartupState):
    # TODO: process_history(start_block=startup_state.last_block_seen)
    return {"message": "Starting...", "block_number": startup_state.last_block_seen}


# Can handle some initialization on startup, like models or network connections
@app.on_worker_startup()
def worker_startup(state: TaskiqState):
    state.block_count = 0
    # state.db = MyDB()
    return {"message": "Worker started."}


# This is how we trigger off of new blocks
@app.on_(chain.blocks)
# context must be a type annotated kwarg to be provided to the task
def exec_block(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
    context.state.block_count += 1
    return len(block.transactions)


# This is how we trigger off of events
# Set new_block_timeout to adjust the expected block time.
# TODO: remove start block once process history implemented
@app.on_(manager.Mint, start_block=START_BLOCK)
def exec_manager_mint(log: ContractLog):
    click.echo(
        f"Manager minted position with tokenId {log.tokenId} at block {log.block_number}."
    )
    position = None
    try:
        position = manager.positions(log.tokenId)
        click.echo(f"Position currently has attributes: {position}")

        health_factor = (
            position.margin / position.safeMarginMinimum
            if position.safeMarginMinimum > 0
            else 0
        )
        click.echo(f"Position current health factor: {health_factor}")

        # add to DB if not yet liquidated
        if not position.liquidated:
            click.echo(f"Adding position with tokenId {log.tokenId} to database ...")
            DB[log.tokenId] = position
    except ContractLogicError as err:
        click.secho(
            f"Contract logic error when getting position: {err}", blink=True, bold=True
        )
    return {"token_id": log.tokenId, "position": position}


# Just in case you need to release some resources or something
@app.on_worker_shutdown()
def worker_shutdown(state):
    return {
        "message": f"Worker stopped after handling {state.block_count} blocks.",
        "block_count": state.block_count,
    }


# A final job to execute on Silverback shutdown
@app.on_shutdown()
def app_shutdown(state):
    return {"message": "Stopping..."}
