import click
import os
import pandas as pd

from typing import Annotated, Any, Dict, List, Tuple  # NOTE: Only Python 3.9+

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


# Calculates the health factor for a position
def _get_health_factor(position: Any) -> float:
    return (
        position.margin / position.safeMarginMinimum
        if position.safeMarginMinimum > 0
        else 0
    )


# @dev entries is list of tuples of (tokenId, result_position)
def _entries_to_data(entries: List[Tuple]) -> List[Dict]:
    data = []
    for token_id, position in entries:
        d = position.__dict__
        d.update({"tokenId": token_id, "healthFactor": _get_health_factor(position)})
        data.append(d)
    return data


# Has position in db
def _has_position_in_db(
    token_id: int, context: Annotated[Context, TaskiqDepends()]
) -> bool:
    return token_id in context.state.db.index


# Creates positions in db
# @dev entries is list of tuples of (tokenId, result_position)
def _create_positions_in_db(
    entries: List[Tuple], context: Annotated[Context, TaskiqDepends()]
):
    token_ids = [token_id for token_id, _ in entries]
    for token_id in token_ids:
        if token_id in context.state.db.index:
            raise Exception(f"tokenId {token_id} already exists in DB")

    data = _entries_to_data(entries)
    df = pd.DataFrame(data)
    df = df.set_index("tokenId")

    context.state.db = pd.concat([context.state.db, df])
    context.state.db.sort_values(by=["healthFactor"], inplace=True)
    click.echo(f"Created DB entries for tokenIds {token_ids}: {context.state.db}")


# Updates positions in db
# @dev entries is list of tuples of (tokenId, result_position)
def _update_positions_in_db(
    entries: List[Tuple], context: Annotated[Context, TaskiqDepends()]
):
    token_ids = [token_id for token_id, _ in entries]
    context.state.db.loc[token_ids]  # reverts with key error if not all exist

    data = _entries_to_data(entries)
    df = pd.DataFrame(data)
    df = df.set_index("tokenId")

    context.state.db.update(df)
    context.state.db.sort_values(by=["healthFactor"], inplace=True)
    click.echo(f"Updated DB entries for tokenIds {token_ids}: {context.state.db}")


# Deletes positions from db
def _delete_positions_from_db(
    entries: List[Tuple], context: Annotated[Context, TaskiqDepends()]
):
    token_ids = [token_id for token_id, _ in entries]
    token_ids_to_keep = [
        token_id for token_id in context.state.db.index if token_id not in token_ids
    ]
    context.state.db = context.state.db.loc[token_ids_to_keep]
    click.echo(f"Deleted DB entries for tokenIds {token_ids}: {context.state.db}")


# Gets a list of the token IDs stored in the db
def _get_token_ids_in_db(context: Annotated[Context, TaskiqDepends()]) -> List[int]:
    return context.state.db.index.to_list()


@app.on_startup()
def app_startup(startup_state: SilverbackStartupState):
    # TODO: process_history(start_block=startup_state.last_block_seen)
    return {"message": "Starting...", "block_number": startup_state.last_block_seen}


# Can handle some initialization on startup, like models or network connections
@app.on_worker_startup()
def worker_startup(state: TaskiqState):
    state.block_count = 0
    state.db = pd.DataFrame()  # in memory DB for now
    return {"message": "Worker started."}


# This is how we trigger off of new blocks
@app.on_(chain.blocks)
# context must be a type annotated kwarg to be provided to the task
def exec_block(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
    # TODO: chunk?
    token_ids = _get_token_ids_in_db(context)
    click.echo(
        f"Fetching position updates at block {block.number} for tokenIds: {token_ids}"
    )
    positions = [manager.positions(token_id) for token_id in token_ids]

    click.echo(f"Updating positions at block {block.number} for tokenIds ...")
    entries = list(zip(token_ids, positions))
    click.echo(f"Entries tokenIds fetched: {token_ids}")

    # remove liquidated positions from DB
    entries_liquidated = list(filter(lambda e: e[1].liquidated, entries))
    click.echo(
        f"Liquidated entries tokenIds to delete from DB: {[token_id for token_id, _ in entries_liquidated]}"
    )
    _delete_positions_from_db(entries_liquidated, context)

    # update non liquidated positions in DB
    entries_updated = list(filter(lambda e: (not e[1].liquidated), entries))
    click.echo(
        f"Synced entries tokenIds to update in DB: {[token_id for token_id, _ in entries_updated]}"
    )
    _update_positions_in_db(entries_updated, context)

    context.state.block_count += 1
    return len(block.transactions)


# This is how we trigger off of events
# Set new_block_timeout to adjust the expected block time.
# TODO: remove start block once process history implemented
@app.on_(manager.Mint, start_block=START_BLOCK)
def exec_manager_mint(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    click.echo(
        f"Manager minted position with tokenId {log.tokenId} at block {log.block_number}."
    )
    position = None
    try:
        position = manager.positions(log.tokenId)
        click.echo(f"Position currently has attributes: {position}")
        health_factor = _get_health_factor(position)
        click.echo(f"Position current health factor: {health_factor}")

        # add to DB if not yet liquidated
        if not position.liquidated:
            click.echo(f"Adding position with tokenId {log.tokenId} to database ...")
            entries = [(log.tokenId, position)]
            if not _has_position_in_db(log.tokenId, context):
                _create_positions_in_db(entries, context)
            else:
                _update_positions_in_db(entries, context)
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
