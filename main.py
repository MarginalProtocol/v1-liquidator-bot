import click
import os
import pandas as pd

from typing import Annotated, Any, Dict, List, Tuple  # NOTE: Only Python 3.9+

from ape import chain, Contract
from ape.api import BlockAPI
from ape.exceptions import TransactionError
from ape.types import ContractLog
from ape_aws.accounts import KmsAccount

from taskiq import Context, TaskiqDepends, TaskiqState

from silverback import AppState, SilverbackApp


# Do this to initialize your app
app = SilverbackApp()

# Position viewer contract
position_viewer = Contract(os.environ["CONTRACT_ADDRESS_POSITION_VIEWER"])

# Marginal v1 pool contract
pool = Contract(os.environ["CONTRACT_ADDRESS_MARGV1_POOL"])

# Multicall (mds1)
# @dev Ref @mds1/multicall/src/Multicall3.sol
multicall3 = Contract("0xcA11bde05977b3631167028862bE2a173976CA11")

# TODO: Remove once add DB with process history
START_BLOCK = os.environ.get("START_BLOCK", None)
if START_BLOCK is not None:
    START_BLOCK = int(START_BLOCK)

# Recipient of liquidation rewards
RECIPIENT_ADDRESS = os.environ.get("RECIPIENT_ADDRESS", None)

# Maximum fraction of gas limit to use for multicall liquidations expressed as denominator
MAX_FRACTION_GAS_LIMIT_DENOMINATOR = os.environ.get(
    "MAX_FRACTION_GAS_LIMIT_DENOMINATOR", 6
)

# Gas estimate for the pool liquidate function
LIQUIDATE_GAS_ESTIMATE = 150_000

# Buffer to add to transaction fee estimate: txn_fee *= 1 + BUFFER
TXN_FEE_BUFFER = os.environ.get("TXN_FEE_BUFFER", 0.125)

# Whether to execute transaction through private mempool
TXN_PRIVATE = os.environ.get("TXN_PRIVATE", False)

# Required confirmations to wait for transaction to go through
TXN_REQUIRED_CONFIRMATIONS = os.environ.get("TXN_REQUIRED_CONFIRMATIONS", 1)

# Whether to ask to enable autosign for local account
PROMPT_AUTOSIGN = app.signer and not isinstance(app.signer, KmsAccount)


# Calculates the health factor for a position
def _get_health_factor(position: Any) -> float:
    return (
        position.margin / position.safeMarginMinimum
        if position.safeMarginMinimum > 0
        else 0
    )


# @dev entries is list of tuples of (owner, id, result_position)
def _entries_to_data(entries: List[Tuple]) -> List[Dict]:
    data = []
    for owner, id, position in entries:
        d = position.__dict__
        d.update(
            {
                "owner": owner,
                "id": id,
                "healthFactor": _get_health_factor(position),
            }
        )
        data.append(d)
    return data


# Has position in db
def _has_position_in_db(id: int, context: Annotated[Context, TaskiqDepends()]) -> bool:
    return id in context.state.db.index


# Creates positions in db
# @dev entries is list of tuples of (owner, id, result_position)
def _create_positions_in_db(
    entries: List[Tuple], context: Annotated[Context, TaskiqDepends()]
):
    data = _entries_to_data(entries)
    df = pd.DataFrame(data)
    df = df.set_index("id")

    context.state.db = pd.concat([context.state.db, df])
    context.state.db.sort_values(by=["healthFactor"], inplace=True)
    click.echo(f"Created DB entries for {len(entries)} entries: {context.state.db}")


# Updates positions in db
# @dev entries is list of tuples of (owner, id, result_position)
def _update_positions_in_db(
    entries: List[Tuple], context: Annotated[Context, TaskiqDepends()]
):
    ids = [id for owner, id, _ in entries]
    context.state.db.loc[ids]  # reverts with key error if not all exist

    data = _entries_to_data(entries)
    df = pd.DataFrame(data)
    df = df.set_index("id")

    context.state.db.update(df)
    context.state.db.sort_values(by=["healthFactor"], inplace=True)
    click.echo(f"Updated DB entries for {len(entries)} entries: {context.state.db}")


# Deletes positions from db
def _delete_positions_from_db(
    ids: List[int], context: Annotated[Context, TaskiqDepends()]
):
    ids_to_keep = [id for id in context.state.db.index if id not in ids]
    context.state.db = context.state.db.loc[ids_to_keep]
    click.echo(f"Deleted DB entries for {len(ids)} entries: {context.state.db}")


# Gets a list of the position IDs and associated owners stored in the db
# @dev Returned entry keys is tuple of lists of (owners, ids)
def _get_position_entry_keys_in_db(
    context: Annotated[Context, TaskiqDepends()]
) -> Tuple[List[str], List[int]]:
    if context.state.db.empty:
        return ([], [])

    owners = context.state.db["owner"].to_list()
    ids = context.state.db["id"].to_list()
    return (owners, ids)


# Gets a list of unsafe and profitable to liquidate positions from db, returning df records {"index": {"column": "value"}}
def _get_liquidatable_position_records_from_db(
    min_rewards: int, max_records: int, context: Annotated[Context, TaskiqDepends()]
) -> Dict:
    db_filtered = context.state.db[
        (~context.state.db["safe"]) & (context.state.db["rewards"] >= min_rewards)
    ].head(max_records)
    click.echo(
        f"Liquidatable positions > min rewards of {min_rewards} with max records of {max_records}: {db_filtered}"
    )
    return db_filtered.to_dict(orient="index")


# Gets the transaction fee estimate to execute the liquidations
def _get_txn_fee(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
    return int(block.base_fee * LIQUIDATE_GAS_ESTIMATE * (1 + TXN_FEE_BUFFER))


@app.on_startup()
def app_startup(startup_state: AppState):
    # set up autosign if desired
    if PROMPT_AUTOSIGN and click.confirm("Enable autosign?"):
        app.signer.set_autosign(enabled=True)

    # TODO: process_history(start_block=startup_state.last_block_seen)
    return {"message": "Starting...", "block_number": startup_state.last_block_seen}


# Can handle some initialization on startup, like models or network connections
@app.on_worker_startup()
def worker_startup(state: TaskiqState):
    state.block_count = 0
    state.db = pd.DataFrame()  # in memory DB for now
    state.recipient = (
        RECIPIENT_ADDRESS if RECIPIENT_ADDRESS is not None else app.signer.address
    )
    state.signer_balance = app.signer.balance
    return {"message": "Worker started."}


# Profitably liquidates unsafe positions on pools via multicall, returning list of position IDs liquidated
def liquidate_positions(
    block: BlockAPI, context: Annotated[Context, TaskiqDepends()]
) -> List[int]:
    min_rewards = _get_txn_fee(block, context)
    max_gas_limit = app.provider.max_gas // MAX_FRACTION_GAS_LIMIT_DENOMINATOR
    max_records = max_gas_limit // LIQUIDATE_GAS_ESTIMATE

    click.echo(f"Min rewards at block {block.number}: {min_rewards}")
    click.echo(f"Max records at block {block.number}: {max_records}")

    records = _get_liquidatable_position_records_from_db(
        min_rewards, max_records, context
    )
    ids = list(records.keys())
    click.echo(f"Liquidatable position IDs: {ids}")

    if len(ids) == 0:
        return ids

    # format into multicall3 calldata: (address target, bool allowFailure, bytes callData)
    # each calls to pool.liquidate(address recipient, address owner, uint96 id)
    calldata = [
        (
            record["pool"],
            False,
            pool.liquidate.as_transaction(
                context.state.recipient,
                record["owner"],
                int(record["id"]),
            ).data,
        )
        for _, record in records.items()
    ]

    # preview before sending in case of revert
    try:
        click.echo(
            f"Submitting multicall liquidation transaction for position IDs: {ids}"
        )
        multicall3.aggregate3(
            calldata,
            sender=app.signer,
            required_confirmations=TXN_REQUIRED_CONFIRMATIONS,
            private=TXN_PRIVATE,
        )
    except TransactionError as err:
        # didn't liquidate any positions so reset position IDs returned to empty
        click.secho(
            f"Transaction error on position liquidations: {err}",
            blink=True,
            bold=True,
        )
        ids = []

    return ids


# This is how we trigger off of new blocks
@app.on_(chain.blocks)
# context must be a type annotated kwarg to be provided to the task
def exec_block(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
    # TODO: chunk query?
    # @dev entry keys is tuple of lists of (owners, ids)
    owners, ids = _get_position_entry_keys_in_db(context)

    # short circuit if no position ids in db
    if len(ids) == 0:
        click.echo(f"No positions in db at block {block.number} ...")
        context.state.signer_balance = app.signer.balance
        context.state.block_count += 1
        return len(block.transactions)

    click.echo(
        f"Fetching position updates at block {block.number} for position IDs: {ids}"
    )

    positions = [
        position_viewer.positions(pool.address, owner, id)
        for owner, id in list(zip(owners, ids))
    ]

    click.echo(f"Updating positions at block {block.number} for position keys ...")
    entries = list(zip(owners, ids, positions))

    # remove liquidated positions from DB
    entries_liquidated = list(filter(lambda e: e[1].liquidated, entries))
    ids_liquidated = [id for _, id, _ in entries_liquidated]
    click.echo(f"Liquidated entries position IDs to delete from DB: {ids_liquidated}")
    _delete_positions_from_db(ids_liquidated, context)

    # update non liquidated positions in DB
    entries_updated = list(filter(lambda e: (not e[1].liquidated), entries))
    click.echo(
        f"Synced entries position IDs to update in DB: {[id for _, id, _ in entries_updated]}"
    )
    _update_positions_in_db(entries_updated, context)

    # multicall liquidate calls to chain
    ids_liquidated_by_bot = liquidate_positions(block, context)

    # remove additional successful liquidations caused by bot from db
    click.echo(
        f"Liquidated by bot entries position IDs to delete from DB: {ids_liquidated_by_bot}"
    )
    _delete_positions_from_db(ids_liquidated_by_bot, context)

    context.state.signer_balance = app.signer.balance
    context.state.block_count += 1
    return {
        "block_count": context.state.block_count,
        "signer_balance": context.state.signer_balance,
        "positions_count": len(entries_updated) - len(ids_liquidated_by_bot),
    }


# This is how we trigger off of events
# Set new_block_timeout to adjust the expected block time.
@app.on_(pool.Open, start_block=START_BLOCK)
def exec_pool_open(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    click.echo(
        f"Pool opened position with owner {log.owner} and ID {log.id} at block {log.block_number}."
    )
    position = None
    try:
        position = position_viewer.positions(pool.address, log.owner, log.id)
        click.echo(f"Position currently has attributes: {position}")
        health_factor = _get_health_factor(position)
        click.echo(f"Position current health factor: {health_factor}")

        # add to DB if not yet liquidated
        if not position.liquidated:
            click.echo(
                f"Adding position with owner {log.owner} and ID {log.id} to database ..."
            )
            entries = [(log.owner, log.id, position)]
            if not _has_position_in_db(log.id, context):
                _create_positions_in_db(entries, context)
            else:
                _update_positions_in_db(entries, context)
    except TransactionError as err:
        click.secho(
            f"Transaction error on mint event: {err}",
            blink=True,
            bold=True,
        )

    return {"owner": log.owner, "id": log.id, "position": position}


# Just in case you need to release some resources or something
@app.on_worker_shutdown()
def worker_shutdown(state):
    return {
        "message": f"Worker stopped after handling {state.block_count} blocks.",
        "block_count": state.block_count,
    }


# A final job to execute on Silverback shutdown
@app.on_shutdown()
def app_shutdown():
    return {"message": "Stopping..."}
