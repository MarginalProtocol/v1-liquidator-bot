import click
import os
import pandas as pd
import numpy as np

from typing import Annotated, Any, Dict, List, Optional, Tuple  # NOTE: Only Python 3.9+

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

# Start block to process history at if no block persistent storage yet
START_BLOCK = int(os.environ.get("START_BLOCK", chain.blocks.head.number))

# Filepath to persistent storage for positions
STORAGE_FILEPATH = os.environ.get("STORAGE_FILEPATH", ".db/storage.csv")

# Filepath to persistent storage for last block processed
BLOCK_FILEPATH = os.environ.get("BLOCK_FILEPATH", ".db/block.csv")

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


# Loads state of db from persistent storage
def _load_db() -> pd.DataFrame:
    dtype = {
        "size": object,
        "debt": object,
        "margin": object,
        "safeMarginMinimum": object,
        "liquidated": bool,
        "safe": bool,
        "rewards": np.int64,
        "owner": object,
        "healthFactor": np.float64,
    }
    return (
        pd.read_csv(STORAGE_FILEPATH, index_col="id", dtype=dtype)
        if os.path.exists(STORAGE_FILEPATH)
        else pd.DataFrame()
    )


# Saves state of db to persistent storage
def _save_db(db: pd.DataFrame):
    db.to_csv(STORAGE_FILEPATH)


# Loads last blocks processed on pool open event emission and block sync
def _load_block() -> pd.DataFrame:
    return (
        pd.read_csv(BLOCK_FILEPATH)
        if os.path.exists(BLOCK_FILEPATH)
        else pd.DataFrame(
            [
                {
                    "last_block_open": START_BLOCK,
                    "last_block_sync": START_BLOCK,
                }
            ]
        )
    )


# Saves last blocks processed for a pool open event emission and block sync
# @dev used to catchup on opened positions when processing historical blocks on app startup
def _save_block(b: pd.DataFrame):
    b.to_csv(BLOCK_FILEPATH, index=False)


# Updates last blocks processed for a pool open event emission and block sync
# @dev last is a tuple of (last_block_open, last_block_sync)
def _update_block(last: Tuple, b: pd.DataFrame) -> pd.DataFrame:
    last_block_open, last_block_sync = last
    if not last_block_open:
        last_block_open = _get_last_block_open(b)
    if not last_block_sync:
        last_block_sync = _get_last_block_sync(b)

    d = {"last_block_open": last_block_open, "last_block_sync": last_block_sync}
    return pd.DataFrame([d])


# Gets the last block at which processed a pool Open event emission
def _get_last_block_open(b: pd.DataFrame):
    return b["last_block_open"].iloc[0]


# Gets the last block at which processed a pool Open event emission
def _get_last_block_sync(b: pd.DataFrame):
    return b["last_block_sync"].iloc[0]


# Has position in db
def _has_position_in_db(id: int, db: pd.DataFrame) -> bool:
    return id in db.index


# Creates positions in db
# @dev entries is list of tuples of (owner, id, result_position)
def _create_positions_in_db(entries: List[Tuple], db: pd.DataFrame) -> pd.DataFrame:
    data = _entries_to_data(entries)
    df = pd.DataFrame(data)
    df = df.set_index("id")

    db = pd.concat([db, df])
    db.sort_values(by=["healthFactor"], inplace=True)

    click.echo(f"Created DB entries for {len(entries)} entries: {db}")
    return db


# Updates positions in db
# @dev entries is list of tuples of (owner, id, result_position)
def _update_positions_in_db(entries: List[Tuple], db: pd.DataFrame) -> pd.DataFrame:
    ids = [id for owner, id, _ in entries]
    db.loc[ids]  # reverts with key error if not all exist

    data = _entries_to_data(entries)
    df = pd.DataFrame(data)
    df = df.set_index("id")

    db.update(df)
    db.sort_values(by=["healthFactor"], inplace=True)

    click.echo(f"Updated DB entries for {len(entries)} entries: {db}")
    return db


# Deletes positions from db
def _delete_positions_from_db(ids: List[int], db: pd.DataFrame) -> pd.DataFrame:
    ids_to_keep = [id for id in db.index if id not in ids]
    db = db.loc[ids_to_keep]
    click.echo(f"Deleted DB entries for {len(ids)} entries: {db}")
    return db


# Gets a list of the position IDs and associated owners stored in the db
# @dev Returned entry keys is tuple of lists of (owners, ids)
def _get_position_entry_keys_in_db(db: pd.DataFrame) -> Tuple[List[str], List[int]]:
    if db.empty:
        return ([], [])

    owners = db["owner"].to_list()
    ids = db.index.to_list()
    return (owners, ids)


# Gets a list of unsafe and profitable to liquidate positions from db, returning df records {"index": {"column": "value"}}
def _get_liquidatable_position_records_from_db(
    min_rewards: int, max_records: int, db: pd.DataFrame
) -> Dict:
    db_filtered = db[(~db["safe"]) & (db["rewards"] >= min_rewards)].head(max_records)
    click.echo(
        f"Liquidatable positions > min rewards of {min_rewards} with max records of {max_records}: {db_filtered}"
    )
    return db_filtered.to_dict(orient="index")


# Gets the transaction fee estimate to execute the liquidations
def _get_txn_fee(block: BlockAPI):
    return int(block.base_fee * LIQUIDATE_GAS_ESTIMATE * (1 + TXN_FEE_BUFFER))


@app.on_startup()
def app_startup(startup_state: AppState):
    # set up autosign if desired
    if PROMPT_AUTOSIGN and click.confirm("Enable autosign?"):
        app.signer.set_autosign(enabled=True)

    # populate db with positions opened since last block processed
    _process_historical_pool_opens(
        db=_load_db(),
        b=_load_block(),
    )

    return {"message": "Starting...", "block_number": startup_state.last_block_seen}


# Can handle some initialization on startup, like models or network connections
@app.on_worker_startup()
def worker_startup(state: TaskiqState):
    state.block_count = 0
    state.db = _load_db()  # in memory verison of DB for now
    state.block = _load_block()  # in memory version of last blocks
    state.recipient = (
        RECIPIENT_ADDRESS if RECIPIENT_ADDRESS is not None else app.signer.address
    )
    state.signer_balance = app.signer.balance
    return {"message": "Worker started."}


# Profitably liquidates unsafe positions on pools via multicall, returning list of position IDs liquidated
def liquidate_positions(block: BlockAPI, db: pd.DataFrame, recipient: str) -> List[int]:
    min_rewards = _get_txn_fee(block)
    max_gas_limit = app.provider.max_gas // MAX_FRACTION_GAS_LIMIT_DENOMINATOR
    max_records = max_gas_limit // LIQUIDATE_GAS_ESTIMATE

    click.echo(f"Min rewards at block {block.number}: {min_rewards}")
    click.echo(f"Max records at block {block.number}: {max_records}")

    records = _get_liquidatable_position_records_from_db(min_rewards, max_records, db)
    ids = list(records.keys())
    click.echo(f"Liquidatable position IDs: {ids}")

    if len(ids) == 0:
        return ids

    # format into multicall3 calldata: (address target, bool allowFailure, bytes callData)
    # each calls to pool.liquidate(address recipient, address owner, uint96 id)
    calldata = [
        (
            pool.address,
            False,
            pool.liquidate.as_transaction(
                recipient,
                record["owner"],
                id,
            ).data,
        )
        for id, record in records.items()
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
    # in memory in case out of sync with persistent
    context.state.db = _load_db()
    context.state.block = _load_block()

    # TODO: chunk query?
    # @dev entry keys is tuple of lists of (owners, ids)
    owners, ids = _get_position_entry_keys_in_db(context.state.db)

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

    # remove liquidated or settled positions from DB
    entries_removed = list(filter(lambda e: e[2].size == 0, entries))
    ids_removed = [id for _, id, _ in entries_removed]
    click.echo(
        f"Liquidated or settled entries position IDs to delete from DB: {ids_removed}"
    )
    context.state.db = _delete_positions_from_db(ids_removed, context.state.db)

    # update non liquidated positions in DB
    entries_updated = list(filter(lambda e: e[2].size != 0, entries))
    click.echo(
        f"Synced entries position IDs to update in DB: {[id for _, id, _ in entries_updated]}"
    )
    context.state.db = _update_positions_in_db(entries_updated, context.state.db)

    # multicall liquidate calls to chain
    ids_liquidated_by_bot = liquidate_positions(
        block, context.state.db, context.state.recipient
    )

    # remove additional successful liquidations caused by bot from db
    click.echo(
        f"Liquidated by bot entries position IDs to delete from DB: {ids_liquidated_by_bot}"
    )
    context.state.db = _delete_positions_from_db(
        ids_liquidated_by_bot, context.state.db
    )

    # save updated state of in memory db to persistent storage
    click.echo("Saving updated state of DB to persistent storage ...")
    _save_db(context.state.db)

    # update and save state of last block synced
    # @dev last is tuple of (last_block_open, last_block_sync)
    last = (None, block.number)
    context.state.block = _update_block(last, context.state.block)
    _save_block(context.state.block)

    context.state.signer_balance = app.signer.balance
    context.state.block_count += 1
    return {
        "block_count": context.state.block_count,
        "signer_balance": context.state.signer_balance,
        "positions_count": len(entries_updated) - len(ids_liquidated_by_bot),
    }


# Fetches position entry from pool Open contract log
# @dev entry is a tuple of (owner, id, result_position)
def _get_position_entry_from_log(log: ContractLog) -> Tuple:
    click.echo(
        f"Getting entry data for pool.Open contract log on position with owner {log.owner} and ID {log.id} at block {log.block_number}."
    )
    position = None
    try:
        position = position_viewer.positions(pool.address, log.owner, log.id)
        click.echo(f"Position currently has attributes: {position}")

        health_factor = _get_health_factor(position)
        click.echo(f"Position current health factor: {health_factor}")
    except TransactionError as err:
        click.secho(
            f"Transaction error getting position from viewer: {err}",
            blink=True,
            bold=True,
        )

    return (log.owner, log.id, position)


# Executes on pool Open event emission given in memory db
def _exec_pool_open_on_db(
    log: ContractLog, db: pd.DataFrame, b: pd.DataFrame
) -> Tuple[Tuple, pd.DataFrame, pd.DataFrame]:
    entry = _get_position_entry_from_log(log)
    _, _, position = entry

    # add to DB if not yet liquidated or settled
    if position and position.size != 0:
        click.echo(
            f"Adding position with owner {log.owner} and ID {log.id} to database ..."
        )
        if not _has_position_in_db(log.id, db):
            db = _create_positions_in_db([entry], db)
        else:
            db = _update_positions_in_db([entry], db)

        # save updated state of in memory db to persistent storage
        click.echo("Saving updated state of DB to persistent storage ...")
        _save_db(db)

        # update and save state of last block open
        # @dev last is tuple of (last_block_open, last_block_sync)
        last = (log.block_number, None)
        b = _update_block(last, b)
        _save_block(b)

    return (entry, db, b)


# Processes historical pool Open event emissions to populate db
def _process_historical_pool_opens(
    db: pd.DataFrame,
    b: pd.DataFrame,
    stop_block: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    start_block = _get_last_block_open(b)
    stop = stop_block if stop_block else chain.blocks.head.number
    click.echo(
        f"Process historical events from block {start_block} to block {stop} ..."
    )

    for log in pool.Open.range(start_or_stop=start_block, stop=stop):
        entry, db, b = _exec_pool_open_on_db(log, db, b)
        click.echo(f"Processed log ID {log.id} for pool entry: {entry}")

    click.echo(f"Processed historical events from block {start_block} to block {stop}.")
    return db, b


# This is how we trigger off of events
# Set new_block_timeout to adjust the expected block time.
@app.on_(pool.Open)
def exec_pool_open(log: ContractLog, context: Annotated[Context, TaskiqDepends()]):
    # in memory in case out of sync with persistent
    context.state.db = _load_db()
    context.state.block = _load_block()

    entry, context.state.db, context.state.block = _exec_pool_open_on_db(
        log,
        context.state.db,
        context.state.block,
    )
    _, _, position = entry
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
