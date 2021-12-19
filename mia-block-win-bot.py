import base64
import json
import logging
import time

import click
import requests
from discord_webhook import DiscordWebhook


GC_PROJECT_ID = "1019088744576"
logging.basicConfig(level=logging.INFO)


def store_secret(secret_id: str, value: str) -> None:
    logging.info(f"writing secret to secret manager: {secret_id}")
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    parent = client.secret_path(GC_PROJECT_ID, secret_id)
    payload = value.encode("UTF-8")

    # Add secret version.
    response = client.add_secret_version(request={"parent": parent, "payload": {"data": payload}})

    # Print the new secret version name.
    logging.info("Added secret version: {}".format(response.name))

    
def get_secret(secret_id: str) -> str:
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{GC_PROJECT_ID}/secrets/{secret_id}/versions/latest"
    secret = client.access_secret_version(request={"name": secret_name}).payload.data.decode("UTF-8")

    return secret


def post_to_discord(webhook_endpoint: str, message: str) -> None:
    """post message to supplied webhook endpoint"""

    webhook = DiscordWebhook(url=webhook_endpoint, content=message)
    response = webhook.execute()


def get_wallet_mining_history(wallet_address: str):
    wallet_history_endpoint = f"https://miamining.com/blocks?miner={wallet_address}"
    return requests.get(wallet_history_endpoint).json()


def get_config(key: str) -> str:

    logging.info(f"fetching config: {key}")

    config =  get_secret("mia-block-win-notifier-config")
    config_json = json.loads(config)

    if key in config_json:
        return config_json[key]
    else:
        return None


def get_last_recorded_win_block() -> int:
    return get_secret("mia-block-win-notifier-last-won-block")


def record_win_block(blockheight: int, prod: bool) -> None:

    if prod:
        secret_id = "mia-block-win-notifier-last-won-block"
        logging.info(f"recording blockheight {blockheight} at {secret_id}")

        store_secret(secret_id=secret_id, value=str(blockheight))

    else:
        logging.info(f"test mode, skipping recording, would've recorded blockheight {blockheight}")


def get_pool_total() -> float:
    return get_config(key=wallet_funds)


def get_current_block() -> int:
    current_block_endpoint = 'https://miamining.com/blocks/current_block'
    return int(requests.get(current_block_endpoint).json()['currentBlock'])


def check_wins(wallet_address: str, starting_blockheight: int, last_won_block: str, prod: bool = False):

    # fetch history
    history_json = get_wallet_mining_history(wallet_address=wallet_address)
    current_block = get_current_block()

    # process history
    blocks_won = {}
    blocks_lost = {}
    stx_commited = 0

    for blockheight, block in list(history_json.items()):

        # remove blocks that the wallet did not mine
        if wallet_address not in block['miners'].keys():
            del history_json[blockheight]
            continue

        # remove blocks before starting block for a pool
        if int(blockheight) < starting_blockheight:
            del history_json[blockheight]
            continue

        # skip blocks after current block for win and loss counts
        if int(blockheight) > current_block:
            continue

        if "winner" in block:

            if block["winner"] == wallet_address:
                # wallet won the block
                blocks_won[blockheight] = block
            else:
                # wallet lost the block
                blocks_lost[blockheight] = block

            # in either case count how much the wallet committed
            stx_commited += block["miners"][wallet_address]


    if len(blocks_won.keys()) == 0:
        error = f"No winning blocks found among {len(history_json.items())} blocks returned by history endpoint"
        logging.error(error)
        raise Exception(error)

    newest_won_blockheight = max(blocks_won.keys())

    # is this a new win?
    if newest_won_blockheight == last_won_block:
        post_content = None

    else:

        winning_block = blocks_won[newest_won_blockheight]
        winning_bid = winning_block["miners"][wallet_address] / 1000000
        winning_block_total_bid = sum(winning_block["miners"].values()) / 1000000
        winning_probability = 100 * winning_bid / winning_block_total_bid

        mia_won = 100000 * len(blocks_won) #TODO This should change to match issuance schedule https://docs.citycoins.co/citycoins-core-protocol/issuance-schedule
        cost_basis = stx_commited / 1000000 / mia_won
        winnings_per_100_stx = mia_won / get_pool_total() * 100 * 0.96

        pool_remaining = get_pool_total() - stx_commited/1000000

        pool_id = get_config(key="pool_id")
        
        # compose message to post to discord
        post_content = "\n".join(
            [
                "\n\n\n--",
                f"{pool_id} wins another Block - `#{newest_won_blockheight}`\n",
                
                f":trophy: Winnings:",
                f"```",
                f"{len(blocks_won)} Total blocks won | {mia_won} Total MIA won | at {cost_basis:.4f} STX/MIA",
                f"100 STX contribution to the pool wins {winnings_per_100_stx:.2f} after fees",
                f"```",

                f":ice_cube: Block Counts:",
                f"```",
                f"{len(blocks_won)} Won + {len(blocks_lost)} Lost + {len(history_json) - len(blocks_won) - len(blocks_lost)} Pending = {len(history_json)} Total Mined",
                f"```",
                
                f":hammer: Bidding:",
                f"```",
                f"Total         : {stx_commited/1000000:.1f} STX spent | {pool_remaining:.1f} STX remaining | {stx_commited/1000000/(len(blocks_won)+len(blocks_lost)):.1f} STX Average bid",
                f"In this block : {winning_bid:.2f} STX bid in {winning_block_total_bid:.2f} STX | Win Probability {winning_probability:.2f} %"
                f"```",
            ]
        )

    return post_content, newest_won_blockheight


# gcf start
def gcf_start(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    prod = json.loads(pubsub_message)['prod']
    logging.info(f"Prod Mode: {prod}")

    wallet_address = get_config(key="wallet_address")
    starting_blockheight = int(get_config(key="mining_start_block"))
    last_recorded_win = get_last_recorded_win_block()
    
    response, newest_won_blockheight = check_wins(
        wallet_address=wallet_address, 
        starting_blockheight=starting_blockheight,
        last_won_block=last_recorded_win
    )

    print(response, newest_won_blockheight)
                
    prod_discord_webhook = get_config(key="discord_webhook")
    test_discord_webhook = get_config(key="discord_webhook_test")

    if response:
        logging.info(f"Newest won block: {newest_won_blockheight}, Last recorded win: {last_recorded_win}")
        logging.info(response)
        post_to_discord(webhook_endpoint=test_discord_webhook, message=response)

        if prod:
            post_to_discord(webhook_endpoint=prod_discord_webhook, message=response)
            record_win_block(blockheight=newest_won_blockheight, prod=prod)
            

if __name__ == '__main__':

    # integration test, fetches values from GCP secret manager, requires GCP credentials
    gcf_start(
        {
            "data": base64.b64encode('{"prod": false}'.encode("utf-8")),
        }, 
        context=None,
    )


