import os
from google.cloud import spanner
import random
import string
from google.oauth2 import service_account
import base64
import uuid
import argparse
import time
from google.cloud import spanner
import mango  # nopep8

class StreamDeFiFutures():

    def __init__(self):
        pass

    @staticmethod
    def insert(instance_dict):
        instance_id = "defi"
        database_id = "futuresfunding"
        creds = service_account.Credentials.from_service_account_file("key.json")
        spanner_client = spanner.Client(credentials=creds)
        instance = spanner_client.instance(instance_id)
        db = instance.database(database_id)


        fields = []
        values = []
        for field in instance_dict:
            fields.append(field)
            values.append(instance_dict[field])
        fieldt = tuple(fields)
        valuest = tuple(values)
        with db.batch() as batch:
            batch.insert(table="StreamDeFiFutures", columns=fieldt,
                         values=[valuest], )

        print("Inserted instance data." + str(instance_dict))


        


#connector start 
parser = argparse.ArgumentParser(
    description="Shows the current funding rates for a perp market in a Mango Markets Group."
)

mango.ContextBuilder.add_command_line_parameters(parser)

args: argparse.Namespace = mango.parse_args(parser)

while True:
    try:
        with mango.ContextBuilder.from_command_line_parameters(args) as context:

            stream = StreamDeFiFutures()
            # GET PERP FUNDING RATES
            perps = []
            perps.append("SOL-PERP")
            '''perps.append("ETH-PERP")
            perps.append("BTC-PERP")
            perps.append("MNGO-PERP")
            perps.append("ADA-PERP")
            perps.append("BNB-PERP")
            perps.append("RAY-PERP")
            perps.append("SRM-PERP")
            perps.append("GMT-PERP")
            perps.append("FTT-PERP")
            perps.append("AVAX-PERP")'''

            perp_markets_list = []
            for perp_temp in perps:
                perp_market = mango.PerpMarket.ensure(mango.market(context, perp_temp))
                perp_markets_list.append(perp_market)

            while (True):
                try:
                    time.sleep(2)
                    for perpi in perp_markets_list:
                        time.sleep(2)
                        funding = perpi.fetch_funding(context)
                        orderbook = perpi.fetch_orderbook(context)

                        inst4 = {}
                        inst4['DATA_KEY'] = str(uuid.uuid4())
                        inst4['FUNDING'] = round(funding.rate,9)
                        inst4['OPEN_INTEREST'] = round(funding.open_interest,9)
                        inst4['ORACLE_PRICE'] = round(funding.oracle_price,9)
                        inst4['MID_PRICE'] = round(orderbook.mid_price,9)
                        inst4['LONG_FUNDING'] = round(perpi.underlying_perp_market.long_funding,9)
                        inst4['SHORT_FUNDING'] = round(perpi.underlying_perp_market.short_funding,9)
                        inst4['APR'] = round(funding.extrapolated_apr,9)
                        inst4['APY'] = round(funding.extrapolated_apy,9)
                        inst4['SPREAD'] = round(orderbook.spread,9)
                        inst4['SYMBOL'] = perpi.symbol
                        inst4['OBSERVATION_TIME'] = spanner.COMMIT_TIMESTAMP

                        stream.insert(inst4)


                except Exception as e:
                        print("Oops!", e.__class__, "occurred.")


    except Exception as ex:
        print(str(ex))

    finally:
        # this block is always executed
        # regardless of exception generation.
        print('Restarting the service - something went wrong , wait for 10 sec')
        time.sleep(10)


