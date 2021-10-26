#!/usr/bin/env python3

import json
import sys
import argparse
import time
import requests
import singer
import backoff

from datetime import date, datetime, timedelta

REQUIRED_CONFIG_KEYS = [
    "access_key",
]

base_url = 'http://data.fixer.io/api/'

logger = singer.get_logger()
session = requests.Session()
DATE_FORMAT='%Y-%m-%d'

def parse_response(r):
    flattened = r['rates']
    # flattened['currency'] = r['currency']
    flattened[r['base']] = 1.0
    # flattened['rate'] = r['USD']
    flattened['date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(r['date'], DATE_FORMAT))
    # return flattened
    # r['date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(r['date'], DATE_FORMAT))
    return flattened

schema = {"type": "object",
    "properties": {
        "date": {
            "type": "string",
            "format": "date-time"
        },
        "AED":{
            "type": "number"
        },
        "AFN": {
            "type": "number"
        },
        "ALL": {
            "type": "number"
        },
        "AMD": {
            "type": "number"
        },
        "ANG": {
            "type": "number"
        },
        "AOA": {
            "type": "number"
        },
        "ARS": {
            "type": "number"
        },
        "AUD": {
            "type": "number"
        },
        "AWG": {
            "type": "number"
        },
        "AZN": {
            "type": "number"
        },
        "BAM": {
            "type": "number"
        },
        "BBD": {
            "type": "number"
        },
        "BDT": {
            "type": "number"
        },
        "BGN": {
            "type": "number"
        },
        "BHD": {
            "type": "number"
        },
        "BIF": {
            "type": "number"
        },
        "BMD": {
            "type": "number"
        },
        "BND": {
            "type": "number"
        },
        "BOB": {
            "type": "number"
        },
        "BRL": {
            "type": "number"
        },
        "BSD": {
            "type": "number"
        },
        "BTC": {
            "type": "number"
        },
        "BTN": {
            "type": "number"
        },
        "BWP": {
            "type": "number"
        },
        "BYN" {
            "type": "number"
        },
        "BYR": {
            "type": "number"
        },
        "BZD": {
            "type": "number"
        },
        "CAD": {
            "type": "number"
        },
        "CDF": {
            "type": "number"
        },
        "CHF": {
            "type": "number"
        },
        "CLF": {
            "type": "number"
        },
        "CLP": {
            "type": "number"
        },
        "CNY": {
            "type": "number"
        },
        "COP": {
            "type": "number"
        },
        "CRC": {
            "type": "number"
        },
        "CUC": {
            "type": "number"
        },
        "CUP": {
            "type": "number"
        },
        "CVE": {
            "type": "number"
        },
        "CZK": {
            "type": "number"
        },
        "DJF": {
            "type": "number"
        },
        "DKK": {
            "type": "number"
        },
        "DOP": {
            "type": "number"
        },
        "DZD": {
            "type": "number"
        },
        "EGP": {
            "type": "number"
        },
        "ERN": {
            "type": "number"
        },
        "ETB": {
            "type": "number"
        },
        "EUR": {
            "type": "number"
        },
        "FJD": {
            "type": "number"
        },
        "FKP": {
            "type": "number"
        },
        "GBP": {
            "type": "number"
        },
        "GEL": {
            "type": "number"
        },
        "GGP": {
            "type": "number"
        },
        "GHS": {
            "type": "number"
        },
        "GIP": {
            "type": "number"
        },
        "GMD": {
            "type": "number"
        },
        "GNF": {
            "type": "number"
        },
        "GTQ": {
            "type": "number"
        },
        "GYD": {
            "type": "number"
        },
        "HKD": {
            "type": "number"
        },
        "HNL": {
            "type": "number"
        },
        "HRK": {
            "type": "number"
        },
        "HTG": {
            "type": "number"
        },
        "HUF": {
            "type": "number"
        },
        "IDR": {
            "type": "number"
        },
        "ILS": {
            "type": "number"
        },
        "IMP": {
            "type": "number"
        },
        "INR": {
            "type": "number"
        },
        "IQD": {
            "type": "number"
        },
        "IRR": {
            "type": "number"
        },
        "ISK": {
            "type": "number"
        },
        "JEP": {
            "type": "number"
        },
        "JMD": {
            "type": "number"
        },
        "JOD": {
            "type": "number"
        },
        "JPY": {
            "type": "number"
        },
        "KES": {
            "type": "number"
        },
        "KGS": {
            "type": "number"
        },
        "KHR": {
            "type": "number"
        },
        "KMF": {
            "type": "number"
        },
        "KPW": {
            "type": "number"
        },
        "KRW": {
            "type": "number"
        },
        "KWD": {
            "type": "number"
        },
        "KYD": {
            "type": "number"
        },
        "KZT": {
            "type": "number"
        },
        "LAK": {
            "type": "number"
        },
        "LBP": {
            "type": "number"
        },
        "LKR": {
            "type": "number"
        },
        "LRD": {
            "type": "number"
        },
        "LSL": {
            "type": "number"
        },
        "LTL": {
            "type": "number"
        },
        "LVL": {
            "type": "number"
        },
        "LYD": {
            "type": "number"
        },
        "MAD": {
            "type": "number"
        },
        "MDL": {
            "type": "number"
        },
        "MGA": {
            "type": "number"
        },
        "MKD": {
            "type": "number"
        },
        "MMK": {
            "type": "number"
        },
        "MNT": {
            "type": "number"
        },
        "MOP": {
            "type": "number"
        },
        "MRO": {
            "type": "number"
        },
        "MUR": {
            "type": "number"
        },
        "MVR": {
            "type": "number"
        },
        "MWK": {
            "type": "number"
        },
        "MXN": {
            "type": "number"
        },
        "MYR": {
            "type": "number"
        },
        "MZN": {
            "type": "number"
        },
        "NAD": {
            "type": "number"
        },
        "NGN": {
            "type": "number"
        },
        "NIO": {
            "type": "number"
        },
        "NOK": {
            "type": "number"
        },
        "NPR": {
            "type": "number"
        },
        "NZD": {
            "type": "number"
        },
        "OMR": {
            "type": "number"
        },
        "PAB": {
            "type": "number"
        },
        "PEN": {
            "type": "number"
        },
        "PGK": {
            "type": "number"
        },
        "PHP": {
            "type": "number"
        },
        "PKR": {
            "type": "number"
        },
        "PLN": {
            "type": "number"
        },
        "PYG": {
            "type": "number"
        },
        "QAR": {
            "type": "number"
        },
        "RON": {
            "type": "number"
        },
        "RSD": {
            "type": "number"
        },
        "RUB": {
            "type": "number"
        },
        "RWF": {
            "type": "number"
        },
        "SAR": {
            "type": "number"
        },
        "SBD": {
            "type": "number"
        },
        "SCR": {
            "type": "number"
        },
        "SDG": {
            "type": "number"
        },
        "SEK": {
            "type": "number"
        },
        "SGD": {
            "type": "number"
        },
        "SHP": {
            "type": "number"
        },
        "SLL": {
            "type": "number"
        },
        "SOS": {
            "type": "number"
        },
        "SRD": {
            "type": "number"
        },
        "STD": {
            "type": "number"
        },
        "SVC": {
            "type": "number"
        },
        "SYP": {
            "type": "number"
        },
        "SZL": {
            "type": "number"
        },
        "THB": {
            "type": "number"
        },
        "TJS": {
            "type": "number"
        },
        "TMT": {
            "type": "number"
        },
        "TND": {
            "type": "number"
        },
        "TOP": {
            "type": "number"
        },
        "TRY": {
            "type": "number"
        },
        "TTD": {
            "type": "number"
        },
        "TWD": {
            "type": "number"
        },
        "TZS": {
            "type": "number"
        },
        "UAH": {
            "type": "number"
        },
        "UGX": {
            "type": "number"
        },
        "USD": {
            "type": "number"
        },
        "UYU": {
            "type": "number"
        },
        "UZS": {
            "type": "number"
        },
        "VEF": {
            "type": "number"
        },
        "VND": {
            "type": "number"
        },
        "VUV": {
            "type": "number"
        },
        "WST": {
            "type": "number"
        },
        "XAF": {
            "type": "number"
        },
        "XAG": {
            "type": "number"
        },
        "XAU": {
            "type": "number"
        },
        "XCD": {
            "type": "number"
        },
        "XDR": {
            "type": "number"
        },
        "XOF": {
            "type": "number"
        },
        "XPF": {
            "type": "number"
        },
        "YER": {
            "type": "number"
        },
        "ZAR": {
            "type": "number"
        },
        "ZMK": {
            "type": "number"
        },
        "ZMW": {
            "type": "number"
        },
        "ZWL": {
            "type": "number"
        }
    }
}

def giveup(error):
    logger.error(error.response.text)
    response = error.response
    return not (response.status_code == 429 or
                response.status_code >= 500)

@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      giveup=giveup,
                      interval=30)
def request(url, params):
    response = requests.get(url=url, params=params)
    response.raise_for_status()
    return response

def do_sync(base, start_date, access_key, symbols=None):
    logger.info('Replicating exchange rate data from fixer.io starting from {}'.format(start_date))
    
#     bookmark_property = "rate"
    
    # singer.write_schema('exchange_rate', schema, 'date', bookmark_properties=[bookmark_property])
    singer.write_schema('exchange_rate', schema, 'date')
    state = {'start_date': start_date}
    next_date = start_date

    try:
        while True:
            if symbols:
                response = request(base_url + next_date, {'base': base, 'access_key': access_key, 'symbols': ','.join(symbols)})
            else:
                response = request(base_url + next_date, {'base': base, 'access_key': access_key})
            payload = response.json()

            if datetime.strptime(next_date, DATE_FORMAT) > datetime.utcnow():
                break
            elif payload.get('error'):
                raise RuntimeError(payload['error'])
            else:
                singer.write_records('exchange_rate', [parse_response(payload)])
                state = {'start_date': next_date}
                next_date = (datetime.strptime(next_date, DATE_FORMAT) + timedelta(days=1)).strftime(DATE_FORMAT)

    except requests.exceptions.RequestException as e:
        logger.fatal('Error on ' + e.request.url +
                     '; received status ' + str(e.response.status_code) +
                     ': ' + e.response.text)
        singer.write_state(state)
        sys.exit(-1)

    singer.write_state(state)
    logger.info('Tap exiting normally')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=False)
    parser.add_argument(
        '-s', '--state', help='State file', required=False)

    #args = parser.parse_args()
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.config:
        config = args.config
    else:
        config = {}

    if args.state:
        state = args.state
    else:
        state = {}

    start_date = state.get('start_date',
                           config.get('start_date', datetime.utcnow().strftime(DATE_FORMAT)))
    access_key = state.get('access_key', config.get('access_key'))

    do_sync(config.get('base', 'USD'), start_date, access_key, symbols=config.get('symbols', None))


if __name__ == '__main__':
    main()
