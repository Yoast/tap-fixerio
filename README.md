# tap-fixerio

A [Singer Tap] Tap to extract currency exchange rate data from [fixer.io].

## How to use it

### Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac] or [Ubuntu].

First, get an access key from [fixer.io](http://fixer.io). You need at
least the basic plan to get exchange rates from a base currency (such as
USD).

Then, convert `config.sample.json` to
`~/singer.io/tap_fixerio_config.json`; fill out your parameters.

It's recommended to use a virtualenv:

```bash
python3 -m venv ~/.virtualenvs/tap-fixerio
source ~/.virtualenvs/tap-fixerio/bin/activate
pip install -U pip setuptools
pip install -e '.'
```

Set up the `target-csv` virtual environment according to the instructions
[here](https://github.com/singer-io/target-csv/blob/master/README.md).
These commands will install `tap-fixerio`  with pip, and then run it:

```bash
~/.virtualenvs/tap-fixerio/bin/tap-fixerio --config \ ~/singer.io/tap_fixerio_config.json | target-csv
```

The data will be written to a file called `exchange_rate.csv` in your
working directory.

```
$ cat exchange_rate.csv
AUD,BGN,BRL,CAD,CHF,CNY,CZK,DKK,GBP,HKD,HRK,HUF,IDR,ILS,INR,JPY,KRW,MXN,MYR,NOK,NZD,PHP,PLN,RON,RUB,SEK,SGD,THB,TRY,ZAR,EUR,USD,date
1.3023,1.8435,3.0889,1.3109,1.0038,6.869,25.47,7.0076,0.79652,7.7614,7.0011,290.88,13317.0,3.6988,66.608,112.21,1129.4,19.694,4.4405,8.3292,1.3867,50.198,4.0632,4.2577,58.105,8.9724,1.4037,34.882,3.581,12.915,0.9426,1.0,2017-02-24T00:00:00Z
```

---

Copyright &copy; 2017 Stitch

[Singer Tap]: https://singer.io
[fixer.io]: https://fixer.io
[Mac]: http://docs.python-guide.org/en/latest/starting/install3/osx/
[Ubuntu]: https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04
