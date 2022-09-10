# fix

A reference implementation of a FIX client built for communicating with FTX exchanges. This example code is intended to aid in understanding the steps necessary for connecting to FTX exchanges via FIX over TCP protocol. All WAN communication must be encrypted with SSL/TLS. This example code shows how to set up SSL/TLS on the socket.

Some notes:

 * It has been tested on Python 3.10.
 * Uses `simplefix` and `gevent` as its primary dependencies.
 * Establishes a connection, logs in, and then sends heartbeats.
 * Outputs a bunch of debug logs for clarity.

## How to use this example code

### Steps

Note that on Windows you will need to substitue `env/bin` with `env\Scripts`.

 - In your terminal, change directory to this directory.
 - Run `./setup-env`. It may give you further instructions to follow. This command will create a virtual environment nested under this directory.
 - Make sure the terminal environment contains your API credentials:
   - `FTX_API_KEY` and `FTX_API_SECRET`
   - These environment variables should be set to the values given to you on the FTX website. See `https://ftx.us/settings/api` in the US, or `https://ftx.com/settings/api` for international API Key setup.
 - Run `env/bin/python -m fix.fix ftxus` for ftx.us access.
 - Run `env/bin/python -m fix.fix ftx` for ftx.com access.

