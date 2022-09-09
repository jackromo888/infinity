# fix

A reference implementation of a FIX client built for communicating with FTX
exchanges.

 * Is built in Python 3.10
 * Uses `simplefix` and `gevent` as its primary dependencies.
 * Establishes a connection, logs in, and then sends heartbeats.
 * Outputs a bunch of logs for debugging and explanation.
