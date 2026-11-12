"""
Connect to the first reachable of several RabbitMQ addresses with
`PlainCredentials(erase_on_connect=True)`, without breaking failover or
reconnects.

`erase_on_connect=True` makes pika wipe the credentials copy owned by each
connection attempt once that attempt has sent its PLAIN response. Three rules
keep that compatible with multiple addresses and reconnects:

1. Give every `ConnectionParameters` its own credentials object from a
   provider. pika deep-copies the parameters for every connection attempt, so
   in-workflow retries (TCP failure, handshake failure, next address) always
   start from the intact parameters copy and never need a re-fetch.
2. On an application-level reconnect, ask the provider for fresh credentials
   and build new parameters. Never reuse `connection.params` of a previous
   connection: that is the erased copy, and reconnecting from it sends a
   blank username and password.
3. If your residency policy requires it, discard your own references once the
   connection is up. pika erases only the copies it owns; the object the
   provider returned stays valid until you drop it.

The provider below returns fixed guest credentials so the example runs
against a local broker; a real deployment would fetch a short-lived secret
from a vault on every call. The first address is intentionally dead so the
run exercises failover to the second.
"""

import pika

ADDRESSES = (('localhost', 5673), ('localhost', 5672))


def provide_credentials():
    """Return a fresh credentials object good for one connection."""
    return pika.PlainCredentials('guest', 'guest', erase_on_connect=True)


def build_params():
    """Build one parameters object per address, each with fresh credentials."""
    return [
        pika.ConnectionParameters(host=host,
                                  port=port,
                                  credentials=provide_credentials())
        for host, port in ADDRESSES
    ]


def main():
    for cycle in range(2):
        # Every reconnect builds new parameters from the provider; the
        # previous connection's params object is never reused.
        connection = pika.BlockingConnection(build_params())
        print(f'cycle {cycle}: connected to '
              f'{connection.params.host}:{connection.params.port}')
        connection.close()


if __name__ == '__main__':
    main()
