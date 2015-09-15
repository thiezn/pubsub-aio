#!/usr/bin/env python3

from hashlib import sha512


# At the moment this library isn't used yet. Would be good to incoroporate this
# into the model to have a clean way of identifying a unique node

def create_network_id(addr_port_tuple):
    """ Given a tuple of peer address and port this
    will generate a unique network_id.

    will raise a ValueError if the tuple is empty
    """

    if addr_port_tuple:
        return sha512(addr_port_tuple.encode('ascii')).hexdigest()
    else:
        raise ValueError('Cannot create network_id from '
                         'empty address/port tuple')


class PeerNode:
    """ A class containing peer information """

    def __init__(self, version, address, port, last_seen=0.0):
        """ Initialise the peer node with an unique id,
        the version number of the pub/sub software and
        the address/port the node is listening on. last_seen
        is indicating when the last connection was made to the
        node (defaults to 0)
        """
        self.network_id = create_network_id((address, port))
        self.version = version
        self.address = address
        self.port = port
        self.last_seen = last_seen

    def dump(self):
        """ Returns a dictionary representative of the node that
        can be serialised into JSON. Useful for backups?
        """
        return {'network_id': self.network_id,
                'version': self.version,
                'address': self.address,
                'port': self.port}
