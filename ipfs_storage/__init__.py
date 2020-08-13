# updated: 2020/08/12
# author: jeffbr13
# author: davidchern
import threading

import ipfshttpclient

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible

__version__ = '0.1.0'


@deconstructible
class IPFSStorage(Storage):

    def __init__(self, api_url=None, gateway_url=None):
        self.ipfs_api_url = api_url or getattr(
            settings, 'IPFS_STORAGE_API_URL', '/ip4/127.0.0.1/tcp/5001/http'
        )
        self.gateway_url = gateway_url or getattr(
            settings, 'IPFS_STORAGE_GATEWAY_URL', 'https://ipfs.io/ipfs/'
        )
        self._connections = threading.local()

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('_connections', None)
        return state

    def __setstate__(self, state):
        state['_connections'] = threading.local()
        self.__dict__ = state

    @property
    def connection(self):
        connection = getattr(self._connections, 'connection', None)
        if connection is None:
            self._connections.connection = ipfshttpclient.connect(
                self.ipfs_api_url
            )
        return self._connections.connection

    def _open(self, name, mode='rb'):
        return ContentFile(self.connection.cat(name), name=name)

    def _save(self, name, content):
        multihash = self.connection.add_bytes(content.__iter__())
        self.connection.pin.add(multihash)
        return multihash

    def get_valid_name(self, name):
        """Returns name. Only provided for compatibility with Storage interface."""
        return name

    def get_available_name(self, name, max_length=None):
        """Returns name. Only provided for compatibility with Storage interface."""
        return name

    def size(self, name):
        """Total size, in bytes, of IPFS content with multihash `name`."""
        return self.connection.object.stat(name)['CumulativeSize']

    def delete(self, name):
        """Unpin IPFS content from the daemon."""
        self.connection.pin.rm(name)

    def url(self, name):
        return '{gateway_url}{multihash}'.format(gateway_url=self.gateway_url,
                                                 multihash=name)
