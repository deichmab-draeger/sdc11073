from __future__ import annotations

import time
from typing import TYPE_CHECKING, Iterable
from threading import Thread, Lock
from dataclasses import dataclass
from sdc11073 import loghelper

if TYPE_CHECKING:
    from sdc11073.wsdiscovery.wsdimpl import WSDiscovery
    from sdc11073.wsdiscovery.service import Service
    from sdc11073.consumer.consumerimpl import SdcConsumer
    from sdc11073.xml_types import wsd_types
    from lxml.etree import QName

@dataclass
class ConsumerEntry:
    service: Service
    consumer: SdcConsumer

class ReconnectAgent:
    """Reconnect consumers to provider with same epr via wsdiscovery search.

    Using the unique epr allows to reconnect even if address of provider changed.
    That could happen because of changed port number after a reboot or a changed ip (dhcp).
    """
    def __init__(self, wsdiscovery: WSDiscovery,
                 types: Iterable[QName] | None = None,
                 scopes: wsd_types.ScopesType | None = None,
                 search_timeout: float| int = 5):
        self._wsdiscovery = wsdiscovery
        self._types = types
        self._scopes = scopes
        self._search_timeout = search_timeout
        self._logger = loghelper.get_logger_adapter('sdc.reconnect')
        self._thread = Thread(target=self._run, name='reconnect-agent', daemon=True)

        self._consumers: list[ConsumerEntry] = []
        self._consumers_lock = Lock()
        self._have_disconnected_consumers: bool = False
        self._thread.start()

    def keep_connected(self, service: Service, consumer: SdcConsumer):
        # check if this is a duplicate
        with self._consumers_lock:
            for entry in self._consumers:
                if entry.consumer is consumer:
                    raise ValueError('consumer already known')
            self._consumers.append(ConsumerEntry(service, consumer))

    def forget(self, consumer: SdcConsumer):
        """Stop handling this consumer.

        Raises a ValueError if consumer is unknown."""
        with self._consumers_lock:
            for entry in self._consumers:
                if entry.consumer is consumer:
                    self._logger.info('removed consumer for epr %s', entry.service.epr)
                    self._consumers.remove(entry)
                    return
        raise ValueError('consumer unknown')

    def _run(self):
        while True:
            self._logger.debug('check reconnect')
            with self._consumers_lock:
                disconnected = [e for e in self._consumers if not e.consumer.is_connected]
            self._logger.debug('%d consumers disconnected', len(disconnected))
            if disconnected:
                services = self._wsdiscovery.search_services(self._types, self._scopes, timeout=self._search_timeout)
                self._logger.debug('%d services found', len(services))
                services_dict = {s.epr: s for s in services}
                with self._consumers_lock:
                    # create list again, last create is self._search_timeout old
                    disconnected = [e for e in self._consumers if not e.consumer.is_connected]
                    for entry in disconnected:
                        epr = entry.service.epr
                        self._logger.debug('check if disconnected epr %s is in %r',epr, services_dict.keys())
                        if entry.service.epr in services_dict:
                            wsd_service = services_dict[epr]
                            if len(wsd_service.x_addrs): # ignore services without x-address
                                self._logger.info('try to reconnect to %s on %r',
                                                  epr, wsd_service.x_addrs[0])
                                device_location = wsd_service.x_addrs[0]
                                entry.consumer.restart(new_device_location=device_location)

                time.sleep(10) # allow consumers to establish connection before next check
            else:
                time.sleep(1)