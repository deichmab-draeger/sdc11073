from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Callable

from sdc11073 import loghelper

if TYPE_CHECKING:
    from .soapclient import SoapClientProtocol

    _SoapClientFactory = Callable[[str, list[str]], SoapClientProtocol]
    _UnreachableCallback = Callable[[], None]


class _SoapClientEntry:
    """Keeps a soap client for one netloc, and a list of callbacks for each epr."""

    def __init__(self, soap_client: SoapClientProtocol | None, epr: str, unreachable_callback: _UnreachableCallback):
        self.soap_client = soap_client
        self.callbacks = defaultdict(list)
        self.callbacks[epr].append(unreachable_callback)

    def add_callback(self, epr: str, unreachable_callback: _UnreachableCallback):
        callback_list = self.callbacks[epr]
        if unreachable_callback in callback_list:
            raise ValueError(f'callback already registered for epr {epr}')
        callback_list.append(unreachable_callback)

    def forget_callback(self, unreachable_callback: _UnreachableCallback):
        for epr, callback_list in self.callbacks.items():
            if unreachable_callback in callback_list:
                callback_list.remove(unreachable_callback)
                if len(callback_list) == 0:
                    self.callbacks.pop(epr)
                break
        self._close_soap_client_if_empty()

    def forget_epr(self, epr: str):
        if epr not in self.callbacks:
            return
        for callback in self.callbacks[epr]:
            callback()
        self.callbacks.pop(epr)
        self._close_soap_client_if_empty()

    def forget_all(self):
        for callback_list in self.callbacks.values():
            for callback in callback_list:
                callback()
        self.callbacks.clear()
        self._close_soap_client_if_empty()

    def has_callback(self, unreachable_callback: _UnreachableCallback) -> bool:
        return any(unreachable_callback in callback_list for callback_list in self.callbacks.values())

    def _close_soap_client_if_empty(self):
        if len(self.callbacks) == 0 and self.soap_client is not None:
            self.soap_client.close()
            self.soap_client = None


class SoapClientPool:
    """Pool of soap clients with reference count."""

    # ToDo: distinguish between unreachable netloc and unreachable epr

    def __init__(self, soap_client_factory: _SoapClientFactory, log_prefix: str):
        self._soap_client_factory = soap_client_factory
        self._soap_clients: dict[str, _SoapClientEntry] = {}
        self._logger = loghelper.get_logger_adapter('sdc.device.soap_client_pool', log_prefix)

    def register_netloc_user(self, netloc: str, epr: str, unreachable_callback: _UnreachableCallback) -> None:
        """Associate an unreachable_callback to a network location + epr."""
        self._logger.debug('registered netloc {} epr {}', netloc, epr)  # noqa: PLE1205
        entry = self._soap_clients.get(netloc)
        if entry is None:
            # for now only register the callback, the soap client will be created later on get_soap_client call.
            self._soap_clients[netloc] = _SoapClientEntry(None, epr, unreachable_callback)
            return
        if unreachable_callback not in entry.callbacks:
            entry.add_callback(epr, unreachable_callback)

    def get_soap_client(self, netloc: str,
                        accepted_encodings: list[str]) -> SoapClientProtocol:
        """Return a soap client for netloc.

        Parameter epr is used to check if this netloc/ epr combo has already been registered.
        Method creates a new soap client if it did not exist yet.
        It also associates the user_ref (subscription) to the network location.
        """
        self._logger.debug('requested soap client for netloc {}', netloc)  # noqa: PLE1205
        if netloc not in self._soap_clients:
            raise ValueError(f'netloc {netloc} is unknown. Register before calling this method.')
        entry = self._soap_clients[netloc]
        if entry.soap_client is None:
            soap_client = self._soap_client_factory(netloc, accepted_encodings)
            entry.soap_client = soap_client
        return entry.soap_client

    def forget_callback(self, unreachable_callback: _UnreachableCallback) -> None:
        """Remove the user reference from the network location.

        If no more associations exist, the soap connection gets closed and the soap client deleted.
        """
        self._logger.debug('forget unreachable_callback')
        for netloc, entry in self._soap_clients.items():
            if entry.has_callback(unreachable_callback):
                entry.forget_callback(unreachable_callback)
                if len(entry.callbacks) == 0:
                    self._soap_clients.pop(netloc)
                return
        raise ValueError('callback is unknown.')

    def report_unreachable_netloc(self, netloc: str) -> None:
        """All user references for the unreachable network location will be informed.

        Then soap client gets closed and deleted.
        """
        self._logger.debug('unreachable netloc {}', netloc)  # noqa: PLE1205
        if netloc not in self._soap_clients:
            raise ValueError(f'netloc {netloc} is unknown.')
        self._soap_clients.pop(netloc).forget_all()

    def report_unreachable_epr(self, netloc: str, epr: str) -> None:
        """All user references for the unreachable epr will be informed.

        The soap client gets only closed and deleted if no more registrations for netloc exist.
        The epr should be unique, but this is data from extern and cannot be trusted.
        It is possible that the same epr is used on different net locations.
        The netloc parameter is needed to handle the case of duplicate eprs.
        """
        self._logger.debug('unreachable netloc {}', netloc)  # noqa: PLE1205
        if netloc not in self._soap_clients:
            raise ValueError(f'netloc {netloc} is unknown.')
        entry = self._soap_clients[netloc]
        entry.forget_epr(epr)
        if len(entry.callbacks) == 0:
            self._soap_clients.pop(netloc)

    def close_all(self):
        """Close all connections."""
        for entry in self._soap_clients.values():
            entry.forget_all()
        self._soap_clients.clear()
