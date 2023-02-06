from __future__ import annotations

import http.client
import socket
import time
import traceback
import uuid
from collections import deque
from typing import TYPE_CHECKING, Protocol, Callable, Any
from urllib.parse import urlparse

from lxml import etree as etree_

from .. import isoduration
from .. import loghelper
from .. import multikey
from .. import observableproperties
from ..addressing import ReferenceParameters, Address
from ..etc import apply_map, short_filter_string
from ..pysoap.soapclient import HTTPReturnCodeError
from ..pysoap.soapenvelope import SoapFault, FaultCodeEnum

if TYPE_CHECKING:
    from ..definitions_base import BaseDefinitions
    from ..pysoap.msgfactory import MessageFactoryDevice, CreatedMessage
    from ..dispatch import RequestData
    from ..pysoap.msgreader import SubscribeRequest

MAX_ROUNDTRIP_VALUES = 20


class _RoundTripData:
    def __init__(self, values, abs_max):
        if values:
            self.values = list(values)  # make a copy
            self.min = min(values)
            self.max = max(values)
            self.avg = sum(values) / len(values)
            self.abs_max = abs_max
        else:
            self.values = None
            self.min = None
            self.max = None
            self.avg = None
            self.abs_max = None

    def __repr__(self):
        return f'min={self.min:.4f} max={self.max:.4f} avg={self.avg:.4f} absmax={self.abs_max:.4f}'


def _mk_dispatch_identifier(reference_parameters: ReferenceParameters, path_suffix: str):
    # this is always our own reference parameter. We know that is has max. one element,
    # and the text is the identifier of the subscription
    if path_suffix == '':
        path_suffix = None
    if reference_parameters.has_parameters:
        return reference_parameters.parameters[0].text, path_suffix
    return None, path_suffix


class _UserRef(Protocol):
    def on_unreachable(self, netloc):
        ...


_SoapClientFactory = Callable[[str, list[str]], Any]


class _SoapClientEntry:
    def __init__(self, soap_client, user_ref: _UserRef):
        self.soap_client = soap_client
        self.user_refs = [user_ref]


class SoapClientPool:
    """pool of soap clients with reference count"""

    def __init__(self, soap_client_factory: _SoapClientFactory):
        self._soap_client_factory = soap_client_factory
        self._soap_clients = {}

    def get_soap_client(self, netloc: str, accepted_encodings: list[str], user_ref: Any) -> Any:
        entry = self._soap_clients.get(netloc)
        if entry is None:
            soap_client = self._soap_client_factory(netloc, accepted_encodings)
            self._soap_clients[netloc] = _SoapClientEntry(soap_client, user_ref)
            return soap_client
        if user_ref not in entry.user_refs:
            entry.user_refs.append(user_ref)
        return entry.soap_client

    def forget(self, netloc: str, user_ref: Any) -> None:
        entry = self._soap_clients.get(netloc)
        if entry is None:
            raise ValueError
        entry.user_refs.remove(user_ref)
        if len(entry.user_refs) == 0:
            entry.soap_client.close()
            self._soap_clients.pop(netloc)

    def report_unreachable(self, netloc: str) -> None:
        entry = self._soap_clients.get(netloc)
        if entry is None:
            return
        for user_ref in entry.user_refs:
            user_ref.on_unreachable(netloc)
        self._soap_clients.pop(netloc)


class SubscriptionBase:
    MAX_NOTIFY_ERRORS = 1
    IDENT_TAG = etree_.QName('http.local.com', 'MyDevIdentifier')

    def __init__(self, subscribe_request, base_urls, max_subscription_duration, msg_factory):
        """

        :param subscribe_request:
        :param base_urls:
        :param max_subscription_duration:
        :param msg_factory:
        """
        self.mode = subscribe_request.mode
        self.base_urls = base_urls
        self._msg_factory = msg_factory
        self.notify_to_address = subscribe_request.notify_to_address
        self.notify_to_url = urlparse(subscribe_request.notify_to_address)

        self.notify_ref_params = subscribe_request.notify_ref_params

        self.end_to_address = subscribe_request.end_to_address
        if self.end_to_address is not None:
            self._end_to_url = urlparse(self.end_to_address)
        else:
            self._end_to_url = None
        self.end_to_ref_params = subscribe_request.end_to_ref_params
        self.identifier_uuid = uuid.uuid4()
        self.reference_parameters = ReferenceParameters(None)  # default: no reference parameters
        self.path_suffix = None  # used for path based dispatching

        self._max_subscription_duration = max_subscription_duration
        self._started = None
        self._expire_seconds = None
        self.renew(subscribe_request.expires)  # sets self._started and self._expire_seconds
        self.filters = subscribe_request.subscription_filters

        self._accepted_encodings = subscribe_request.accepted_encodings
        self._soap_client = None

        self.notify_errors = 0
        self._is_closed = False
        self._is_connection_error = False
        self.last_roundtrip_times = deque(
            maxlen=MAX_ROUNDTRIP_VALUES)  # a list of last n roundtrip times for notifications
        self.max_roundtrip_time = 0

    def set_reference_parameter(self):
        """Create a ReferenceParameters instance with a reference parameter"""
        reference_parameter = etree_.Element(self.IDENT_TAG)
        reference_parameter.text = self.identifier_uuid.hex
        self.reference_parameters = ReferenceParameters([reference_parameter])

    def set_soap_client(self, soap_client):
        self._soap_client = soap_client

    def renew(self, expires):
        self._started = time.monotonic()
        if expires:
            self._expire_seconds = min(expires, self._max_subscription_duration)
        else:
            self._expire_seconds = self._max_subscription_duration

    @property
    def soap_client(self):
        return self._soap_client

    @property
    def remaining_seconds(self):
        duration = int(self._expire_seconds - (time.monotonic() - self._started))
        return 0 if duration < 0 else duration

    @property
    def expire_string(self):
        return isoduration.duration_string(self.remaining_seconds)

    @property
    def has_delivery_failure(self):
        return self.notify_errors >= self.MAX_NOTIFY_ERRORS

    @property
    def has_connection_error(self):
        return self._is_connection_error

    @property
    def is_valid(self):
        if self._is_closed:
            return False
        return self.remaining_seconds > 0 and not self.has_delivery_failure

    def matches(self, action):
        action = action.strip()  # just to be sure there are no spaces....
        for filter_string in self.filters:
            if filter_string.endswith(action):
                return True
        return False

    def send_notification_end_message(self, code='SourceShuttingDown',
                                      reason='Event source going off line.'):
        url = self.base_urls[0]
        my_addr = f'{url.scheme}:{url.netloc}/{url.path}'

        if not self.is_valid:
            return
        if self._soap_client is None:
            return
        message = self._msg_factory.mk_notification_end_message(self, my_addr, code, reason)
        try:
            url = self._end_to_url or self.notify_to_url
            self._soap_client.post_message_to(url.path, message,
                                              msg='send_notification_end_message')
            self.notify_errors = 0
            self._is_connection_error = False
            self._is_closed = True
        except Exception:
            # it does not matter that we could not send the message - end is end ;)
            pass

    def close(self):
        self._is_closed = True

    def is_closed(self):
        return self._is_closed

    def __repr__(self):
        try:
            if self.notify_ref_params is None:
                ref_ident = '<none>'
            else:
                ref_ident = str(
                    self.notify_ref_params)
        except TypeError:
            ref_ident = '<unknown>'
        return f'Subscription(notify_to={self.notify_to_address} ident={ref_ident}, ' \
               f'my identifier={self.identifier_uuid.hex}, expires={self.remaining_seconds}, ' \
               f'filter={short_filter_string(self.filters)})'

    def get_roundtrip_stats(self):
        if len(self.last_roundtrip_times) > 0:
            return _RoundTripData(self.last_roundtrip_times, self.max_roundtrip_time)
        return _RoundTripData(None, None)

    def short_filter_names(self):
        return tuple([f.split('/')[-1] for f in self.filters])

class DevSubscription(SubscriptionBase):

    def send_notification_report(self, msg_factory, body_node, action, doc_nsmap):
        if not self.is_valid:
            return
        addr = Address(addr_to=self.notify_to_address,
                       action=action,
                       addr_from=None,
                       reply_to=None,
                       fault_to=None,
                       reference_parameters=None)
        message = msg_factory.mk_notification_message(addr, body_node, self.notify_ref_params, doc_nsmap)
        try:
            roundtrip_timer = observableproperties.SingleValueCollector(self._soap_client, 'roundtrip_time')

            self._soap_client.post_message_to(self.notify_to_url.path, message,
                                              msg=f'send_notification_report {action}')
            try:
                roundtrip_time = roundtrip_timer.result(0)
                self.last_roundtrip_times.append(roundtrip_time)
                self.max_roundtrip_time = max(self.max_roundtrip_time, roundtrip_time)
            except observableproperties.CollectTimeoutError:
                pass
            self.notify_errors = 0
            self._is_connection_error = False
        except HTTPReturnCodeError:
            self.notify_errors += 1
            raise
        except Exception:  # any other exception is handled as an unreachable location (disconnected)
            self.notify_errors += 1
            self._is_connection_error = True
            raise


class SubscriptionsManagerBase:
    """This implementation uses ReferenceParameters to identify subscriptions."""
    DEFAULT_MAX_SUBSCR_DURATION = 7200  # max. possible duration of a subscription
    # observable has tuple(action, mdib_version_group, body_node)
    sent_to_subscribers = observableproperties.ObservableProperty(fire_only_on_changed_value=False)

    def __init__(self,
                 sdc_definitions: BaseDefinitions,
                 msg_factory: MessageFactoryDevice,
                 soap_client_pool: SoapClientPool,
                 max_subscription_duration: [float, None] = None,
                 log_prefix: str = None,
                 ):
        self.sdc_definitions = sdc_definitions
        self._msg_factory = msg_factory
        self._soap_client_pool: SoapClientPool = soap_client_pool
        self._logger = loghelper.get_logger_adapter('sdc.device.subscrMgr', log_prefix)
        self._max_subscription_duration = max_subscription_duration or self.DEFAULT_MAX_SUBSCR_DURATION
        self._subscriptions = multikey.MultiKeyLookup()
        self._subscriptions.add_index(
            'dispatch_identifier',
            multikey.UIndexDefinition(lambda obj: _mk_dispatch_identifier(obj.reference_parameters, obj.path_suffix)))
        self._subscriptions.add_index('identifier', multikey.UIndexDefinition(lambda obj: obj.identifier_uuid.hex))
        self._subscriptions.add_index('netloc', multikey.IndexDefinition(
            lambda obj: obj.notify_to_url.netloc))
        self.base_urls = None

    def set_base_urls(self, base_urls):
        self.base_urls = base_urls

    def _mk_subscription_instance(self, subscribe_request):
        return DevSubscription(subscribe_request, self.base_urls, self._max_subscription_duration,
                                msg_factory=self._msg_factory)

    def on_subscribe_request(self, request_data: RequestData,
                             subscribe_request: SubscribeRequest) -> CreatedMessage:

        subscription = self._mk_subscription_instance(subscribe_request)
        # assign a soap client
        key = subscription.notify_to_url.netloc
        soap_client = self._soap_client_pool.get_soap_client(key, subscribe_request.accepted_encodings, self)
        subscription.set_soap_client(soap_client)
        with self._subscriptions.lock:
            self._subscriptions.add_object(subscription)
        self._logger.info('new {}', subscription)
        response = self._msg_factory.mk_subscribe_response_message(request_data, subscription, self.base_urls)
        return response

    def on_unsubscribe_request(self, request_data: RequestData) -> CreatedMessage:
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            fault = SoapFault(code=FaultCodeEnum.RECEIVER,
                              reason='unknown Subscription identifier',
                              sub_code=self.sdc_definitions.data_model.ns_helper.wseTag('InvalidMessage')
                              )
            response = self._msg_factory.mk_fault_message(request_data.message_data, fault)
        else:
            subscription.close()
            with self._subscriptions.lock:
                self._subscriptions.remove_object(subscription)
            self._logger.info('unsubscribe: object found and removed (Xaddr = {}, filter = {})',
                              subscription.notify_to_address,
                              subscription.filters)
            # now check if we can close the soap client
            key = subscription.notify_to_url.netloc
            subscriptions_with_same_soap_client = self._subscriptions.netloc.get(key, [])
            if len(subscriptions_with_same_soap_client) == 0:
                self._soap_client_pool.forget(key, self)
            response = self._msg_factory.mk_unsubscribe_response_message(request_data)
        return response

    def on_get_status_request(self, request_data: RequestData) -> CreatedMessage:
        self._logger.debug('on_get_status_request {}', lambda: request_data.message_data.p_msg.raw_data)
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            fault = SoapFault(code=FaultCodeEnum.RECEIVER,
                              reason='unknown Subscription identifier',
                              sub_code=self.sdc_definitions.data_model.ns_helper.wseTag('InvalidMessage')
                              )
            response = self._msg_factory.mk_fault_message(request_data.message_data, fault)
        else:
            response = self._msg_factory.mk_getstatus_response_message(request_data, subscription.remaining_seconds)
        return response

    def on_renew_request(self, request_data: RequestData) -> CreatedMessage:
        reader = request_data.message_data.msg_reader
        expires = reader.read_renew_request(request_data.message_data)
        subscription = self._get_subscription_for_request(request_data)
        if subscription is None:
            fault = SoapFault(code=FaultCodeEnum.RECEIVER,
                              reason='unknown Subscription identifier',
                              sub_code=self.sdc_definitions.data_model.ns_helper.wseTag('UnableToRenew')
                              )
            response = self._msg_factory.mk_fault_message(request_data.message_data, fault)

        else:
            subscription.renew(expires)
            response = self._msg_factory.mk_renew_response_message(request_data, subscription.remaining_seconds)
        return response

    def stop_all(self, send_subscription_end: bool):
        self.end_all_subscriptions(send_subscription_end)

    def end_all_subscriptions(self, send_subscription_end: bool):
        with self._subscriptions.lock:
            if send_subscription_end:
                apply_map(lambda subscription: subscription.send_notification_end_message(),
                          self._subscriptions.objects)
            self._subscriptions.clear()

    def _get_subscription_for_request(self, request_data):
        reader = request_data.message_data.msg_reader
        reference_parameters = reader.read_header_reference_parameters(request_data.message_data)
        path_suffix = '/'.join(request_data.path_elements)  # not consumed path elements
        dispatch_identifier = _mk_dispatch_identifier(reference_parameters, path_suffix)
        with self._subscriptions.lock:
            subscription = self._subscriptions.dispatch_identifier.get_one(dispatch_identifier, allow_none=True)
        if subscription is None:
            self._logger.warning('{}: unknown Subscription identifier "{}" from {}',
                                 request_data.message_data.q_name, dispatch_identifier, request_data.peer_name)
        return subscription

    def send_to_subscribers(self, body_node, action, mdib_version_group, nsmapper, what):
        subscribers = self._get_subscriptions_for_action(action)
        self.sent_to_subscribers = (action, mdib_version_group, body_node)  # update observable
        for subscriber in subscribers:
            if what:
                self._logger.debug('{}: sending report to {}', what, subscriber.notify_to_address)
            try:
                self._send_notification_report(
                    subscriber, body_node, action,
                    nsmapper.partial_map(nsmapper.PM, nsmapper.S12, nsmapper.WSA, nsmapper.WSE))
            except:
                pass
        self._do_housekeeping()

    def _send_notification_report(self, subscription, body_node, action, doc_nsmap):
        try:
            subscription.send_notification_report(self._msg_factory, body_node, action, doc_nsmap)
        except ConnectionRefusedError as ex:
            self._logger.error('could not send notification report: {!r}:  subscr = {}', ex, subscription)
        except HTTPReturnCodeError as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: HTTP status= {}, reason={}, {}', ex.status,
                               ex.reason, subscription)
        except http.client.NotConnected as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report: {!r}:  subscr = {}', ex, subscription)
        except socket.timeout as ex:
            # this is an error related to the connection => log error and continue
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)
        except etree_.DocumentInvalid as ex:
            # this is an error related to the document, it cannot be sent to any subscriber => re-raise
            self._logger.error('Invalid Document: {!r}\n{}', ex, etree_.tostring(body_node))
            raise
        except Exception as ex:
            # this should never happen! => re-raise
            self._logger.error('could not send notification report error= {!r}: {}', ex, subscription)
            raise

    def _get_subscriptions_for_action(self, action):
        with self._subscriptions.lock:
            return [s for s in self._subscriptions.objects if s.matches(action)]

    def on_unreachable(self, netloc):
        """This is info from another subscription manager.
        Remove all subscriptions with same location."""
        with self._subscriptions.lock:
            for subscription in list(self._subscriptions.objects):
                if subscription.notify_to_url.netloc == netloc:
                    subscription.close()
                    self._subscriptions.remove_object(subscription)

    def _do_housekeeping(self):
        """ remove expired or invalid subscriptions"""
        with self._subscriptions.lock:
            invalid_subscriptions = [s for s in self._subscriptions.objects if not s.is_valid]
        unreachable_netlocs = []
        for invalid_subscription in invalid_subscriptions:
            if invalid_subscription.has_connection_error:
                # the network location is unreachable, we can remove all subscriptions that use this location
                unreachable_netlocs.append(invalid_subscription.soap_client.netloc)
                try:
                    invalid_subscription.close()
                    invalid_subscription.soap_client.close()
                except OSError:
                    self._logger.error('error in soap client.close(): {}', traceback.format_exc())

            self._logger.info('deleting {}, errors={}', invalid_subscription,
                              invalid_subscription.notify_errors)

            with self._subscriptions.lock:
                self._subscriptions.remove_object(invalid_subscription)

        # now find all subscriptions that have the same address
        with self._subscriptions.lock:
            also_unreachables = [s for s in self._subscriptions.objects if
                                 s.soap_client is not None and s.soap_client.netloc in unreachable_netlocs]
            for unreachable in also_unreachables:
                self._logger.info('deleting also subscription {}, same endpoint', unreachable)
                self._subscriptions.remove_object(unreachable)

        # tell connection pool that net locations are unreachable:
        for netloc in unreachable_netlocs:
            self._soap_client_pool.forget(netloc, self)
            self._soap_client_pool.report_unreachable(netloc)


class SubscriptionsManagerPath(SubscriptionsManagerBase):
    """This implementation uses path dispatching to identify subscriptions."""

    def _mk_subscription_instance(self, subscribe_request):
        subscription = super()._mk_subscription_instance(subscribe_request)
        subscription.path_suffix = subscription.identifier_uuid.hex
        return subscription


class SubscriptionsManagerReferenceParam(SubscriptionsManagerBase):
    """This implementation uses reference parameters to identify subscriptions."""

    def _mk_subscription_instance(self, subscribe_request):
        subscription = super()._mk_subscription_instance(subscribe_request)
        # add  a reference parameter
        subscription.set_reference_parameter()
        return subscription
