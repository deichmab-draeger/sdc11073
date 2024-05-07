from __future__ import annotations

import logging
import pathlib
import threading
from urllib.parse import SplitResult
from decimal import Decimal
from typing import TYPE_CHECKING

from lxml import etree as etree_

from sdc11073.entitymdib.providermdib import EntityProviderMdib
from sdc11073.namespaces import default_ns_helper as ns_hlp
# from sdc11073.provider import SdcProvider
from sdc11073.entitymdib.provider import SdcProviderEntity
from sdc11073.provider.subscriptionmgr import BicepsSubscription
from sdc11073.xml_types import pm_types, pm_qnames as pm
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types.dpws_types import ThisModelType, ThisDeviceType
from sdc11073.xml_types.eventing_types import Subscribe

if TYPE_CHECKING:
    import sdc11073.certloader
    import uuid
    from sdc11073.pysoap.soapclientpool import SoapClientPool
    from sdc11073.provider.providerimpl import WsDiscoveryProtocol
    from sdc11073.provider.components import SdcProviderComponents

ports_lock = threading.Lock()
_ports = 10000

_mockhttpservers = {}

_logger = logging.getLogger('sdc.mock')


def dec_list(*args):
    return [Decimal(x) for x in args]


def _findServer(netloc):
    dev_addr = netloc.split(':')
    dev_addr = tuple([dev_addr[0], int(dev_addr[1])])  # make port number an integer
    for key, srv in _mockhttpservers.items():
        if tuple(key) == dev_addr:
            return srv
    raise KeyError('{} is not in {}'.format(dev_addr, _mockhttpservers.keys()))



class SomeDeviceEntity(SdcProviderEntity):
    """A device used for unit tests. Some values are predefined."""

    def __init__(self, wsdiscovery: WsDiscoveryProtocol,
                 mdib_xml_data: bytes,
                 epr: str | uuid.UUID | None = None,
                 validate: bool = True,
                 ssl_context_container: sdc11073.certloader.SSLContextContainer | None = None,
                 max_subscription_duration: int = 15,
                 log_prefix: str = '',
                 default_components: SdcProviderComponents | None = None,
                 specific_components: SdcProviderComponents | None = None,
                 chunk_size: int = 0):
        model = ThisModelType(manufacturer='Example Manufacturer',
                              manufacturer_url='www.example-manufacturer.com',
                              model_name='SomeDevice',
                              model_number='1.0',
                              model_url='www.example-manufacturer.com/whatever/you/want/model',
                              presentation_url='www.example-manufacturer.com/whatever/you/want/presentation')
        device = ThisDeviceType(friendly_name='Py SomeDevice',
                                firmware_version='0.99',
                                serial_number='12345')

        device_mdib_container = EntityProviderMdib.from_string(mdib_xml_data, log_prefix=log_prefix)
        device_mdib_container.instance_id = 1  # set the optional value
        # set Metadata
        mds_entities = device_mdib_container.entities.NODETYPE.get(pm.MdsDescriptor)
        for mds_entity in mds_entities:
            if mds_entity.descriptor.MetaData is not None:
                mds_entity.descriptor.MetaData.Manufacturer.append(pm_types.LocalizedText('Example Manufacturer'))
                mds_entity.descriptor.MetaData.ModelName.append(pm_types.LocalizedText(model.ModelName[0].text))
                mds_entity.descriptor.MetaData.SerialNumber.append('ABCD-1234')
                mds_entity.descriptor.MetaData.ModelNumber = '0.99'
        super().__init__(wsdiscovery, model, device, device_mdib_container, epr, validate,
                         ssl_context_container=ssl_context_container,
                         max_subscription_duration = max_subscription_duration,
                         log_prefix=log_prefix,
                         default_components=default_components,
                         specific_components=specific_components,
                         chunk_size=chunk_size)

    @classmethod
    def from_mdib_file(cls,
                       wsdiscovery: WsDiscoveryProtocol,
                       epr: str | uuid.UUID | None,
                       mdib_xml_path: str | pathlib.Path,
                       validate: bool =True,
                       ssl_context_container: sdc11073.certloader.SSLContextContainer | None = None,
                       max_subscription_duration: int = 15,
                       log_prefix: str = '',
                       default_components: SdcProviderComponents | None = None,
                       specific_components: SdcProviderComponents | None = None,
                       chunk_size: int = 0) -> SomeDeviceEntity:
        """Construct class with path to a mdib file."""
        mdib_xml_path = pathlib.Path(mdib_xml_path)
        if not mdib_xml_path.is_absolute():
            mdib_xml_path = pathlib.Path(__file__).parent.joinpath(mdib_xml_path)
        return cls(wsdiscovery, mdib_xml_path.read_bytes(), epr, validate, ssl_context_container,
                   max_subscription_duration = max_subscription_duration,
                   log_prefix=log_prefix,
                   default_components=default_components, specific_components=specific_components,
                   chunk_size=chunk_size)
