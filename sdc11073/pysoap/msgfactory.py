from __future__ import annotations

from io import BytesIO
from typing import Optional, TYPE_CHECKING

from lxml import etree as etree_

from .msgreader import validate_node
from .soapenvelope import Soap12Envelope
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from ..schema_resolver import SchemaResolver
from ..schema_resolver import mk_schema_validator

if TYPE_CHECKING:
    from sdc11073.xml_types.msg_types import MessageType

_LANGUAGE_ATTR = '{http://www.w3.org/XML/1998/namespace}lang'


class CreatedMessage:
    def __init__(self, message, msg_factory):
        self.p_msg = message
        self.msg_factory = msg_factory

    def serialize(self, pretty=False, request_manipulator=None, validate=True):
        return self.msg_factory.serialize_message(self, pretty, request_manipulator, validate)


# pylint: disable=no-self-use


class MessageFactory:
    """This class creates soap messages. It is used in two phases:
     1) call one of the mk_xxx methods. All return a CreatedMessage instance that contains the data provided in the call
     2) call the serialize method of the CreatedMessage instance to get the xml representation
     """

    def __init__(self, sdc_definitions, logger, validate=True):
        self._logger = logger
        self._sdc_definitions = sdc_definitions

        self._validate = validate
        self._xml_schema = mk_schema_validator(SchemaResolver(sdc_definitions))

    def serialize_message(self, message: CreatedMessage, pretty=False,
                          request_manipulator=None, validate=True) -> bytes:
        """

        :param message: a CreatedMessage instance
        :param pretty:
        :param request_manipulator: can modify data before sending
        :param validate: if False, no validation is performed, independent of constructor setting
        :return: bytes
        """
        p_msg = message.p_msg
        nsh = self._sdc_definitions.data_model.ns_helper
        tmp = BytesIO()
        root = etree_.Element(nsh.s12Tag('Envelope'), nsmap=p_msg.nsmap)

        header_node = etree_.SubElement(root, nsh.s12Tag('Header'))
        if p_msg.header_info_block:
            info_node = p_msg.header_info_block.as_etree_node('tmp', {})
            header_node.extend(info_node[:])
        header_node.extend(p_msg.header_nodes)
        body_node = etree_.SubElement(root, nsh.s12Tag('Body'), nsmap=p_msg.nsmap)
        if p_msg.payload_element is not None:
            body_node.append(p_msg.payload_element)
        if validate:
            self._validate_node(root)

        doc = etree_.ElementTree(element=root)
        if hasattr(request_manipulator, 'manipulate_domtree'):
            _doc = request_manipulator.manipulate_domtree(doc)
            if _doc:
                doc = _doc
        doc.write(tmp, encoding='UTF-8', xml_declaration=True, pretty_print=pretty)
        return tmp.getvalue()

    def mk_soap_message(self,
                        header_info: HeaderInformationBlock,
                        payload: MessageType,
                        ns_map: Optional[list] = None):
        nsh = self._sdc_definitions.data_model.ns_helper
        ns_set = {nsh.S12, nsh.WSA, nsh.MSG, nsh.PM}  # default
        ns_set.update(payload.additional_namespaces)
        if ns_map:
            ns_set.update(ns_map)
        my_ns_map = nsh.partial_map(*ns_set)
        soap_envelope = Soap12Envelope(my_ns_map)
        soap_envelope.set_header_info_block(header_info)
        soap_envelope.payload_element = payload.as_etree_node(payload.NODETYPE, my_ns_map)
        return CreatedMessage(soap_envelope, self)

    def mk_soap_message_etree_payload(self,
                                      header_info: HeaderInformationBlock,
                                      payload_element: Optional[etree_.Element] = None):
        nsh = self._sdc_definitions.data_model.ns_helper
        my_ns_map = nsh.partial_map(nsh.S12, nsh.WSE, nsh.WSA)
        soap_envelope = Soap12Envelope(my_ns_map)
        soap_envelope.set_header_info_block(header_info)
        soap_envelope.payload_element = payload_element
        return CreatedMessage(soap_envelope, self)

    def mk_reply_soap_message(self,
                              request,
                              response_payload: MessageType,
                              ns_map: Optional[list] = None):
        nsh = self._sdc_definitions.data_model.ns_helper
        ns_set = {nsh.S12, nsh.WSA, nsh.MSG, nsh.PM}  # default
        ns_set.update(response_payload.additional_namespaces)
        if ns_map:
            ns_set.update(ns_map)
        my_ns_map = nsh.partial_map(*ns_set)
        soap_envelope = Soap12Envelope(my_ns_map)
        reply_address = request.message_data.p_msg.header_info_block.mk_reply_header_block(
            action=response_payload.action)
        soap_envelope.set_header_info_block(reply_address)
        soap_envelope.payload_element = response_payload.as_etree_node(response_payload.NODETYPE, my_ns_map)
        return CreatedMessage(soap_envelope, self)

    def _validate_node(self, node):
        if self._validate:
            validate_node(node, self._xml_schema, self._logger)
