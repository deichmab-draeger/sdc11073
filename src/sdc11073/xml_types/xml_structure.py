"""Classes in this module are used to declare the place where a xml value is located inside a document.

They also provide a mapping between XML data types (which are always stings in specific formats) and
python types. By doing so these classes completely hide the XML nature of data.
The basic offered types are Element, list of elements, attribute, and list of attributes.
They are the buildings blocks that are needed to declare XML data types.
Container properties represent values in xml nodes.
"""
from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Callable

from lxml import etree as etree_

from sdc11073 import xml_utils
from sdc11073.exceptions import ApiUsageError
from sdc11073.namespaces import QN_TYPE, docname_from_qname, text_to_qname

from . import isoduration
from .dataconverters import (
    BooleanConverter,
    ClassCheckConverter,
    DecimalConverter,
    DurationConverter,
    EnumConverter,
    IntegerConverter,
    ListConverter,
    NullConverter,
    StringConverter,
    TimestampConverter,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from decimal import Decimal

    from sdc11073.mdib.containerbase import ContainerBase
    from sdc11073.namespaces import NamespaceHelper
    from sdc11073.xml_types.basetypes import XMLTypeBase

    from .dataconverters import DataConverterProtocol
    from .isoduration import DurationType

STRICT_TYPES = True  # if True, only the expected types are excepted.
MANDATORY_VALUE_CHECKING = True  # checks if mandatory values are present when xml is generated


class ElementNotFoundError(Exception):  # noqa: D101
    pass


class _NumberStack:
    # uses as a part of _local_var_name in _XmlStructureBaseProperty.
    # This makes duplicate names impossible
    _value = 0

    @classmethod
    def unique_number(cls) -> str:
        cls._value += 1
        return str(cls._value)


class _XmlStructureBaseProperty(ABC):
    """_XmlStructureBaseProperty defines a python property that converts between Python Data Types and XML data types.

    It has knowledge about two things:
    - how to covert data from xml to python type and vice versa
    - name/ location of the xml data in a node.

    All derived Properties have the same interface:
    __get__ and __set__ : read and write access, using Python data types.
    update_from_node: reads the value from XML data and converts it to Python data type.
    update_xml_value: convert the Python data type to XML type and write it to XML node.
    """

    supports_xml_value = False  # only some properties support it
    def __init__(self, local_var_name: str,  # noqa: PLR0913
                 value_converter: DataConverterProtocol,
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False):
        """Construct an instance.

        :param local_var_name: a member with this same is added to instance
        :param value_converter: DataConverterProtocol
        :param default_py_value: initial value when initialized
                                 (should be set for mandatory elements, otherwise created xml might violate schema)
                                 and if the xml element does not exist.
        :param implied_py_value: for optional elements, this is the value that shall be implied if
                                 xml element does not exist.
                                 This value is for information only! Access only via class possible.
        :param is_optional: reflects if this element is optional in schema
        """
        if implied_py_value is not None and default_py_value is not None:
            raise ValueError('set only one of default_py_value and implied_py_value')
        if not is_optional and implied_py_value is not None:
            raise ValueError('is_optional == False and implied_py_value != None is not allowed ')
        if not hasattr(value_converter, 'check_valid'):
            raise TypeError
        self._converter = value_converter
        if STRICT_TYPES:
            if default_py_value is not None:
                self._converter.check_valid(default_py_value)
            if implied_py_value is not None:
                self._converter.check_valid(implied_py_value)
        self._default_py_value = None
        self._implied_py_value = None
        if default_py_value is not None:
            self._default_py_value = default_py_value
        if implied_py_value is not None:
            self._implied_py_value = implied_py_value
        self._is_optional = is_optional
        # _local_var_name and _local_var_name_xml are names of member variables that are added to instance
        # They contain the python value and the xml value.
        # This allows to implement the following features:
        # - access to the xml string without any reformatting or rounding effects (float values)
        # - unless the provider application changed a (python data type )value programmatically,
        #   the value is sent to consumer exactly like it was read from mdib file.
        # - the provider can alternatively set the value as xml string
        #
        # _local_var_name and _local_var_name_xml are used in the following way:
        # - update_from_node:
        #   - always sets the _local_var_name value
        #   - set the _local_var_name_xml if a string is base of the value. That means all attributes,
        #     and elements text. No subtrees that represent a complex value (e.g. CodedValue, ...)
        # - update_xml_value:
        #   - if _local_var_name_xml exists and is not None, this value is used.
        #   - else if _local_var_name is not None, this value is used
        #   - else the fault value is used (can also be None)

        self._local_var_name = local_var_name
        self._local_var_name_xml = f'{local_var_name}_xml'

    @property
    def is_optional(self) -> bool:
        return self._is_optional

    def __get__(self, instance, owner) -> Any:  # noqa: ANN001
        """Return a python value, use the locally stored value."""
        if instance is None:  # if called via class
            return self
        try:
            value = getattr(instance, self._local_var_name)
        except AttributeError:
            value = None
        if value is None:
            value = self._implied_py_value
        return value

    def get_actual_value(self, instance: Any) -> Any | None:
        """Return the actual value without considering default value and implied value.

        E.g. return None if no value in xml exists.
        :param instance: the instance that has the property as member
        """
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            return None

    def get_xml_value(self, instance: Any) -> str | None:
        """Return the xml string if available.

        Raise a TypeError if not supported
        """
        if not self.supports_xml_value:
            raise TypeError(f'xml value not supported in {self.__class__.__name__}({instance.__class__.__name__})')
        try:
            return getattr(instance, self._local_var_name_xml)
        except AttributeError:
            return None

    def __set__(self, instance: Any, py_value: Any):
        """Value is the representation on the program side, e.g a float."""
        if STRICT_TYPES:
            self._converter.check_valid(py_value)
        setattr(instance, self._local_var_name, py_value)
        if self.supports_xml_value:
            # set call invalidates the xml string
            if py_value is None:
                setattr(instance, self._local_var_name_xml, None)
            else:
                setattr(instance, self._local_var_name_xml, self._get_xml_value_from_py_value(py_value))

    def init_instance_data(self, instance: Any):
        """Set initial values to default_py_value.

        This method is used internally and should not be called by application.
        :param instance: the instance that has the property as member
        :return: None
        """
        if self._default_py_value is not None:
            setattr(instance, self._local_var_name, copy.deepcopy(self._default_py_value))

    @abstractmethod
    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Update node with current data from instance.

        This method is used internally and should not be called by application.
        """

    @abstractmethod
    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        """

    def _get_xml_value_from_py_value(self, py_value: Any) -> None:  # noqa: ARG002
        """Convert the python data type to string that can be used in xml.

        Default implementation only returns None, this is correct for all cases where supports_xml_value is False.
        """
        return


class _AttributeBase(_XmlStructureBaseProperty):
    """Base class that represents an XML Attribute.

    The XML Representation is a string.
    The python representation is determined by value_converter.
    """

    supports_xml_value = True

    def __init__(self, attribute_name: str,  # noqa: PLR0913
                 value_converter: DataConverterProtocol | None = None,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = True):
        """Construct an instance.

        :param attribute_name: name of the attribute in xml node
        :param value_converter: converter between xml value and python value
        :param default_py_value: see base class doc.
        :param implied_py_value: see base class doc.
        :param is_optional: see base class doc.
        """
        if isinstance(attribute_name, etree_.QName):
            local_var_name = f'_a_{attribute_name.localname}_{_NumberStack.unique_number()}'
        else:
            local_var_name = f'_a_{attribute_name.lower()}_{_NumberStack.unique_number()}'
        super().__init__(local_var_name, value_converter, default_py_value, implied_py_value, is_optional)
        self._attribute_name = attribute_name

    def _get_xml_value_from_node(self, node: xml_utils.LxmlElement) -> str | None:
        return node.attrib.get(self._attribute_name)

    def _get_py_value_from_xml_value(self, xml_value: Any, nsmap: dict) -> Any:  # noqa: ARG002
        return self._converter.to_py(xml_value)

    def _get_xml_value_from_py_value(self, py_value: Any) -> str | None:
        return None if py_value is None else self._converter.to_xml(py_value)

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement | None):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return: value
        :return:
        """
        xml_value = None
        py_value = None
        if node is not None:
            xml_value = self._get_xml_value_from_node(node)
            if xml_value is not None:
                py_value = self._get_py_value_from_xml_value(xml_value, node.nsmap)
        setattr(instance, self._local_var_name, py_value)
        setattr(instance, self._local_var_name_xml, xml_value)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            xml_value = getattr(instance, self._local_var_name_xml)
            if xml_value is not None:
                node.set(self._attribute_name, xml_value)
                return
        except AttributeError:
            pass
        # continue with py_value
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            # this can happen if there is no default value defined and __set__ has never been called
            py_value = None

        if py_value is None:
            if MANDATORY_VALUE_CHECKING and not self.is_optional:
                raise ValueError(f'mandatory value {self._attribute_name} missing')
            try:
                if self._attribute_name in node.attrib:
                    del node.attrib[self._attribute_name]
            except ElementNotFoundError:
                return
        else:
            xml_value = self._get_xml_value_from_py_value(py_value)
            if xml_value is not None:
                node.set(self._attribute_name, xml_value)

    def __str__(self) -> str:
        return f'{self.__class__.__name__} attribute {self._attribute_name}'



class StringAttributeProperty(_AttributeBase):
    """Python representation is a string."""

    def __init__(self, attribute_name: str,
                 default_py_value: Any = None,
                 implied_py_value: Any = None, is_optional: bool = True):
        super().__init__(attribute_name, StringConverter, default_py_value, implied_py_value, is_optional)


class AnyURIAttributeProperty(StringAttributeProperty):
    """Represents an AnyURIAttribute."""


class CodeIdentifierAttributeProperty(StringAttributeProperty):
    """Represents a CodeIdentifier attribute."""


class HandleAttributeProperty(StringAttributeProperty):
    """Represents a Handle attribute."""


class HandleRefAttributeProperty(StringAttributeProperty):
    """Represents a HandleRef attribute."""


class SymbolicCodeNameAttributeProperty(StringAttributeProperty):
    """Represents a SymbolicCodeName attribute."""


class ExtensionAttributeProperty(StringAttributeProperty):
    """Represents an Extension attribute."""


class LocalizedTextRefAttributeProperty(StringAttributeProperty):
    """Represents a LocalizedTextRef attribute."""


class TimeZoneAttributeProperty(StringAttributeProperty):
    """Represents a TimeZone attribute."""


class EnumAttributeProperty(_AttributeBase):
    """Base class for enum attributes."""

    def __init__(self, attribute_name: str,  # noqa: PLR0913
                 enum_cls: Any,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, EnumConverter(enum_cls), default_py_value, implied_py_value, is_optional)


class TimestampAttributeProperty(_AttributeBase):
    """Represents a Timestamp attribute.

    XML notation is integer in milliseconds.
    Python is a float in seconds.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=TimestampConverter(),
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class CurrentTimestampAttributeProperty(_AttributeBase):
    """Represents a special Timestamp attribute used for ClockState, it always writes current time to node.

    Setting the value from python is possible, but makes no sense.
    """

    def __init__(self, attribute_name: str,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=TimestampConverter(),
                         default_py_value=None, is_optional=is_optional)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        setattr(instance, self._local_var_name, time.time())
        setattr(instance, self._local_var_name_xml, None)
        super().update_xml_value(instance, node)


class DecimalAttributeProperty(_AttributeBase):
    """Represents a Decimal attribute."""

    def __init__(self, attribute_name: str,
                 default_py_value: Decimal | None = None,
                 implied_py_value: Decimal | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=DecimalConverter(),
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class QualityIndicatorAttributeProperty(DecimalAttributeProperty):
    """Represents a QualityIndicator attribute, a value between 0 and 1."""


class DurationAttributeProperty(_AttributeBase):
    """Represents a Duration attribute.

    XML notation is integer in milliseconds.
    Python is a float in seconds.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: DurationType | None = None,
                 implied_py_value: DurationType | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=DurationConverter(),
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class IntegerAttributeProperty(_AttributeBase):
    """Represents an Integer attribute.

    XML notation is an integer, python is an integer.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: int | None = None,
                 implied_py_value: int | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=IntegerConverter(),
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class UnsignedIntAttributeProperty(IntegerAttributeProperty):
    """Represents an UnsignedInt attribute.

    Python has no unsigned int, therefore this is the same as IntegerAttributeProperty.
    """


class VersionCounterAttributeProperty(UnsignedIntAttributeProperty):
    """Represents a VersionCounter attribute.

    VersionCounter in BICEPS is unsigned long.
    Python has no unsigned long, therefore this is the same as IntegerAttributeProperty.
    """


class ReferencedVersionAttributeProperty(VersionCounterAttributeProperty):
    """Represents an ReferencedVersion attribute."""


class BooleanAttributeProperty(_AttributeBase):
    """Represents a Boolean attribute.

    XML notation is 'true' or 'false'.
    Python is a bool.
    """

    def __init__(self, attribute_name: str,
                 default_py_value: bool | None = None,
                 implied_py_value: bool | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=BooleanConverter,
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)


class QNameAttributeProperty(_AttributeBase):
    """Represents a qualified name attribute.

    XML Representation is a prefix:name string, Python representation is a QName.
    """

    # xml value is not supported because the xml value can only be generated if namespace prefix is known
    supports_xml_value = False

    def __init__(self, attribute_name: str,
                 default_py_value: etree_.QName | None = None,
                 implied_py_value: etree_.QName | None = None,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter=ClassCheckConverter(etree_.QName),
                         default_py_value=default_py_value, implied_py_value=implied_py_value, is_optional=is_optional)

    def _get_py_value_from_xml_value(self, xml_value: Any, nsmap: dict) -> Any:
        return text_to_qname(xml_value, nsmap)

    def _get_xml_value_from_py_value(self, py_value: etree_.QName) -> str | None:
        return str(py_value)


class _AttributeListBase(_AttributeBase):
    """Base class for a list of values as attribute.

    XML Representation is a string which is a space separated list.
    Python representation is a list of Any (type depends on ListConverter),
    else a list of converted values.
    """

    _converter: ListConverter

    def __init__(self, attribute_name: str,
                 value_converter: ListConverter,
                 is_optional: bool = True):
        super().__init__(attribute_name, value_converter, is_optional=is_optional)

    def __get__(self, instance, owner):  # noqa: ANN001
        """Return a python value, use the locally stored value."""
        if instance is None:  # if called via class
            return self
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            setattr(instance, self._local_var_name, [])
            return getattr(instance, self._local_var_name)

    def init_instance_data(self, instance: Any):
        setattr(instance, self._local_var_name, [])

    def _get_py_value_from_xml_value(self, xml_value: str, nsmap: dict) -> list[Any]:  # noqa: ARG002
        split_result = xml_value.split(' ')
        return [self._converter.elem_to_py(val) for val in split_result if val]

    def _get_xml_value_from_py_value(self, py_value: Any) -> str | None:
        if py_value is None:
            return None
        return ' '.join([self._converter.elem_to_xml(v) for v in py_value])

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        try:
            xml_value = getattr(instance, self._local_var_name_xml)
            if xml_value is not None:
                node.set(self._attribute_name, xml_value)
        except AttributeError:
            pass

        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None
        if not py_value and self.is_optional:  # is None:
            try:
                if self._attribute_name in node.attrib:
                    del node.attrib[self._attribute_name]
            except ElementNotFoundError:
                return
        else:
            if py_value is None:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._attribute_name} missing')
                xml_value = ''
            else:
                xml_value = self._get_xml_value_from_py_value(py_value)
            node.set(self._attribute_name, xml_value)


class _StringAttributeListBase(_AttributeListBase):
    """Base class for a list of strings as attribute.

    XML Representation is a string which is a space separated list.
    Python representation is a list of strings.
    """

    def __init__(self, attribute_name: str, value_converter: DataConverterProtocol | None = None):
        converter = value_converter or ListConverter(ClassCheckConverter(str))
        super().__init__(attribute_name, converter)


class HandleRefListAttributeProperty(_StringAttributeListBase):
    """Represents a list of HandleRef attribute."""


class EntryRefListAttributeProperty(_StringAttributeListBase):
    """Represents a list of EntryRef attribute."""


class OperationRefListAttributeProperty(_StringAttributeListBase):
    """Represents a list of OperationRef attribute."""


class AlertConditionRefListAttributeProperty(_StringAttributeListBase):
    """Represents a list of AlertConditionRef attribute."""


class DecimalListAttributeProperty(_AttributeListBase):
    """Represents a list of Decimal attribute.

    XML representation: an attribute string that represents 0...n decimals, separated with spaces.
    Python representation: List of Decimal if attribute is set (can be an empty list!), otherwise None.
    """

    def __init__(self, attribute_name: str):
        super().__init__(attribute_name, ListConverter(DecimalConverter))



class _ElementBase(_XmlStructureBaseProperty, ABC):
    """_ElementBase represents an XML Element."""

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_converter: DataConverterProtocol,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = False):
        """Construct the representation of a (sub) element in xml.

        :param sub_element_name: a QName or None. If None, the property represents the node itself,
                                 otherwise the sub node with given name.
        :param value_converter: see base class doc.
        :param default_py_value: see base class doc.
        :param implied_py_value: see base class doc.
        :param is_optional: see base class doc.
        """
        if sub_element_name is None:
            local_var_name = f'_e_{_NumberStack.unique_number()}'
        else:
            local_var_name = f'_e_{sub_element_name.localname.lower()}_{_NumberStack.unique_number()}'
        super().__init__(local_var_name, value_converter, default_py_value, implied_py_value, is_optional)
        self._sub_element_name = sub_element_name

    @staticmethod
    def _get_element_by_child_name(node: xml_utils.LxmlElement,
                                   sub_element_name: etree_.QName | None,
                                   create_missing_nodes: bool) -> xml_utils.LxmlElement:
        if sub_element_name is None:
            return node
        sub_node = node.find(sub_element_name)
        if sub_node is None:
            if not create_missing_nodes:
                raise ElementNotFoundError(f'Element {sub_element_name} not found in {node.tag}')
            sub_node = etree_.SubElement(node, sub_element_name)  # create this node
        return sub_node

    def remove_sub_element(self, node: xml_utils.LxmlElement):
        if self._sub_element_name is None:
            return
        sub_node = node.find(self._sub_element_name)
        if sub_node is not None:
            node.remove(sub_node)

    def _get_node(self, node: xml_utils.LxmlElement) -> xml_utils.LxmlElement | None :
        """Return the sub node that contains the value.

        :return: None if the element was not found, else the string.
        """
        try:
            return self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=False)
        except ElementNotFoundError:
            return None  # element was not found, return None

    def __str__(self) -> str:
        return f'{self.__class__.__name__} in sub element {self._sub_element_name}'


class NodeTextProperty(_ElementBase):
    """Represents the text of an XML Element.

    Python representation depends on value converter.
    """

    supports_xml_value = True
    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_converter: DataConverterProtocol,
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, value_converter,
                         default_py_value,
                         implied_py_value,
                         is_optional)
        self._min_length = min_length

    def _get_xml_value_from_node(self, node: xml_utils.LxmlElement) -> str | None :
        """Read value from node.

        :return: None if the element was not found, else the string.
        """
        return None if node is None else node.text

    def _get_py_value_from_xml_value(self, xml_value: Any, nsmap: dict) -> Any:  # noqa: ARG002
        return self._converter.to_py(xml_value)

    def _get_xml_value_from_py_value(self, py_value: Any) -> str | None:
        return self._converter.to_xml(py_value)

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return: value
        :return:
        """
        xml_value = None
        py_value = None
        sub_node = self._get_node(node)
        if sub_node is not None:
            xml_value = self._get_xml_value_from_node(sub_node)
            py_value = self._get_py_value_from_xml_value(xml_value, sub_node.nsmap)
        setattr(instance, self._local_var_name, py_value)
        setattr(instance, self._local_var_name_xml, xml_value)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            xml_value = getattr(instance, self._local_var_name_xml)
        except AttributeError:
            xml_value = None

        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None and xml_value is None:
            if MANDATORY_VALUE_CHECKING and not self.is_optional and self._min_length:
                raise ValueError(f'mandatory value {self._sub_element_name} missing')

            if not self._sub_element_name:
                # remove text of this element
                node.text = None
            elif not self.is_optional:
                # create element, is has to be there
                self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            if xml_value is not None:
                sub_node.text = xml_value
            else:
                sub_node.text = self._get_xml_value_from_py_value(py_value)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} in sub-element {self._sub_element_name}'


class NodeStringProperty(NodeTextProperty):
    """Represents the text of an XML Element.

    Python representation is a string.
    libxml sets text of element to None, if text in xml is empty. In this case the python value is an empty string.
    if the xml element that should contain the text does not exist, the python value is None.
    """

    def __init__(self, sub_element_name: etree_.QName | None = None,  # noqa: PLR0913
                 default_py_value: str | None = None,
                 implied_py_value: str | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, StringConverter, default_py_value, implied_py_value,
                         is_optional, min_length)


class AnyUriTextElement(NodeStringProperty):
    """For now the same as NodeStringProperty ,but later it could be handy to add uri type checking."""


class NodeEnumTextProperty(NodeTextProperty):
    """Represents the text of an XML Element.

    Python representation is an enum.
    """

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 enum_cls: Any,
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, EnumConverter(enum_cls), default_py_value, implied_py_value,
                         is_optional, min_length=1)
        self.enum_cls = enum_cls


class NodeEnumQNameProperty(NodeTextProperty):
    """Represents a qualified name as text of an XML Element.

    Python representation is an Enum of QName, XML is prefix:localname.
    """

    # xml value is not supported because the xml value can only be generated if namespace prefix is known
    supports_xml_value = False

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 enum_cls: Any,
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, EnumConverter(enum_cls), default_py_value, implied_py_value,
                         is_optional, min_length=1)
        self.enum_cls = enum_cls

    def _get_py_value_from_xml_value(self, xml_value: str, nsmap: dict) -> Any:
        prefix, localname = xml_value.split(':')
        namespace = nsmap[prefix]
        q_name = etree_.QName(namespace, localname)
        return self._converter.to_py(q_name)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Set _local_var_name_xml here (namespaces are known here), and then call parent class update_xml_value."""
        py_value = getattr(instance, self._local_var_name)
        if py_value is not None:
            xml_value = docname_from_qname(py_value.value, node.nsmap)
            setattr(instance, self._local_var_name_xml, xml_value)
        super().update_xml_value(instance, node)


class NodeIntProperty(NodeTextProperty):
    """Python representation is an int."""

    def __init__(self, sub_element_name: etree_.QName | None = None,  # noqa: PLR0913
                 default_py_value: int | None = None,
                 implied_py_value: int | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, IntegerConverter, default_py_value, implied_py_value,
                         is_optional, min_length)

class NodeDecimalProperty(NodeTextProperty):
    """Python representation is an int."""

    def __init__(self, sub_element_name: etree_.QName | None = None,  # noqa: PLR0913
                 default_py_value: Decimal | None = None,
                 implied_py_value: Decimal | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, DecimalConverter(), default_py_value, implied_py_value,
                         is_optional, min_length)

class NodeDurationProperty(NodeTextProperty):
    """Python representation is an int."""

    def __init__(self, sub_element_name: etree_.QName | None = None,  # noqa: PLR0913
                 default_py_value: isoduration.DurationType | None = None,
                 implied_py_value: isoduration.DurationType | None = None,
                 is_optional: bool = False,
                 min_length: int = 0):
        super().__init__(sub_element_name, DurationConverter, default_py_value, implied_py_value,
                         is_optional, min_length)


class NodeTextQNameProperty(NodeTextProperty):
    """The handled data is a single qualified name in the text of an element in the form prefix:localname."""

    # xml value is not supported because the xml value can only be generated if namespace prefix is known
    supports_xml_value = False

    def __init__(self, sub_element_name: etree_.QName | None,
                 default_py_value: etree_.QName | None = None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, ClassCheckConverter(etree_.QName), default_py_value,
                         is_optional=is_optional)

    def _get_py_value_from_xml_value(self, xml_value: str, nsmap: dict) -> etree_.QName:
        return text_to_qname(xml_value, nsmap)

    def _get_xml_value_from_py_value(self, py_value: etree_.QName) -> str | None:
        return str(py_value)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Set _local_var_name_xml here (namespaces are known here), and then call parent class update_xml_value."""
        py_value = getattr(instance, self._local_var_name)
        xml_value = docname_from_qname(py_value, node.nsmap)
        setattr(instance, self._local_var_name_xml, xml_value)
        super().update_xml_value(instance, node)


def _compare_extension(left: xml_utils.LxmlElement, right: xml_utils.LxmlElement) -> bool:
    # xml comparison
    try:
        if left.tag != right.tag:  # compare expanded names
            return False
        if dict(left.attrib) != dict(right.attrib):  # unclear how lxml _Attrib compares
            return False
    except AttributeError:  # right side is not an Element type because expected attributes are missing
        return False

    # ignore comments
    left_children = [child for child in left if not isinstance(child, etree_._Comment)]  # noqa: SLF001
    right_children = [child for child in right if not isinstance(child, etree_._Comment)]  # noqa: SLF001

    if len(left_children) != len(right_children):  # compare children count
        return False
    if len(left_children) == 0 and len(right_children) == 0:
        if left.text != right.text:  # mixed content is not allowed. only compare text if there are no children
            return False
    return all(map(_compare_extension, left_children, right_children))  # compare children but keep order


class ExtensionLocalValue(list[xml_utils.LxmlElement]):
    """Helper class for ext:Extension content.

    It offers a specific __eq__ method, because the lxml method is not helpful.
    The lxml __eq__ only returns True if both Elements are the same instance.
    Here the trees are compared element for element.
    """

    compare_method: Callable[[xml_utils.LxmlElement, xml_utils.LxmlElement], bool] = _compare_extension
    """may be overwritten by user if a custom comparison behaviour is required"""

    def __eq__(self, other: Sequence) -> bool:
        try:
            if len(self) != len(other):
                return False
        except TypeError: # len of other cannot be determined
            return False
        return all(self.__class__.compare_method(left, right) for left, right in zip(self, other))


class _SubElementBase(_ElementBase, ABC):
    """Element that has child nodes.

    No specific xml value is available.
    """

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return: value
        :return:
        """
        sub_node = self._get_node(node)
        value = self._get_py_value_from_sub_node(sub_node)
        setattr(instance, self._local_var_name, value)

    @abstractmethod
    def _get_py_value_from_sub_node(self, sub_node: xml_utils.LxmlElement):
        """Instantiate a class."""


class ExtensionNodeProperty(_SubElementBase):
    """Represents an ext:Extension Element that contains 0...n child elements of any kind.

    The python representation is an ExtensionLocalValue with list of elements.
    """

    def __init__(self, sub_element_name: etree_.QName | None, default_py_value: Any | None = None):
        super().__init__(sub_element_name, ClassCheckConverter(ExtensionLocalValue), default_py_value,
                         is_optional=True)

    def __set__(self, instance: Any, value: Iterable):
        if not isinstance(value, ExtensionLocalValue):
            value = ExtensionLocalValue(value)
        super().__set__(instance, value)

    def __get__(self, instance, owner):  # noqa: ANN001
        """Return a python value, uses the locally stored value."""
        if instance is None:  # if called via class
            return self
        try:
            value = getattr(instance, self._local_var_name)
        except AttributeError:
            value = None
        if value is None:
            value = ExtensionLocalValue()
            setattr(instance, self._local_var_name, value)
        return value

    def _get_py_value_from_sub_node(self, sub_node: xml_utils.LxmlElement) -> ExtensionLocalValue:
        return ExtensionLocalValue() if sub_node is None else ExtensionLocalValue(sub_node[:])

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node.

        The Extension Element is only added if there is at least one element available in local list.
        """
        try:
            extension_local_value = getattr(instance, self._local_var_name)
        except AttributeError:
            return  # nothing to add
        if extension_local_value is None or len(extension_local_value) == 0:
            return  # nothing to add
        sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
        sub_node.extend(xml_utils.copy_node_wo_parent(x) for x in extension_local_value)


class AnyEtreeNodeProperty(_SubElementBase):
    """Represents an Element that contains xml tree of any kind."""

    def __init__(self, sub_element_name: etree_.QName | None, is_optional: bool = False):
        super().__init__(sub_element_name, NullConverter, default_py_value=None,
                         is_optional=is_optional)

    def _get_py_value_from_sub_node(self, sub_node: xml_utils.LxmlElement) -> Any:
        return None if sub_node is None else sub_node[:]

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None:
            if self.is_optional:
                sub_node = node.find(self._sub_element_name)
                if sub_node is not None:
                    node.remove(sub_node)
            elif MANDATORY_VALUE_CHECKING:
                raise ValueError(f'mandatory value {self._sub_element_name} missing')
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            if isinstance(py_value, xml_utils.LxmlElement):
                sub_node.append(py_value)
            else:
                sub_node.extend(py_value)


class SubElementProperty(_SubElementBase):
    """Uses a value that has an "as_etree_node" method."""

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_class: type[XMLTypeBase],
                 default_py_value: Any | None = None,
                 implied_py_value: Any | None = None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, ClassCheckConverter(value_class), default_py_value, implied_py_value,
                         is_optional)
        self.value_class = value_class

    def _get_py_value_from_sub_node(self, sub_node: xml_utils.LxmlElement) -> Any:
        """Read value from node."""
        if sub_node is None:
            return self._default_py_value
        value_class = self.value_class.value_class_from_node(sub_node)
        return value_class.from_node(sub_node)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = self._default_py_value

        if py_value is None:
            if not self.is_optional:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._sub_element_name} missing')
                etree_.SubElement(node, self._sub_element_name, nsmap=node.nsmap)
        else:
            sub_node = py_value.as_etree_node(self._sub_element_name, node.nsmap)
            if hasattr(py_value, 'NODETYPE') and hasattr(self.value_class, 'NODETYPE') \
                    and py_value.NODETYPE != self.value_class.NODETYPE:
                # set xsi type
                sub_node.set(QN_TYPE, docname_from_qname(py_value.NODETYPE, node.nsmap))
            node.append(sub_node)


class ContainerProperty(_SubElementBase):
    """ContainerProperty supports xsi:type information from xml and instantiates value accordingly."""

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_class: type[ContainerBase],
                 cls_getter: Callable[[etree_.QName], type],
                 ns_helper: NamespaceHelper,
                 is_optional: bool = False):
        """Construct a ContainerProperty.

        :param sub_element_name: see doc of base class
        :param value_class: Default value class if no xsi:type is found
        :param cls_getter: function that returns a class for xsi:type QName
        :param ns_helper: name space helper that knows current prefixes
        :param is_optional: see doc of base class
        """
        super().__init__(sub_element_name, ClassCheckConverter(value_class), is_optional=is_optional)
        self.value_class = value_class
        self._cls_getter = cls_getter
        self._ns_helper = ns_helper

    def _get_py_value_from_sub_node(self, sub_node: xml_utils.LxmlElement) -> Any:
        if sub_node is None:
            return self._default_py_value
        node_type_str = sub_node.get(QN_TYPE)
        if node_type_str is not None:
            node_type = text_to_qname(node_type_str, sub_node.nsmap)
            value_class = self._cls_getter(node_type)
        else:
            value_class = self.value_class
        return value_class.from_node(sub_node)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = self._default_py_value

        if py_value is None:
            if not self.is_optional:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._sub_element_name} missing')
                etree_.SubElement(node, self._sub_element_name, nsmap=node.nsmap)
        else:
            self.remove_sub_element(node)
            sub_node = py_value.mk_node(self._sub_element_name, self._ns_helper)
            if py_value.NODETYPE != self.value_class.NODETYPE:
                # set xsi type
                sub_node.set(QN_TYPE, docname_from_qname(py_value.NODETYPE, node.nsmap))
            node.append(sub_node)


class _ElementListProperty(_ElementBase, ABC):
    """Base class for all elements that are a list on the python side."""

    def __get__(self, instance, owner):  # noqa: ANN001
        """Return a python value, uses the locally stored value."""
        if instance is None:  # if called via class
            return self
        try:
            return getattr(instance, self._local_var_name)
        except AttributeError:
            setattr(instance, self._local_var_name, [])
            return getattr(instance, self._local_var_name)

    def __set__(self, instance: Any, py_value: Iterable[Any] | None):
        # if isinstance(py_value, tuple):
        #     py_value = list(py_value)
        if py_value is not None and  not isinstance(py_value, list):
            py_value = list(py_value)
        super().__set__(instance, py_value)

    def init_instance_data(self, instance: Any):
        setattr(instance, self._local_var_name, [])


class _SingleElementListProperty(_ElementListProperty, ABC):
    """Base class for all elements that are a list on the python side and a single element on the xml side."""

    supports_xml_value = True

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return: value
        :return:
        """
        xml_value = None
        py_values = None
        sub_node = self._get_node(node)
        if sub_node is not None and sub_node.text is not None:
            xml_value = sub_node.text
            if xml_value not in (None, ''):
                py_values = [self._get_py_value_from_xml_value(xml_value, sub_node.nsmap)
                             for xml_value in xml_value.split()]
        setattr(instance, self._local_var_name, py_values)
        setattr(instance, self._local_var_name_xml, xml_value)

    @abstractmethod
    def _get_py_value_from_xml_value(self, xml_value: str, nsmap: dict) -> list[Any]:
        """Return a python value."""

class _SubElementListProperty(_ElementListProperty, ABC):
    """Base class for all elements that are a list on the python side and a list of sub elements on the xml side."""

    def _get_nodes(self, node: xml_utils.LxmlElement| None) -> list[xml_utils.LxmlElement]:
        """Return child node with expected name or None."""
        try:
            return node.findall(self._sub_element_name)
        except ElementNotFoundError:
            return []


class SubElementListProperty(_SubElementListProperty):
    """SubElementListProperty is  a list of values that have an "as_etree_node" method.

    Used if maxOccurs="Unbounded" in BICEPS_ParticipantModel.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 value_class: type[XMLTypeBase],
                 is_optional: bool = True):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)
        self.value_class = value_class


    def _get_py_value_from_sub_node(self, sub_node: xml_utils.LxmlElement) -> Any:
        value_class = self.value_class.value_class_from_node(sub_node)
        return value_class.from_node(sub_node)

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return: value
        :return:
        """
        sub_nodes = self._get_nodes(node)
        py_values = []
        for current_node in sub_nodes:
            py_values.append(self._get_py_value_from_sub_node(current_node))
        setattr(instance, self._local_var_name, py_values)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = self._default_py_value

        if py_value is not None:
            for val in py_value:
                sub_node = val.as_etree_node(self._sub_element_name, node.nsmap)
                if hasattr(val, 'NODETYPE') and hasattr(self.value_class, 'NODETYPE') \
                        and val.NODETYPE != self.value_class.NODETYPE:
                    # set xsi type
                    sub_node.set(QN_TYPE, docname_from_qname(val.NODETYPE, node.nsmap))
                node.append(sub_node)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} datatype {self.value_class.__name__} in subelement {self._sub_element_name}'


class ContainerListProperty(_SubElementListProperty):
    """ContainerListProperty is a property with a list of elements, each supports xsi:type information.

    Used if maxOccurs="Unbounded" in BICEPS_ParticipantModel.
    """

    def __init__(self, sub_element_name: etree_.QName | None,  # noqa: PLR0913
                 value_class: type[ContainerBase],
                 cls_getter: Callable[[etree_.QName], type],
                 ns_helper: NamespaceHelper,
                 is_optional: bool = True):
        """Construct a list of Containers.

        :param sub_element_name: see doc of base class
        :param value_class: Default value class if no xsi:type is found
        :param cls_getter: function that returns a class for xsi:type QName
        :param ns_helper: name space helper that knows current prefixes
        :param is_optional: see doc of base class
        """
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)
        self.value_class = value_class
        self._cls_getter = cls_getter
        self._ns_helper = ns_helper

    def _get_py_value_from_sub_node(self, sub_node: xml_utils.LxmlElement) -> Any:
        node_type_str = sub_node.get(QN_TYPE)
        if node_type_str is not None:
            node_type = text_to_qname(node_type_str, sub_node.nsmap)
            value_class = self._cls_getter(node_type)
        else:
            value_class = self.value_class
        return value_class.from_node(sub_node)

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        :return: value
        :return:
        """
        sub_nodes = self._get_nodes(node)
        py_values = []
        for current_node in sub_nodes:
            py_values.append(self._get_py_value_from_sub_node(current_node))
        setattr(instance, self._local_var_name, py_values)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = self._default_py_value

        nodes = node.findall(self._sub_element_name)
        for _node in nodes:
            node.remove(_node)
        # ... and create new ones
        if py_value is not None:
            for val in py_value:
                sub_node = val.mk_node(self._sub_element_name, self._ns_helper)
                if val.NODETYPE != self.value_class.NODETYPE:
                    # set xsi type
                    sub_node.set(QN_TYPE, docname_from_qname(val.NODETYPE, node.nsmap))
                node.append(sub_node)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} datatype {self.value_class.__name__} in subelement {self._sub_element_name}'


class SubElementTextListProperty(_SubElementListProperty):
    """SubElementTextListProperty represents a list of strings.

    On xml side every string is a text of a sub element.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 value_class: Any,
                 is_optional: bool = True):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)), is_optional=is_optional)

    def _get_py_value_from_xml_value(self, xml_value: str, nsmap: dict) -> Any:  # noqa: ARG002
        return self._converter.elem_to_py(xml_value)

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        """
        sub_nodes = self._get_nodes(node)
        xml_values = []
        py_values = []
        for current_node in sub_nodes:
            xml_value = current_node.text
            xml_values.append(xml_value)
            py_values.append(self._get_py_value_from_xml_value(xml_value, current_node.nsmap))
        setattr(instance, self._local_var_name, py_values)
        setattr(instance, self._local_var_name_xml, xml_values)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            xml_values = getattr(instance, self._local_var_name_xml)
        except AttributeError:
            xml_values = None

        try:
            py_values = getattr(instance, self._local_var_name)
        except AttributeError:
            py_values = self._default_py_value

        if (py_values is None or len(py_values) == 0) and (xml_values is None or len(xml_values) == 0):
            return

        nodes = node.findall(self._sub_element_name)
        for _node in nodes:
            node.remove(_node)
        # ... and create new ones
        values_list = xml_values if xml_values is not None and  len(xml_values) > 0 else py_values
        for val in values_list:
            child = etree_.SubElement(node, self._sub_element_name)
            try:
                child.text = val
            except TypeError as ex:
                # re-raise with better info about data
                raise TypeError(f'{ex} in {self}') from ex

    def __str__(self) -> str:
        return f'{self.__class__.__name__} in sub-element {self._sub_element_name}'


class SubElementStringListProperty(SubElementTextListProperty):
    """SubElementStringListProperty represents a list of strings.

    On xml side every string is a text of a sub element.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 is_optional: bool = True):
        super().__init__(sub_element_name, str, is_optional=is_optional)


class SubElementHandleRefListProperty(SubElementStringListProperty):
    """Represents a list of Handles."""


class SubElementWithSubElementListProperty(SubElementProperty):
    """Class represents an optional Element that is only present if its value class is not empty.

    value_class must have an is_empty method.
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 default_py_value: Any,
                 value_class: type[XMLTypeBase]):
        assert hasattr(value_class, 'is_empty')
        super().__init__(sub_element_name,
                         default_py_value=default_py_value,
                         value_class=value_class)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = self._default_py_value

        if py_value is None or py_value.is_empty():
            return
        self.remove_sub_element(node)
        node.append(py_value.as_etree_node(self._sub_element_name, node.nsmap))

    def __set__(self, instance: Any, py_value: Any):
        if isinstance(py_value, self.value_class):
            super().__set__(instance, py_value)
        else:
            raise ApiUsageError(f'do not set {self._sub_element_name} directly, use child member!')


class AnyEtreeNodeListProperty(_ElementListProperty):
    """class represents a list of lxml elements."""

    def __init__(self, sub_element_name: etree_.QName | None, is_optional: bool = True):
        super().__init__(sub_element_name,
                         ListConverter(ClassCheckConverter(xml_utils.LxmlElement)),
                         is_optional=is_optional)

    def update_from_node(self, instance: Any, node: xml_utils.LxmlElement):
        """Update instance data with data from node.

        This method is used internally and should not be called by application.
        :param instance:the instance that has the property as member
        :param node:the etree node that provides the value
        """
        values = []
        sub_node = self._get_node(node)
        if sub_node is not None:
            values = sub_node[:]
        setattr(instance, self._local_var_name, values)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:
            py_value = None

        if py_value is None or len(py_value) == 0:
            if self.is_optional:
                self.remove_sub_element(node)
            return

        sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
        sub_node.extend(py_value)

    def __str__(self) -> str:
        return f'{self.__class__.__name__} in subelement {self._sub_element_name}'


class NodeTextListProperty(_SingleElementListProperty):
    """The handled data is a list of words (string without whitespace). The xml text is the joined list of words."""

    def __init__(self, sub_element_name: etree_.QName | None,
                 value_class: Any,
                 is_optional: bool = False):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(value_class)),
                         is_optional=is_optional)

    def _get_py_value_from_xml_value(self, xml_value: Any, nsmap: dict) -> Any:  # noqa: ARG002
        """Return xml_value without conversion."""
        return self._converter.elem_to_py(xml_value)

    def _get_xml_value_from_py_value(self, py_value: list[str] | None) -> str |None:
        return None if py_value is None else ' '.join(py_value)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            xml_value = getattr(instance, self._local_var_name_xml)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            xml_value = None

        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None and xml_value is None:
            if not self._sub_element_name:
                # update text of this element
                node.text = ''
            elif self.is_optional:
                sub_node = node.find(self._sub_element_name)
                if sub_node is not None:
                    node.remove(sub_node)
            else:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._sub_element_name} missing')
                sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            if xml_value is not None:
                sub_node.text = xml_value
            else:
                sub_node.text = self._get_xml_value_from_py_value(py_value)


class NodeTextQNameListProperty(_SingleElementListProperty):
    """The handled data is a list of qualified names.

    The xml text is the joined list of qnames in the form prefix:localname.
    """

    # xml value is not supported because the xml value can only be generated if namespace prefix is known
    supports_xml_value = False

    def __init__(self, sub_element_name: etree_.QName | None,
                 is_optional: bool = False):
        super().__init__(sub_element_name, ListConverter(ClassCheckConverter(etree_.QName)),
                         is_optional=is_optional)

    def _get_py_value_from_xml_value(self, xml_value: str, nsmap: dict) -> etree_.QName:
        """Return a QName."""
        return text_to_qname(xml_value, nsmap)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            xml_value = getattr(instance, self._local_var_name_xml)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            xml_value = None

        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = None

        if py_value is None and xml_value is None:
            # if not self._sub_element_name:
            #     # update text of this element
            #     node.text = ''
            # elif self.is_optional:
            #     sub_node = node.find(self._sub_element_name)
            #     if sub_node is not None:
            #         node.remove(sub_node)
            # elif not self.is_optional:
            if self._sub_element_name is not None and not self.is_optional:
                if MANDATORY_VALUE_CHECKING and not self.is_optional:
                    raise ValueError(f'mandatory value {self._sub_element_name} missing')
                sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
                sub_node.text = None
        else:
            sub_node = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
            if xml_value is not None:
                sub_node.text = xml_value
            else:
                tmp = []
                for q_name in py_value:
                    # by setting each qname as text, namespace prefixes are generated automatically
                    sub_node.text = q_name
                    tmp.append(sub_node.text)
                sub_node.text = ' '.join(tmp)


class DateOfBirthProperty(NodeTextProperty):
    """DateOfBirthProperty represents the DateOfBirth type of BICEPS.

        <xsd:simpleType>
            <xsd:union memberTypes="xsd:dateTime xsd:date xsd:gYearMonth xsd:gYear"/>
        </xsd:simpleType>
    xsd:dateTime is YYYY-MM-DDThh:mm:ss.sss
    xsd:date is YYYY-MM-DD format. All components are required
    xsd:gYearMonth is YYYY-MM
    xsd:gYear is YYYY
    If the timepoint of birth matters, the value SHALL be populated with a time zone.

    Time zone info can be provided:
       UTC can be specified by appending a Z character, e.g. 2002-09-24Z
       other timezones by adding a positive or negative time behind the date, e.g. 2002.09-24-06:00, 2002-09-24+06:00
    xsd:time is hh:mm:ss format, e.g. 9:30:10, 9:30:10.5. All components are required.
    Time zone handling is identical to date type

    The corresponding Python types are datetime.Date (=> not time point available)
    or datetime.Datetime (with time point attribute).
    """

    def __init__(self, sub_element_name: etree_.QName | None,
                 default_py_value: Any = None,
                 implied_py_value: Any = None,
                 is_optional: bool = True):
        super().__init__(sub_element_name, ClassCheckConverter(datetime, date),
                         default_py_value, implied_py_value, is_optional)

    def _get_xml_value_from_node(self, node: xml_utils.LxmlElement) -> Any:
        return None if node is None else node.text

    def _get_py_value_from_xml_value(self, xml_value: str, nsmap: dict) \
            -> isoduration.DateTypeUnion | None:  # noqa: ARG002
        return isoduration.parse_date_time(xml_value)

    def _get_xml_value_from_py_value(self, py_value: Any) -> str | None:
        return py_value if isinstance(py_value, str) else self._mk_datestring(py_value)

    def update_xml_value(self, instance: Any, node: xml_utils.LxmlElement):
        """Write value to node."""
        try:
            xml_value = getattr(instance, self._local_var_name_xml)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            xml_value = None

        try:
            py_value = getattr(instance, self._local_var_name)
        except AttributeError:  # set to None (it is in the responsibility of the called method to do the right thing)
            py_value = self._default_py_value

        if py_value is None and xml_value is None:
            self.remove_sub_element(node)
            return

        sub_element = self._get_element_by_child_name(node, self._sub_element_name, create_missing_nodes=True)
        if xml_value is not None:
            sub_element.text = xml_value
        else:
            sub_element.text = self._get_xml_value_from_py_value(py_value)  # date_string

    @staticmethod
    def mk_value_object(date_string: str) -> isoduration.DateTypeUnion | None:
        """Parse isoduration string."""
        return isoduration.parse_date_time(date_string)

    @staticmethod
    def _mk_datestring(date_object: date | datetime | isoduration.GYear | isoduration.GYearMonth | None) -> str:
        """Create isoduration string."""
        return isoduration.date_time_string(date_object)
