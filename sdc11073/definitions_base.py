import os
from abc import ABC, abstractmethod

from .namespaces import default_ns_helper as ns_hlp
from .namespaces import shall_normalize

schemaFolder = os.path.join(os.path.dirname(__file__), 'xsd')


class ProtocolsRegistry(type):
    """
    base class that has the only purpose to register classes that use this as meta class
    """
    protocols = []

    def __new__(cls, name, *arg, **kwarg):
        new_cls = super().__new__(cls, name, *arg, **kwarg)
        if name != 'BaseDefinitions':  # ignore the base class itself
            cls.protocols.append(new_cls)
        return new_cls

class AbstractDataModel(ABC):

    @abstractmethod
    def get_descriptor_container_class(self, type_qname):
        raise NotImplementedError

    def mk_descriptor_container(self, type_qname, *args, **kwargs):
        cls = self.get_descriptor_container_class(type_qname)
        return cls(*args, **kwargs)

    @abstractmethod
    def get_state_container_class(self, type_qname):
        raise NotImplementedError

    def get_state_class_for_descriptor(self, descriptor_container):
        state_class_qtype = descriptor_container.STATE_QNAME
        if state_class_qtype is None:
            raise TypeError(f'No state association for {descriptor_container.__class__.__name__}')
        return self.get_state_container_class(state_class_qtype)

    def mk_state_container(self, descriptor_container):
        cls = self.get_state_class_for_descriptor(descriptor_container)
        if cls is None:
            raise TypeError(
                f'No state container class for descr={descriptor_container.__class__.__name__}, '
                f'name={descriptor_container.NODETYPE}, '
                f'type={descriptor_container.nodeType}')
        return cls(descriptor_container)

    @property
    @abstractmethod
    def pm_types(self):
        """Gives access to a module with participant model types"""
        raise NotImplementedError

    @property
    @abstractmethod
    def pm_names(self):
        """Gives access to a module with all qualified names of the BICEPS participant model"""
        raise NotImplementedError

    @property
    @abstractmethod
    def msg_names(self):
        """Gives access to a module with all qualified names of the BICEPS message model"""
        raise NotImplementedError

    @property
    @abstractmethod
    def ns_helper(self):
        """Gives access to a module with all name spaces used"""
        raise NotImplementedError


# definitions that group all relevant dependencies for BICEPS versions
class BaseDefinitions(metaclass=ProtocolsRegistry):
    """ Base class for central definitions used by SDC.
    It defines namespaces and handlers for the protocol.
    Derive from this class in order to define different protocol handling."""
    SchemaFilePaths = None
    # set the following namespaces in derived classes:
    MedicalDeviceTypeNamespace = None
    #BICEPSNamespace = None
    #MessageModelNamespace = None
    #ParticipantModelNamespace = None
    #ExtensionPointNamespace = None
    MedicalDeviceType = None
    ActionsNamespace = None
    DefaultSdcDeviceComponents = None
    DefaultSdcClientComponents = None
    #MDPWSNameSpace = None
    data_model = None

    Actions = None

    # @classmethod
    # def ns_matches(cls, namespace):
    #     """ This method checks if this definition set is the correct one for a given namespace"""
    #     return namespace in (cls.MedicalDeviceTypeNamespace, cls.BICEPSNamespace, cls.MessageModelNamespace,
    #                          cls.ParticipantModelNamespace, cls.ExtensionPointNamespace, cls.MedicalDeviceType)
    @classmethod
    def types_match(cls, types):
        """ This method checks if this definition set is the correct one for a given namespace"""
        return cls.MedicalDeviceType in types

    @classmethod
    def normalize_xml_text(cls, xml_text: bytes) -> bytes:
        """ replace BICEPS namespaces with internal namespaces"""
        if not shall_normalize():
            return xml_text
        for namespace, internal_ns in ((cls.MessageModelNamespace, ns_hlp.MSG.namespace),
                                       (cls.ParticipantModelNamespace, ns_hlp.PM.namespace),
                                       (cls.ExtensionPointNamespace, ns_hlp.EXT.namespace),
                                       (cls.MDPWSNameSpace, ns_hlp.MDPWS.namespace)):
            xml_text = xml_text.replace(f'"{namespace}"'.encode('utf-8'),
                                        f'"{internal_ns}"'.encode('utf-8'))
        return xml_text

    @classmethod
    def denormalize_xml_text(cls, xml_text: bytes) -> bytes:
        """ replace internal namespaces with BICEPS namespaces"""
        if not shall_normalize():
            return xml_text
        for namespace, internal_ns in ((cls.MessageModelNamespace.encode('utf-8'), b'__BICEPS_MessageModel__'),
                                       (cls.ParticipantModelNamespace.encode('utf-8'), b'__BICEPS_ParticipantModel__'),
                                       (cls.ExtensionPointNamespace.encode('utf-8'), b'__ExtensionPoint__'),
                                       (cls.MDPWSNameSpace.encode('utf-8'), b'__MDPWS__')):
            xml_text = xml_text.replace(internal_ns, namespace)
        return xml_text

    @classmethod
    def get_schema_file_path(cls, url):
        return cls.SchemaFilePaths.schema_location_lookup.get(url)
