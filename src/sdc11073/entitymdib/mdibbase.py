from __future__ import annotations

import traceback
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, cast

from lxml import etree as etree_

from sdc11073 import multikey
from sdc11073 import observableproperties as properties
from sdc11073.etc import apply_map
from sdc11073.xml_types.pm_types import Coding, have_matching_codes
from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorContainer
from sdc11073.mdib.entityaccess import EntityBase, Entity, MultiStateEntity

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073.definitions_base import BaseDefinitions
    from sdc11073.loghelper import LoggerAdapter
    from sdc11073.xml_types.pm_types import CodedValue
    from sdc11073 import xml_utils

    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractMultiStateContainer, AbstractStateContainer


@dataclass
class MdibVersionGroup:
    """These 3 values define an mdib version."""

    mdib_version: int
    sequence_id: str
    instance_id: int | None


class _MultikeyWithVersionLookup(multikey.MultiKeyLookup):
    """_MultikeyWithVersionLookup keeps track of versions of removed objects.

    If a descriptor od state gets removed from mdib and later is added again, the version of that instance
    must be greater than the last version before it was removed.
    """

    def __init__(self):
        super().__init__()
        self.handle_version_lookup = {}

    def remove_object(self, obj: Any):
        if obj is not None:
            self._save_version(obj)
        super().remove_object(obj)

    def remove_object_no_lock(self, obj: Any):
        """Remove object from table without locking."""
        if obj is not None:
            self._save_version(obj)
        super().remove_object_no_lock(obj)

    def remove_objects_no_lock(self, objects: list[Any]):
        apply_map(self._save_version, [obj for obj in objects if obj is not None])
        super().remove_objects_no_lock(objects)


class EntityLookupConsumer(multikey.MultiKeyLookup):
    """EntityLookup is the table-like storage for descriptors.

    It has the following search indices:
     - handle is the index for descriptor.Handle.
     - parent_handle is the index for descriptor.parent_handle, it finds all children of a queried descriptor.
     - NODETYPE is the index for descriptor.NODETYPE. It finds all children of a queried type.
       This index works only for exact matches, class hierarchy is unknown here. E.g. AlertDescriptor only returns
       AlertDescriptor objects, not LimitAlertDescriptor!
     - coding is the index for descriptor.coding.
     - condition_signaled is the index for descriptor.ConditionSignaled, it finds only AlertSignalDescriptors.
     - source is the index for descriptor.Source, it finds only AlertConditionDescriptors.
    """

    handle: multikey.UIndexDefinition[str, list[Entity | MultiStateEntity]]
    parent_handle: multikey.IndexDefinition[str, list[Entity | MultiStateEntity]]
    NODETYPE: multikey.IndexDefinition[QName, list[Entity | MultiStateEntity]]
    coding: multikey.IndexDefinition[Coding, list[Entity | MultiStateEntity]]
    condition_signaled: multikey.IndexDefinition[str, list[Entity | MultiStateEntity]]
    source: multikey.IndexDefinition[str, list[Entity | MultiStateEntity]]
    state_handle: multikey.UIndexDefinition[str, list[Entity | MultiStateEntity]]
    # Todo: implement state_handle index

    def __init__(self):
        super().__init__()
        self.add_index('handle', multikey.UIndexDefinition(lambda entity: entity.descriptor.Handle))
        self.add_index('parent_handle', multikey.IndexDefinition(lambda entity: entity.descriptor.parent_handle))
        self.add_index('NODETYPE', multikey.IndexDefinition(lambda entity: entity.descriptor.NODETYPE))
        self.add_index('coding', multikey.IndexDefinition(lambda entity: entity.descriptor.coding))
        self.add_index('condition_signaled',
                       multikey.IndexDefinition(lambda entity: entity.descriptor.ConditionSignaled, index_none_values=False))
        # an index to find all alert conditions for a metric (AlertCondition is the only class that has a
        # "Source" attribute, therefore this simple approach without type testing is sufficient):
        self.add_index('source',
                       multikey.IndexDefinition1n(lambda entity: [s.text for s in entity.descriptor.Source], index_none_values=False))
        self.add_index('state_handle',
                       multikey.IndexDefinition1n(lambda entity: entity.state_handles, index_none_values=False))

def _assert_is_entity(obj):
    if not isinstance(obj, EntityBase):
        raise ValueError(f'class {obj.__class__.__name__} cannot be part of entity lookup')


class EntityLookupProvider(multikey.MultiKeyLookup):
    """EntityLookup is the table-like storage for descriptors.

    It has the following search indices:
     - handle is the index for descriptor.Handle.
     - parent_handle is the index for descriptor.parent_handle, it finds all children of a queried descriptor.
     - NODETYPE is the index for descriptor.NODETYPE. It finds all children of a queried type.
       This index works only for exact matches, class hierarchy is unknown here. E.g. AlertDescriptor only returns
       AlertDescriptor objects, not LimitAlertDescriptor!
     - coding is the index for descriptor.coding.
     - condition_signaled is the index for descriptor.ConditionSignaled, it finds only AlertSignalDescriptors.
     - source is the index for descriptor.Source, it finds only AlertConditionDescriptors.
     - state_handle is the index for the handles of multi states (patient context, location context).
    """

    handle: multikey.UIndexDefinition[str, list[Entity | MultiStateEntity]]
    parent_handle: multikey.IndexDefinition[str, list[Entity | MultiStateEntity]]
    NODETYPE: multikey.IndexDefinition[QName, list[Entity | MultiStateEntity]]
    coding: multikey.IndexDefinition[Coding, list[Entity | MultiStateEntity]]
    condition_signaled: multikey.IndexDefinition[str, list[Entity | MultiStateEntity]]
    source: multikey.IndexDefinition[str, list[Entity | MultiStateEntity]]
    state_handle: multikey.UIndexDefinition[str, list[Entity | MultiStateEntity]]

    def __init__(self):
        super().__init__()
        self.add_index('handle', multikey.UIndexDefinition(lambda entity: entity.descriptor.Handle))
        self.add_index('parent_handle', multikey.IndexDefinition(lambda entity: entity.descriptor.parent_handle))
        self.add_index('NODETYPE', multikey.IndexDefinition(lambda entity: entity.descriptor.NODETYPE))
        self.add_index('coding', multikey.IndexDefinition(lambda entity: entity.descriptor.coding))
        self.add_index('condition_signaled',
                       multikey.IndexDefinition(lambda entity: entity.descriptor.ConditionSignaled, index_none_values=False))
        # an index to find all alert conditions for a metric (AlertCondition is the only class that has a
        # "Source" attribute, therefore this simple approach without type testing is sufficient):
        self.add_index('source',
                       multikey.IndexDefinition1n(lambda entity: [s.text for s in entity.descriptor.Source], index_none_values=False))
        self.add_index('state_handle',
                       multikey.IndexDefinition1n(lambda entity: entity.state_handles, index_none_values=False))
        self.handle_version_lookup_descr = {}
        self.handle_version_lookup_state = {}



    def _save_descriptor_version(self, obj: AbstractDescriptorContainer):
        # Todo: fix. obj is now an entity, but it must be a state container or descriptor container
        self.handle_version_lookup_descr[obj.Handle] = obj.DescriptorVersion

    def set_descriptor_version(self, obj: AbstractDescriptorContainer):
        """Set DescriptorVersion of obj if descriptor with same handle existed before."""
        # Todo: fix. obj is now an entity, but it must be a state container or descriptor container
        version = self.handle_version_lookup_descr.get(obj.Handle)
        if version is not None:
            obj.DescriptorVersion = version + 1

    def _save_state_version(self, obj: AbstractStateContainer):
        # Todo: fix. obj is now an entity, but it must be a state container or descriptor container
        if not obj.is_context_state:
            self.handle_version_lookup_state[obj.DescriptorHandle] = obj.StateVersion

    def set_state_version(self, obj: AbstractStateContainer):
        """Set DescriptorVersion of obj if descriptor with same handle existed before."""
        # Todo: fix. obj is now an entity, but it must be a state container or descriptor container
        if not obj.is_context_state:
            version = self.handle_version_lookup_state.get(obj.DescriptorHandle)
            if version is not None:
                obj.StateVersion = version + 1

    def add_object(self, obj: Entity | MultiStateEntity):
        """Append object with locking."""
        with self._lock:
            self.add_object_no_lock(obj)

    def add_containers(self, descr: AbstractDescriptorContainer, state: AbstractStateContainer | None):
        """Append Entity(descr, state) with locking."""
        self.add_object(Entity(descr, state))

    def add_multi_state_containers(self, descr: AbstractDescriptorContainer, states: list[AbstractMultiStateContainer]):
        """Append MultiStateEntity(descr, states) with locking."""
        entity = MultiStateEntity(descr)
        for state in states:
            entity.add_state(state)
        self.add_object(entity)

    def add_object_no_lock(self, obj: Entity | MultiStateEntity):
        """Append object without locking."""
        _assert_is_entity(obj)
        super().add_object_no_lock(obj)

    def add_objects(self, objects: list[Entity | MultiStateEntity]):
        """Append objects with locking."""
        with self._lock:
            self.add_objects_no_lock(objects)

    def add_objects_no_lock(self, objects: list[Entity | MultiStateEntity]):
        """Append objects without locking."""
        apply_map(self.add_object_no_lock, objects)



    def remove_object(self, obj: Entity | MultiStateEntity):
        """Remove object from table."""
        keys = self._object_ids.get(id(obj))
        if keys is None:
            return
        with self._lock:
            self.remove_object_no_lock(obj)

    def remove_object_no_lock(self, entity: Entity | MultiStateEntity):
        """Remove object from table without locking."""
        self._save_descriptor_version(entity.descriptor)
        if not entity.is_multi_state and entity.state is not None:
            self._save_state_version(entity.state)
        super().remove_object_no_lock(entity)

    def remove_objects(self, objects: list[Entity | MultiStateEntity]):
        """Remove objects from table with locking."""
        with self._lock:
            self.remove_objects_no_lock(objects)

    def remove_objects_no_lock(self, objects: list[Entity | MultiStateEntity]):
        """Remove objects from table without locking."""
        apply_map(self.remove_object_no_lock, objects)


class EntityMdibBase:
    """Base class with common functionality of provider mdib and consumer mdib."""

    # these observables can be used to watch any change of data in the mdib. They contain lists of containers that were changed.
    # every transaction (device mdib) or notification (client mdib) will report their changes here.
    metrics_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    waveform_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    alert_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    context_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    component_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    new_descriptors_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    updated_descriptors_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    deleted_entities_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    description_modifications = properties.ObservableProperty(fire_only_on_changed_value=False)
    operation_by_handle = properties.ObservableProperty(fire_only_on_changed_value=False)
    sequence_id = properties.ObservableProperty()
    instance_id = properties.ObservableProperty()

    table_cls = None  # class for entities lookup

    def __init__(self, sdc_definitions: type[BaseDefinitions], logger: LoggerAdapter):
        """Construct MdibBase.

        :param sdc_definitions: a class derived from BaseDefinitions
        """
        self.sdc_definitions = sdc_definitions
        self.data_model = sdc_definitions.data_model
        self._logger = logger
        self.nsmapper = sdc_definitions.data_model.ns_helper
        self.mdib_version = 0
        self.sequence_id = ''  # needs to be set to a reasonable value by derived class
        self.instance_id = None  # None or an unsigned int
        self.log_prefix = ''
        self.entities = self.table_cls()  #EntityLookup()
        self.mdib_lock = Lock()
        self.mdstate_version = 0
        self.mddescription_version = 0

    @property
    def logger(self) -> LoggerAdapter:
        """Return the logger."""
        return self._logger

    @property
    def mdib_version_group(self) -> MdibVersionGroup:
        """"Get current version data."""
        return MdibVersionGroup(self.mdib_version, self.sequence_id, self.instance_id)

    def add_description_containers(self, descriptor_containers: list[AbstractDescriptorContainer]):
        """Initialize descriptions member with provided descriptors."""
        new_descriptor_by_handle = {}
        with self.entities.lock:
            for descriptor_container in descriptor_containers:
                if descriptor_container.is_context_descriptor:
                    self.entities.add_object_no_lock(MultiStateEntity(descriptor_container))
                else:
                    self.entities.add_object_no_lock(Entity(descriptor_container, None))
                new_descriptor_by_handle[descriptor_container.Handle] = descriptor_container

        # finally update observable property
        if new_descriptor_by_handle:
            self.new_descriptors_by_handle = new_descriptor_by_handle

    def clear_states(self):
        """Remove all states and context states."""
        with self.entities.lock:
            for entity in self.entities.objects:
                entity.clear_states()

        # clear also the observable properties
        self.metrics_by_handle = None
        self.waveform_by_handle = None
        self.alert_by_handle = None
        self.context_by_handle = None
        self.component_by_handle = None
        self.operation_by_handle = None

    def add_state_containers(self, state_containers: list[AbstractStateContainer | AbstractMultiStateContainer]):
        """Add states to self.entities.

        This method does not update the observables, so use with care!
        :param state_containers: a list of StateContainer objects.
        """
        for state_container in state_containers:
            try:
                entity = self.entities.handle.get_one(state_container.DescriptorHandle)
                entity.add_state(state_container)
            except KeyError as ex:
                if state_container.is_context_state:
                    self._logger.error('add_state_containers: {}, Handle={}; {}',  # noqa: PLE1205
                                       ex, state_container.Handle, traceback.format_exc())
                else:
                    self._logger.error('add_state_containers: {}, DescriptorHandle={}; {}', # noqa: PLE1205
                                       ex, state_container.DescriptorHandle, traceback.format_exc())

    def _reconstruct_md_description(self) -> xml_utils.LxmlElement:
        """Build dom tree of descriptors from current data."""
        pm = self.data_model.pm_names
        doc_nsmap = self.nsmapper.ns_map
        root_entities = self.entities.parent_handle.get(None) or []
        md_description_node = etree_.Element(pm.MdDescription,
                                             attrib={'DescriptionVersion': str(self.mddescription_version)},
                                             nsmap=doc_nsmap)
        for root_entity in root_entities:
            self.make_descriptor_node(root_entity, md_description_node, tag=pm.Mds, set_xsi_type=False)
        return md_description_node

    def make_descriptor_node(self,
                             entity: EntityBase,
                             parent_node: xml_utils.LxmlElement,
                             tag: etree_.QName,
                             set_xsi_type: bool = True) -> xml_utils.LxmlElement:
        """Create a lxml etree node with subtree from instance data.

        :param entity: an Entity or MultiStateEntity instance
        :param parent_node: parent node
        :param tag: tag of node
        :param set_xsi_type: if true, the NODETYPE will be used to set the xsi:type attribute of the node
        :return: an etree node.
        """
        ns_map = self.nsmapper.partial_map(self.nsmapper.PM, self.nsmapper.XSI) \
            if set_xsi_type else self.nsmapper.partial_map(self.nsmapper.PM)
        node = etree_.SubElement(parent_node,
                                 tag,
                                 attrib={'Handle': entity.descriptor.Handle},
                                 nsmap=ns_map)
        entity.descriptor.update_node(node, self.nsmapper, set_xsi_type)  # create all
        child_list = self.entities.parent_handle.get(entity.descriptor.Handle, [])
        # append all child containers, then bring all child elements in correct order
        for child in child_list:
            child_tag, set_xsi = entity.descriptor.tag_name_for_child_descriptor(child.descriptor.NODETYPE)
            self.make_descriptor_node(child, node, child_tag, set_xsi)
        entity.descriptor.sort_child_nodes(node)
        return node

    def _reconstruct_mdib(self, add_context_states: bool) -> xml_utils.LxmlElement:
        """Build dom tree of mdib from current data.

        If add_context_states is False, context states are not included.
        """
        pm = self.data_model.pm_names
        msg = self.data_model.msg_names
        doc_nsmap = self.nsmapper.ns_map
        mdib_node = etree_.Element(msg.Mdib, nsmap=doc_nsmap)
        mdib_node.set('MdibVersion', str(self.mdib_version))
        mdib_node.set('SequenceId', self.sequence_id)
        if self.instance_id is not None:
            mdib_node.set('InstanceId', str(self.instance_id))
        md_description_node = self._reconstruct_md_description()
        mdib_node.append(md_description_node)

        # add a list of states
        md_state_node = etree_.SubElement(mdib_node, pm.MdState,
                                          attrib={'StateVersion': str(self.mdstate_version)},
                                          nsmap=doc_nsmap)
        tag = pm.State
        for entity in self.entities.objects:
            if not entity.is_multi_state:
                if entity.state:
                    md_state_node.append(entity.state.mk_state_node(tag, self.nsmapper))
        if add_context_states:
            for entity in self.entities.objects:
                if entity.is_multi_state:
                    for state in entity.states.values():
                        md_state_node.append(state.mk_state_node(tag, self.nsmapper))

        return mdib_node

    def reconstruct_md_description(self) -> (xml_utils.LxmlElement, MdibVersionGroup):
        """Build dom tree of descriptors from current data."""
        with self.mdib_lock:
            node = self._reconstruct_md_description()
            return node, self.mdib_version_group

    def reconstruct_mdib(self) -> (xml_utils.LxmlElement, MdibVersionGroup):
        """Build dom tree from current data.

        This method does not include context states!
        """
        with self.mdib_lock:
            return self._reconstruct_mdib(add_context_states=False), self.mdib_version_group

    def reconstruct_mdib_with_context_states(self) -> (xml_utils.LxmlElement, MdibVersionGroup):
        """Build dom tree from current data.

        This method includes the context states.
        """
        with self.mdib_lock:
            return self._reconstruct_mdib(add_context_states=True), self.mdib_version_group

    def _get_child_entities_by_code(self, parent_handle: str, code: Coding) -> list[Entity | MultiStateEntity]:
        entities = self.entities.parent_handle.get(parent_handle, [])
        if len(entities) == 0:
            return []
        with_types = [e for e in entities if e.descriptor.Type is not None]
        return [e for e in with_types if have_matching_codes(e.descriptor.Type, code)]

    def get_metric_entity_by_code(self,
                                      vmd_code: [Coding, CodedValue],
                                      channel_code: [Coding, CodedValue],
                                      metric_code: [Coding, CodedValue]) -> Entity | None:
        """get_metric_descriptor_by_code is the "correct" way to find a descriptor.

        Using handles is shaky, because they have no meaning and can change over time!
        """
        pm = self.data_model.pm_names
        all_vmd_entities = self.entities.NODETYPE.get(pm.VmdDescriptor, [])
        matching_vmd_ent_list = [e for e in all_vmd_entities if have_matching_codes(e.descriptor.Type, vmd_code)]
        for vmd_ent in matching_vmd_ent_list:
            matching_channel_entities = self._get_child_entities_by_code(vmd_ent.descriptor.Handle, channel_code)
            for channel_ent in matching_channel_entities:
                matching_metric_entities = self._get_child_entities_by_code(channel_ent.descriptor.Handle, metric_code)
                if len(matching_metric_entities) == 1:
                    return matching_metric_entities[0]
                if len(matching_metric_entities) > 1:
                    raise ValueError(
                        f'found multiple metrics for vmd={vmd_code} channel={channel_code} metric={metric_code}')
        return None

    def get_operation_entities_for_metric(self,
                                  vmd_code: [Coding, CodedValue],
                                  channel_code: [Coding, CodedValue],
                                  metric_code: [Coding, CodedValue]) -> list[Entity]:
        """get_operations_for_metric is the "correct" way to find an operation.

        Using handles is shaky, because they have no meaning and can change over time!
        """
        entity = self.get_metric_entity_by_code(vmd_code, channel_code, metric_code)
        return self.get_operation_entities_for_descriptor_handle(entity.descriptor.Handle)

    def get_operation_entities_for_descriptor_handle(self, descriptor_handle: str,
                                                        **additional_filters: Any) -> list[Entity]:
        """Get operation descriptors that have descriptor_handle as OperationTarget.

        :param descriptor_handle: the handle for that operations shall be found
        :return: a list with operation descriptors that have descriptor_handle as OperationTarget. List can be empty
        :additionalFilters: optional filters for the key = name of member attribute, value = expected value
            example: NODETYPE=pm.SetContextStateOperationDescriptor filters for SetContextStateOperation descriptors
        """
        all_operation_entities = self.get_operation_entities()
        my_operation_entities = [e for e in all_operation_entities if cast(AbstractOperationDescriptorContainer, e.descriptor).OperationTarget == descriptor_handle]
        for key, value in additional_filters.items():
            my_operation_entities = [op for op in my_operation_entities if getattr(op.descriptor, key) == value]
        return my_operation_entities

    def get_operation_entities(self) -> list[Entity]:
        """Get a list of all operation descriptors."""
        pm = self.data_model.pm_names
        result = []
        for node_type in (pm.SetValueOperationDescriptor,
                          pm.SetStringOperationDescriptor,
                          pm.ActivateOperationDescriptor,
                          pm.SetContextStateOperationDescriptor,
                          pm.SetMetricStateOperationDescriptor,
                          pm.SetComponentStateOperationDescriptor,
                          pm.SetAlertStateOperationDescriptor):
            result.extend(self.entities.NODETYPE.get(node_type, []))
        return result

    def select_entities(self, *codings: list[Coding | CodedValue | str]) -> list[Entity | MultiStateEntity]:
        """Return all descriptor containers that match a path defined by list of codings.

        Example:
        -------
        [Coding('70041')] returns all containers that have Coding('70041') in its Type
        [Coding('70041'), Coding('69650')] : returns all descriptors with Coding('69650')
                                     and parent descriptor Coding('70041')
        [Coding('70041'), Coding('69650'), Coding('69651')] : returns all descriptors with Coding('69651') and
                                     parent descriptor Coding('69650') and parent's parent descriptor Coding('70041')
        It is not necessary that path starts at the top of a mds, it can start anywhere.
        :param codings: each element can be a string (which is handled as a Coding with DEFAULT_CODING_SYSTEM),
                         a Coding or a CodedValue.
        """
        selected_entities = self.entities.objects  # start with all objects
        for counter, coding in enumerate(codings):
            # normalize coding
            if isinstance(coding, str):
                coding = Coding(coding)  # noqa: PLW2901
            if counter > 0:
                # replace selected_objects with all children of selected objects
                all_handles = [o.descriptor.Handle for o in selected_entities]  # pylint: disable=not-an-iterable
                selected_entities = []
                for handle in all_handles:
                    selected_entities.extend(self.entities.descriptor.parent_handle.get(handle, []))
            # filter current list
            selected_entities = [o for o in selected_entities if
                                o.descriptor.Type is not None and have_matching_codes(o.descriptor.Type, coding)]
        return selected_entities

    def get_all_entities_in_subtree(self, root_entity: Entity,
                                       depth_first: bool = True,
                                       include_root: bool = True) -> list[Entity | MultiStateEntity]:
        """Return the tree below descriptor_container as a flat list.

        :param root_entity: entity
        :param depth_first: determines order of returned list.
               If depth_first=True result has all leaves on top, otherwise at the end.
        :param include_root: if True descriptor_container itself is also part of returned list
        :return: a list of DescriptorContainer objects.
        """
        result = []

        def _getchildren(parent: Entity | MultiStateEntity):
            child_entities = self.entities.parent_handle.get(parent.descriptor.Handle, [])
            if not depth_first:
                result.extend(child_entities)
            apply_map(_getchildren, child_entities)
            if depth_first:
                result.extend(child_entities)

        if include_root and not depth_first:
            result.append(root_entity)
        _getchildren(root_entity)
        if include_root and depth_first:
            result.append(root_entity)
        return result

    def rm_entities(self, entities: list[Entity | MultiStateEntity]):
        """Delete descriptors and all related states."""
        deleted_entities = {}
        for entity in entities:
            self._logger.debug('rm entity {} handle {}', # noqa: PLE1205
                               entity.descriptor.NODETYPE, entity.descriptor.Handle)
            self.entities.remove_object(entity)
            deleted_entities[entity.descriptor.Handle] = entity.descriptor

        if deleted_entities:
            self.deleted_entities_by_handle = deleted_entities  # update observable

    def rm_entity_by_handle(self, handle: str):
        """Delete descriptor and the subtree and related states."""
        entity = self.entities.handle.get_one(handle, allow_none=True)
        if entity is not None:
            all_entities = self.get_all_entities_in_subtree(entity)
            self.rm_entities(all_entities)

    def get_entity(self, handle: str, allow_none: bool) -> Entity | None:
        """Return descriptor and state as Entity."""
        entity = self.entities.handle.get_one(handle, allow_none)
        if entity is None:
            return None
        if entity.is_multi_state:
            raise ValueError('Multi State entity instead of single state entity')
        return entity

    def get_context_entity(self, handle: str, allow_none: bool) -> MultiStateEntity | None:
        """Return descriptor and states as MultiStateEntity."""
        entity = self.entities.handle.get_one(handle, allow_none)
        if entity is None:
            return None
        if not entity.is_multi_state:
            raise ValueError('Single State entity instead of multi state entity')
        return entity

    def has_multiple_mds(self) -> bool:
        """Check if there is more than one mds in mdib (convenience method)."""
        all_mds_descriptors = self.entities.NODETYPE.get(self.data_model.pm_names.MdsDescriptor)
        return len(all_mds_descriptors) > 1
