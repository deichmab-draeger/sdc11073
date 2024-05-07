from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

from sdc11073.xml_types.pm_types import RetrievabilityMethod
from .mdibbase import MultiStateEntity, Entity

if TYPE_CHECKING:
    from sdc11073.location import SdcLocation
    from sdc11073.xml_types.pm_types import (
        CodedValue,
        InstanceIdentifier,
        MetricAvailability,
        MetricCategory,
        SafetyClassification,
    )

    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorProtocol
    from .providermdib import EntityProviderMdib


class ProviderMdibMethods:
    """Extra methods for provider mdib tht are not core functionality."""

    def __init__(self, provider_mdib: EntityProviderMdib):
        self._mdib = provider_mdib
        self.descriptor_factory = DescriptorFactory(provider_mdib)
        self.default_instance_identifiers = (provider_mdib.data_model.pm_types.InstanceIdentifier(
            root='rootWithNoMeaning', extension_string='System'),)

    def ensure_location_context_descriptor(self):
        """Create a LocationContextDescriptor for every SystemContextDescriptor that has none."""
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        location_entities = mdib.entities.NODETYPE.get(pm.LocationContextDescriptor)
        system_context_entities = mdib.entities.NODETYPE.get_one(pm.SystemContextDescriptor)
        descr_cls = mdib.data_model.get_descriptor_container_class(pm.LocationContextDescriptor)
        for system_context_entity in system_context_entities:
            location_entities = [e for e in location_entities if e.descriptor.parent_handle == system_context_entity.descriptor.Handle]
            if not location_entities:
                descr_container = descr_cls(handle=uuid.uuid4().hex, parent_handle=system_context_entity.descriptor.Handle)
                descr_container.SafetyClassification = mdib.data_model.pm_types.SafetyClassification.INF
                mdib.entities.add_object(MultiStateEntity(descr_container))

    def ensure_patient_context_descriptor(self):
        """Create a PatientContextDescriptor for every SystemContextDescriptor that has none."""
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        location_entities = mdib.entities.NODETYPE.get(pm.LocationContextDescriptor)
        system_context_entities = mdib.entities.NODETYPE.get_one(pm.SystemContextDescriptor)
        descr_cls = mdib.data_model.get_descriptor_container_class(pm.PatientContextDescriptor)
        for system_context_entity in system_context_entities:
            location_entities = [e for e in location_entities if e.descriptor.parent_handle == system_context_entity.descriptor.Handle]
            if not location_entities:
                descr_container = descr_cls(handle=uuid.uuid4().hex, parent_handle=system_context_entity.descriptor.Handle)
                descr_container.SafetyClassification = mdib.data_model.pm_types.SafetyClassification.INF
                mdib.entities.add_object(MultiStateEntity(descr_container))

    def set_location(self,
                     sdc_location: SdcLocation,
                     location_context_handle: str,
                     validators: list[InstanceIdentifier] | None = None,
                     set_associated: bool = True):
        """Create a location context state.

        This method updates only the mdib data!
        Use the SdcProvider.set_location method if you want to publish the address on the network.
        :param sdc_location: a sdc11073.location.SdcLocation instance
        :param location_context_handle: the handle of the location context descriptor.
        :param validators: a list of pysdc.pmtypes.InstanceIdentifier objects or None
        :param set_associated: if True, BindingTime, BindingMdibVersion and ContextAssociation are set
        """
        mdib = self._mdib
        with mdib.context_state_transaction() as mgr:
            location_context_entity = mdib.entities.handle.get_one(location_context_handle)
            # set all to currently associated Locations to Disassociated
            associated_locations = [loc for loc in location_context_entity.states.values() if
                                    loc.ContextAssociation == mdib.data_model.pm_types.ContextAssociation.ASSOCIATED]
            for location in associated_locations:
                location_context = mgr.get_context_state(location.Handle)

                location_context.ContextAssociation = mdib.data_model.pm_types.ContextAssociation.DISASSOCIATED
                # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
                location_context.UnbindingMdibVersion = mdib.mdib_version

            new_location = mgr.mk_context_state(location_context_handle, set_associated=set_associated)
            new_location.update_from_sdc_location(sdc_location)
            if validators is not None:
                new_location.Validator = validators

    def mk_state_containers_for_all_descriptors(self):
        """Create a state container for every descriptor that is missing a state in mdib.

        The model requires that there is a state for every descriptor (exception: multi-states)
        """
        mdib = self._mdib
        pm = mdib.data_model.pm_names
        for entity in mdib.entities.objects:
            if not entity.descriptor.is_context_descriptor and entity.state is None:
                state_cls = mdib.data_model.get_state_class_for_descriptor(entity.descriptor)
                state = state_cls(entity.descriptor)
                # add some initial values where needed
                if state.is_alert_condition:
                    state.DeterminationTime = time.time()
                elif state.NODETYPE == pm.AlertSystemState:  # noqa: SIM300
                    state.LastSelfCheck = time.time()
                    state.SelfCheckCount = 1
                elif state.NODETYPE == pm.ClockState:  # noqa: SIM300
                    state.LastSet = time.time()

                if mdib.current_transaction is not None:
                    mdib.current_transaction.add_state(state)

                entity.add_state(state)
                mdib.entities.update_object(entity)

    def update_retrievability_lists(self):
        """Update internal lists, based on current mdib descriptors."""
        mdib = self._mdib
        with mdib.mdib_lock:
            del mdib._retrievability_episodic[:]  # noqa: SLF001
            mdib.retrievability_periodic.clear()
            for entity in mdib.entities.objects:
                for r in entity.descriptor.get_retrievability():
                    for r_by in r.By:
                        if r_by.Method == RetrievabilityMethod.EPISODIC:
                            mdib._retrievability_episodic.append(entity.descriptor.Handle)  # noqa: SLF001
                        elif r_by.Method == RetrievabilityMethod.PERIODIC:
                            period_float = r_by.UpdatePeriod
                            period_ms = int(period_float * 1000.0)
                            mdib.retrievability_periodic[period_ms].append(entity.descriptor.Handle)

    def set_all_source_mds(self):
        """Set the source mds member of all descriptors.

        This makes handling of SourceMds separation in messages less demanding on CPU.
        """
        pm_qnames = self._mdib.data_model.pm_names
        all_mds_entities = self._mdib.entities.NODETYPE.get(pm_qnames.MdsDescriptor)
        for mds_entity in all_mds_entities:
            for entity in self._mdib.get_all_entities_in_subtree(mds_entity):
                entity.descriptor.set_source_mds(mds_entity.descriptor.Handle)

    def _get_mds_entity(self, entity: Entity | MultiStateEntity) -> Entity | None:
        """Get the parent mds entity for a given entity."""
        tmp = entity
        mds_entity = None
        expected_type = self._mdib.data_model.pm_names.MdsDescriptor
        while mds_entity is None:
            if tmp.descriptor.NODETYPE == expected_type:  # noqa: SIM300
                return tmp
            parent_handle = tmp.descriptor.parent_handle
            tmp = self._mdib.entities.handle.get_one(parent_handle, allow_none=True)
            if tmp is None:
                if self._mdib.current_transaction:
                    # check id current transaction has this parent
                    tmp = self._mdib.current_transaction.actual_descriptor(parent_handle)
            if tmp is None:
                raise KeyError(f'could not find mds descriptor for handle {entity.descriptor.Handle}')
        return None

    def set_source_mds(self, entity: Entity | MultiStateEntity):
        """Find the parent mds for descriptor_container and set source mds of descriptor_container."""
        mds_entity = self._get_mds_entity(entity)
        entity.descriptor.set_source_mds(mds_entity.descriptor.Handle)


class DescriptorFactory:
    """DescriptorFactory provides some methods to make creation of descriptors easier."""

    def __init__(self, mdib: EntityProviderMdib):
        self._mdib = mdib

    @staticmethod
    def _create_descriptor_container(container_cls: type,
                                     handle: str,
                                     parent_handle: str,
                                     coded_value: CodedValue,
                                     safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        obj = container_cls(handle=handle, parent_handle=parent_handle)
        obj.SafetyClassification = safety_classification
        obj.Type = coded_value
        return obj

    def create_vmd_descriptor_container(self,
                                        handle: str,
                                        parent_handle: str,
                                        coded_value: CodedValue,
                                        safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        """Create an VmdDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.VmdDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)

    def create_channel_descriptor_container(self,
                                            handle: str,
                                            parent_handle: str,
                                            coded_value: CodedValue,
                                            safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        """Create a ChannelDescriptorContainer with the given properties.

        :param handle: Handle of the new container.
        :param parent_handle: Handle of the parent.
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value.
        :return: the created object.
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.ChannelDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)

    def create_string_metric_descriptor_container(self,  # noqa: PLR0913
                                                  handle: str,
                                                  parent_handle: str,
                                                  coded_value: CodedValue,
                                                  safety_classification: SafetyClassification,
                                                  unit: CodedValue,
                                                  metric_availability: MetricAvailability,
                                                  metric_category: MetricCategory) -> AbstractDescriptorProtocol:
        """Create a StringMetricDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param unit: a CodedValue
        :param metric_availability: pmtypes.MetricAvailability
        :param metric_category: pmtypes.MetricCategory
        :return: the created object
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.StringMetricDescriptor)
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        obj.Unit = unit
        obj.MetricAvailability = metric_availability
        obj.MetricCategory = metric_category
        return obj

    def create_enum_string_metric_descriptor_container(self,  # noqa: PLR0913
                                                       handle: str,
                                                       parent_handle: str,
                                                       coded_value: CodedValue,
                                                       safety_classification: SafetyClassification,
                                                       unit: CodedValue,
                                                       allowed_values: Any,
                                                       metric_availability: MetricAvailability,
                                                       metric_category: MetricCategory) -> AbstractDescriptorProtocol:
        """Create an EnumStringMetricDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :param unit: pmtypes.CodedValue
        :param allowed_values:
        :param metric_availability: pmtypes.MetricAvailability
        :param metric_category: pmtypes.MetricCategory
        :return: the created object.
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.EnumStringMetricDescriptor)
        obj = self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
        obj.Unit = unit
        obj.MetricAvailability = metric_availability
        obj.MetricCategory = metric_category
        obj.AllowedValue = allowed_values
        return obj

    def create_clock_descriptor_container(self,
                                          handle: str,
                                          parent_handle: str,
                                          coded_value: CodedValue,
                                          safety_classification: SafetyClassification) -> AbstractDescriptorProtocol:
        """Create a ClockDescriptorContainer with the given properties.

        :param handle: Handle of the new container
        :param parent_handle: Handle of the parent
        :param coded_value: a pmtypes.CodedValue instance that defines what this object represents in medical terms.
        :param safety_classification: a pmtypes.SafetyClassification value
        :return: the created object.
        """
        model = self._mdib.data_model
        cls = model.get_descriptor_container_class(model.pm_names.ClockDescriptor)
        return self._create_descriptor_container(cls, handle, parent_handle, coded_value, safety_classification)
