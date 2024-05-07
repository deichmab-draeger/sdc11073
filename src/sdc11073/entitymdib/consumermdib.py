from __future__ import annotations

import time
import traceback
from threading import Lock
from typing import TYPE_CHECKING, Any, Callable

from sdc11073 import loghelper
from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib.consumermdib import ConsumerRtBuffer, _BufferedData

from . import mdibbase
from .consumermdibxtra import ConsumerMdibMethods

if TYPE_CHECKING:
    from sdc11073.consumer import SdcConsumer
    from sdc11073.mdib.statecontainers import AbstractStateContainer, RealTimeSampleArrayMetricStateContainer
    from sdc11073.pysoap.msgreader import MdibVersionGroupReader
    from sdc11073.xml_types.msg_types import (
        DescriptionModificationReport,
        EpisodicAlertReport,
        EpisodicComponentReport,
        EpisodicContextReport,
        EpisodicMetricReport,
        OperationInvokedReport,
    )


class EntityConsumerMdib(mdibbase.EntityMdibBase):
    """ConsumerMdib is a mirror of a provider mdib. Updates are performed by an SdcConsumer."""

    MDIB_VERSION_CHECK_DISABLED = False  # for testing purpose you can disable checking of mdib version, so that every notification is accepted.

    table_cls = mdibbase.EntityLookupConsumer

    def __init__(self,
                 sdc_client: SdcConsumer,
                 extras_cls: type | None = None,
                 max_realtime_samples: int = 100):
        """Construct a ConsumerMdib instance.

        :param sdc_client: a SdcConsumer instance
        :param  extras_cls: extended functionality
        :param max_realtime_samples: determines how many real time samples are stored per RealtimeSampleArray
        """
        super().__init__(sdc_client.sdc_definitions,
                         loghelper.get_logger_adapter('sdc.client.mdib', sdc_client.log_prefix))
        self._sdc_client = sdc_client
        if extras_cls is None:
            extras_cls = ConsumerMdibMethods
        self._xtra = extras_cls(self, self._logger)
        self._is_initialized = False
        self.rt_buffers = {}  # key  is a handle, value is a ConsumerRtBuffer
        self._max_realtime_samples = max_realtime_samples
        self._last_wf_age_log = time.time()
        # a buffer for notifications that are received before initial get_mdib is done
        self._buffered_notifications = []
        self._buffered_notifications_lock = Lock()
        self._sequence_id_changed_flag = False

    @property
    def xtra(self) -> Any:
        """Give access to extended functionality."""
        return self._xtra

    @property
    def sdc_client(self) -> SdcConsumer:
        """Give access to sdc client."""
        return self._sdc_client

    @property
    def is_initialized(self) -> bool:
        """Returns True if everything has been set up completely."""
        return self._is_initialized

    def init_mdib(self):
        """Binds own notification handlers to observables of sdc client and calls GetMdib.

        Client mdib is initialized from GetMdibResponse, and from then on updated from incoming notifications.
        :return: None
        """
        if self._is_initialized:
            raise ApiUsageError('ConsumerMdib is already initialized')
        # first start receiving notifications, then call get_mdib.
        # Otherwise, we might miss notifications.
        self._xtra.bind_to_client_observables()
        self.reload_all()
        self._sdc_client.set_mdib(self)  # pylint: disable=protected-access
        self._logger.info('initializing mdib done')

    def reload_all(self):
        """Delete all data and reloads everything. Useful e.g. after sequence id change.

        This method is not called automatically, the application has to take care.
        :return: None
        """
        self._is_initialized = False
        self._sequence_id_changed_flag = False
        self.entities.clear()
        self.clear_states()

        get_service = self._sdc_client.client('Get')
        self._logger.info('initializing mdib...')
        response = get_service.get_mdib()  # GetRequestResult
        self._logger.info('creating description containers...')
        descriptor_containers, state_containers = response.result
        self.add_description_containers(descriptor_containers)
        self._logger.info('creating state containers...')
        self.add_state_containers(state_containers)

        mdib_version_group = response.mdib_version_group
        if mdib_version_group.mdib_version is not None:
            self.mdib_version = mdib_version_group.mdib_version
            self._logger.info('setting initial mdib version to {}', mdib_version_group.mdib_version)  # noqa: PLE1205
        else:
            self._logger.warning('found no mdib version in GetMdib response, assuming "0"')
            self.mdib_version = 0
        self.sequence_id = mdib_version_group.sequence_id
        self._logger.info('setting initial sequence id to {}', mdib_version_group.sequence_id)  # noqa: PLE1205
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id
        self._logger.info('setting initial instance id to {}', mdib_version_group.instance_id)  # noqa: PLE1205

        # Todo: re implement this. How to check if there are already context states?
        # retrieve context states only if there were none in mdib
        all_context_entities = [e for e in self.entities.objects if e.descriptor.is_context_descriptor]
        if sum([len(e.states) for e in all_context_entities]) == 0:
            self._get_context_states()
        else:
            self._logger.info('found context states in GetMdib Result, will not call getContextStates')

        # process buffered notifications
        with self._buffered_notifications_lock:
            for buffered_report in self._buffered_notifications:
                buffered_report.handler(buffered_report.mdib_version_group,
                                        buffered_report.data,
                                        is_buffered_report=True)
            del self._buffered_notifications[:]
            self._is_initialized = True

    def _buffer_data(self, mdib_version_group: MdibVersionGroupReader,
                     data: Any,
                     func: Callable) -> bool:
        """Write notification to a temporary buffer, as long as mdib is not initialized.

        :param mdib_version_group:
        :param data:
        :param func: the callable that shall be called later for delayed handling of report
        :return: True if buffered, False if report shall be processed immediately
        """
        if self._is_initialized:
            # no reason to buffer
            return False

        # get lock and check if we need to write to buffer
        with self._buffered_notifications_lock:
            if not self._is_initialized:
                self._buffered_notifications.append(_BufferedData(mdib_version_group, data, func))
                return True
            return False

    def _get_context_states(self, handles: list[str] | None = None):
        try:
            self._logger.debug('new Query, handles={}', handles)  # noqa: PLE1205
            time.sleep(0.001)
            context_service = self._sdc_client.client('Context')
            self._logger.info('requesting context states...')
            response = context_service.get_context_states(handles)
            context_state_containers = response.result.ContextState

            self._logger.debug('got {} context states', len(context_state_containers))  # noqa: PLE1205
            with self.entities.lock:
                for state_container in context_state_containers:
                    entity = self.entities.handle.get_one(state_container.DescriptorHandle)
                    if state_container.Handle in entity.states:
                        # update existing state
                        old_state_container = entity.states[state_container.Handle]
                        if old_state_container.StateVersion != state_container.StateVersion:
                            self._logger.debug('update {} ==> {}',  # noqa: PLE1205
                                               old_state_container, state_container)
                            old_state_container.update_from_node(state_container.node)
                            self.entities.update_object_no_lock(entity)
                        else:
                            # same version, it should be identical: check that and log an error if it is different
                            # Todo: decide if it is good to ignore the new version
                            difference = state_container.diff(old_state_container)
                            if difference:
                                self._logger.error('no state version update but different!\n{ \n{}',  # noqa: PLE1205
                                                   difference)

                    else:
                        self._logger.debug('new ContextState {}', state_container)  # noqa: PLE1205
                        entity.states[state_container.Handle] = state_container
                        self.entities.update_object_no_lock(entity)
        except Exception:  # noqa: BLE001
            self._logger.error(traceback.format_exc())
        finally:
            self._logger.info('_get_context_states done')

    def _can_accept_version(self, mdib_version: int, sequence_id: str, log_prefix: str) -> bool:
        if not self._can_accept_mdib_version(log_prefix, mdib_version):
            return False
        if self._sequence_id_changed(sequence_id):
            return False
        return True

    def _can_accept_mdib_version(self, log_prefix: str, new_mdib_version: int) -> bool:
        if self.MDIB_VERSION_CHECK_DISABLED:
            return True
        if new_mdib_version is None:
            self._logger.error('{}: could not check MdibVersion!', log_prefix)  # noqa: PLE1205
        else:
            # log deviations from expected mdib version
            if new_mdib_version < self.mdib_version:
                self._logger.warning('{}: ignoring too old Mdib version, have {}, got {}',  # noqa: PLE1205
                                     log_prefix, self.mdib_version, new_mdib_version)
            elif (new_mdib_version - self.mdib_version) > 1:
                if self._sdc_client.all_subscribed:
                    self._logger.warning('{}: expect mdib_version {}, got {}',  # noqa: PLE1205
                                         log_prefix, self.mdib_version + 1, new_mdib_version)
            # it is possible to receive multiple notifications with the same mdib version => compare ">="
            if new_mdib_version >= self.mdib_version:
                return True
        return False

    def _sequence_id_changed(self, sequence_id: str) -> bool:
        if self.sequence_id != sequence_id:
            self._sequence_id_changed_flag = True
            self.sequence_id = sequence_id
        return self._sequence_id_changed_flag

    def _update_from_mdib_version_group(self, mdib_version_group: MdibVersionGroupReader):
        if mdib_version_group.mdib_version != self.mdib_version:
            self.mdib_version = mdib_version_group.mdib_version
        if mdib_version_group.sequence_id != self.sequence_id:
            self.sequence_id = mdib_version_group.sequence_id
        if mdib_version_group.instance_id != self.instance_id:
            self.instance_id = mdib_version_group.instance_id

    def _update_wf_states(self, report_type: str,
                          state_containers: list[RealTimeSampleArrayMetricStateContainer],
                          is_buffered_report: bool) -> dict[str, RealTimeSampleArrayMetricStateContainer]:
        """Update mdib with incoming waveform states."""
        states_by_handle = {}
        for state_container in state_containers:
            entity = self.entities.handle.get_one(state_container.DescriptorHandle)
            old_state_container = entity.state
            if old_state_container is not None:
                if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                            report_type,
                                                            is_buffered_report):
                    old_state_container.update_from_other_container(state_container)
                    self.entities.update_object(entity)
                    states_by_handle[old_state_container.DescriptorHandle] = old_state_container
            else:
                self._logger.error('{}: got a new state {}',  # noqa: PLE1205
                                   report_type,
                                   state_container.DescriptorHandle)
                entity.state = state_container
                self.entities.update_object(entity)
                states_by_handle[state_container.DescriptorHandle] = state_container
        return states_by_handle

    def _process_incoming_states_report(self, report_type: str,
                                        report: EpisodicMetricReport | EpisodicAlertReport | OperationInvokedReport | EpisodicComponentReport,
                                        is_buffered_report: bool) -> dict:
        """Update mdib with incoming states."""
        states_by_handle = {}
        for report_part in report.ReportPart:
            for state_container in report_part.values_list:
                entity = self.entities.handle.get_one(state_container.DescriptorHandle)
                old_state_container = entity.state
                if old_state_container is not None:
                    if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                report_type,
                                                                is_buffered_report):
                        old_state_container.update_from_other_container(state_container)
                        self.entities.update_object(entity)
                        states_by_handle[old_state_container.DescriptorHandle] = old_state_container
                else:
                    self._logger.error('{}: got a new state {}',  # noqa: PLE1205
                                       report_type,
                                       state_container.DescriptorHandle)
                    entity.state = state_container
                    self.entities.update_object(entity)
                    states_by_handle[state_container.DescriptorHandle] = state_container
        return states_by_handle

    def _process_incoming_context_states_report(self, report_type: str,
                                                report: EpisodicContextReport,
                                                is_buffered_report: bool) -> dict:
        """Update mdib with incoming states."""
        states_by_handle = {}
        for report_part in report.ReportPart:
            for state_container in report_part.values_list:
                entity = self.entities.handle.get_one(state_container.DescriptorHandle)
                old_state_container = entity.states.get(state_container.Handle)
                if old_state_container is not None:
                    if self._has_new_state_usable_state_version(old_state_container, state_container,
                                                                report_type,
                                                                is_buffered_report):
                        old_state_container.update_from_other_container(state_container)
                        self.entities.update_object(entity)
                        states_by_handle[old_state_container.DescriptorHandle] = old_state_container
                else:
                    self._logger.info(  # noqa: PLE1205
                        '{}: new context state handle = {} Descriptor Handle={} Assoc={}, Validators={}',
                        report_type, state_container.Handle, state_container.DescriptorHandle,
                        state_container.ContextAssociation, state_container.Validator)
                    entity.add_state(state_container)
                    self.entities.update_object(entity)
                    states_by_handle[state_container.DescriptorHandle] = state_container
        return states_by_handle

    def process_incoming_metric_states_report(self, mdib_version_group: MdibVersionGroupReader,
                                              report: EpisodicMetricReport,
                                              is_buffered_report: bool = False):
        """Add data from EpisodicMetricReport to mdib."""
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_metric_states_report):
            return
        states_by_handle = {}
        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id,
                                                'metric states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'metric states', report, is_buffered_report)
        finally:
            self.metrics_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_alert_states_report(self, mdib_version_group: MdibVersionGroupReader,
                                             report: EpisodicAlertReport,
                                             is_buffered_report: bool = False):
        """Add data from EpisodicAlertReport to mdib."""
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_alert_states_report):
            return
        states_by_handle = {}
        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id,
                                                'alert states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'alert states', report, is_buffered_report)
        finally:
            self.alert_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_operational_states_report(self, mdib_version_group: MdibVersionGroupReader,
                                                   report: OperationInvokedReport,
                                                   is_buffered_report: bool = False):
        """Add data from OperationInvokedReport to mdib."""
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_operational_states_report):
            return
        states_by_handle = {}

        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id,
                                                'operational states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'operational states', report, is_buffered_report)
        finally:
            self.operation_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_waveform_states(self, mdib_version_group: MdibVersionGroupReader,
                                         state_containers: list[RealTimeSampleArrayMetricStateContainer],
                                         is_buffered_report: bool = False) -> dict[
                                                                                  str, RealTimeSampleArrayMetricStateContainer] | None:
        """Add data from state_containers to mdib."""
        if not is_buffered_report and self._buffer_data(mdib_version_group, state_containers,
                                                        self.process_incoming_waveform_states):
            return None
        states_by_handle = {}
        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id,
                                                'waveform states'):
                    return None
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._update_wf_states(
                    'waveform states', state_containers, is_buffered_report)

                # add to Waveform Buffer
                for state_container in states_by_handle.values():
                    state_container: RealTimeSampleArrayMetricStateContainer
                    descriptor_container = state_container.descriptor_container
                    d_handle = state_container.DescriptorHandle
                    rt_buffer = self.rt_buffers.get(d_handle)
                    if rt_buffer is None:
                        sample_period = 0  # default
                        if descriptor_container is not None:
                            # read sample period
                            sample_period = descriptor_container.SamplePeriod or 0
                        rt_buffer = ConsumerRtBuffer(sample_period=sample_period,
                                                     max_samples=self._max_realtime_samples)
                        self.rt_buffers[d_handle] = rt_buffer
                    rt_sample_containers = rt_buffer.mk_rt_sample_containers(state_container)
                    rt_buffer.add_rt_sample_containers(rt_sample_containers)
        finally:
            if states_by_handle is not None:
                self.waveform_by_handle = states_by_handle
        return states_by_handle

    def process_incoming_context_states_report(self, mdib_version_group: MdibVersionGroupReader,
                                               report: EpisodicContextReport,
                                               is_buffered_report: bool = False):
        """Add data from EpisodicContextReport to mdib."""
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_context_states_report):
            return

        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id,
                                                'context states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_context_states_report(
                    'context states', report, is_buffered_report)
        finally:
            self.context_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_component_states_report(self, mdib_version_group: MdibVersionGroupReader,
                                                 report: EpisodicComponentReport,
                                                 is_buffered_report: bool = False):
        """Add data from EpisodicComponentReport to mdib."""
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_component_states_report):
            return

        try:
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id,
                                                'component states'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                states_by_handle = self._process_incoming_states_report(
                    'component states', report, is_buffered_report)
        finally:
            self.component_by_handle = states_by_handle  # used by wait_metric_matches method

    def process_incoming_description_modifications(self, mdib_version_group: MdibVersionGroupReader,
                                                   report: DescriptionModificationReport,
                                                   is_buffered_report: bool = False):
        """Add data from DescriptionModificationReport to mdib."""
        if not is_buffered_report and self._buffer_data(mdib_version_group, report,
                                                        self.process_incoming_description_modifications):
            return

        new_descriptor_by_handle = {}
        updated_descriptor_by_handle = {}
        try:
            dmt = self.sdc_definitions.data_model.msg_types.DescriptionModificationType
            with self.mdib_lock:
                if not self._can_accept_version(mdib_version_group.mdib_version, mdib_version_group.sequence_id,
                                                'descriptors'):
                    return
                self._update_from_mdib_version_group(mdib_version_group)
                for report_part in report.ReportPart:
                    modification_type = report_part.ModificationType
                    if modification_type == dmt.CREATE:
                        new_states_containers = report_part.State
                        for descriptor_container in report_part.Descriptor:
                            if descriptor_container.is_context_descriptor:
                                entity = mdibbase.MultiStateEntity(descriptor_container)
                                for st in new_states_containers:
                                    if st.DescriptorHandle == descriptor_container.Handle:
                                        entity.add_state(st)
                            else:
                                entity = mdibbase.Entity(descriptor_container)
                                for st in new_states_containers:
                                    if st.DescriptorHandle == descriptor_container.Handle:
                                        entity.add_state(st)
                                        break

                            self.entities.add_object(entity)
                            self._logger.debug(  # noqa: PLE1205
                                'process_incoming_descriptors: created description "{}" (parent="{}")',
                                descriptor_container.Handle, descriptor_container.parent_handle)
                            new_descriptor_by_handle[descriptor_container.Handle] = descriptor_container
                    elif modification_type == dmt.UPDATE:
                        updated_descriptor_containers = report_part.Descriptor
                        updated_state_containers = report_part.State
                        for descriptor_container in updated_descriptor_containers:
                            self._logger.info(  # noqa: PLE1205
                                'process_incoming_descriptors: update descriptor "{}" (parent="{}")',
                                descriptor_container.Handle, descriptor_container.parent_handle)
                            entity = self.entities.handle.get_one(descriptor_container.Handle,
                                                                  allow_none=True)
                            if entity is None:
                                self._logger.error(  # noqa: PLE1205
                                    'process_incoming_descriptors: got update of descriptor "{}", but it did not exist in mdib!',
                                    descriptor_container.Handle)
                            else:
                                states = [st for st in updated_state_containers if
                                          st.DescriptorHandle == descriptor_container.Handle]
                                if descriptor_container.is_context_descriptor:
                                    entity.descriptor.update_from_other_container(descriptor_container)
                                    # update all existing context states, add new ones, and
                                    for st in states:
                                        if st.Handle in entity.states:
                                            entity.states[st.Handle].update_from_other_container(st)
                                        else:
                                            entity.add_state(st)
                                    # remove states from entity that are not in list of updated states
                                    updated_state_handles = [st.Handle for st in states]
                                    for h in entity.state_handles:
                                        if h not in updated_state_handles:
                                            del entity.states[h]
                                            self.entities.update_object(entity)
                                elif len(states) == 1:
                                    entity.descriptor.update_from_other_container(descriptor_container)
                                    entity.state.update_from_other_container(states[0])
                                    self.entities.update_object(entity)
                                else:
                                    self._logger.error(  # noqa: PLE1205
                                        'process_incoming_descriptors: got update of descriptor "{}", but {} states',
                                        descriptor_container.Handle, len(states))
                                updated_descriptor_by_handle[descriptor_container.Handle] = descriptor_container
                    elif modification_type == dmt.DELETE:
                        deleted_descriptor_containers = report_part.Descriptor
                        for descriptor_container in deleted_descriptor_containers:
                            self._logger.debug(  # noqa: PLE1205
                                'process_incoming_descriptors: remove descriptor "{}" (parent="{}")',
                                descriptor_container.Handle, descriptor_container.parent_handle)
                            self.rm_entity_by_handle(descriptor_container.Handle)  # this removes also sub tree
                    else:
                        raise ValueError(
                            f'unknown modification type {modification_type} in description modification report')

        finally:
            self.description_modifications = report  # update observable for complete report
            # update observables for every report part separately
            if new_descriptor_by_handle:
                self.new_descriptors_by_handle = new_descriptor_by_handle
            if updated_descriptor_by_handle:
                self.updated_descriptors_by_handle = updated_descriptor_by_handle

    def _has_new_state_usable_state_version(self,
                                            old_state_container: AbstractStateContainer,
                                            new_state_container: AbstractStateContainer,
                                            report_name: str,
                                            is_buffered_report: bool) -> bool:
        """Compare state versions old vs new.

        :param old_state_container:
        :param new_state_container:
        :param report_name: used for logging
        :return: True if new state is ok for mdib , otherwise False.
        """
        diff = int(new_state_container.StateVersion) - int(old_state_container.StateVersion)
        # diff == 0 can happen if there is only a descriptor version update
        if diff == 1:  # this is the perfect version
            return True
        if diff > 1:
            self._logger.error('{}: missed {} states for state DescriptorHandle={} ({}->{})',  # noqa: PLE1205
                               report_name,
                               diff - 1, old_state_container.DescriptorHandle,
                               old_state_container.StateVersion, new_state_container.StateVersion)
            return True  # the new version is newer, therefore it can be added to mdib
        if diff < 0:
            if not is_buffered_report:
                self._logger.error(  # noqa: PLE1205
                    '{}: reduced state version for state DescriptorHandle={} ({}->{}) ',
                    report_name, old_state_container.DescriptorHandle,
                    old_state_container.StateVersion, new_state_container.StateVersion)
            return False
        diffs = old_state_container.diff(new_state_container)  # compares all xml attributes
        if diffs:
            self._logger.error(  # noqa: PLE1205
                '{}: repeated state version {} for state {}, DescriptorHandle={}, but states have different data:{}',
                report_name, old_state_container.StateVersion, old_state_container.__class__.__name__,
                old_state_container.DescriptorHandle, diffs)
        return False
