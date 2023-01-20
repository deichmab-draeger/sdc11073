import datetime
import logging
import sys
import time
import unittest
from decimal import Decimal

from sdc11073 import commlog
from sdc11073 import loghelper
from sdc11073 import observableproperties
from sdc11073 import pm_qnames as pm
from sdc11073 import pmtypes
from sdc11073 import msgtypes
from sdc11073.location import SdcLocation
from sdc11073.loghelper import basic_logging_setup
from sdc11073.mdib import ClientMdibContainer
from sdc11073.mdib.devicewaveform import Annotator
from sdc11073.roles.nomenclature import NomenclatureCodes as nc
from sdc11073.sdcclient import SdcClient
from sdc11073.sdcdevice import waveforms
from sdc11073.wsdiscovery import WSDiscoveryWhitelist
from tests.mockstuff import SomeDevice

ENABLE_COMMLOG = False
if ENABLE_COMMLOG:
    comm_logger = commlog.CommLogger(log_folder=r'c:\temp\sdc_commlog',
                                     log_out=True,
                                     log_in=True,
                                     broadcast_ip_filter=None)
    commlog.set_communication_logger(comm_logger)

CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value


def provide_realtime_data(sdc_device):
    waveform_provider = sdc_device.mdib.xtra.waveform_provider
    if waveform_provider is None:
        return
    paw = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=1.1, sampleperiod=0.01)
    waveform_provider.register_waveform_generator('0x34F05500', paw)  # '0x34F05500 MBUSX_RESP_THERAPY2.00H_Paw'

    flow = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=1.2, sampleperiod=0.01)
    waveform_provider.register_waveform_generator('0x34F05501', flow)  # '0x34F05501 MBUSX_RESP_THERAPY2.01H_Flow'

    co2 = waveforms.TriangleGenerator(min_value=0, max_value=20, waveformperiod=1.0, sampleperiod=0.01)
    waveform_provider.register_waveform_generator('0x34F05506',
                                                  co2)  # '0x34F05506 MBUSX_RESP_THERAPY2.06H_CO2_Signal'

    # make SinusGenerator (0x34F05501) the annotator source
    annotator = Annotator(annotation=pmtypes.Annotation(pmtypes.CodedValue('a', 'b')),
                          trigger_handle='0x34F05501',
                          annotated_handles=['0x34F05500', '0x34F05501', '0x34F05506'])
    waveform_provider.register_annotation_generator(annotator)


class Test_BuiltinOperations(unittest.TestCase):
    """ Test role providers (located in sdc11073.roles)

    """
    def setUp(self):
        basic_logging_setup()

        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='fac1', poc='CU1', bed='Bed')
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        ns_mapper = self.sdc_device.mdib.nsmapper
        #ToDo: set default namespace
        # ns_mapper._prefixmap['__BICEPS_ParticipantModel__'] = None  # make this the default namespace
        self.sdc_device.start_all(periodic_reports_interval=1.0)
        self._loc_validators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._loc_validators)
        provide_realtime_data(self.sdc_device)

        time.sleep(0.5)  # allow init of devices to complete

        x_addr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(x_addr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE)
        self.sdc_client.start_all(subscribe_periodic_reports=True, async_dispatch=False)
        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        if self.sdc_client:
            self.sdc_client.stop_all()
        if self.sdc_device:
            self.sdc_device.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_set_patient_context_operation(self):
        """client calls corresponding operation of GenericContextProvider.
        - verify that operation is successful.
         verify that a notification device->client also updates the client mdib."""
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()
        patient_descriptor_container = client_mdib.descriptions.NODETYPE.get_one(pm.PatientContextDescriptor)
        # initially the device shall not have any patient
        patient_context_state_container = client_mdib.context_states.NODETYPE.get_one(
            pm.PatientContext, allow_none=True)
        self.assertIsNone(patient_context_state_container)

        my_operations = client_mdib.get_operation_descriptors_for_descriptor_handle(
            patient_descriptor_container.Handle,
            NODETYPE=pm.SetContextStateOperationDescriptor)
        self.assertEqual(len(my_operations), 1)
        operation_handle = my_operations[0].Handle
        print('Handle for SetContextSTate Operation = {}'.format(operation_handle))
        context = self.sdc_client.client('Context')

        # insert a new patient with wrong handle, this shall fail
        proposed_context = context.mk_proposed_context_object(patient_descriptor_container.Handle)
        proposed_context.Handle = 'some_nonexisting_handle'
        proposed_context.CoreData.Givenname = 'Karl'
        proposed_context.CoreData.Middlename = ['M.']
        proposed_context.CoreData.Familyname = 'Klammer'
        proposed_context.CoreData.Birthname = 'Bourne'
        proposed_context.CoreData.Title = 'Dr.'
        proposed_context.CoreData.Sex = pmtypes.T_Sex.MALE
        proposed_context.CoreData.PatientType = pmtypes.PatientType.ADULT
        proposed_context.CoreData.set_birthdate('2000-12-12')
        proposed_context.CoreData.Height = pmtypes.Measurement(Decimal('88.2'), pmtypes.CodedValue('abc', 'def'))
        proposed_context.CoreData.Weight = pmtypes.Measurement(Decimal('68.2'), pmtypes.CodedValue('abc'))
        proposed_context.CoreData.Race = pmtypes.CodedValue('somerace')
        self.log_watcher.setPaused(True)
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FAILED)
        self.log_watcher.setPaused(False)

        # insert a new patient with correct handle, this shall succeed
        proposed_context.Handle = patient_descriptor_container.Handle
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

        # check client side patient context, this shall have been set via notification
        patient_context_state_container = client_mdib.context_states.NODETYPE.get_one(pm.PatientContextState)
        self.assertEqual(patient_context_state_container.CoreData.Givenname, 'Karl')
        self.assertEqual(patient_context_state_container.CoreData.Middlename, ['M.'])
        self.assertEqual(patient_context_state_container.CoreData.Familyname, 'Klammer')
        self.assertEqual(patient_context_state_container.CoreData.Birthname, 'Bourne')
        self.assertEqual(patient_context_state_container.CoreData.Title, 'Dr.')
        self.assertEqual(patient_context_state_container.CoreData.Sex, 'M')
        self.assertEqual(patient_context_state_container.CoreData.PatientType, pmtypes.PatientType.ADULT)
        self.assertEqual(patient_context_state_container.CoreData.Height.MeasuredValue, Decimal('88.2'))
        self.assertEqual(patient_context_state_container.CoreData.Weight.MeasuredValue, Decimal('68.2'))
        self.assertEqual(patient_context_state_container.CoreData.Race, pmtypes.CodedValue('somerace'))
        self.assertNotEqual(patient_context_state_container.Handle,
                            patient_descriptor_container.Handle)  # device replaced it with its own handle
        self.assertEqual(patient_context_state_container.ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)

        # test update of the patient
        proposed_context = context.mk_proposed_context_object(patient_descriptor_container.Handle,
                                                             handle=patient_context_state_container.Handle)
        proposed_context.CoreData.Givenname = 'Karla'
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        patient_context_state_container = client_mdib.context_states.handle.get_one(
            patient_context_state_container.Handle)
        self.assertEqual(patient_context_state_container.CoreData.Givenname, 'Karla')
        self.assertEqual(patient_context_state_container.CoreData.Familyname, 'Klammer')

        # set new patient, check binding mdib versions and context association
        proposed_context = context.mk_proposed_context_object(patient_descriptor_container.Handle)
        proposed_context.CoreData.Givenname = 'Heidi'
        proposed_context.CoreData.Middlename = ['M.']
        proposed_context.CoreData.Familyname = 'Klammer'
        proposed_context.CoreData.Birthname = 'Bourne'
        proposed_context.CoreData.Title = 'Dr.'
        proposed_context.CoreData.Sex = pmtypes.T_Sex.FEMALE
        proposed_context.CoreData.PatientType = pmtypes.PatientType.ADULT
        proposed_context.CoreData.set_birthdate('2000-12-12')
        proposed_context.CoreData.Height = pmtypes.Measurement(Decimal('88.2'), pmtypes.CodedValue('abc', 'def'))
        proposed_context.CoreData.Weight = pmtypes.Measurement(Decimal('68.2'), pmtypes.CodedValue('abc'))
        proposed_context.CoreData.Race = pmtypes.CodedValue('somerace')
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        patient_context_state_containers = client_mdib.context_states.NODETYPE.get(pm.PatientContextState, [])
        # sort by BindingMdibVersion
        patient_context_state_containers.sort(key=lambda obj: obj.BindingMdibVersion)
        self.assertEqual(len(patient_context_state_containers), 2)
        old_patient = patient_context_state_containers[0]
        new_patient = patient_context_state_containers[1]
        self.assertEqual(old_patient.ContextAssociation, pmtypes.ContextAssociation.DISASSOCIATED)
        self.assertEqual(new_patient.ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)

        # create a patient locally on device, then test update from client
        coll = observableproperties.SingleValueCollector(self.sdc_client, 'episodic_context_report')
        with self.sdc_device.mdib.transaction_manager() as mgr:
            st = mgr.mk_context_state(patient_descriptor_container.Handle)
            st.CoreData.Givenname = 'Max123'
            st.CoreData.Middlename = ['Willy']
            st.CoreData.Birthname = 'Mustermann'
            st.CoreData.Familyname = 'Musterfrau'
            st.CoreData.Title = 'Rex'
            st.CoreData.Sex = pmtypes.T_Sex.MALE
            st.CoreData.PatientType = pmtypes.PatientType.ADULT
            st.CoreData.Height = pmtypes.Measurement(Decimal('88.2'), pmtypes.CodedValue('abc', 'def'))
            st.CoreData.Weight = pmtypes.Measurement(Decimal('68.2'), pmtypes.CodedValue('abc'))
            st.CoreData.Race = pmtypes.CodedValue('123', 'def')
            st.CoreData.DateOfBirth = datetime.datetime(2012, 3, 15, 13, 12, 11)
        coll.result(timeout=NOTIFICATION_TIMEOUT)
        patient_context_state_containers = client_mdib.context_states.NODETYPE.get(pm.PatientContextState)
        my_patients = [p for p in patient_context_state_containers if p.CoreData.Givenname == 'Max123']
        self.assertEqual(len(my_patients), 1)
        my_patient = my_patients[0]
        proposed_context = context.mk_proposed_context_object(patient_descriptor_container.Handle, my_patient.Handle)
        proposed_context.CoreData.Givenname = 'Karl123'
        future = context.set_context_state(operation_handle, [proposed_context])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        my_patient2 = self.sdc_device.mdib.context_states.handle.get_one(my_patient.Handle)
        self.assertEqual(my_patient2.CoreData.Givenname, 'Karl123')

    def test_location_context(self):
        # initially the device shall have one location, and the client must have it in its mdib
        device_mdib = self.sdc_device.mdib
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()

        dev_locations = device_mdib.context_states.NODETYPE.get(pm.LocationContextState)
        cl_locations = client_mdib.context_states.NODETYPE.get(pm.LocationContextState)
        self.assertEqual(len(dev_locations), 1)
        self.assertEqual(len(cl_locations), 1)
        self.assertEqual(dev_locations[0].Handle, cl_locations[0].Handle)
        self.assertEqual(cl_locations[0].ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)
        self.assertEqual(cl_locations[0].BindingMdibVersion, 0)  # created at the beginning
        self.assertEqual(cl_locations[0].UnbindingMdibVersion, None)

        for i in range(10):
            current_bed = 'Bed_{}'.format(i)
            new_location = SdcLocation(fac='fac1', poc='CU2', bed=current_bed)
            coll = observableproperties.SingleValueCollector(client_mdib, 'context_by_handle')
            self.sdc_device.set_location(new_location)
            coll.result(timeout=NOTIFICATION_TIMEOUT)
            dev_locations = device_mdib.context_states.NODETYPE.get(pm.LocationContextState)
            cl_locations = client_mdib.context_states.NODETYPE.get(pm.LocationContextState)
            self.assertEqual(len(dev_locations), i + 2)
            self.assertEqual(len(cl_locations), i + 2)

            # sort by mdib_version
            dev_locations.sort(key=lambda a: a.BindingMdibVersion)
            cl_locations.sort(key=lambda a: a.BindingMdibVersion)
            # Plausibility check that the new location has expected data
            self.assertEqual(dev_locations[-1].LocationDetail.PoC, new_location.poc)
            self.assertEqual(cl_locations[-1].LocationDetail.PoC, new_location.poc)
            self.assertEqual(dev_locations[-1].LocationDetail.Bed, new_location.bed)
            self.assertEqual(cl_locations[-1].LocationDetail.Bed, new_location.bed)
            self.assertEqual(dev_locations[-1].ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)
            self.assertEqual(cl_locations[-1].ContextAssociation, pmtypes.ContextAssociation.ASSOCIATED)
            self.assertEqual(dev_locations[-1].UnbindingMdibVersion, None)
            self.assertEqual(cl_locations[-1].UnbindingMdibVersion, None)

            for j, loc in enumerate(dev_locations[:-1]):
                self.assertEqual(loc.ContextAssociation, pmtypes.ContextAssociation.DISASSOCIATED)
                self.assertEqual(loc.UnbindingMdibVersion, dev_locations[j + 1].BindingMdibVersion + 1)

            for j, loc in enumerate(cl_locations[:-1]):
                self.assertEqual(loc.ContextAssociation, pmtypes.ContextAssociation.DISASSOCIATED)
                self.assertEqual(loc.UnbindingMdibVersion, cl_locations[j + 1].BindingMdibVersion + 1)

    def test_audio_pause(self):
        """Tests AudioPauseProvider

        """
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)

        set_service = self.sdc_client.client('Set')
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()
        coding = pmtypes.Coding(nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
        operation = self.sdc_device.mdib.descriptions.coding.get_one(coding)
        future = set_service.activate(operation_handle=operation.Handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        # the whole tests only makes sense if there is an alert system
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)
        for alert_system_descriptor in alert_system_descriptors:
            state = self.sdc_client.mdib.states.descriptorHandle.get_one(alert_system_descriptor.Handle)
            # we know that the state has only one SystemSignalActivation entity, which is audible and should be paused now
            self.assertEqual(state.SystemSignalActivation[0].State, pmtypes.AlertActivation.PAUSED)

        coding = pmtypes.Coding(nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)
        operation = self.sdc_device.mdib.descriptions.coding.get_one(coding)
        future = set_service.activate(operation_handle=operation.Handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        # the whole tests only makes sense if there is an alert system
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)
        for alert_system_descriptor in alert_system_descriptors:
            state = self.sdc_client.mdib.states.descriptorHandle.get_one(alert_system_descriptor.Handle)
            self.assertEqual(state.SystemSignalActivation[0].State, pmtypes.AlertActivation.ON)

    def test_audio_pause_two_clients(self):
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)

        set_service = self.sdc_client.client('Set')
        client_mdib1 = ClientMdibContainer(self.sdc_client)
        client_mdib1.init_mdib()

        # connect a 2nd client
        x_addr = self.sdc_device.get_xaddrs()
        sdc_client2 = SdcClient(x_addr[0],
                                sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                ssl_context=None,
                                validate=CLIENT_VALIDATE)
        sdc_client2.start_all(subscribe_periodic_reports=True, async_dispatch=False)
        client_mdib2 = ClientMdibContainer(sdc_client2)
        client_mdib2.init_mdib()
        clients = (self.sdc_client, sdc_client2)
        coding = pmtypes.Coding(nc.MDC_OP_SET_ALL_ALARMS_AUDIO_PAUSE)
        operation = self.sdc_device.mdib.descriptions.coding.get_one(coding)
        future = set_service.activate(operation_handle=operation.Handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        # the whole tests only makes sense if there is an alert system
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)
        for alert_system_descriptor in alert_system_descriptors:
            for client in clients:
                state = client.mdib.states.descriptorHandle.get_one(alert_system_descriptor.Handle)
                # we know that the state has only one SystemSignalActivation entity, which is audible and should be paused now
                self.assertEqual(state.SystemSignalActivation[0].State, pmtypes.AlertActivation.PAUSED)

        coding = pmtypes.Coding(nc.MDC_OP_SET_CANCEL_ALARMS_AUDIO_PAUSE)
        operation = self.sdc_device.mdib.descriptions.coding.get_one(coding)
        future = set_service.activate(operation_handle=operation.Handle, arguments=None)
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        time.sleep(0.5)  # allow notifications to arrive
        # the whole tests only makes sense if there is an alert system
        alert_system_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.AlertSystemDescriptor)
        self.assertTrue(alert_system_descriptors is not None)
        self.assertGreater(len(alert_system_descriptors), 0)
        for alert_system_descriptor in alert_system_descriptors:
            for client in clients:
                state = client.mdib.states.descriptorHandle.get_one(alert_system_descriptor.Handle)
                self.assertEqual(state.SystemSignalActivation[0].State, pmtypes.AlertActivation.ON)

    def test_set_ntp_server(self):
        set_service = self.sdc_client.client('Set')
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()
        coding = pmtypes.Coding(nc.MDC_OP_SET_TIME_SYNC_REF_SRC)
        my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding, allow_none=True)
        if my_operation_descriptor is None:
            # try old code:
            coding = pmtypes.Coding(nc.OP_SET_NTP)
            my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding)

        operation_handle = my_operation_descriptor.Handle
        for value in ('169.254.0.199', '169.254.0.199:1234'):
            print('ntp server', value)
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msgtypes.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            state = client_mdib.states.descriptorHandle.get_one(my_operation_descriptor.OperationTarget)
            if state.NODETYPE == pm.MdsState:
                # look for the ClockState child
                clock_descriptors = client_mdib.descriptions.NODETYPE.get(pm.ClockDescriptor, [])
                clock_descriptors = [c for c in clock_descriptors if c.descriptor_handle == state.descriptorHandle]
                if len(clock_descriptors) == 1:
                    state = client_mdib.states.descriptorHandle.get_one(clock_descriptors[0].Handle)

            self.assertEqual(state.ReferenceSource[0].text, value)

    def test_set_time_zone(self):
        set_service = self.sdc_client.client('Set')
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()

        coding = pmtypes.Coding(nc.MDC_ACT_SET_TIME_ZONE)
        my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding, allow_none=True)
        if my_operation_descriptor is None:
            # use old code:
            coding = pmtypes.Coding(nc.OP_SET_TZ)
            my_operation_descriptor = self.sdc_device.mdib.descriptions.coding.get_one(coding)

        operation_handle = my_operation_descriptor.Handle
        for value in ('+03:00', '-03:00'):  # are these correct values?
            print('time zone', value)
            future = set_service.set_string(operation_handle=operation_handle, requested_string=value)
            result = future.result(timeout=SET_TIMEOUT)
            state = result.InvocationInfo.InvocationState
            self.assertEqual(state, msgtypes.InvocationState.FINISHED)
            self.assertIsNone(result.InvocationInfo.InvocationError)
            self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))

            # verify that the corresponding state has been updated
            state = client_mdib.states.descriptorHandle.get_one(my_operation_descriptor.OperationTarget)
            if state.NODETYPE == pm.MdsState:
                # look for the ClockState child
                clock_descriptors = client_mdib.descriptions.NODETYPE.get(pm.ClockDescriptor, [])
                clock_descriptors = [c for c in clock_descriptors if c.parent_handle == state.DescriptorHandle]
                if len(clock_descriptors) == 1:
                    state = client_mdib.states.descriptorHandle.get_one(clock_descriptors[0].Handle)
            self.assertEqual(state.TimeZone, value)

    def test_set_metric_state(self):
        # first we need to add a set_metric_state Operation
        sco_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.ScoDescriptor)
        cls = self.sdc_device.mdib.data_model.get_descriptor_container_class(pm.SetMetricStateOperationDescriptor)
        operation_target_handle = '0x34F001D5'
        my_code = pmtypes.CodedValue('99999')
        my_operation_descriptor = cls('HANDLE_FOR_MY_TEST', sco_descriptors[0].Handle)
        my_operation_descriptor.Type = my_code
        my_operation_descriptor.SafetyClassification = pmtypes.SafetyClassification.INF
        my_operation_descriptor.OperationTarget = operation_target_handle
        self.sdc_device.mdib.descriptions.add_object(my_operation_descriptor)
        op = self.sdc_device.product_roles.metric_provider.make_operation_instance(
            my_operation_descriptor, self.sdc_device.sco_operations_registry.operation_cls_getter)
        self.sdc_device.sco_operations_registry.register_operation(op)
        self.sdc_device.mdib.xtra.mk_state_containers_for_all_descriptors()
        setService = self.sdc_client.client('Set')
        clientMdib = ClientMdibContainer(self.sdc_client)
        clientMdib.init_mdib()

        operation_handle = my_operation_descriptor.Handle
        proposed_metric_state = clientMdib.xtra.mk_proposed_state(operation_target_handle)
        self.assertIsNone(proposed_metric_state.LifeTimePeriod)  # just to be sure that we know the correct intitial value
        before_state_version = proposed_metric_state.StateVersion
        newLifeTimePeriod = 42.5
        proposed_metric_state.LifeTimePeriod = newLifeTimePeriod
        future = setService.set_metric_state(operation_handle=operation_handle,
                                             proposed_metric_states=[proposed_metric_state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        updated_metric_state = clientMdib.states.descriptorHandle.get_one(operation_target_handle)
        self.assertEqual(updated_metric_state.StateVersion, before_state_version + 1)
        self.assertAlmostEqual(updated_metric_state.LifeTimePeriod, newLifeTimePeriod)

    def test_set_component_state(self):
        """ tests GenericSetComponentStateOperationProvider"""
        operation_target_handle = '2.1.2.1'  # a channel
        # first we need to add a set_component_state Operation
        sco_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(pm.ScoDescriptor)
        cls = self.sdc_device.mdib.data_model.get_descriptor_container_class(pm.SetComponentStateOperationDescriptor)
        my_operation_descriptor = cls('HANDLE_FOR_MY_TEST', sco_descriptors[0].Handle)
        my_operation_descriptor.SafetyClassification = pmtypes.SafetyClassification.INF

        my_operation_descriptor.OperationTarget = operation_target_handle
        my_operation_descriptor.Type = pmtypes.CodedValue('999998')
        self.sdc_device.mdib.descriptions.add_object(my_operation_descriptor)
        op = self.sdc_device.product_roles.make_operation_instance(
            my_operation_descriptor, self.sdc_device.sco_operations_registry.operation_cls_getter)
        self.sdc_device.sco_operations_registry.register_operation(op)
        self.sdc_device.mdib.xtra.mk_state_containers_for_all_descriptors()
        set_service = self.sdc_client.client('Set')
        client_mdib = ClientMdibContainer(self.sdc_client)
        client_mdib.init_mdib()

        operation_handle = my_operation_descriptor.Handle
        proposed_component_state = client_mdib.xtra.mk_proposed_state(operation_target_handle)
        self.assertIsNone(
            proposed_component_state.OperatingHours)  # just to be sure that we know the correct intitial value
        before_state_version = proposed_component_state.StateVersion
        new_operating_hours = 42
        proposed_component_state.OperatingHours = new_operating_hours
        future = set_service.set_component_state(operation_handle=operation_handle,
                                                proposed_component_states=[proposed_component_state])
        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msgtypes.InvocationState.FINISHED)
        self.assertIsNone(result.InvocationInfo.InvocationError)
        self.assertEqual(0, len(result.InvocationInfo.InvocationErrorMessage))
        updated_component_state = client_mdib.states.descriptorHandle.get_one(operation_target_handle)
        self.assertEqual(updated_component_state.StateVersion, before_state_version + 1)
        self.assertEqual(updated_component_state.OperatingHours, new_operating_hours)
