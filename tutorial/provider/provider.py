import time
import uuid
from decimal import Decimal

from sdc11073.location import SdcLocation
from sdc11073.mdib import DeviceMdibContainer
from sdc11073.mdib.statecontainers import EnsembleContextStateContainer
from sdc11073.sdcdevice import SdcDevice
from sdc11073.wsdiscovery import WSDiscoverySingleAdapter
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types import pm_types
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
from sdc11073.loghelper import basic_logging_setup

# example SDC provider (device) that sends out metrics frequently


# The provider we use, should match the one in consumer example
# The UUID is created from a base
baseUUID = uuid.UUID('{cc013678-79f6-403c-998f-3cc0cc050230}')
my_uuid = uuid.uuid5(baseUUID, "12345")


# setting the local ensemble context upfront
def set_local_ensemble_context(mdib: DeviceMdibContainer, ensemble: str):
    descriptor_container = mdib.descriptions.NODETYPE.getOne(pm.EnsembleContextDescriptor)
    if not descriptor_container:
        print("No ensemble contexts in mdib")
        return
    all_ensemble_contexts = mdib.context_states.descriptorHandle.get(descriptor_container.Handle, [])
    with mdib.transaction_manager() as my_mgr:
        # set all to currently associated Locations to Disassociated
        associated_ensembles = [l for l in all_ensemble_contexts if
                                l.ContextAssociation == pm_types.ContextAssociation.ASSOCIATED]
        for l in associated_ensembles:
            ensemble_context = my_mgr.get_context_state(l.descriptorHandle, l.Handle)
            ensemble_context.ContextAssociation = pm_types.ContextAssociation.DISASSOCIATED
            # UnbindingMdibVersion is the first version in which it is no longer bound ( == this version)
            ensemble_context.UnbindingMdibVersion = mdib.mdib_version

        new_ens_state = EnsembleContextStateContainer(descriptor_container)
        new_ens_state.Handle = uuid.uuid4().hex
        new_ens_state.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
        new_ens_state.Identification = [pm_types.InstanceIdentifier(root="1.2.3", extension_string=ensemble)]
        my_mgr.add_state(new_ens_state)


if __name__ == '__main__':
    basic_logging_setup()  # enable logging
    # start with discovery (MDPWS) that is running on the named adapter "Ethernet"
    # (replace as you need it on your machine, e.g. "enet0" or "Ethernet")
    my_discovery = WSDiscoverySingleAdapter("Ethernet")
    # start the discovery
    my_discovery.start()
    # create a local mdib that will be sent out on the network, the mdib is based on an XML file
    my_mdib = DeviceMdibContainer.from_mdib_file("mdib.xml")
    print("My UUID is {}".format(my_uuid))
    # set a location context to allow easy discovery
    my_location = SdcLocation(fac='HOSP', poc='CU2', bed='BedSim')
    # set model information for discovery
    this_model = ThisModelType(manufacturer='Draeger',
                               manufacturer_url='www.draeger.com',
                               model_name='TestDevice',
                               model_number='1.0',
                               model_url='www.draeger.com/model',
                               presentation_url='www.draeger.com/model/presentation')
    this_device = ThisDeviceType(friendly_name='TestDevice',
                                 firmware_version='Version1',
                                 serial_number='12345')
    # create a device (provider) class that will do all the SDC magic
    sdc_device = SdcDevice(ws_discovery=my_discovery,
                           epr=my_uuid,
                           this_model=this_model,
                           this_device=this_device,
                           device_mdib_container=my_mdib)
    # start the local device and make it discoverable
    sdc_device.start_all()
    # set the local ensemble context to ease discovery based on ensemble ID
    set_local_ensemble_context(my_mdib, "MyEnsemble")
    # set the location on our device
    sdc_device.set_location(my_location)
    # get all metrics from the mdib (as described in the file)
    all_metric_descriptors = my_mdib.descriptions.NODETYPE.get(pm.NumericMetricDescriptor)
    # now change all the metrics in one transaction
    with my_mdib.transaction_manager() as mgr:
        for metric_descr in all_metric_descriptors:
            # get the metric state of this specific metric
            st = mgr.get_state(metric_descr.Handle)
            # create a value in case it is not there yet
            st.mk_metric_value()
            # set the value and some other fields to a fixed value
            st.MetricValue.Value = Decimal(1.0)
            st.MetricValue.ActiveDeterminationPeriod = 1494554822450
            st.MetricValue.Validity = pm_types.MeasurementValidity.VALID
            st.ActivationState = pm_types.ComponentActivation.ON
    metric_value = Decimal(0)
    # now iterate forever and change the value every few seconds
    while True:
        metric_value += 1
        with my_mdib.transaction_manager() as mgr:
            for metric_descr in all_metric_descriptors:
                st = mgr.get_state(metric_descr.Handle)
                st.MetricValue.Value = metric_value
        time.sleep(5)
