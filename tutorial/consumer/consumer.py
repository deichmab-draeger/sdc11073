import time
import traceback
import uuid

from sdc11073 import observableproperties
from sdc11073.definitions_sdc import SDC_v1_Definitions
from sdc11073.mdib import ClientMdibContainer
from sdc11073.sdcclient import SdcClient
from sdc11073.wsdiscovery import WSDiscoverySingleAdapter
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types import pm_types
from sdc11073.loghelper import basic_logging_setup


# This example shows how to implement a very simple SDC Consumer (client)
# It will scan for SDC Providers and connect to on well known UUID

# The provider we connect to is known by its UUID
# The UUID is created from a base
baseUUID = uuid.UUID('{cc013678-79f6-403c-998f-3cc0cc050230}')
device_A_UUID = uuid.uuid5(baseUUID, "12345")


# callback function that will be called upon metric updates from the provider
def on_metric_update(metrics_by_handle):
    # we get all changed handles as parameter, iterate over them and output
    for one_handle in metrics_by_handle:
        print("Got update on: {}".format(one_handle))


def set_ensemble_context(the_mdib: ClientMdibContainer, the_client: SdcClient):
    # calling operation on remote device
    print("Trying to set ensemble context of device A")
    # first we get the container to the element in the MDIB
    descriptor_container = the_mdib.descriptions.NODETYPE.get_one(pm.EnsembleContextDescriptor)
    # get the context of our provider(client)
    context_client = the_client.context_service_client
    # start with empty operation handle and try to find the one we need
    operation_handle = None
    # iterate over all matching handles (can be 0..n)
    for one_op in the_mdib.descriptions.NODETYPE.get(pm.SetContextStateOperationDescriptor, []):
        if one_op.OperationTarget == descriptor_container.Handle:
            operation_handle = one_op.Handle
    # now we should have an operation handle to work with
    # create a new ensemble context as parameter to this operation
    new_ensemble_context = context_client.mk_proposed_context_object(descriptor_container.Handle)
    new_ensemble_context.ContextAssociation = pm_types.ContextAssociation.ASSOCIATED
    new_ensemble_context.Identification = [
        pm_types.InstanceIdentifier(root="1.2.3", extension_string="SupervisorSuperEnsemble")]
    # execute the remote operation (based on handle) with the newly created ensemble as parameter
    context_client.set_context_state(operation_handle, [new_ensemble_context])


# main entry, will start to scan for the known provider and connect
# runs forever and consumes metrics ever after
if __name__ == '__main__':
    basic_logging_setup()  # enable logging

    # start with discovery (MDPWS) that is running on the named adapter "Ethernet"
    # (replace as you need it on your machine, e.g. "enet0" or "Ethernet)
    my_discovery = WSDiscoverySingleAdapter("Ethernet")
    # start the discovery
    my_discovery.start()
    # we want to search until we found one device with this client
    found_device = False
    # loop until we found our provider
    while not found_device:
        # we now search explicitly for MedicalDevices on the network
        # this will send a probe to the network and wait for responses
        # See MDPWS discovery mechanisms for details
        services = my_discovery.search_services(types=SDC_v1_Definitions.MedicalDeviceTypesFilter)

        # now iterate through the discovered services to check if we foundDevice
        # the specific provider we search for
        for one_service in services:
            try:
                print("Got service: {}".format(one_service.epr))
                # the EndPointReference is created based on the UUID of the Provider
                if one_service.epr == device_A_UUID.urn:
                    print("Got a match: {}".format(one_service))
                    # now create a new SDCClient (=Consumer) that can be used
                    # for all interactions with the communication partner
                    my_client = SdcClient.from_wsd_service(one_service, ssl_context=None)
                    # start all services on the client to make sure we get updates
                    my_client.start_all()
                    # all data interactions happen through the MDIB (MedicalDeviceInformationBase)
                    # that contains data as described in the BICEPS standard
                    # this variable will contain the data from the provider
                    my_mdib = ClientMdibContainer(my_client)
                    my_mdib.init_mdib()
                    # we can subscribe to updates in the MDIB through the
                    # Observable Properties in order to get a callback on
                    # specific changes in the MDIB
                    observableproperties.bind(my_mdib, metrics_by_handle=on_metric_update)
                    # in order to end the 'scan' loop
                    found_device = True

                    # now we demonstrate how to call a remote operation on the consumer
                    set_ensemble_context(my_mdib, my_client)
            except:
                print(traceback.format_exc())
                print("Problem in discovery, ignoring it")

    # endless loop to keep the client running and get notified on metric changes through callback
    while True:
        time.sleep(1)
