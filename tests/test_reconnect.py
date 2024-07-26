import logging
import sys
import time
import traceback
import unittest.mock

from sdc11073 import loghelper
from sdc11073.consumer import SdcConsumer
from sdc11073.reconnect import ReconnectAgent
from sdc11073.loghelper import basic_logging_setup, get_logger_adapter
from sdc11073.provider.components import (default_sdc_provider_components_async)
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_types
from tests import utils
from tests.mockstuff import SomeDevice

mdib_70041 = '70041_MDIB_multi.xml'

class Test_Client_SomeDevice(unittest.TestCase):
    def setUp(self):
        basic_logging_setup()
        self.logger = get_logger_adapter('sdc.test')
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        self.logger.info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.epr = '1234567'
        self.provider = SomeDevice.from_mdib_file(self.wsd, self.epr, mdib_70041,
                                                    default_components=default_sdc_provider_components_async,
                                                    max_subscription_duration=10)  # shorter duration for faster tests
        # in order to test correct handling of default namespaces, we make participant model the default namespace
        self.provider.start_all()
        self._loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        self.provider.set_location(utils.random_location(), self._loc_validators)

        time.sleep(0.5)  # allow init of devices to complete
        # no deferred action handling for easier debugging

        # x_addr = self.provider.get_xaddrs()
        # self.consumer = SdcConsumer(x_addr[0],
        #                               sdc_definitions=self.provider.mdib.sdc_definitions,
        #                               ssl_context_container=None,
        #                               validate=True)
        # self.consumer.start_all()  # with periodic reports and system error report
        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        self.logger.info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        try:
            if self.provider:
                self.provider.stop_all()
            # if self.consumer:
            #     self.consumer.stop_all(unsubscribe=False)
            self.wsd.stop()
        except:
            sys.stderr.write(traceback.format_exc())
        try:
            self.log_watcher.check()
        except loghelper.LogWatchError as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_reconnect(self):
        consumer = None
        my_wsd = None
        try:
            logging.getLogger('sdc.device').setLevel(logging.ERROR)
            logging.getLogger('sdc.reconnect').setLevel(logging.DEBUG)

            # need a 2nd WSDiscovery instance to detect provider
            my_wsd = WSDiscovery('127.0.0.1')
            my_wsd.start()
            reco = ReconnectAgent(my_wsd, search_timeout=4)  # make it a bit faster

            # search for provider and connect
            services = my_wsd.search_services()
            self.assertTrue(len(services) > 0)
            my_services = [s for s in services if s.epr == self.epr]
            self.assertEqual(len(my_services), 1)
            my_service = my_services[0]
            consumer = SdcConsumer.from_wsd_service(my_service, ssl_context_container=None, validate=True)
            consumer.start_all()
            # add to ReconnectAgent
            reco.keep_connected(my_service, consumer)
            self.assertTrue(consumer.is_connected)
            self.logger.info('stopping provider now')
            self.provider.stop_all(send_subscription_end=False)
            for _ in range(10):
                if consumer.is_connected:
                    time.sleep(1)
                else:
                    break
            self.assertFalse(consumer.is_connected)
            self.logger.info('starting provider now')
            self.provider.start_all()
            self.provider.publish()
            for t_reconnect in range(15):
                if consumer.is_connected:
                    self.logger.info('consumer reconnect after %d seconds', t_reconnect)
                    break
                else:
                    time.sleep(1)
            self.assertTrue(consumer.is_connected)

            # now let reco forget consumer
            reco.forget(consumer)
            self.assertEqual(len(reco._consumers), 0)

        finally:
            if my_wsd is not None:
                my_wsd.stop()
            if consumer is not None:
                consumer.stop_all()