from unittest import TestCase

from sdc11073.pysoap.soapclientpool import SoapClientPool
from sdc11073.pysoap.soapclient import SoapClientProtocol


class DummySoapClient(SoapClientProtocol):
    """Implements parts of SopaClientProtocol that are needed for test."""

    def __init__(self, netloc: str, accepted_encodings: list[str]):
        self.netloc = netloc
        self.accepted_encodings = accepted_encodings
        self._is_closed = False

    def close(self):
        """."""
        self._is_closed = True

    def is_closed(self) -> bool:
        """."""
        return self._is_closed


def mk_dummy_client(netloc: str, accepted_encodings: list[str]) -> SoapClientProtocol:
    """Create a SoapClientProtocol (Factory function)."""
    return DummySoapClient(netloc, accepted_encodings)


class DummyConnection:
    """Emulates a subscription."""

    def __init__(self, netloc: str, epr: str):
        self.netloc = netloc
        self.epr = epr
        self.is_reachable = True

    def on_unreachable(self):
        """Handle unreachable condition."""
        self.is_reachable = False


class TestSoapClientPool(TestCase):

    def setUp(self):
        self.netloc1 = '127.0.0.1'
        self.netloc2 = '127.0.0.2'
        self.epr_1a = 'epr_1a'
        self.epr_1b = 'epr_1b'
        self.epr_2a = 'epr_2a'
        self.epr_2b = 'epr_2b'
        self.soapclientpool = SoapClientPool(mk_dummy_client, '')
        self.connection1a = DummyConnection(self.netloc1, self.epr_1a)
        self.connection1b = DummyConnection(self.netloc1, self.epr_1b)
        self.connection2a = DummyConnection(self.netloc2, self.epr_2a)
        self.connection2b = DummyConnection(self.netloc2, self.epr_2b)
        for conn in (self.connection1a, self.connection1b, self.connection2a, self.connection2b):
            self.soapclientpool.register_netloc_user(conn.netloc, conn.epr, conn.on_unreachable)

    def test_get_soap_client(self):
        """Verify that a soap client is instantiated with correct parameters."""
        dummy_soap_client = self.soapclientpool.get_soap_client(self.connection1a.netloc, ['gzip'])
        self.assertEqual(dummy_soap_client.netloc, self.netloc1)
        self.assertEqual(dummy_soap_client.accepted_encodings, ['gzip'])
        self.assertFalse(dummy_soap_client.is_closed())

    def test_forget_callback(self):
        """Verify that forget_callback works."""
        dummy_soap_client = self.soapclientpool.get_soap_client(self.connection1a.netloc, ['gzip'])
        # forget the first callback=> this shall keep the soap client connected because connection 1a is still there.
        self.soapclientpool.forget_callback(self.connection1a.on_unreachable)
        self.assertFalse(dummy_soap_client.is_closed())
        self.assertTrue(self.netloc1 in self.soapclientpool._soap_clients)
        self.assertTrue(self.soapclientpool._soap_clients[self.netloc1].soap_client is dummy_soap_client)
        # forget the other callback, now connection to netloc1 shall be closed.
        self.soapclientpool.forget_callback(self.connection1b.on_unreachable)
        self.assertFalse(self.netloc1 in self.soapclientpool._soap_clients)
        self.assertTrue(dummy_soap_client.is_closed())

    def test_report_unreachable_epr(self):
        """Verify that report_unreachable works as expected."""
        self.soapclientpool.report_unreachable_epr(self.connection1a.netloc, self.connection1a.epr)
        self.assertFalse(self.connection1a.is_reachable)
        self.assertTrue(self.connection1b.is_reachable)
        self.assertFalse(self.connection1a.epr in self.soapclientpool._soap_clients[self.connection1a.netloc].callbacks)
        # connection to netloc2 shall still be there
        self.assertTrue(self.connection2a.is_reachable)
        self.assertTrue(self.connection2b.is_reachable)
        self.assertTrue(self.netloc2 in self.soapclientpool._soap_clients)

        dummy_soap_client = self.soapclientpool.get_soap_client(self.connection2a.netloc, ['gzip'])

        self.assertFalse(dummy_soap_client.is_closed())
        self.soapclientpool.report_unreachable_epr(self.connection2a.netloc, self.connection2a.epr)
        self.assertFalse(dummy_soap_client.is_closed())

    def test_report_unreachable_netloc(self):
        """Verify that report_unreachable works as expected."""
        self.soapclientpool.report_unreachable_netloc(self.netloc1)
        self.assertFalse(self.connection1a.is_reachable)
        self.assertFalse(self.connection1b.is_reachable)
        self.assertFalse(self.netloc1 in self.soapclientpool._soap_clients)
        # connection to netloc2 shall still be there
        self.assertTrue(self.connection2a.is_reachable)
        self.assertTrue(self.connection2b.is_reachable)
        self.assertTrue(self.netloc2 in self.soapclientpool._soap_clients)

        dummy_soap_client = self.soapclientpool.get_soap_client(self.connection2a.netloc, ['gzip'])

        self.assertFalse(dummy_soap_client.is_closed())
        self.soapclientpool.report_unreachable_netloc(self.netloc2)
        self.assertTrue(dummy_soap_client.is_closed())
