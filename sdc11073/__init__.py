from . import pysoap
from . import namespaces
from . import mdib
from . import location
from . import sdcclient
from . import sdcdevice
from . import commlog

try:
    from . import version
    __version__ = version.VERSION
except ImportError:
    __version__ = '0.0.0'
