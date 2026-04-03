"""Edge agent."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("edge-agent")
except PackageNotFoundError:
    __version__ = "0.1.11"
