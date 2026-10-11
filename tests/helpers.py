"""Small owned-resource fixtures; no shared broker or database state."""

from contextlib import contextmanager, ExitStack
import os
from pathlib import Path
import threading
from unittest.mock import patch

from xordination.server import BrokerServer
from xordination.store import Store


@contextmanager
def owned_broker(path, **options):
    """Yield a fresh Store, running server and endpoint; retain caller's path."""
    with ExitStack() as cleanup:
        store = Store(str(path))
        cleanup.callback(store.close)
        server = BrokerServer(("127.0.0.1", 0), store, **options)
        cleanup.callback(server.server_close)
        thread = threading.Thread(target=server.serve_forever,
                                  kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()

        def stop():
            server.stopping.set()
            server.shutdown()
            thread.join(timeout=2)
            if thread.is_alive():
                raise AssertionError("Owned broker thread did not stop")

        cleanup.callback(stop)
        yield store, server, "http://127.0.0.1:" + str(server.server_port)


@contextmanager
def observe_fsync(targets, *, fail=None):
    """Observe selected filesystem objects; leave unrelated real fsyncs intact."""
    targets = tuple(Path(target) for target in targets)
    fail = Path(fail) if fail is not None else None
    observed = []
    original = os.fsync

    def fsync(descriptor):
        opened = os.fstat(descriptor)
        for target in targets:
            try:
                info = target.stat()
            except FileNotFoundError:
                continue
            if (opened.st_dev, opened.st_ino) == (info.st_dev, info.st_ino):
                observed.append(target)
                if target == fail:
                    raise OSError("Injected fsync failure for selected test object")
                break
        original(descriptor)

    with patch("xordination.cli.os.fsync", side_effect=fsync):
        yield observed
