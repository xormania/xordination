"""Full-duplex JSON-lines transport for an owned native harness process.

Derived from multi-harness-proof's MIT-licensed RPC transport. Server requests
run separately from the reader, so a held tool or approval cannot block peer
messages or responses. This transport never attaches to or kills foreign PIDs.
"""

import asyncio
import inspect
import json


class RPCError(RuntimeError):
    def __init__(self, error):
        self.code = error.get("code")
        self.error = error
        super().__init__(str(error.get("message", "Native RPC error")))


class RPC:
    def __init__(self, process, request_handler, notification_handler, *, jsonrpc=True, owned=True):
        self.process = process
        self.request_handler = request_handler
        self.notification_handler = notification_handler
        self.jsonrpc = jsonrpc
        self.owned = owned
        self.next_id = 0
        self.pending = {}
        self.tasks = set()
        self.write_lock = asyncio.Lock()
        self.dead = asyncio.Event()
        self.failure = None
        self.reader = asyncio.create_task(self.read())

    def packet(self, **fields):
        return {**({"jsonrpc": "2.0"} if self.jsonrpc else {}), **fields}

    async def write(self, packet):
        wire = (json.dumps(packet, ensure_ascii=False, allow_nan=False) + "\n").encode()
        async with self.write_lock:
            if self.dead.is_set() or self.process.stdin.is_closing():
                raise ConnectionError(self.failure or "Harness RPC closed")
            self.process.stdin.write(wire)
            await self.process.stdin.drain()

    async def request(self, method, params, *, timeout=120, on_sent=None):
        self.next_id += 1
        ident = self.next_id
        future = asyncio.get_running_loop().create_future()
        self.pending[ident] = future
        try:
            await self.write(self.packet(id=ident, method=method, params=params))
            if on_sent:
                value = on_sent()
                if inspect.isawaitable(value):
                    await value
            return await asyncio.wait_for(future, timeout)
        finally:
            self.pending.pop(ident, None)
            if not future.done():
                future.cancel()

    async def notify(self, method, params=None):
        await self.write(self.packet(method=method, params=params or {}))

    async def answer(self, packet):
        try:
            value = await self.request_handler(packet["method"], packet.get("params", {}))
            response = self.packet(id=packet["id"], result=value)
        except RPCError as error:
            response = self.packet(id=packet["id"], error=error.error)
        except Exception as error:
            response = self.packet(id=packet["id"], error={"code": -32603, "message": str(error)[:1000]})
        try:
            await self.write(response)
        except (OSError, ConnectionError):
            pass

    async def read(self):
        try:
            while line := await self.process.stdout.readline():
                packet = json.loads(line)
                if not isinstance(packet, dict):
                    raise ValueError("Native protocol frame is not an object")
                if "method" in packet:
                    if not isinstance(packet["method"], str) or not isinstance(packet.get("params", {}), dict):
                        raise ValueError("Invalid native protocol request")
                    if "id" in packet:
                        task = asyncio.create_task(self.answer(packet))
                        self.tasks.add(task)
                        task.add_done_callback(self.tasks.discard)
                    else:
                        await self.notification_handler(packet["method"], packet.get("params", {}))
                elif packet.get("id") in self.pending:
                    future = self.pending[packet["id"]]
                    if not future.done():
                        if "error" in packet:
                            future.set_exception(RPCError(packet["error"]))
                        else:
                            future.set_result(packet.get("result", {}))
        except (ValueError, OSError, ConnectionError, asyncio.LimitOverrunError) as error:
            self.failure = type(error).__name__ + ": " + str(error)[:500]
        except Exception as error:
            self.failure = "Native notification handler failed: " + str(error)[:500]
        finally:
            self.failure = self.failure or "Harness closed stdout"
            self.dead.set()
            for future in list(self.pending.values()):
                if not future.done():
                    future.set_exception(ConnectionError(self.failure))

    async def close(self):
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.owned and self.process.returncode is None:
            try:
                self.process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        if not self.reader.done():
            self.reader.cancel()
        await asyncio.gather(self.reader, return_exceptions=True)
