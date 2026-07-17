'''
Node classes used by integration tests. Kept in their own top-level module (the
tests add this directory to PYTHONPATH) so worker subprocesses can import them by
class path, exactly as they would a real user's node.
'''
import asyncio

from videoflow.core.node import ProcessorNode


class AsyncDoubler(ProcessorNode):
    '''A processor whose process() is an async coroutine — exercises the task's async bridge.'''
    async def process(self, x):
        await asyncio.sleep(0.001)
        return x * 2

class CtxPartitionTagger(ProcessorNode):
    '''Uses the injected ctx to set a partition key derived from the value, then passes the value through.'''
    def process(self, x, ctx = None):
        if ctx is not None:
            ctx.set_partition_key(f'k{x % 3}')
        return x
