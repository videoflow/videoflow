from __future__ import absolute_import, division, print_function

import json
from typing import Any

import requests

from ..core.node import ConsumerNode


class CommandlineConsumer(ConsumerNode):
    '''
    Writes the input received to the command line.

    - Arguments:
        - sep: separator to use between tokens.
        - end: end of line character
    '''
    def __init__(self, sep : str = ' ', end : str = '\n', **kwargs) -> None:

        self._end = end
        self._sep = sep
        super(CommandlineConsumer, self).__init__(**kwargs)

    def consume(self, item : Any) -> None:
        '''
        Prints `item` to the command line, adding an end of line character after it.

        - Arguments:
            - item: It can be anything that can be printed with the ``print()`` function
        '''
        print(item, sep = self._sep, end = self._end)

class VoidConsumer(ConsumerNode):
    '''
    Ignores the input received.
    Helpful in debugging flows.
    '''
    def __init__(self, **kwargs) -> None:
        super(VoidConsumer, self).__init__(**kwargs)

    def consume(self, item : Any) -> None:
        '''
        Does nothing with the item passed
        '''
        pass


class WebhookConsumer(ConsumerNode):
    def __init__(self, host : str, method : str = "post", **kwargs) -> None:
        # TODO: Add other pertinent parameters to the init method.
        self.host = host
        self.method = method
        super(WebhookConsumer, self).__init__(**kwargs)

    def consume(self, item : Any) -> None:
        # convert item to json
        try:
            item = json.loads(item)
        except TypeError:
            print("Not consuming item is not json serializable")

        requests.post(self.host,item)


class FileAppenderConsumer(ConsumerNode):
    '''
    Appends a text representation of each received item, one per line, to a file.

    - Arguments:
        - filepath: path to the file to append to (created if missing). The folder \
            must already exist.
    '''
    def __init__(self, filepath : str, **kwargs) -> None:
        self._filepath = filepath
        super(FileAppenderConsumer, self).__init__(**kwargs)

    def consume(self, item : Any) -> None:
        with open(self._filepath, 'a') as f:
            f.write(f'{item}\n')
