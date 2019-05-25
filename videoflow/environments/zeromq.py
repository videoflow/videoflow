from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import numpy as np
import pickle
import zlib
import zmq

REFERENCE_SOCKET_NUMBER = 5000

def termination_socket_from_task_id(task_id : int):
    return REFERENCE_SOCKET_NUMBER - task_id

def socket_from_task_id(task_id : int):
    return REFERENCE_SOCKET_NUMBER + task_id

def task_id_from_socket(socket_number : int):
    return socket_number - REFERENCE_SOCKET_NUMBER

def task_id_from_termination_socket(socket_number : int):
    return REFERENCE_SOCKET_NUMBER - socket_number

def recv_zipped_pickle(socket: zmq.Socket, flags: int=0):
    """
    Receive a sent zipped pickle.
    """
    message = socket.recv(flags)
    object = zlib.decompress(message)
    return pickle.loads(object)

def send_zipped_pickle(socket: zmq.Socket, obj: any, flags: int=0, protocol: int=-1):
    """
    Pickle an object, and zip the pickle before sending it
    """
    object = pickle.dumps(obj, protocol)
    compressed_object = zlib.compress(object)
    return socket.send(compressed_object, flags=flags)


def send_array(socket: zmq.Socket, array: np.array, flags: int=0, copy: bool=True, track: bool=False):
    """
    Send a numpy array with metadata, type and shape
    """
    dictionary = dict(
        dtype = str(array.dtype),
        shape = array.shape,
    )
    socket.send_json(dictionary, flags|zmq.SNDMORE)
    return socket.send(array, flags, copy=copy, track=track)


def recv_array(socket: zmq.Socket, flags: int=0, copy: int=True, track: bool=False):
    """
    Receive a numpy array
    """
    dictionary = socket.recv_json(flags=flags)
    message = socket.recv(flags=flags, copy=copy, track=track)
    buffer = memoryview(message)
    array = np.frombuffer(buffer, dtype=dictionary['dtype'])
    return array.reshape(dictionary['shape'])
