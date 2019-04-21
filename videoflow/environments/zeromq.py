import numpy as np
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

def get_next_message(socket_address):
    pass

def pick_next_message(self._termination_socket_address):
    '''
    - DOES NOT BLOCK
    Returns None if no message is present
    Returns message if there is one.
    '''
    pass

def publish_next_message(socket_address, message):
    pass

def recv_zipped_pickle(socket: zmq.Socket, flags: int=0):
    """
    Receive a sent zipped pickle.
    """
    message = socket.recv(flags)
    object = zlib.decompress(message)
    return pickle.loads(object)


def send_zipped_pickle(socket: zmq.Socket, obj: Any, flags: int=0, protocol: int=-1):
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
    Recieve a numpy array
    """
    dictionary = socket.recv_json(flags=flags)
    message = socket.recv(flags=flags, copy=copy, track=track)
    buffer = memoryview(message)
    array = np.frombuffer(buffer, dtype=dictionary['dtype'])
    return array.reshape(dictionary['shape'])