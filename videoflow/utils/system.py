import subprocess

def get_number_of_gpus() -> int:
    '''
    Returns the number of gpus in the system
    '''
    try:
        n = str(subprocess.check_output(["nvidia-smi", "-L"])).count('UUID')
        return n
    except FileNotFoundError:
        return 0

def get_available_gpus() -> [int]:
    '''
    Returns the list of ids of the gpus available to the process calling the function.
    It first gets the set of ids of the gpus in the system.  Then it gets the set of ids marked as
    available by ``CUDA_VISIBLE_DEVICES``. It returns the intersection of those
    two sets as a list.
    '''
    # Remember that an empty ``CUDA_VISIBLE_DEVICES`` means that all devices are visible.
    pass
    