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
    