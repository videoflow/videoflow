import base64
import os

import datetime
import json
import logging
import pickle
import time

import numpy
from .node import Node, ConsumerNode


class StatesConsumer(ConsumerNode):
    def __init__(self, flow_name=None, states_folder="./", save_interval=5, num_states=3):
        """

        :param flow_name: unique name of the flow, states will be saved and restored with ref to that name
        :param states_folder: folder to save the states file
        :param save_interval: after 'save_interval' num of iterations save the states
        :param num_states: number of states to maintain at a given time
        """
        self.logtype_states = "states"
        self.name = flow_name
        assert self.name is not None, "Flow name must be present in the config dictionary"
        self.save_interval = save_interval
        self._count = 0
        self.file = os.path.join(states_folder, f".{self.name}.states")
        self.states = []
        self._num_states = num_states
        super(StatesConsumer, self).__init__(metadata=True)

    def add_state(self, state):
        if len(self.states) > self._num_states:
            self.states.pop()
        self.states.append(state)

    def consume(self, *metadata):

        if self._count % self.save_interval == 0:
            state = {}
            for idx, entry in enumerate(metadata):
                for log_type in [self.logtype_states]:
                    node_id = str(self._parents[idx])
                    value = entry.get(log_type, None)
                    # encode to unicode
                    value = value.decode("latin-1") if value is not None else None
                    state[idx] = {"state": value, "name": node_id}
                state["timestamp"] = str(datetime.datetime.now())

            self.add_state(state)
            if self.states:
                # TODO: store the states on any kind of consumer (webhook/redis/.. etc)
                with open(self.file, "w") as f:
                    f.flush()
                    json.dump(self.states, f)
                self._logger.info(f"States saved at {self.file}, Timestamp: {str(state['timestamp'])}")
        self._count += 1

    def read_states(self):
        '''
        This method reads the states from the file
        '''
        if os.path.isfile(self.file):
            with open(self.file, "r") as f:
                states = json.load(f)

            if states is None or len(states) == 0:
                return False
            else:
                self.states = states
                return True
        else:
            return False

    def get_latest_state(self):
        latest_state = \
        sorted(self.states, key=lambda x: datetime.datetime.strptime(x["timestamp"], "%Y-%m-%d %H:%M:%S.%f"))[-1]
        return latest_state

    def _set_state_to_node(self, state_dict, node):
        '''
        This method tries updates the node object with values present in the state_dict.
        It checks the name of the node and the name presnt in the state_dict to verify they are of same type
        - Arguments
            - state_dict: dictionary wih attribute and value of the node that will be resoterd
            - node: `videoflow.core.Node` object
        '''
        name = state_dict["name"]
        if name == str(node):
            node_match = True
            state = state_dict["state"]
            if state is not None:
                state_node = pickle.loads(state.encode("latin-1"))
                node.__dict__.update(vars(state_node))

        else:
            node_match = False
            self._logger.warning(f"Cannot restore states for node {str(name)} with {name}\n"
                                 f"Make sure the nodes order is consitent and the name of the flow is correct\n"
                                 f"..Aborting restore states")

        return node_match

    def restore_states(self):
        '''
        This method reads the states from the given source and restore it in all the nodes

        '''
        if self.read_states():
            latest_state = self.get_latest_state()
            restore_status = True

            for idx, node in enumerate(self._parents):
                state = latest_state[str(idx)]
                if not self._set_state_to_node(state, node):
                    restore_status = False
                    break

            if restore_status:
                self._logger.info("Restored state successfully.... states written time : {}"
                                  .format(latest_state["timestamp"]))

        else:
            self._logger.info(f"No states available for flow {self.name}")
