class Node():
    def __init__(self):
        self._parents = None
    
    def __repr__(self):
        return self.__class__.__name__
    
    def __call__(self, *parents):
        self._parents = []
        for parent in parents:
            assert isinstance(parent, Node) and not isinstance(parent, Leaf),
                    '%s is not a non-leaf node' % str(parent)
            self._parents.append(parent)

    @property
    def parents(self):
        return self._parents

class Leaf(Node):
    def __init__(self):
        pass
    

        
    
