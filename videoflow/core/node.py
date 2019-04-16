class Node:
    def __init__(self):
        self._parents = None
        self._children = set()
    
    def __repr__(self):
        return self.__class__.__name__
    
    def __eq__(self, other):
        return self is other
    
    def __hash__(self):
        return id(self)
    
    def __call__(self, *parents):
        self._parents = set()
        for parent in parents:
            assert isinstance(parent, Node) and not isinstance(parent, Leaf),
                    '%s is not a non-leaf node' % str(parent)
            self._parents.add(parent)
            parent.add_child(self)
        
    def add_child(self, child):
        self._children.add(child)

    @property
    def parents(self):
        return self._parents
    
    @property
    def children(self):
        return self._children

class Leaf(Node):
    def __init__(self):
        super(Leaf, self).__init__()
    

        
    
