class Variable:
    def __init__(self, name, value, onSite=None) -> None:
        self.name = name
        self.value = value
        self.lastCommittedTime = -1
        # if this variable is not a copy, onSite will be None; 
        # otherwise, onSite will be the site number where this copy is on 
        self.onSite = onSite