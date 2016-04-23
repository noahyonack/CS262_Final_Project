#class to test if cloudpickle can handle externally defined classes
class testClass():
    def __init__(self, value):
        self.value = value

    def add(self):
        return(self.value + 1)