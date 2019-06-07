class Metrics(object):
    def __init__(self):
        self.metrics = {}

    def inc(self, name):
        if name not in self.metrics:
            self.metrics[name] = 1
        else:
            self.metrics[name] += 1
