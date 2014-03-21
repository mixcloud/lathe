import time


class Throttle(object):
    """
    Throttle class to prevent an action being performed too often

    If the minimum_intervale has not passed this class blocks until
    it has.
    """
    def __init__(self, minimum_interval, time=time):
        self.minimum_interval = minimum_interval
        self.last_throttle = None
        self.time = time

    def wait(self):
        now = self.time.clock()
        if self.last_throttle:
            interval = now - self.last_throttle
            if interval < self.minimum_interval:
                self.time.sleep(self.minimum_interval - interval)
        self.last_throttle = now
