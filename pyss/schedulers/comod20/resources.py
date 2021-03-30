import logging

logger = logging.getLogger(__name__.split('.')[-1])

class Resource(object):

  def __init__(self, max, type="nodes"):
    self.max = max
    self.used = 0
    self.type = type

  def release(self, value):
    self.used -= value
    if self.used < 0:
      logger.error("underflow of resource %s", self.type)
      self.used = 0

  def claim(self, value):
    overflow = False
    self.used += value
    if self.used > self.max:
      overflow = True
      logger.error("overflow of resource %s", self.type)
    return not overflow

  def is_enough_available(self, value):
    if self.used + value > self.max:
      return False
    return True

  def get_used(self):
    return self.used

  def get_available(self):
    return self.max - self.used