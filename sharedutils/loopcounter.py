import collections
import datetime
import sys
import time 

__all__ = ["LoopCounter", "LoopCounterGroup"]
SPEED = '{SPEED}'
TIME = '{TIME}'
COUNT = '{COUNT}'
NAME = '{NAME}'


class LoopCounter(object):

  def __init__(self, name=None, start=None, total=None):
    self.name = name if name else ""
    self.count = start if start else 0
    self.startts = datetime.datetime.now()
    self.total = total if total else None
    self.last_print_time = None

  def __str__(self):
    str = ""
    if self.name:
      str += "{name}: ".format(name=self.name)
    str += "%s" % self.count
    return str

  @property
  def time(self):
    diff = datetime.datetime.now() - self.startts
    return diff.total_seconds()

  @property
  def speed(self):
    return round(self.count * 1.0 / self.time, 2)

  @property
  def percent_completed(self):
    return round(self.count * 1.0 / self.total * 100, 2)

  def __iadd__(self, other):
    self.count += other
    return self

  def formatted(self, formatstr, minimum=None, maximum=None):
    ret = formatstr
    if SPEED in formatstr:
      ret = ret.replace(SPEED, str(self.speed))
    if COUNT in formatstr:
      ret = ret.replace(COUNT, str(self.count))
    if NAME in formatstr:
      ret = ret.replace(NAME, str(self.name))
    if TIME in formatstr:
      ret = ret.replace(TIME, str(self.time))
    return ret

  @property
  def summary(self):
    formatstr = "{NAME}: " if self.name else ""
    if self.total:
      formatstr += "%2.0f" % self.percent_completed + "% complete. "
    formatstr += "Processed {COUNT} items @ {SPEED} items/sec in {TIME} seconds"
    return self.formatted(formatstr)

  def should_print(self, threshold=100, low_interval=10, high_interval=100):
    ret = False
    if not self.last_print_time:
      self.last_print_time = time.time()

    delta = time.time() - self.last_print_time

    if self.count > high_interval and delta < 1:
      ret = False
    else:
      ret = (self.count % low_interval == 0 and self.count <= threshold) or (self.count % threshold == 0 and self.count > high_interval)
  
    if ret:
      self.last_print_time = time.time()
    return ret

class LoopCounterGroup(object):

  def __init__(self):
    self.ctrs = {}

  def _factory(self, name):
    if name in self.ctrs:
      return self.ctrs['name']
    else:
      self.ctrs['name'] = LoopCounter(name)
      return self.ctrs['name']

  def __getitem__(self, attr):
    return self._factory(attr)


if __name__ == '__main__':
  TOTAL = 1000
  ctr = LoopCounter(name='myctr1', total=TOTAL)

  for i in range(0, TOTAL):
    ctr += 1
    #sleep(random.randint(1,3)*.01)
    if ctr.should_print():
      print(ctr.summary)

  ctrs = LoopCounterGroup()
  x = ctrs['x']
  x += 1
  print(x.summary)
