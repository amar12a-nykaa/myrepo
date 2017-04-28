import os
import os.path 
import arrow
import omniture
import sys
import IPython; embed = IPython.embed
PRINT_CONSTANTS = False
analytics = omniture.authenticate('soumen.seth:FSN E-Commerce', '770f388b78d019017d5e8bd7a63883fb')
dir_path = os.path.dirname(os.path.realpath(__file__))

suite = analytics.suites['fsnecommerceprod']

if PRINT_CONSTANTS:
  with open(os.path.join(dir_path, "analytics.suites.txt"), 'w') as f:
    f.write("%s" % suite)
  with open(os.path.join(dir_path, "metrics.txt"), 'w') as f:
    f.write("%s" % suite.metrics)
  with open(os.path.join(dir_path, "elements.txt"), 'w') as f:
    f.write("%s" % suite.elements)

  print("Wrote constants in files. Exiting now.")
  sys.exit()



startdate = arrow.now().replace(days=-30).format('YYYY-MM-DD')
enddate = arrow.now().replace(days=-1).format('YYYY-MM-DD')

report = suite.report \
		.element('evar6', top=50, startingWith=0) \
		.metric('instances') \
    .range(startdate, enddate)\
		.run()
		#.metric('orders') \
    #.granularity('day')\
print(" --- ")
print(report)

#print(report.keys())


