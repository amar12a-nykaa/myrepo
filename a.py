import omniture
analytics = omniture.authenticate('soumen.seth:FSN E-Commerce', '770f388b78d019017d5e8bd7a63883fb')

print(analytics.suites)
#import IPython; IPython.embed()
suite = analytics.suites['fsnecommerceprod']
print("suite:")
print(suite)
print("suite.metrics: %s" % suite.metrics)
#print("suite.elements: %s" %suite.elements)
#print("suite.segments: %s" % suite.segments)

for x in suite.metrics:
  print(x)


print(suite.metrics['Home Page CTR'])

report = suite.report \
		.element('page') \
		.metric('pageviews') \
		.run()
print(" --- ")
print(report)

print(report.keys())
