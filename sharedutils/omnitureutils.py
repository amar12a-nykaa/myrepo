import re
import arrow
import omniture

class OmnitureUtils:
	analytics = None
	suites = {}

	@classmethod 
	def init(cls):
		if not cls.analytics:
			cls.analytics = omniture.authenticate('soumen.seth:FSN E-Commerce', '770f388b78d019017d5e8bd7a63883fb')
			cls.suites['web'] = cls.analytics.suites['fsnecommerceprod']
			cls.suites['mobile'] = cls.analytics.suites['fsnecommercemobileappprod']

	@classmethod 
	def get_suite_web(cls):
		cls.init()
		return cls.suites['web']
 
	@classmethod 
	def get_suite_mobile(cls):
		cls.init()
		return cls.suites['mobile']


	@classmethod 
	def valid_date(cls, s): 
		try: 
			if re.search("^-?[0-9]+$", s): 
				adddays = int(s) 
				assert abs(adddays) < 500, "Reports can be fetched only 500 days in past."  
				now = arrow.utcnow() 
				return now.replace(days=adddays).format('YYYY-MM-DD') 
			else: 
				return arrow.get(s, 'YYYY-MM-DD').format('YYYY-MM-DD') 
				#return datetime.datetime.strptime(s, "%Y-%m-%d") 
		except ValueError: 
			msg = "Not a valid date: '{0}'.".format(s) 
			raise argparse.ArgumentTypeError(msg) 

