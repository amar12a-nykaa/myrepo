from collections import OrderedDict


class OfferUtils:

	@staticmethod
	def merge_offer_data(doc, offers_data):
		nykaa_offers = offers_data.get('nykaa', [])
		doc['offers'] = nykaa_offers
		doc['offer_count'] = len(doc['offers'])
		doc['offer_ids'] = []
		doc['offer_facet'] = []
		for offer in nykaa_offers:
			doc['key'] = []
			doc['key'].append(offer)
			offer_facet = OrderedDict()
			offer_facet['id'] = offer.get("id")
			offer_facet['name'] = offer.get("name")
			doc['offer_facet'].append(offer_facet)
			doc['offer_ids'].append(offer.get("id"))

		nykaaman_offers = offers_data.get('nykaaman', [])
		doc['nykaaman_offers'] = nykaaman_offers
		doc['nykaaman_offer_count'] = len(doc['nykaaman_offers'])
		doc['nykaaman_offer_ids'] = []
		doc['nykaaman_offer_facet'] = []
		for offer in nykaaman_offers:
			nykaaman_offer_facet = OrderedDict()
			nykaaman_offer_facet['id'] = offer['id']
			nykaaman_offer_facet['name'] = offer['name']
			doc['nykaaman_offer_facet'].append(nykaaman_offer_facet)
			doc['nykaaman_offer_ids'].append(offer['id'])

		nykaa_pro_offers = offers_data.get('nykaa_pro', [])
		doc['nykaa_pro_offers'] = nykaa_pro_offers
		doc['nykaa_pro_offer_count'] = len(doc['nykaa_pro_offers'])
		doc['nykaa_pro_offer_ids'] = []
		doc['nykaa_pro_offer_facet'] = []
		for offer in nykaa_pro_offers:
			nykaa_pro_offer_facet = OrderedDict()
			nykaa_pro_offer_facet['id'] = offer['id']
			nykaa_pro_offer_facet['name'] = offer['name']
			doc['nykaa_pro_offer_facet'].append(nykaa_pro_offer_facet)
			doc['nykaa_pro_offer_ids'].append(offer['id'])
		return doc