import sys

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

correct_term_list = ["everyuth","kerastase","farsali","krylon","armaf","Cosrx","focallure","ennscloset","studiowest","odonil","gucci","kryolan",
                     "sephora","foreo","footwear","rhoda","Fenty","Hilary","spoolie","jovees","devacurl","biore","quirky","stay","parampara","dermadew",
                     "kokoglam","embryolisse","tigi","mediker","dermacol","Anastasia","essie","sale","bajaj","burberry","sesa","sigma","spencer","puna",
                     "modicare","hugo","gelatin","stila","ordinary","spawake","mederma","mauri","benetint","amaira","meon","tony","renee","boxes","aashka",
                     "prepair","meilin","krea","dress","sivanna","mosturiser","etude","kadhi","laneige","gucci","jaclyn","hilary","anastasia","becca","sigma",
                     "farsali","majirel","satthwa","fenty","vibrator","focallure","krylon","tigi","armaf","cosrx","soumis","studiowest","evion","darmacol","odonil",
                     "comedogenic","suthol","suwasthi","kerastase","nexus","footwear","badescu","rebonding","jeffree","devacurl","odbo","sesderma","tilbury","dildo",
                     "glatt","essie","ethiglo","prada","dermadew","trylo","nycil","cipla","biore","giambattista","luxliss","parampara","dyson","episoft","vcare",
                     "ofra","nizoral","elnett","mediker","photostable","urbangabru","ketomac","popfeel","igora","wishtrend","jefree","hillary","skeyndor","raga"]



def getValue1(query):
    es_query = """{
                 "size":0,
                 "suggest": 
                  { 
                    "text":"%s", 
                    "phrase_suggestion": 
                    { 
                      "phrase": { 
                        "field": "title_brand_category"
                      }
                    }
                  }
              }""" % (query)
    es_result = Utils.makeESRequest(es_query, "livecore")
    if len(es_result['suggest']['phrase_suggestion'][0]['options']) > 0:
        return es_result['suggest']['phrase_suggestion'][0]['options'][0]['text']
    return query


def getValue2(query):
    es_query = """{
                 "size":0,
                 "suggest": 
                  { 
                    "text":"%s", 
                    "term_suggester": 
                    { 
                      "term": { 
                        "field": "title_brand_category",
                        "sort": "score"
                      }
                    }
                  }
              }""" % (query)
    es_result = Utils.makeESRequest(es_query, "livecore")
    if es_result.get('suggest', {}).get('term_suggester'):
        modified_query = query.lower()
        for term_suggestion in es_result['suggest']['term_suggester']:
            if term_suggestion.get('options') and term_suggestion['options'][0]['score'] > 0.7:
                confidence = term_suggestion['options'][0]['score']
                new_term = term_suggestion['options'][0]['text']
                for suggestion in term_suggestion['options'][1:]:
                    if suggestion['score'] == confidence:
                        try:
                            if suggestion['text'].index(term_suggestion['text']) == 0:
                                new_term = suggestion['text']
                                break
                        except:
                            pass
                modified_query = modified_query.replace(term_suggestion['text'], new_term)
        return modified_query
    return query


def getValue3(query):
    es_query = """{
         "size": 1,
         "query": {
             "match": {
               "title_brand_category": "%s"
             }
         }, 
         "suggest": 
          { 
            "text":"%s", 
            "term_suggester": 
            { 
              "term": { 
                "field": "title_brand_category"
              }
            }
          }
    }""" % (query,query)
    es_result = Utils.makeESRequest(es_query, "livecore")
    doc_found = es_result['hits']['hits'][0]['_source']['title_brand_category'] if len(es_result['hits']['hits']) > 0 else ""
    doc_found = doc_found.lower()
    if es_result.get('suggest', {}).get('term_suggester'):
        modified_query = query.lower()
        for term_suggestion in es_result['suggest']['term_suggester']:
            if term_suggestion.get('text') in doc_found:
                continue
            if term_suggestion.get('text') in correct_term_list:
                continue
            if term_suggestion.get('options') and term_suggestion['options'][0]['score'] > 0.7:
                frequency = term_suggestion['options'][0]['freq']
                new_term = term_suggestion['options'][0]['text']
                for suggestion in term_suggestion['options'][1:]:
                    if suggestion['freq'] > frequency and suggestion['score'] > 0.7:
                        frequency = suggestion['freq']
                        new_term = suggestion['text']
                modified_query = modified_query.replace(term_suggestion['text'], new_term)
        return modified_query
    return query


def getValue4(query):
    es_query = """{
        "size":0,
        "suggest": 
         { 
           "text":"%s", 
           "phrase_suggestion": 
           { 
             "phrase": { 
               "field": "entity",
               "direct_generator" : [{
                 "field" : "entity",
                 "suggest_mode" : "always"
               }]
             }
           }
         }
    }""" % (query)
    es_result = Utils.makeESRequest(es_query, "autocomplete")
    if len(es_result['suggest']['phrase_suggestion'][0]['options']) > 0:
        return es_result['suggest']['phrase_suggestion'][0]['options'][0]['text']
    return query


def chk():
    # result = defaultdict(lambda : defaultdict(str))
    queryDict = {"aelovera gel":"aloevera gel",
"aleo vera gel":"aloe vera gel",
"avacado oil":"Avocado oil",
"barun":"braun",
"barun product":"braun product",
"beuty blender":"beauty blender",
"Bindhi":"Bindi",
"biotiqu shampoo":"biotique shampoo",
"blasher":"blusher",
"blue haven":"blue heaven",
"blue haven lipstick":"Blue heaven lipstick",
"blunt shampoo":"Bblunt shampoo",
"bodi shop lip balm":"body shop lip balm",
"bodi shop shower gel":"body shop shower gel",
"bodi shop tea tree":"body shop tea tree",
"boitique":"biotique",
"botique products":"Biotique products",
"boutique face pack":"biotique face pack",
"boutique face wash":"biotique face wash",
"boutique toner":"biotique toner",
"bringraj oil":"bhringraj oil",
"carcoal mask":"Charcoal mask",
"caster oil":"castor oil",
"charcol powder":"charcoal powder",
"citaphil":"cetaphil",
"cleasing milk":"cleansing milk",
"Colourbar":"colorbar",
"colourbar matte lipstick":"Colorbar matte lipstick",
"conceler":"concealer",
"countour":"contour",
"countour stick":"contour stick",
"cryon lipstick":"Crayon lipstick",
"dazzler eyeliner":"dazller eyeliner",
"ell 18 matte lipstick":"elle 18 matte lipstick",
"esencial oil":"essential oil",
"Ethnic Jewelry":"ethnic jewellery",
"eye shadow pallet":"eye shadow pallete",
"eye shadow pallete":"eye shadow palette",
"eye shadow pallette":"eyeshadow palette",
"eye shadow platte":"eye shadow palette",
"eyeshadow palett":"eyeshadow palette",
"eyeshadow pallete":"eyeshadow palette",
"face mosturiser":"Face moisturiser",
"face shop marksheets":"face shop masksheets",
"face sirum":"face serum",
"face srub":"face scrub",
"faces lip cryon":"faces lip crayon",
"faundation":"foundation",
"fecial kit":"Facial kit",
"ganier":"Garnier",
"garnier miscellar water":"garnier micellar water",
"gillter eyeshadow":"Glitter eyeshadow",
"hair drier":"hair dryer",
"hair staightner":"hair staightener",
"hair staightning brush":"Hair straightening brush",
"hair syrum":"hair serum",
"hilighter":"highlighter",
"Himalya":"himalaya",
"himalyan herbal protien hair cream":"himalayan herbal protein hair cream",
"inisfree":"innisfree",
"inisfree mask":"Innisfree mask",
"inisfree sheet mask":"Innisfree sheet mask",
"jhonson baby":"johnson baby",
"jiva":"jeva",
"jucy chemistry":"Juicy chemistry",
"jwellery":"Jewellery",
"kajol":"kajal",
"keihls":"kiehls",
"khaadi":"khadi",
"khadi sampoo":"Khadi shampoo",
"kilo milano":"Kiko milano",
"lackme lipstick":"lakme lipstick",
"lakeme foundation":"lakme foundation",
"lakeme rose powder":"lakme rose powder",
"lakhme lip love":"lakme lip love",
"lakm 9 5 matt lip color":"lakme 9 5 matt lip color",
"lakm enrich matt lipstick":"lakme enrich matt lipstick",
"lakme cryon":"Lakme crayon",
"lakme enrich lip cryon":"lakme enrich lip crayon",
"lakme lip caryon":"lakme lip crayon",
"lakme mouse":"lakme mousse",
"lame lipstick":"lakme lipstick",
"layer wattagirl":"layer wottagirl",
"lekme":"lakme",
"lekme foundation":"Lakme foundation",
"lekme lipstick":"lakme lipstick",
"levon serum":"livon serum",
"Lip blam":"lip balm",
"lip cryon":"lip crayon",
"lipstic":"lipstick",
"lipstick pallete":"lipstick palette",
"lipstick platte":"lipstick palette",
"lipstics":"lipsticks",
"liquid highliter":"liquid highlighter",
"loeral":"Loreal",
"lofa":"loofah",
"lofah":"Loofah",
"long lasting lipstic":"long lasting lipstick",
"long lasting matty lipstick":"Long lasting matte lipstick",
"loreal infalliable foundation":"loreal infallible foundation",
"loreal sampoo":"loreal shampoo",
"lose powder":"loose powder",
"lotu sunscreen":"lotus sunscreen",
"louts":"lotus",
"lufa":"loofah",
"mabelline":"maybelline",
"mabelline bb cream":"maybelline bb cream",
"mabelline compact":"maybelline compact",
"mabelline concealer":"maybelline concealer",
"mabelline eyeliner":"Maybelline eyeliner",
"mabelline foundation":"maybelline foundation",
"mabelline kajal":"maybelline kajal",
"mabelline lip balm":"Maybelline lip balm",
"mabelline lipstick":"maybelline lipstick",
"mac highliter":"mac highlighter",
"mac patrickstarr":"mac patrickstarrr",
"mac rubi woo":"mac ruby woo",
"marksheet":"masksheet",
"maskara":"mascara",
"maybeline fit me concealer":"maybelline fit me concealer",
"maybellin eyeshadow":"maybelline eyeshadow",
"maybellin mascara":"maybelline mascara",
"maybelline highliter":"maybelline highlighter",
"maybelline new york colour sensional lip stick":"maybelline new york colour sensational lip stick",
"menicure pedicure kit":"Manicure pedicure kit",
"menstural cup":"Menstrual cup",
"miscellar water":"micellar water",
"miss clair":"miss claire",
"miss clair soft matt lip cream":"miss claire soft matt lip cream",
"mosturiser":"moisturiser",
"mosturizer":"moisturizer",
"mybelline fit me foundation":"maybelline fit me foundation",
"naykaa":"nykaa",
"naykaa lipstick":"nykaa lipstick",
"naykaa liquid lipstick":"Nykaa liquid lipstick",
"naykaa matte lipstick":"Nykaa matte lipstick",
"naykaa nail polish":"nykaa nail polish",
"Neil polish":"Nail polish",
"neutrogena oil free mositureser":"neutrogena oil free moisturiser",
"nutrogena":"neutrogena",
"nutrogena face wash":"neutrogena face wash",
"nutrogena sunscreen":"neutrogena sunscreen",
"nyaka nailpolish":"nykaa nailpolish",
"nyka kajal":"nykaa kajal",
"nyka lipsticks":"nykaa lipsticks",
"nyka shower gel":"nykaa shower gel",
"Nyx seude metallic":"nyx suede metallic",
"pac concealor brush":"pac concealer brush",
"pamolive body wash":"palmolive body wash",
"panteen oil replacement":"pantene oil replacement",
"plam":"Plum",
"praimar":"primer",
"purfume":"perfume",
"Quraa":"Qraa",
"sampoo":"shampoo",
"sarees":"Saree",
"seba med":"sebamed",
"shehnaz hussain":"Shahnaz husain",
"sheseido":"Shiseido",
"shwarzkopf shampoo":"Schwarzkopf shampoo",
"suger smudge me not liqued lipstick":"sugar smudge me not liquid lipstick",
"suger smudge me not liqued lipstick":"sugar smudge me not liquid lipstick",
"sunslik shampoo":"sunsilk shampoo",
"swarzkopf":"Schwarzkopf",
"tijori":"Tjori",
"tonner":"toner",
"tresseme":"tresemme",
"vadi":"vaadi",
"vadi herbals":"vaadi herbals",
"vadi lip balm":"vaadi lip balm",
"vadi soap":"Vaadi soap",
"vaselin bodi lotion":"vaseline body lotion",
"vesline":"vaseline",
"wattagirl":"wottagirl",
"wishper":"wisper",
"wishper":"whisper",
"wisper":"whisper",
"yadley":"yardley"}
    for key, value in queryDict.items():
        print("%s:::%s:::%s:::%s:::%s:::%s" % (
        key, value, getValue1(key), getValue2(key), getValue3(key), getValue4(key)))



def chkPositive():
    es_query = """{
        "query": {
            "bool": {
              "must_not": {
                "term" : {
                  "type" : "search_query"
                }
              }
            }
        },
        "_source" : ["entity", "type"],
        "size" : 10000
        }"""
    es_result = Utils.makeESRequest(es_query, "autocomplete")
    for row in es_result['hits']['hits']:
        try:
            entity = row['_source']['entity']
            if entity[-2:] == " s":
                entity = entity[:-2]
            result = getValue3(entity)
            if entity.lower() != result.lower():
                print(entity.lower(), result.lower(), row['_source']['type'])
        except Exception as e:
            print(e)



if __name__ == '__main__':
    #chk()
    # chkPositive()
    s = getValue3("blue haven")
    print(s)