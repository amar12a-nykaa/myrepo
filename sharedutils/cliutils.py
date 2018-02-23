""" CLI are such powerful tools. This module provides utility routines so that you can create CLIs with least effort and more consistency. """
import argparse
import csv
import datetime
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, timedelta

import arrow
from IPython import embed


class CliUtils(object):

  @staticmethod
  def normalize_stores(teststores):
    """validates all the stores. Raises and exception is anything is wrong. Converts incomplete name of stores to their full names."""
    if not teststores:
      teststores = STORES
    elif isinstance(teststores, str):
      teststores = teststores.split(",")
    stores = []
    err = False
    for teststore in teststores:
      matched_stores = []
      for store in STORES:
        if re.search("^" + teststore, store):
          matched_stores.append(store)
      if not matched_stores:
        print("ERROR in CLI options. Unknown store %s" % teststore)
        err = True
      elif len(matched_stores) > 1:
        print("ERROR in CLI options. Ambiguous store %s. It matches with %s" % (
            teststore, ", ".join(matched_stores)))
        err = True
      else:
        stores.append(matched_stores[0])
    if err:
      sys.exit()
    return sorted(stores)

  @staticmethod
  def normalize_query(query):
    if isinstance(query, dict):
      return query
    assert isinstance(query, str), 'query string or dict was expected. Received ' + str(type(query))
    try:
      query = eval(query)
      #print(query)
      assert isinstance(query, dict)
    except Exception as e:
      print(e)
      print("[ERROR] query is not a valid python dictionary object.")
      sys.exit()
    return query

  @staticmethod
  def available_prefix_with_terminator_action(available_choices, terminator_character):
    class AvailableChoicesWithPrefixAction(argparse.Action):
      def __call__(self, parser, args, values, option_string=None):
        ret = []
        inputval = values
        matched_choices = []
        for choice in available_choices:
          if re.search("^" + inputval.split(terminator_character)[0], choice):
            matched_choices.append(choice)
        if len(matched_choices) == 0:
          parser.error("ERROR in CLI options. Unknown choice %s" % inputval)
        elif len(matched_choices) > 1:
          parser.error("ERROR in CLI options. Ambiguous choice %s. It matches with %s" % (inputval, ",".join(matched_choices)))
        else:
          ret = matched_choices[0] + terminator_character + terminator_character.join(inputval.split(terminator_character)[1:])
        setattr(args, self.dest, ret)
    return AvailableChoicesWithPrefixAction
        
  @staticmethod
  def available_choices_action(available_choices):
    class AvailableChoicesShortnerAction(argparse.Action):
      def __call__(self, parser, args, values, option_string=None):
        ret = []
        for inputval in values.split(","):
          matched_choices = []
          for choice in available_choices:
            if re.search("^" + inputval, choice):
              matched_choices.append(choice)
          if len(matched_choices) == 0:
            parser.error("ERROR in CLI options. Unknown choice %s" % inputval)
          elif len(matched_choices) > 1:
            parser.error("ERROR in CLI options. Ambiguous choice %s. It matches with %s" % (inputval, ",".join(matched_choices)))
          else:
            ret.append(matched_choices[0])
        ret.sort()
        setattr(args, self.dest, ret)
    return AvailableChoicesShortnerAction

  
  @staticmethod
  def add_standard_option(parser, standard_option, *args, **kwargs):
    assert 'action' not in kwargs, "action cannot be passed for standard options."
    if standard_option == 'store':
      parser.add_argument("--store", action=CliUtils.available_choices_action(STORES), default=STORES, *args, **kwargs)
    elif standard_option == 'query':
      parser.add_argument("--query", action=CliUtils.MongoQueryAction, *args, **kwargs)
    elif standard_option == 'id':
      parser.add_argument("--id", action=CliUtils.available_prefix_with_terminator_action(STORES, terminator_character=":"), *args, **kwargs)
    else:
      raise Exception("Unsupported standard option: %s" % standard_option)

  @staticmethod
  def valid_date(s):
    try:
      if re.search("^-?[0-9]+$", s):
        adddays = int(s)
        now = arrow.utcnow()
        return now.replace(days=adddays).format('YYYY-MM-DD')
      else:
        return arrow.get(s, 'YYYY-MM-DD').format('YYYY-MM-DD')
    except ValueError:
      msg = "Not a valid date: '{0}'.".format(s)
      raise argparse.ArgumentTypeError(msg)


  class MongoQueryAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
      assert isinstance(values, str), 'query string was expected. Received ' + str(type(query))
      try:
        query = eval(values)
        assert isinstance(query, dict)
      except Exception as e:
        print(e)
        parser.error("[ERROR] query is not a valid python dictionary object.")
      setattr(args, self.dest, query)

if __name__ == "__main__":
  #print((CliUtils.normalize_stores(['flip', 'ama', 'shop'])))
  #print(CliUtils.normalize_query('{"a":"b", "x": True, "y":True}'))

  # Demonstration of how to use cliutils 
  import argparse
  parser = argparse.ArgumentParser()
  #parser.add_argument("--store")
  CliUtils.add_standard_option(parser, 'store')
  CliUtils.add_standard_option(parser, 'query')
  CliUtils.add_standard_option(parser, 'id')

  print("Hardcoded example:")
  print(parser.parse_args('--store fl --query {"a":1}'.split()))
  argv = vars(parser.parse_args())
  print("=== === ")
  print("Parsed Arguments: %s" % argv)
