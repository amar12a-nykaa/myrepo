#!/usr/bin/python
import time
import timeit
import argparse

from upload import uploadFile
from calculate import calculate_feedback
from index import index_engine


if __name__ == '__main__':
    startts = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    argv = vars(parser.parse_args())
    script_start = timeit.default_timer()
    try:
        print("uploading feedback file")
        uploadFile()

        print("calculating feedback")
        start = timeit.default_timer()
        filename = calculate_feedback(days=argv["days"])
        duration = timeit.default_timer() - start
        print("Time taken for calculation: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(duration)))

        print("indexing feedback")
        start = timeit.default_timer()
        index_engine(inactive=True, swap=True, filename=filename)
        duration = timeit.default_timer() - start
        print("Time taken for index: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(duration)))
    except Exception as ex:
        print(ex)

    script_stop = timeit.default_timer()
    script_duration = script_stop - script_start

    print("\n\nFinished running feedback pipeline.\n\n")
    print("Time taken for script: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
