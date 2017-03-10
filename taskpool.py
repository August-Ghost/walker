# -*- coding: utf-8 -*-
"""
For task dispatch, thread safe.
"""

import Queue
try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading

import cPickle


# Almost 130k elements for each slice.
# Enlarge it if necessary.
MAX_FILTER_LEN = 131071


class DistinctFilter(object):
    """
    A filter to remove the duplicates.
    """
    def __init__(self, dumpfile="filter_dump"):
        """
        :param dumpfile: name of the file to dump filters.
        """
        self.__current_filter = set()
        self.__dump_file = dumpfile
        self.__if_sliced = False
        self.__lock = _threading.Lock()
        open(dumpfile, "w").close()

    def __save_current_filter(self):
        # Dump self.__current_filter into external storage to control memory usage.
        if len(self.__current_filter) >= MAX_FILTER_LEN:
            with open(self.__dump_file, "ab") as dump_file:
                try:
                    cPickle.dump(self.__current_filter, file=dump_file, protocol=2)
                except Exception as e:
                    raise e
                else:
                    self.__current_filter.clear()
                    self.__if_sliced = True

    def filter(self, src):
        """
        :param src: string or iterable to be filtered.
        :return: filtered src, all duplicated items are removed.
        """
        try:
            self.__lock.acquire()
        except Exception as e:
            raise e
        else:
            unfiltered = {src} if isinstance(src, str) else set(src)
            filtered = unfiltered.difference(self.__current_filter)
            # A little trick to optimise
            d_u = filtered.difference_update
            if self.__if_sliced == True:
                # Check every dumped set.
                with open(self.__dump_file) as dump_file:
                    while True:
                        try:
                            d_u(cPickle.load(dump_file))
                        except EOFError:
                            break
            self.__current_filter.update(filtered)
            self.__save_current_filter()
            return filtered
        finally:
            self.__lock.release()

    def freeze(self):
        # A little trick for prickle support
        self.__lock = None

    def resume(self):
        self.__lock = _threading.Lock()



class Q(Queue.Queue, object):
    """
    A subclass of Queue.Queue which can be prickled.
    """
    def freeze(self):
        # A little trick for prickle support.
        # Hint: An instance with Lock object can not be pickled.
        self.mutex = None
        self.not_empty = None
        self.not_full = None
        self.all_tasks_done = None

    def resume(self):
        self.mutex = _threading.Lock()
        self.not_empty = _threading.Condition(self.mutex)
        self.not_full = _threading.Condition(self.mutex)
        self.all_tasks_done = _threading.Condition(self.mutex)


class TaskPool(object):
    def __init__(self, distinct_filter_dump=None):
        self.__task_queue = Q()
        self.__distinct_filter = DistinctFilter(dumpfile=distinct_filter_dump)

    def get(self, timeout=10):
        task = self.__task_queue.get(timeout=timeout)
        self.__task_queue.task_done()
        return task

    def put(self, src):
        """
        :param src: an iterable of tasks
        :return: None
        """
        map(self.__task_queue.put, self.__distinct_filter.filter(src=src))

    def freeze(self):
        # A little trick for prickle support.
        # Hint: An instance with Lock object can not be pickled.
        self.__task_queue.freeze()
        self.__distinct_filter.freeze()

    def resume(self):
        self.__task_queue.resume()
        self.__distinct_filter.resume()