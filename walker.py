# -*- coding: utf-8 -*-

import time
import signal
import logging
import parameter
import cPickle
from Queue import Empty
from  threading import active_count
from taskpool import TaskPool
from worker import Worker
from docopt import docopt, DocoptExit
from sys import exit

NOW = str(time.time())[:-3]
LOGGER = logging.getLogger("Walker")


def getHandler(hdlr_cls, lvl=logging.DEBUG, format=None, *args, **kargs):
    formatter = logging.Formatter("%(asctime)s - %(thread)s - %(message)s" if not format \
                                      else format)
    file_hdlr = hdlr_cls(*args, **kargs)
    file_hdlr.setFormatter(formatter)
    if lvl:
        file_hdlr.setLevel(lvl)
    return file_hdlr


class Sheepherder(object):
    @classmethod
    def sys_recover(cls, dmpfile):
        with open(dmpfile, "rb") as dmp_file:
            arguments = cPickle.load(dmp_file)
            task_pool = cPickle.load(dmp_file)
            task_pool.resume()
        return cls(config=arguments, resume=True, taskpool=task_pool)

    def __init__(self, config, resume=False, taskpool=None):
        self.resume = resume
        self.workers = {}
        self.__config = config
        self.__config_dump = config["--continue"][0] if config["--continue"] else None
        self.__worker_prepare(taskpool)
        signal.signal(signal.SIGINT, self.sys_freeze)
        signal.signal(signal.SIGTERM, self.sys_freeze)
        signal.signal(signal.SIGBREAK, self.sys_freeze)

    def __worker_prepare(self, taskpool=None):
        Worker.setWorkerConfig(rootlogger=LOGGER,
                               basedomain=self.__config["--basedomain"] if self.__config["--basedomain"]  else None,
                               errorpage=self.__config["--errorpage"] if self.__config["--errorpage"]  else None,
                               taskpool=TaskPool(distinct_filter_dump="filter_dump_{now}".format(
                                   now=NOW)) if not taskpool else taskpool)

        if not self.resume:
            Worker.Worker_taskpool.put(self.__config["<src>"])

        if self.__config["--debug"]:
            map(Worker.add_debuglog_handler,
                (getHandler(logging.FileHandler, filename=dbg_log_file) for dbg_log_file in self.__config["--dbglog"]))
            Worker.add_debuglog_handler(getHandler(logging.StreamHandler))

        if self.__config["--syslog"]:
            map(Worker.add_syslog_handler,
                (getHandler(hdlr_cls=logging.FileHandler, filename=sys_log_file) for sys_log_file in
                 self.__config["--syslog"]))
        elif not self.__config["--syslog"] and not self.__config["--debug"]:
            Worker.add_syslog_handler(getHandler(hdlr_cls=logging.StreamHandler))

        if self.__config["--errlog"]:
            map(Worker.add_errorlog_handler,
                (getHandler(hdlr_cls=logging.FileHandler, filename=err_log_file) for err_log_file in
                 self.__config["--errlog"]))
        elif not self.__config["--errlog"] and not self.__config["--debug"]:
            Worker.add_errorlog_handler(getHandler(hdlr_cls=logging.StreamHandler))

    def sys_freeze(self):
        Worker.terminate()
        while active_count() > 1:
            time.sleep(0.01)
        if self.__config_dump:
            with open(self.__config_dump, "wb") as dmp_file:
                cPickle.dump(self.__config, dmp_file, protocol=2)
                Worker.Worker_taskpool.freeze()
                cPickle.dump(Worker.Worker_taskpool, dmp_file, protocol=2)
        exit(0)

    def shepherd(self):
        num_of_threads = int(self.__config["--num-of-threads"][0])
        self.workers = {w.name: w for w in (Worker() for i in range(0, num_of_threads))}
        map(lambda w: w.start(), self.workers.itervalues())

        quit_threshold = 2 ** (num_of_threads / 2 - 1) if num_of_threads / 2 else 1
        stage = 1

        # We have to keep main thread awake to process signal, but will not let it quit.
        while stage <= quit_threshold:
            try:
                abortive_worker, err = Worker.Worker_abortive_instance.get(timeout=1)
            except Empty:
                stage = (stage - 2 ) if stage >=0 else 1
                continue
            else:
                stage *= 2
                newborn_worker = Worker()
                del self.workers[abortive_worker.name]
                self.workers[newborn_worker.name] = newborn_worker
                newborn_worker.start()
        self.sys_freeze()


if __name__ == "__main__":
    arguments = docopt(parameter.__doc__)
    if arguments["--recover"]:
        sheepherder = Sheepherder.sys_recover(arguments["--recover"].pop())
    else:
        if not arguments["<src>"]:
            raise DocoptExit()
        else:
            sheepherder = Sheepherder(arguments)
    sheepherder.shepherd()