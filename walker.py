# -*- coding: utf-8 -*-
import time
import signal
import logging
import parameter
import cPickle
from Queue import Empty
from  threading import active_count, current_thread, enumerate
from taskpool import TaskPool
from worker import Worker
from docopt import docopt, DocoptExit
from sys import exit


NOW = str(time.time())[:-3]
LOGGER = logging.getLogger("Walker")
CONTEXT_DUMP = None
ARGS = None


def getHandler(hdlr_cls, lvl=logging.DEBUG, format=None, *args, **kargs):
    formatter = logging.Formatter("%(asctime)s - %(thread)s - %(message)s" if not format\
        else format)
    file_hdlr = hdlr_cls(*args, **kargs)
    file_hdlr.setFormatter(formatter)
    if lvl:
        file_hdlr.setLevel(lvl)
    return file_hdlr


def sys_freeze(sig, frm):
    Worker.terminate()
    while active_count() > 1:
        time.sleep(0.01)
    if CONTEXT_DUMP:
        with open(CONTEXT_DUMP, "wb") as dmp_file:
            cPickle.dump(ARGS, dmp_file, protocol=2)
            Worker.Worker_taskpool.freeze()
            cPickle.dump(Worker.Worker_taskpool, dmp_file, protocol=2)
    exit(0)


def sys_recover(dmpfile):
    global ARGS
    with open(dmpfile, "rb") as dmp_file:
        arguments = cPickle.load(dmp_file)
        ARGS = arguments
        task_pool = cPickle.load(dmp_file)
        task_pool.resume()
    sys_init(arguments, taskpool=task_pool)
    sys_start(arguments, resume=True)


def sys_init(args, taskpool=None):
    global CONTEXT_DUMP
    Worker.setWorkerConfig(rootlogger=LOGGER,
                           basedomain=args["--basedomain"] if args["--basedomain"]  else None,
                           errorpage=args["--errorpage"] if args["--errorpage"]  else None,
                           taskpool=TaskPool(distinct_filter_dump="filter_dump_{now}".format(now=NOW)) if not taskpool else taskpool)

    if args["--debug"]:
        map(Worker.add_debuglog_handler,
            (getHandler(logging.FileHandler, filename=dbg_log_file) for dbg_log_file in args["--dbglog"]))
        Worker.add_debuglog_handler(getHandler(logging.StreamHandler))

    if args["--syslog"]:
        map(Worker.add_syslog_handler,
            (getHandler(hdlr_cls=logging.FileHandler, filename=sys_log_file) for sys_log_file in args["--syslog"]))
    elif not args["--syslog"] and not args["--debug"]:
        Worker.add_syslog_handler(getHandler(hdlr_cls=logging.StreamHandler))

    if args["--errlog"]:
        map(Worker.add_errorlog_handler,
            (getHandler(hdlr_cls=logging.FileHandler, filename=err_log_file) for err_log_file in args["--errlog"]))
    elif not args["--errlog"] and not args["--debug"]:
        Worker.add_errorlog_handler(getHandler(hdlr_cls=logging.StreamHandler))

    if args["--continue"]:
        CONTEXT_DUMP = args["--continue"][0]


def sys_start(args, resume=False):
    if not resume:
        Worker.Worker_taskpool.put(args["<src>"])
    num_of_threads = args["--num-of-threads"][0]
    workers = {w.name: w for w in (Worker() for i in range(0, int(num_of_threads)))}

    signal.signal(signal.SIGINT, sys_freeze)
    signal.signal(signal.SIGTERM, sys_freeze)
    signal.signal(signal.SIGBREAK, sys_freeze)

    map(lambda w: w.start(), workers.itervalues())
    # We have to keep main thread awake ot process signal, but will not let it quit.
    while 1:
        map(lambda w: w.join(1024), workers.itervalues())





if __name__ == "__main__":
    arguments = docopt(parameter.__doc__)
    ARGS = arguments
    if arguments["--recover"]:
        sys_recover(arguments["--recover"].pop())
    else:
        if not arguments["<src>"]:
            raise DocoptExit()
        else:
            sys_init(arguments)
            sys_start(arguments)
