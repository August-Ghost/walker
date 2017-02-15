# walker
Walker is a tiny script for web page availability check.

Features:
--------------
* Multi-thread based.
* Ability to resume from break point.

Usage:
--------------
    worker.py [-h | --help]
    worker.py [-r | --recover <dumpfile>]
    worker.py [<src>... [-d | --debug][-b | --basedomain <bsdomain>...][-e | --errorpage <errpage>...][-c | --continue <dumpfile>][-n | --num-of-threads <num>][--dbglog <dbglog>...][--syslog <syslog>...][--errlog <errlog>...]]

Option:
--------------
    -h --help    Show this.
    -r --recover <dumpfile>    Recover. If set, walker will recover previous status from the specified file.
    <src>...    Source url, could be multi-provided.
    -d --debug    Debug flag. Set if debug info is desired. Default to False.
    -b --basedomain <bsdomain>...   Base domain. Links not under the base domain will not be processed.
    -e --errorpage <errpage>...    Error page. Walker will assume a error occurred and log it if directed to a url in error page.
    -c --continue <dumpfile>    Continue. If set, walker will dump current status to the specified file if terminated.
    -n --num-of-threads <num>    Number of threads. Set the number of workers. [default: 4]
    --dbglog <dbglog>...    Debug log. Debug info will be log to this file, otherwise stdout.
    --syslog <syslog>...    System log. System info will be log to this file, otherwise stdout.
    --errlog <errlog>...    Error log. Error info will be log to this file, otherwise stdout.

Example:
--------------
    walker.py -h
    walker.py -r worker.dump
    walker.py https://m.sohu.com -d -b m.sohu.com -e https://m.sohu.com/404_2.html -c worker.dump -n 8 --syslog syslog.log --errlog errlog.log --dbglog dbglog.log