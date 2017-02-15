# -*- coding: utf-8 -*-
"""
Process grab task.
"""
import logging
import requests
import threading
from Queue import Empty
from bs4 import BeautifulSoup as bs
from url import URL


class Worker(threading.Thread):
    """
    A thread-based worker, for sending requests and processing web page.
    """
    Worker_debug_logger = None
    Worker_sys_logger = None
    Worker_error_logger = None
    Worker_terminating = False
    Worker_taskpool = None
    Worker_basedomain = None
    Worker_errorpage = None

    @classmethod
    def setWorkerConfig(cls, taskpool, basedomain=None, errorpage=None, rootlogger=None):
        """
        Set Worker class attributes.
        :param taskpool: The Taskpool instance from which workers get task.
        :param basedomain: URLs not under basedomain will not be processed.
        :param errorpage: If we are directed to a url in errorpage, we assume an error occurred.
        :param rootlogger: A logger provided to logging info.
        :return: None
        """
        cls.Worker_taskpool = taskpool
        cls.Worker_basedomain = basedomain if basedomain else tuple()
        cls.Worker_errorpage = errorpage if errorpage else tuple()
        # Add a NullHandler to each logger.
        # By default, a Streamhandler will be attached to the logger
        # if no handler attached, which may not be desired.
        root_lg = rootlogger if rootlogger else logging.getLogger("Worker")
        cls.Worker_debug_logger = root_lg.getChild("Worker.Debug")
        cls.Worker_debug_logger.setLevel(logging.DEBUG)
        cls.Worker_debug_logger.addHandler(logging.NullHandler())

        cls.Worker_sys_logger = root_lg.getChild("Worker.sys")
        cls.Worker_sys_logger.setLevel(logging.INFO)
        cls.Worker_sys_logger.addHandler(logging.NullHandler())

        cls.Worker_error_logger = root_lg.getChild("Worker.Warning")
        cls.Worker_error_logger.setLevel(logging.WARNING)
        cls.Worker_error_logger.addHandler(logging.NullHandler())

    @classmethod
    def add_debuglog_handler(cls, handler):
        hdlr = handler
        hdlr.setLevel(logging.DEBUG)
        if cls.Worker_debug_logger:
            cls.Worker_debug_logger.addHandler(hdlr)
        else:
            raise AttributeError("Logger Worker_debug_logger unset.")

    @classmethod
    def add_syslog_handler(cls, handler):
        hdlr = handler
        hdlr.setLevel(logging.DEBUG)
        if cls.Worker_sys_logger:
            cls.Worker_sys_logger.addHandler(hdlr)
        else:
            raise AttributeError("Logger Worker_sys_logger unset.")

    @classmethod
    def add_errorlog_handler(cls, handler):
        hdlr = handler
        hdlr.setLevel(logging.DEBUG)
        if cls.Worker_error_logger:
            cls.Worker_error_logger.addHandler(hdlr)
        else:
            raise AttributeError("Logger Worker_error_logger unset.")

    @classmethod
    def terminate(cls):
        """
        To terminate worker instances properly.
        :return: None
        """
        cls.Worker_terminating = True

    @classmethod
    def revive(cls):
        cls.Worker_terminating = False

    def __init__(self):
        # Enable HTTP keep-alive feature
        self.__session = requests.session()
        self.task = None
        self.__basedomain = Worker.Worker_basedomain
        self.__taskpool = Worker.Worker_taskpool
        self.__error_page = Worker.Worker_errorpage
        self.__soup = None
        self.__response = None
        super(Worker, self).__init__(name=id(self))

    def __request(self):
        """
        Send request to target.
        :return: None
        """
        self.__response = None
        try:
            self.__response = self.__session.get(self.task)
        except requests.exceptions.HTTPError as e:
            # If the remote server return a response with 4xx or 5xx code,
            # requests will raise a HTTPError
                self.__response = e.response

    def __process(self):
        """
        Collect urls from the web page.
        """
        task_url = URL(self.task)
        self.__soup = bs(self.__response.content, "html.parser")
        # Get all links from current page.Remove duplicated links.
        url_set = set(item.get("href")
                      for item in self.__soup.find_all(lambda tag: tag.get("href") and "javascript" not in tag.get("href")))
        target = set()
        # Construct  new target links
        for item in url_set:
            u = URL(item)
            if u.netloc and u.netloc not in self.__basedomain:
                continue
            else:
                u.standardize(task_url.url)
                std_url = u.url
                target.add(std_url)
        return target

    def run(self):
        msg = "Worker start."
        Worker.Worker_sys_logger.info(msg)
        Worker.Worker_debug_logger.info(msg)
        # Capture all unhandled exceptions,
        # log them, and quit.
        try:
            while 1:
                try:
                    # While Worker_terminating is set to True,
                    # all workers will stop getting new target link and quit.
                    if not Worker.Worker_terminating:
                        self.task = self.__taskpool.get(timeout=10)
                        Worker.Worker_debug_logger.debug(self.task)
                    else:
                        break
                except Empty:
                    # If the thread is idle for 10 minutes,
                    # we exist.
                    msg = "Empty task queue. The worker is going to quit."
                    Worker.Worker_sys_logger.info(msg)
                    Worker.Worker_debug_logger.debug(msg)
                    break
                else:
                    try:
                        self.__request()
                    except requests.exceptions.RequestException as e:
                        msg = "{url} - {error}".format(url=self.task.encode("utf-8"), error=e)
                        Worker.Worker_error_logger.warning(msg)
                        Worker.Worker_debug_logger.warning(msg)
                        continue
                    except Exception as e:
                        # In case of unexpected errors.
                        msg = "{url} - An unexpected error occurred: {err}".format(url=self.task.encode("utf-8"), err=e)
                        Worker.Worker_sys_logger.exception(msg)
                        Worker.Worker_debug_logger.exception(msg)
                    else:
                        # If we are redirected to a domain that not in basedomain ,
                        # ignore it.
                        if self.__basedomain and URL(self.__response.url).netloc not in self.__basedomain:
                            continue
                        else:
                            # Deal with http errors
                            if 400 <= self.__response.status_code <= 599:
                                msg = "{url} - {status}".format(url=self.task.encode("utf-8"),
                                                                    status=self.__response.status_code)
                                Worker.Worker_error_logger.warning(msg)
                                Worker.Worker_debug_logger.warning(msg)
                                continue
                            else:
                                # Deal with redirected errors
                                if self.__response.url in self.__error_page:
                                    msg = "{url} - Be redirected to error page: {error_page}".format(
                                        url=self.task.encode("utf-8"),
                                        error_page=self.__response.url)
                                    Worker.Worker_error_logger.warning(msg)
                                    Worker.Worker_debug_logger.warning(msg)
                                    continue
                                else:
                                    urls = self.__process()
                                    self.__taskpool.put(urls)
        except Exception as e:
            msg = "unhandled error: {err}".format(err=e)
            Worker.Worker_sys_logger.exception(msg)
            Worker.Worker_debug_logger.exception(msg)
        finally:
            msg = "Worker {name} quit.".format(name=self.name)
            Worker.Worker_sys_logger.info(msg)
            Worker.Worker_debug_logger.info(msg)
