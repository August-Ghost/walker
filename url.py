# -*- coding:utf-8 -*-
import urlparse


class URL(object):

    def __init__(self,url, scheme="http"):
        self.scheme, self.netloc, self.path, self.params, self.query, self.fragment = urlparse.urlparse(url)
        self.scheme = scheme if self.netloc and not self.scheme else self.scheme


    def __repr__(self):
        parsed_url = (self.scheme, self.netloc, self.path, self.params, self.query, self.fragment)
        return urlparse.urlunparse(parsed_url)

    @property
    def baseurl(self):
        return urlparse.urlunparse((self.scheme, self.netloc, self.path))

    @property
    def domain(self):
        return self.netloc

    @property
    def url(self):
        return self.__repr__()

    # scheme,netloc,path,params,query,and fragment form a standardized url
    def standardize(self, base):
        if self.scheme and self.netloc:
            pass
        else:
            self.scheme, self.netloc, self.path, _, _, _ = urlparse.urlparse(
                urlparse.urljoin(base, self.url)
            )


    def isunder(self, netloc):
        """
        :param netloc: the domain to check
        :return: True if the url is under the domain
        """
        return self.netloc == netloc
