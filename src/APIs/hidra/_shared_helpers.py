import traceback


class LoggingFunction:
    def out(self, x, exc_info=None):
        if exc_info:
            print (x, traceback.format_exc())
        else:
            print (x)

    def no_out(self, x, exc_info=None):
        pass

    def __init__(self, level="debug"):
        if level == "debug":
            # using output
            self.debug = lambda x, exc_info=None: self.out(x, exc_info)
            self.info = lambda x, exc_info=None: self.out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.out(x, exc_info)
            self.error = lambda x, exc_info=None: self.out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level == "info":
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            # using output
            self.info = lambda x, exc_info=None: self.out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.out(x, exc_info)
            self.error = lambda x, exc_info=None: self.out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level == "warning":
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.info = lambda x, exc_info=None: self.no_out(x, exc_info)
            # using output
            self.warning = lambda x, exc_info=None: self.out(x, exc_info)
            self.error = lambda x, exc_info=None: self.out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level == "error":
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.info = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.no_out(x, exc_info)
            # using output
            self.error = lambda x, exc_info=None: self.out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level == "critical":
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.info = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.error = lambda x, exc_info=None: self.no_out(x, exc_info)
            # using output
            self.critical = lambda x, exc_info=None: self.out(x, exc_info)
        elif level is None:
            # using no output
            self.debug = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.info = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.warning = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.error = lambda x, exc_info=None: self.no_out(x, exc_info)
            self.critical = lambda x, exc_info=None: self.no_out(x, exc_info)
