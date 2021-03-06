# requires dependency on future
from builtins import super  # pylint: disable=redefined-builtin

# System imports
import logging
import logging.handlers
try:
    from Queue import Queue
except ImportError:
    from queue import Queue

from logutils.queue import QueueHandler, QueueListener


class CustomQueueListener(QueueListener):
    def __init__(self, queue, *handlers):
        super().__init__(queue, *handlers)
        """
        Initialise an instance with the specified queue and
        handlers.
        """
        # Changing this to a list from tuple in the parent class
        self.handlers = list(handlers)

    def handle(self, record):
        """
        Override handle a record.

        This just loops through the handlers offering them the record
        to handle.

        :param record: The record to handle.
        """
        record = self.prepare(record)
        for handler in self.handlers:
            # This check is not in the parent class
            if record.levelno >= handler.level:
                handler.handle(record)

    def addHandler(self, hdlr):
        """
        Add the specified handler to this logger.
        """
        if not (hdlr in self.handlers):
            self.handlers.append(hdlr)

    def removeHandler(self, hdlr):
        """
        Remove the specified handler from this logger.
        """
        if hdlr in self.handlers:
            hdlr.close()
            self.handlers.remove(hdlr)


def main():
    # Get queue
    q = Queue(-1)

    # Setup stream handler 1 to output WARNING to console
    h1 = logging.StreamHandler()
    f1 = logging.Formatter("STREAM 1 WARNING: %(threadName)s: %(message)s")
    h1.setFormatter(f1)
    h1.setLevel(logging.WARNING)  # NOT WORKING. This should log >= WARNING

    # Setup stream handler 2 to output INFO to console
    h2 = logging.StreamHandler()
    f2 = logging.Formatter("STREAM 2 INFO: %(threadName)s: %(message)s")
    h2.setFormatter(f2)
    h2.setLevel(logging.INFO)  # NOT WORKING. This should log >= WARNING

    # Start queue listener using the stream handler above
    ql = CustomQueueListener(q, h1, h2)
    ql.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(q)
    root.addHandler(qh)

    root.info("Look out!")  # Create INFO message

    root.warning("Look out2!")  # Create WARNING message

    ql.stop()


if __name__ == "__main__":
    main()
