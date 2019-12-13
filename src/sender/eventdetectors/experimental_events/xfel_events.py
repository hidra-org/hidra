from __future__ import print_function
from __future__ import unicode_literals

import msgpack_numpy
msgpack_numpy.patch()

import collections
import copy
#import json
from multiprocessing import Queue, Process
import os
import sys
#import time
import threading
import zmq
import numpy as np

try:
    from Queue import Empty
except:
    from queue import Empty

from eventdetectorbase import EventDetectorBase
import helpers

KARABO_BRIDGE_PATH = "/home/kuhnm/projects/karabo-bridge-py/euxfel_karabo_bridge"

if KARABO_BRIDGE_PATH not in sys.path:
    sys.path.insert(0, KARABO_BRIDGE_PATH)

from client import Client as KaraboBridge

__author__ = ('Manuela Kuhn <manuela.kuhn@desy.de>',
              'Valerio Mariani <valerio.mariani@desy.de>')

synced_data = None

def Connector(p_id, log_queue, control_queue, con_str, multiproc_queue):
    """Gets data for one channel from karabo and forwards to the multiprocessing queue.

    """

    log = helpers.get_logger("Connector-{}".format(p_id), log_queue)

    log.info("Connecter #{} connecting to Karabo bridge with: {}".format(p_id, con_str))
#    print("Connecter #{} connecting to Karabo bridge with: {}".format(p_id, con_str))

    krb_client = KaraboBridge(con_str)

    control_signal = None

    #for i in range(10):
    while control_signal is None:
        try:
            control_signal = control_queue.get_nowait()
            log.info("Received stopping signal. Abort.")
            break
        except Empty:
            pass

        try:
#            start_t = time.time()
            data = krb_client.next()
#            stop_t = time.time()
#            delay_request = stop_t - start_t

#            data_keys = list(data.keys())
#            source_id = data_keys[0]
#            det_data = data[source_id]
            #print("data keys", data_keys)
#            trainid = det_data['metadata']['timestamp']['tid']

#            sec = det_data['metadata']['timestamp']['sec']
#            frac = det_data['metadata']['timestamp']['frac']
#            ti = np.float64(str(sec) + '.' + str(frac))
#            delay_detector = stop_t - ti


            #proc_t = time.time()
            #log.debug("New data received, forwarding it: {}, {}, request delay: {}, proc time {}"
            #          .format(p_id, trainid, delay_request, proc_t - stop_t))
#            log.debug("New data received, forwarding it: {}, {}, request delay: {}, detector delay: {}"
#                      .format(p_id, trainid, delay_request, delay_detector))
#            print("Connecter #{}: New data received, forwarding it.".format(p_id))
            multiproc_queue.put((p_id, data))
        except:
            log.error("Error in krk_client", exc_info=True)


class Synchronizing(threading.Thread):
    def __init__(self,
                 log_queue,
                 multiproc_queue,
                 run_event,
                 lock,
                 n_connectors,
                 sync_buffer_size):

        self.log = helpers.get_logger("Synchronizing", log_queue)

        self.multiproc_queue = multiproc_queue
        self.all_data = {}
        self.lock = lock
        self.n_connectors = n_connectors

        self.sync_buffer = collections.deque(maxlen=sync_buffer_size)

        self._run_event = run_event

        threading.Thread.__init__(self)

    def run(self):
        global synced_data

        while self._run_event.is_set():
            try:
                #print("mutliproc queue size", self.multiproc_queue.qsize())
                msg = self.multiproc_queue.get()

                if msg == "STOP":
                    self.log.debug("Synchonizing got stop signal. Stopping.")
                    break

                p_id, data = msg

                source_id = list(data.keys())[0]
                data_keys = data[source_id].keys()

                det_data = data[source_id]

                #trainid = det_data['header.trainId']
                trainid = det_data['metadata']['timestamp']['tid']
                #self.log.debug("testing {}".format(test))
#                 trainid = str(trainid)
# #                self.log.debug("Received data from channel {:02}: {}"
# #                               .format(p_id, trainid))

#                 channel_id = "Channel{:02}".format(p_id)
#                 if trainid in self.all_data:
#                     self.all_data[trainid][channel_id] = det_data
#                 else:
#                     self.all_data[trainid] = {
#                         channel_id: det_data
#                     }

#                 n_keys = len(self.all_data[trainid].keys())
#                 if n_keys == self.n_connectors:
#                     self.log.debug("Full image detected: {}".format(trainid))

#                     self.lock.acquire()
#                     synced_data = copy.deepcopy(self.all_data[trainid])
#                     self.lock.release()

#                     del self.all_data[trainid]

#                 print("all_data")
#                 for key in self.all_data:
#                     print(key, self.all_data[key].keys())

                self.sync_buffer.append({"trainid": trainid, "data": det_data,
                                         "channel": source_id})

                trainid_set = tuple(
                    (i,x) for i,x in enumerate(self.sync_buffer)
                    if x['trainid'] == trainid
                )

                if len(trainid_set) == self.n_connectors:

                    self.log.debug("Full image detected: {}".format(trainid))

                    full_image = {x[1]["channel"]: x[1]["data"] for x in trainid_set}

                    with self.lock:
                        synced_data = copy.deepcopy(full_image)

                    for i in sorted(trainid_set, reverse=True):
                        del(self.sync_buffer[i[0]])

                #print('Length deque:', len(self.sync_buffer))
                #print('Buffer:', sorted([x["trainid"] for x in self.sync_buffer]))

            except KeyboardInterrupt:
                print("KeyboardInterrupt detected.")
                raise
            except Exception:
                self.log.info("Stopping thread.")
                raise
                #break

        self.log.info("Stopped while loop in synchronizing thread")

    def stop(self):
        self._run_event.clear()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


class EventDetector(EventDetectorBase):

    def __init__(self, config, log_queue):

        EventDetectorBase.__init__(self,
                                   config=config,
                                   log_queue=log_queue,
                                   logger_name="xfel_events")

#        if helpers.is_windows():
#            required_params = ["context",
#                               "xfel_connections",
#                               "ext_ip",
#                               "data_fetcher_port"]

        required_params = ["context",
                           "xfel_connections",
                           "ipc_path",
                           "main_pid",
                           "sync_buffer_size"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            config,
                                                            self.log)

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {0}"
                          .format(config_reduced))
        else:
            raise Exception("Wrong configuration")


        self.xfel_connections = config["xfel_connections"]
        self.n_connectors = len(self.xfel_connections)

        self.multiproc_queue = Queue()
        self.lock = threading.Lock()
        self.run_event = threading.Event()

        self.control_queue = Queue()

        self._stopped = False

        # for Windows
#        self.data_fetcher_con_str = ("tcp://{}:{}"
#                                     .format(config["ext_ip"],
#                                             config["data_fetcher_port"]))

        self.data_fetcher_con_str = ("ipc://{}/{}_{}"
                                     .format(config["ipc_path"],
                                             config["main_pid"],
                                             "data_fetcher"))

        self.connector_p = []
        for i, con in enumerate(self.xfel_connections):
            con_str = "tcp://{}:{}".format(con[0], con[1])
            p = Process(target=Connector,
                        kwargs=(dict(p_id=i,
                                     log_queue=log_queue,
                                     control_queue=self.control_queue,
                                     con_str=con_str,
                                     multiproc_queue=self.multiproc_queue,)))
            self.connector_p.append(p)

        # use a run event to communicate to thread when to stop
        self.run_event.set()
        self.sync_thread = Synchronizing(log_queue=log_queue,
                                         multiproc_queue=self.multiproc_queue,
                                         run_event=self.run_event,
                                         lock=self.lock,
                                         n_connectors=self.n_connectors,
                                         sync_buffer_size=config['sync_buffer_size'])
        self.sync_thread.start()

        for p in self.connector_p:
            p.start()

        # set up monitoring socket where the events are sent to
        if config["context"] is not None:
            self.context = config["context"]
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        self.data_fetcher_socket = None

        self.create_sockets()

    def create_sockets(self):

        # Create zmq socket to get events
        try:
            self.data_fetcher_socket = self.context.socket(zmq.PUSH)
            self.data_fetcher_socket.bind(self.data_fetcher_con_str)
            self.log.info("Start data_fetcher_socket (bind): '{}'"
                          .format(self.data_fetcher_con_str))
        except:
            self.log.error("Failed to start data_fetcher_socket (bind): '{}'"
                           .format(self.data_fetcher_con_str), exc_info=True)
            raise

    def get_new_event(self):
        global synced_data

        if synced_data is not None:
            self.log.debug("Found synced data.")
            any_key = list(synced_data.keys())[0]

            event_message = {
                "source_path": None,
                "relative_path": None,
                "filename": synced_data[any_key]['metadata']['timestamp']['tid']
            }
            self.log.debug("event_message", event_message)

            event_message_list = [event_message]

            # send data to datafetcher
            self.log.debug("Sending data to datafetcher")
            self.data_fetcher_socket.send_pyobj(synced_data)

            # clear synced_data
            self.lock.acquire()
            synced_data = None
            self.lock.release()
        else:
            event_message_list = []

#        for trainid in synced_data:
#            print("synced_data", trainid, synced_data[trainid].keys())

        return event_message_list

    def stop(self):
        if not self._stopped:
            # stop the subprocesses
            for i in range(self.n_connectors):
                self.control_queue.put("STOP")

            n_checks = 3
            i = 0
            check = True

            # giving processes time to shut down
            time.sleep(1)

            cq_empty = self.control_queue.empty()
            self.log.debug("control queue empty {}".format(cq_empty))
            while not self.control_queue.empty() and check:
                if i < n_checks:
                    time.sleep(1)
                    cq_empty = self.control_queue.empty()
                    self.log.debug("control queue empty 2 {}".format(cq_empty))
                    i += 1
                else:
                    check = False

            # the queue has to be empty for the thread to stop
            while not self.control_queue.empty():
                self.log.debug("empying queue")
                self.multiproc_queue.get()

            # stop the thread
            self.run_event.clear()
            self.multiproc_queue.put("STOP")

            for p in self.connector_p:
    #            p.join()
                p.terminate()

            self._stopped = True

        # close ZMQ
        if self.data_fetcher_socket:
            self.log.info("Closing data_fetcher_socket")
            self.data_fetcher_socket.close(0)
            self.data_fetcher_socket = None

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)



    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    from __init__ import BASE_PATH
    import logging
    import tempfile
    from logutils.queue import QueueHandler

    def start_logging(log_filename):
        log_file = os.path.join(BASE_PATH, "logs", log_filename)
        log_size = 10485760
        verbose = True
        onscreen = "debug"

        log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        if onscreen:
            # Get the log Configuration for the lisener
            h1, h2 = helpers.get_log_handlers(logfile=log_file,
                                              logsize=log_size,
                                              verbose=verbose,
                                              onscreen_log_level=onscreen)

            # Start queue listener using the stream handler above.
            log_queue_listener = helpers.CustomQueueListener(log_queue,
                                                             h1,
                                                             h2)
        else:
            # Get the log Configuration for the lisener
            h1 = helpers.get_log_handlers(logfile=log_file,
                                          logsize=log_size,
                                          verbose=verbose,
                                          onscreen_log_level=onscreen)

            # Start queue listener using the stream handler above
            log_queue_listener = helpers.CustomQueueListener(log_queue, h1)

        log_queue_listener.start()

        return log_queue, log_queue_listener

    def stop_logging(log_queue, log_queue_listener):
        log_queue.put_nowait(None)
        log_queue_listener.stop()

    def set_up_ipc_path():
        ipc_path = os.path.join(tempfile.gettempdir(), "hidra")

        if not os.path.exists(ipc_path):
            os.mkdir(ipc_path)
            os.chmod(ipc_path, 0o777)
            logging.info("Creating directory for IPC communication: {0}"
                         .format(ipc_path))

        return ipc_path

    log_queue, log_queue_listener = start_logging("xfelDetector.log")

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    ipc_path = set_up_ipc_path()

    context = zmq.Context()
    #    current_pid = os.getpid()
    current_pid = 12345

    hostname = "10.253.0.52"
    n_channels = 4
    xfel_connections = []
    for i in range(n_channels):
        port = 4600 + i
        xfel_connections.append((hostname, port))
    print("Connecting to", xfel_connections)

    config = {
        "context": context,
        "xfel_connections": xfel_connections,
        #"xfel_connections": (("localhost", 4545), ("localhost", 4546)),
        "ipc_path": ipc_path,
        "main_pid": current_pid,
        "sync_buffer_size": n_channels * 10
    }

    # set up data fetcher simulation
    data_fetcher_con_str = ("ipc://{}/{}_{}"
                            .format(config["ipc_path"],
                                    config["main_pid"],
                                    "data_fetcher"))

    data_fetcher_socket = context.socket(zmq.PULL)
    data_fetcher_socket.connect(data_fetcher_con_str)
    logging.info("Start data_out_socket (connect): '{0}'"
                     .format(data_fetcher_con_str))

    # getting events
    eventdetector = EventDetector(config, log_queue)

    import time
    try:
        for i in range(100):
            event_list = eventdetector.get_new_event()
            logging.debug("event_list: {0}".format(event_list))

            if event_list:
                print("DataFetcher: receiving data")
                data = data_fetcher_socket.recv_pyobj()
                print("data keys", list(data.keys()))
                sec = data['SPB_DET_AGIPD1M-1/DET/0CH0:xtdf']['metadata']['timestamp']['sec']
                frac = data['SPB_DET_AGIPD1M-1/DET/0CH0:xtdf']['metadata']['timestamp']['frac']
                ti = np.float64(str(sec) + '.' + str(frac))
                print("Delay:", time.time() - ti)
                image = data['SPB_DET_AGIPD1M-1/DET/0CH0:xtdf']['image.data']
                print("image shape", image.shape)

            time.sleep(1)
    finally:
        eventdetector.stop()

        data_fetcher_socket.close(0)

        context.destroy(0)
        stop_logging(log_queue, log_queue_listener)
