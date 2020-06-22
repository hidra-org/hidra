""" Test the asapo producer """
from __future__ import print_function

import threading
import asapo_producer

LOCK = threading.Lock()


def callback(header, err):
    """ Callback function if sending to asapo was successful """
    LOCK.acquire()  # to print
    if err is not None:
        print("could not sent: ", header, err)
    else:
        print("successfully sent: ", header)
    LOCK.release()


def main():
    """ Test to send to asapo """

    # this only works if a beamtime metadata file is located in
    # /var/tmp/asapo/global_shared/online_data/p00/current/beamtime-metadata-<beamtime>.json
    # the content of it is then parsed by asapo

    source = "localhost:8400"
    # source = "asapo-services:8400"
    stream = "hidra_test"
    token = "KmUDdacgBzaOD3NIJvN1NmKGqWKtx0DK-NyPjdpeWkc="  # use beamline token here
    nthreads = 1
    ingest_mode = asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY
    substream = "substream"

    i = 0

    producer = asapo_producer.create_producer(
        endpoint=source,
        beamline="p00",
        beamtime_id="auto",
        stream=stream,
        token=token,
        nthreads=nthreads,
        timeout_sec=1000
    )

    producer.send_file(
        id=i + 1,
        local_path="/tmp/test_data/test2.file",
        exposed_path=stream + "/test2_file",
        ingest_mode=ingest_mode,
        user_meta='{"test":1}',
        substream=substream,
        callback=callback
    )
    producer.wait_requests_finished(2000)


if __name__ == "__main__":
    main()
