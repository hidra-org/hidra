""" Test the asapo producer """
from __future__ import print_function

import threading

import asapo_producer
import asapo_consumer


LOCK = threading.Lock()


def callback(header, err):
    """ Callback function if sending to asapo was successful """
    LOCK.acquire()  # to print
    if err is not None:
        print("could not sent: ", header, err)
    else:
        print("successfuly sent: ", header)
    LOCK.release()


def get_last_id(broker_args, substream):
    """ get last id stored in the asapo stream """

    broker = asapo_consumer.create_server_broker(**broker_args)
    group_id = broker.generate_group_id()
    try:
        _, metadata = broker.get_last(
            group_id=group_id,
            substream=substream,
            meta_only=True
        )
        last_id = metadata["_id"]
    except (asapo_consumer.AsapoEndOfStreamError,
            asapo_consumer.AsapoWrongInputError):
        # stream is empty
        last_id = 0

    return last_id


def main():
    """ Test to send to asapo """

    source = "localhost:8400"
    # source = "asapo-services:8400"
    beamtime = "asapo_test"
#    stream = ""
    stream = "hidra_test"
    token = "KmUDdacgBzaOD3NIJvN1NmKGqWKtx0DK-NyPjdpeWkc="
    nthreads = 1
    ingest_mode = asapo_producer.INGEST_MODE_TRANSFER_METADATA_ONLY
    substream = "substream"

    path = "/asapo_shared/asapo/data"

    # get id
    i = get_last_id(
        broker_args=dict(
            server_name=source,
            source_path=path,
            has_filesystem=False,
            beamtime_id=beamtime,
            stream=stream,
            token=token,
            timeout_ms=1000
        ),
        substream=substream
    )
#    i = 0

    producer = asapo_producer.create_producer(
        endpoint=source,
        beamtime_id=beamtime,
        beamline="my_beamline",
        stream=stream,
        token=token,
        nthreads=nthreads,
        timeout_sec=1000
    )

#    producer.send_data(
#        id=i + 1,
#        exposed_path="name" + str(i),
#        data=None,
#        ingest_mode=ingest_mode,
#        substream=substream,
#        callback=callback
#    )
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
