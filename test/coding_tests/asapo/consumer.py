""" Test the asapo consumer """

from __future__ import print_function

import json

import asapo_consumer


def main():
    """ start a consumer and add data """

    source = "localhost:8400"
    # source = "asapo-services:8400"
    # your mount path from step 2 + "/asapo/data", will work without it as
    # well, until there is a need to read from file
    path = "/asapo_shared/asapo/#data"
    beamtime = "asapo_test"
    token = "KmUDdacgBzaOD3NIJvN1NmKGqWKtx0DK-NyPjdpeWkc="
    group_id = "new"
#    group_id = "boauublgoakuo175n6e0"
    stream = "hidra_test"
#    stream = ""
    substream = "substream"

    broker = asapo_consumer.create_server_broker(
        server_name=source,
        source_path=path,
        has_filesystem=False,
        beamtime_id=beamtime,
        stream=stream,
        token=token,
        timeout_ms=1000
    )  # empty stream "" will default to "detector"

    if group_id == "new":
        group_id = broker.generate_group_id()
        print('generated group id: ', group_id)

    try:
        kwargs = dict(
            group_id=group_id,
            # substream="default",
            substream=substream,
            meta_only=True
        )
        _, meta = broker.get_last(**kwargs)
        # data, meta = broker.get_next(group_id, meta_only=False)
        print('filename: ', meta['name'])
    #    print ('data: ', data.tostring())
        print('meta: ', json.dumps(meta, indent=4, sort_keys=True))
    except asapo_consumer.AsapoConsumerError as err:
        print(err)


if __name__ == "__main__":
    main()
