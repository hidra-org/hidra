from __future__ import print_function
from __future__ import unicode_literals

import json
# import errno

from datafetcherbase import DataFetcherBase, DataHandlingError
from hidra import generate_filepath

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher(DataFetcherBase):

    def __init__(self, config, log_queue, fetcher_id, context):

        DataFetcherBase.__init__(self,
                                 config,
                                 log_queue,
                                 fetcher_id,
                                 "datafetcher_template-{}".format(fetcher_id),
                                 context)

        self.required_params = []

        self.check_config()

    def get_metadata(self, targets, metadata):
        """Implementation of the abstract method get_metadata.
        """

        # Build source file
        self.source_file = generate_filepath(metadata["source_path"],
                                             metadata)

        # Build target file
        # if local_target is not set (== None) generate_filepath returns None
        self.target_file = generate_filepath(self.config["local_target"],
                                             metadata)

        # Extends metadata
        if targets:
            metadata["filesize"] = 0
            metadata["file_mod_time"] = 1481734310.6207027
            metadata["file_create_time"] = 1481734310.6207028
            metadata["chunksize"] = self.config["chunksize"]

    def send_data(self, targets, metadata, open_connections):
        """Implementation of the abstract method send_data.
        """

        if not targets:
            return

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_data = [i for i in targets if i[2] == "data"]

        if not targets_data:
            return

        self.log.debug("Passing multipart-message for file '{}'..."
                       .format(self.source_file))

        for i in range(5):

            chunk_number = i
            file_content = b"test_data_{}".format(chunk_number)

            try:
                # assemble metadata for zmq-message
                chunk_metadata = metadata.copy()
                chunk_metadata["chunk_number"] = chunk_number

                chunk_payload = []
                chunk_payload.append(json.dumps(chunk_metadata)
                                     .encode("utf-8"))
                chunk_payload.append(file_content)
            except:
                self.log.error("Unable to pack multipart-message for file "
                               "'{}'".format(self.source_file), exc_info=True)

            # send message to data targets
            try:
                self.send_to_targets(targets=targets_data,
                                     open_connections=open_connections,
                                     metadata=None,
                                     payload=chunk_payload)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'{}' (chunk {})"
                               .format(self.source_file, chunk_number),
                               exc_info=True)
            except:
                self.log.error("Unable to send multipart-message for file "
                               "'{}' (chunk {})"
                               .format(self.source_file, chunk_number),
                               exc_info=True)

    def finish(self, targets, metadata, open_connections):
        """Implementation of the abstract method finish.
        """

        # targets are of the form [[<host:port>, <prio>, <metadata|data>], ...]
        targets_metadata = [i for i in targets if i[2] == "metadata"]

        # send message to metadata targets
        if targets_metadata:
            try:
                self.send_to_targets(targets=targets_metadata,
                                     open_connections=open_connections,
                                     metadata=metadata,
                                     payload=None)
                self.log.debug("Passing metadata multipart-message for file "
                               "{}...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{}' to '{}'"
                               .format(self.source_file, targets_metadata),
                               exc_info=True)

    def stop(self):
        """Implementation of the abstract method stop.
        """
        pass
