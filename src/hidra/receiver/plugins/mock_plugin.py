from unittest.mock import create_autospec


class PluginSpec:
    def __init__(self, config):
        pass

    def setup(self):
        pass

    def process(self, local_path, metadata, data):
        pass

    def stop(self):
        pass


Plugin = create_autospec(PluginSpec)
