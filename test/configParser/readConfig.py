import ConfigParser


# is needed because configParser always needs a section name
# the used config file consists of key-value pairs only
# source: http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788
class FakeSecHead(object):
    def __init__(self, fp):
        self.fp = fp
        self.sechead = '[asection]\n'

    def readline(self):
        if self.sechead:
            try:
                return self.sechead
            finally:
                self.sechead = None
        else:
            return self.fp.readline()


config = ConfigParser.ConfigParser()
config.readfp(FakeSecHead(open('example.cfg')))

# Set the third, optional argument of get to 1 if you wish to use raw mode.
print config.get('asection', 'foo', 0) # -> "Python is fun!"
print config.get('asection', 'foo', 1) # -> "%(bar)s is %(baz)s!"

# The optional fourth argument is a dict with members that will take
# precedence in interpolation.
print config.get('asection', 'foo', 0, {'bar': 'Documentation',
                                        'baz': 'evil'})
