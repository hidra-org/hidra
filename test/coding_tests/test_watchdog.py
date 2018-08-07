import sys
import os
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler


globalList = []


class MyHandler(PatternMatchingEventHandler):
    patterns=["*.cbf"]

    def process(self, event):
        global globalList
        """
        event.event_type
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """
        print event.event_type, event.src_path
        globalList.append(event.src_path)
        print globalList

#    def on_modified(self, event):
#        self.process(event)

    def on_created(self, event):
        self.process(event)


if __name__ == '__main__':
    from shutil import copyfile
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
    print BASE_PATH


    dataPath = BASE_PATH + "/data/source/local/raw"
    observer = Observer()
    observer.schedule(MyHandler(), path=dataPath)
    observer.start()

    sourceFile = BASE_PATH + "test/test_files/test_file.cbf"
    targetFile = BASE_PATH + "/data/source/local/raw/100.cbf"

    i = 1
    try:
        while i<=3:
            print "copy"
            copyfile(sourceFile, targetFile)
            print "remove", targetFile
            os.remove(targetFile)
            time.sleep(1)
            i += 1
    except KeyboardInterrupt:
        observer.stop()

    observer.stop()
    observer.join()
