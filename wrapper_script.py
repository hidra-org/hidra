import argparse
import subprocess
import os


parser = argparse.ArgumentParser()
parser.add_argument("--mv_source", help = "Move source")
parser.add_argument("--mv_target", help = "Move target")

arguments = parser.parse_args()

source = arguments.mv_source
target = arguments.mv_target

watchFolder = "/space/projects/Live_Viewer/source/"

filepathNormalised   = os.path.normpath ( source )
( parentDir, filename ) = os.path.split ( filepathNormalised )
commonPrefix         = os.path.commonprefix ( [ watchFolder, filepathNormalised ] )
#print "commonPrefix", commonPrefix
relativeBasepath     = os.path.relpath ( filepathNormalised, commonPrefix )
#print "relativeBasepath", relativeBasepath
( relativeParent, blub ) = os.path.split ( relativeBasepath )

print "filename ", filename
print "parentDir ", parentDir
print "relativeParent", relativeParent

my_cmd = 'echo "' +  arguments.mv_source + '"  > /tmp/zeromqllpipe'
print my_cmd
p = subprocess.Popen ( my_cmd, shell=True )
p.communicate()

p = subprocess.Popen ( [ 'mv', arguments.mv_source, arguments.mv_target ],
                stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE,
                universal_newlines = False )
out, err = p.communicate()

