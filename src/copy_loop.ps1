$source = "D:\hidra"

$target = {$source\data\source\local}
$limit  = 10

$files  = {$source\test_file.cbf }
$format = "cbf"

usage() { echo "Usage: $0 [-f <cbf|tif>] [-s <sourcepath>] [-t <targetpath>] [-n <number of files>]" 1>&2; exit 1; }

for ($i=0; $i -le $files; $i++)
    {
        COPY $files $target\$i.$format
    }
