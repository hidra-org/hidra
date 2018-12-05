from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json

# set
instances = {
    "p06": ["test"]
}

with open('instances.txt', 'w') as f:
    f.write(json.dumps(instances, sort_keys=True, indent=4))

with open('instances.txt', 'r') as f:
    read_instances = json.loads(f.read())

print("read", read_instances)
read_instances["p01"] = ["new_test"]

print("write", read_instances)
with open('instances.txt', 'w') as f:
    f.write(json.dumps(read_instances, sort_keys=True, indent=4))
