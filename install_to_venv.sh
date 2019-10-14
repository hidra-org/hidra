name=$1

if [[ -z $name ]]; then
    name=venv
fi

if [[ -e $name ]]; then
    echo "venv $name already exisits"
    exit 1
fi

command=$2
if [[ -z $command ]]; then
    command=python3
fi

if [[ $command == "python3" ]]; then
    command="$command -m venv $name"
else
    command="$command -m virtualenv $name"
fi

$command \
&& source $name/bin/activate \
&& pip install -e api/python \
&& pip install -e app/hidra \
&& echo "Execute \"source $name/bin/activate\" to activate the environment"