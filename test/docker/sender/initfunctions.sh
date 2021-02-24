status() {
    pids=$(pidof $1)
    if [ -z "$pids" ]; then
        echo $1 is not running
        return 3
    else
       echo $1 is running
       return 0
    fi
}


checkpid() {
    local i

    for i in $* ; do
        [ -d "/proc/$i" ] && return 0
    done

    return 1
}


pidofproc() {
    pidof "$1" || return 3
}
