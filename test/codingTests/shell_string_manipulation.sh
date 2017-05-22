#!/bin/sh

extract_parameters()
{
    # ${var#*SubStr}  # will drop begin of string upto first occur of `SubStr`
    # ${var##*SubStr} # will drop begin of string upto last occur of `SubStr`
    # ${var%SubStr*}  # will drop part of string from last occur of `SubStr` to the end
    # ${var%%SubStr*} # will drop part of string from first occur of `SubStr` to the end

    # with zsh:
    #beamline=${v%%_*}; detector=${${${v#*${beamline}}#*_}%_*}; if ((${#detector[@]} == 0)); then config=${beamline}; else config=${beamline}_${detector}; fi; user=${${v#*${config}}##*_}; if ((${#user[@]} == 0)); then user=${beamline}user; fi; echo $beamline; echo $detector; echo $config; echo $user


    beamline=${v%%_*}

    tmp1=${v#*${beamline}}
    tmp2=${tmp1#*_}
    detector=${tmp2%_*}
#    detector=${${${v#*${beamline}}#*_}%_*}
    test -z "$detector" && config=${beamline} || config=${beamline}_${detector}
#    if ((${#detector[@]} == 0)); then
#        config=${beamline}
#    else
#        config=${beamline}_${detector}
#    fi

    tmp1=${v#*${config}}
    user=${tmp1##*_}
#    user=${${v#*${config}}##*_}
    test -z "$user" && user=${beamline}user
#    if ((${#user[@]} == 0)); then
#        user=${beamline}user
#    fi

    echo "beamline: $beamline"
    echo "detector: $detector"
    echo "config  : $config"
    echo "user    : $user"
}

v=p11_lsdma-lab01_p11usr
echo "given option: $v"
extract_parameters
echo

v=p11_lsdma-lab01
echo "given option: $v"
extract_parameters
echo

v=p11
echo "given option: $v"
extract_parameters
echo

v=p11__p11usr
echo "given option: $v"
extract_parameters
echo
