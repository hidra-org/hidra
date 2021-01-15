get_hidra_version()
{
    NEXT_HIDRA_VERSION=$(cat ${HIDRA_DIR}/src/api/python/hidra/utils/_version.py)

    # cut of the first characters
    NEXT_HIDRA_VERSION=${NEXT_HIDRA_VERSION:15}
    NEXT_HIDRA_VERSION=${NEXT_HIDRA_VERSION%?}

    # get the current git version (based on *last* tag)
    GIT_VERSION=$(git describe)

    if [[ "${GIT_VERSION}" =~ - ]]; then
        HIDRA_VERSION=${NEXT_HIDRA_VERSION}.dev${GIT_VERSION#*-}
    else
        HIDRA_VERSION=${NEXT_HIDRA_VERSION}
    fi

    # according to PEP440, local part separator is "+"
    HIDRA_VERSION=${HIDRA_VERSION/-/+}
}