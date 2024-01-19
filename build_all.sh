scripts/package_building/centos_build_hidra.sh
scripts/package_building/alma_build_hidra.sh

scripts/package_building/debian_build_hidra.sh --hidra-location `pwd` --version 9

scripts/package_building/debian_build_hidra.sh --hidra-location `pwd` --version 10

scripts/package_building/debian_build_hidra.sh --hidra-location `pwd` --version 11

scripts/package_building/debian_build_hidra.sh --hidra-location `pwd` --version 12

# bash scripts/package_building/manylinux_build_hidra.sh

pushd scripts/package_building
    bash manylinux_build_hidra_vagrant.sh
    vagrant rsync
popd