#!/bin/bash

# This script creates a user that matches the uid and gid of the given directory
# and executes the given command as this user

# This script was created with help from assistant.desy.de

# Check if directory argument is provided
if [ -z "$1" ]; then
   echo "Usage: $0 <directory> <command> [arguments]"
   exit 1
fi

dir="$1"
echo "Obtaining uid and gid of directory $dir..."

shift

# Check if directory exists
if [ ! -d "$dir" ]; then
   echo "Error: Directory '$dir' does not exist."
   exit 1
fi

# Get the uid and gid of the directory
uid=$(stat -c %u "$dir")
gid=$(stat -c %g "$dir")

# Check if group already exists
if getent group ${gid} &>/dev/null; then
    echo "Group with GID $gid already exists."
else
    # Create the group with the specified GID
    groupname="group_${gid}"
    echo "Creating group '$groupname' with GID $gid..."
    groupadd -g "$gid" "$groupname"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to create group '$groupname'."
        exit 1
    fi
fi

# Check if user already exists
if getent passwd ${uid} &>/dev/null; then
    echo "User with uid $uid already exists."
    username=$(getent passwd ${uid} | cut -d: -f1)
else
    # Create the user with the specified UID and GID
    username="user_${uid}"
    echo "Creating user '$username' with UID $uid and GID $gid.."
    useradd -u "$uid" -g "$gid" -m -s /bin/bash "$username"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to create user '$username'."
        exit 1
    fi
fi

# Switch to the newly created user and execute the command
echo "Switching to user '$username'..."
exec runuser -u "$username" -- "$@"
