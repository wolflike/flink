#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

constructFlinkClassPath() {
    local FLINK_DIST
    local FLINK_CLASSPATH

    while read -d '' -r jarfile ; do
        if [[ "$jarfile" =~ .*/flink-dist[^/]*.jar$ ]]; then
            FLINK_DIST="$FLINK_DIST":"$jarfile"
        elif [[ "$FLINK_CLASSPATH" == "" ]]; then
            FLINK_CLASSPATH="$jarfile";
        else
            FLINK_CLASSPATH="$FLINK_CLASSPATH":"$jarfile"
        fi
    done < <(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

    if [[ "$FLINK_DIST" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Flink distribution jar not found in $FLINK_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi

    echo "$FLINK_CLASSPATH""$FLINK_DIST"
}

findFlinkDistJar() {
    local FLINK_DIST="`find "$FLINK_LIB_DIR" -name 'flink-dist*.jar'`"

    if [[ "$FLINK_DIST" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Flink distribution jar not found in $FLINK_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi

    echo "$FLINK_DIST"
}

# These are used to mangle paths that are passed to java when using
# cygwin. Cygwin paths are like linux paths, i.e. /path/to/somewhere
# but the windows java version expects them in Windows Format, i.e. C:\bla\blub.
# "cygpath" can do the conversion.
manglePath() {
    UNAME=$(uname -s)
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -w "$1"`
    else
        echo $1
    fi
}

manglePathList() {
    UNAME=$(uname -s)
    # a path list, for example a java classpath
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -wp "$1"`
    else
        echo $1
    fi
}

# Looks up a config value by key from a simple YAML-style key-value map.
# $1: key to look up
# $2: default value to return if key does not exist
# $3: config file to read from
readFromConfig() {
    local key=$1
    local defaultValue=$2
    local configFile=$3

    # first extract the value with the given key (1st sed), then trim the result (2nd sed)
    # if a key exists multiple times, take the "last" one (tail)
    local value=`sed -n "s/^[ ]*${key}[ ]*: \([^#]*\).*$/\1/p" "${configFile}" | sed "s/^ *//;s/ *$//" | tail -n 1`

    [ -z "$value" ] && echo "$defaultValue" || echo "$value"
}

########################################################################################################################
# DEFAULT CONFIG VALUES: These values will be used when nothing has been specified in conf/flink-conf.yaml
# -or- the respective environment variables are not set.
########################################################################################################################


# WARNING !!! , these values are only used if there is nothing else is specified in
# conf/flink-conf.yaml

DEFAULT_ENV_PID_DIR="/tmp"                          # Directory to store *.pid files to
DEFAULT_ENV_LOG_MAX=5                               # Maximum number of old log files to keep
DEFAULT_ENV_JAVA_OPTS=""                            # Optional JVM args
DEFAULT_ENV_JAVA_OPTS_JM=""                         # Optional JVM args (JobManager)
DEFAULT_ENV_JAVA_OPTS_TM=""                         # Optional JVM args (TaskManager)
DEFAULT_ENV_JAVA_OPTS_HS=""                         # Optional JVM args (HistoryServer)
DEFAULT_ENV_JAVA_OPTS_CLI=""                        # Optional JVM args (Client)
DEFAULT_ENV_SSH_OPTS=""                             # Optional SSH parameters running in cluster mode
DEFAULT_YARN_CONF_DIR=""                            # YARN Configuration Directory, if necessary
DEFAULT_HADOOP_CONF_DIR=""                          # Hadoop Configuration Directory, if necessary

########################################################################################################################
# CONFIG KEYS: The default values can be overwritten by the following keys in conf/flink-conf.yaml
########################################################################################################################

KEY_JOBM_MEM_SIZE="jobmanager.heap.size"
KEY_JOBM_MEM_MB="jobmanager.heap.mb"

KEY_TASKM_COMPUTE_NUMA="taskmanager.compute.numa"

KEY_ENV_PID_DIR="env.pid.dir"
KEY_ENV_LOG_DIR="env.log.dir"
KEY_ENV_LOG_MAX="env.log.max"
KEY_ENV_YARN_CONF_DIR="env.yarn.conf.dir"
KEY_ENV_HADOOP_CONF_DIR="env.hadoop.conf.dir"
KEY_ENV_JAVA_HOME="env.java.home"
KEY_ENV_JAVA_OPTS="env.java.opts"
KEY_ENV_JAVA_OPTS_JM="env.java.opts.jobmanager"
KEY_ENV_JAVA_OPTS_TM="env.java.opts.taskmanager"
KEY_ENV_JAVA_OPTS_HS="env.java.opts.historyserver"
KEY_ENV_JAVA_OPTS_CLI="env.java.opts.client"
KEY_ENV_SSH_OPTS="env.ssh.opts"
KEY_HIGH_AVAILABILITY="high-availability"
KEY_ZK_HEAP_MB="zookeeper.heap.mb"

########################################################################################################################
# MEMORY SIZE UNIT
########################################################################################################################

BYTES_UNITS=("b" "bytes")
KILO_BYTES_UNITS=("k" "kb" "kibibytes")
MEGA_BYTES_UNITS=("m" "mb" "mebibytes")
GIGA_BYTES_UNITS=("g" "gb" "gibibytes")
TERA_BYTES_UNITS=("t" "tb" "tebibytes")

hasUnit() {
    text=$1

    trimmed=$(echo -e "${text}" | tr -d '[:space:]')

    if [ -z "$trimmed" -o "$trimmed" == " " ]; then
        echo "$trimmed is an empty- or whitespace-only string"
	exit 1
    fi

    len=${#trimmed}
    pos=0

    while [ $pos -lt $len ]; do
	current=${trimmed:pos:1}
	if [[ ! $current < '0' ]] && [[ ! $current > '9' ]]; then
	    let pos+=1
	else
	    break
	fi
    done

    number=${trimmed:0:pos}

    unit=${trimmed:$pos}
    unit=$(echo -e "${unit}" | tr -d '[:space:]')
    unit=$(echo -e "${unit}" | tr '[A-Z]' '[a-z]')

    [[ ! -z "$unit" ]]
}

parseBytes() {
    text=$1

    trimmed=$(echo -e "${text}" | tr -d '[:space:]')

    if [ -z "$trimmed" -o "$trimmed" == " " ]; then
        echo "$trimmed is an empty- or whitespace-only string"
	exit 1
    fi

    len=${#trimmed}
    pos=0

    while [ $pos -lt $len ]; do
	current=${trimmed:pos:1}
	if [[ ! $current < '0' ]] && [[ ! $current > '9' ]]; then
	    let pos+=1
	else
	    break
	fi
    done

    number=${trimmed:0:pos}

    unit=${trimmed:$pos}
    unit=$(echo -e "${unit}" | tr -d '[:space:]')
    unit=$(echo -e "${unit}" | tr '[A-Z]' '[a-z]')

    if [ -z "$number" ]; then
        echo "text does not start with a number"
        exit 1
    fi

    local multiplier
    if [ -z "$unit" ]; then
        multiplier=1
    else
        if matchesAny $unit "${BYTES_UNITS[*]}"; then
            multiplier=1
        elif matchesAny $unit "${KILO_BYTES_UNITS[*]}"; then
                multiplier=1024
        elif matchesAny $unit "${MEGA_BYTES_UNITS[*]}"; then
                multiplier=`expr 1024 \* 1024`
        elif matchesAny $unit "${GIGA_BYTES_UNITS[*]}"; then
                multiplier=`expr 1024 \* 1024 \* 1024`
        elif matchesAny $unit "${TERA_BYTES_UNITS[*]}"; then
                multiplier=`expr 1024 \* 1024 \* 1024 \* 1024`
        else
            echo "[ERROR] Memory size unit $unit does not match any of the recognized units"
            exit 1
        fi
    fi

    ((result=$number * $multiplier))

    if [ $[result / multiplier] != "$number" ]; then
        echo "[ERROR] The value $text cannot be re represented as 64bit number of bytes (numeric overflow)."
        exit 1
    fi

    echo "$result"
}

matchesAny() {
    str=$1
    variants=$2

    for s in ${variants[*]}; do
        if [ $str == $s ]; then
            return 0
        fi
    done

    return 1
}

getKibiBytes() {
    bytes=$1
    echo "$(($bytes >>10))"
}

getMebiBytes() {
    bytes=$1
    echo "$(($bytes >> 20))"
}

getGibiBytes() {
    bytes=$1
    echo "$(($bytes >> 30))"
}

getTebiBytes() {
    bytes=$1
    echo "$(($bytes >> 40))"
}

########################################################################################################################
# PATHS AND CONFIG
########################################################################################################################

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

# Convert relative path to absolute path and resolve directory symlinks
bin=`dirname "$target"`
SYMLINK_RESOLVED_BIN=`cd "$bin"; pwd -P`

# Define the main directory of the flink installation
# If config.sh is called by pyflink-shell.sh in python bin directory(pip installed), then do not need to set the FLINK_HOME here.
if [ -z "$_FLINK_HOME_DETERMINED" ]; then
    FLINK_HOME=`dirname "$SYMLINK_RESOLVED_BIN"`
fi
FLINK_LIB_DIR=$FLINK_HOME/lib
FLINK_PLUGINS_DIR=$FLINK_HOME/plugins
JAVA_RUN=$FLINK_HOME/opt


# These need to be mangled because they are directly passed to java.
# The above lib path is used by the shell script to retrieve jars in a
# directory, so it needs to be unmangled.
FLINK_HOME_DIR_MANGLED=`manglePath "$FLINK_HOME"`
if [ -z "$FLINK_CONF_DIR" ]; then FLINK_CONF_DIR=$FLINK_HOME_DIR_MANGLED/conf; fi
FLINK_BIN_DIR=$FLINK_HOME_DIR_MANGLED/bin
DEFAULT_FLINK_LOG_DIR=$FLINK_HOME_DIR_MANGLED/log
FLINK_CONF_FILE="flink-conf.yaml"
YAML_CONF=${FLINK_CONF_DIR}/${FLINK_CONF_FILE}

### Exported environment variables ###
export FLINK_CONF_DIR
export FLINK_BIN_DIR
export FLINK_PLUGINS_DIR
# export /lib dir to access it during deployment of the Yarn staging files
export FLINK_LIB_DIR
# export /opt dir to access it for the SQL client
export FLINK_OPT_DIR

########################################################################################################################
# ENVIRONMENT VARIABLES
########################################################################################################################

# read JAVA_HOME from config with no default value
MY_JAVA_HOME=$(readFromConfig ${KEY_ENV_JAVA_HOME} "" "${YAML_CONF}")
# check if config specified JAVA_HOME
if [ -z "${MY_JAVA_HOME}" ]; then
    # config did not specify JAVA_HOME. Use system JAVA_HOME
    MY_JAVA_HOME=${JAVA_HOME}
fi
# check if we have a valid JAVA_HOME and if java is not available
if [ -z "${MY_JAVA_HOME}" ] && ! type java > /dev/null 2> /dev/null; then
    echo "Please specify JAVA_HOME. Either in Flink config ./conf/flink-conf.yaml or as system-wide JAVA_HOME."
    exit 1
else
    JAVA_HOME=${MY_JAVA_HOME}
fi

UNAME=$(uname -s)
if [ "${UNAME:0:6}" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    if [[ -d $JAVA_HOME ]]; then
        JAVA_RUN=$JAVA_HOME/bin/java
    else
        JAVA_RUN=java
    fi
fi

# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME}" ]; then
    HOSTNAME=`hostname`
fi

IS_NUMBER="^[0-9]+$"

# Define FLINK_JM_HEAP if it is not already set
if [ -z "${FLINK_JM_HEAP}" ]; then
    FLINK_JM_HEAP=$(readFromConfig ${KEY_JOBM_MEM_SIZE} 0 "${YAML_CONF}")
fi

# Try read old config key, if new key not exists
if [ "${FLINK_JM_HEAP}" == 0 ]; then
    FLINK_JM_HEAP_MB=$(readFromConfig ${KEY_JOBM_MEM_MB} 0 "${YAML_CONF}")
fi

# Verify that NUMA tooling is available
command -v numactl >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    FLINK_TM_COMPUTE_NUMA="false"
else
    # Define FLINK_TM_COMPUTE_NUMA if it is not already set
    if [ -z "${FLINK_TM_COMPUTE_NUMA}" ]; then
        FLINK_TM_COMPUTE_NUMA=$(readFromConfig ${KEY_TASKM_COMPUTE_NUMA} "false" "${YAML_CONF}")
    fi
fi

if [ -z "${MAX_LOG_FILE_NUMBER}" ]; then
    MAX_LOG_FILE_NUMBER=$(readFromConfig ${KEY_ENV_LOG_MAX} ${DEFAULT_ENV_LOG_MAX} "${YAML_CONF}")
fi

if [ -z "${FLINK_LOG_DIR}" ]; then
    FLINK_LOG_DIR=$(readFromConfig ${KEY_ENV_LOG_DIR} "${DEFAULT_FLINK_LOG_DIR}" "${YAML_CONF}")
fi

if [ -z "${YARN_CONF_DIR}" ]; then
    YARN_CONF_DIR=$(readFromConfig ${KEY_ENV_YARN_CONF_DIR} "${DEFAULT_YARN_CONF_DIR}" "${YAML_CONF}")
fi

if [ -z "${HADOOP_CONF_DIR}" ]; then
    HADOOP_CONF_DIR=$(readFromConfig ${KEY_ENV_HADOOP_CONF_DIR} "${DEFAULT_HADOOP_CONF_DIR}" "${YAML_CONF}")
fi

if [ -z "${FLINK_PID_DIR}" ]; then
    FLINK_PID_DIR=$(readFromConfig ${KEY_ENV_PID_DIR} "${DEFAULT_ENV_PID_DIR}" "${YAML_CONF}")
fi

if [ -z "${FLINK_ENV_JAVA_OPTS}" ]; then
    FLINK_ENV_JAVA_OPTS=$(readFromConfig ${KEY_ENV_JAVA_OPTS} "${DEFAULT_ENV_JAVA_OPTS}" "${YAML_CONF}")

    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS="$( echo "${FLINK_ENV_JAVA_OPTS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_JM}" ]; then
    FLINK_ENV_JAVA_OPTS_JM=$(readFromConfig ${KEY_ENV_JAVA_OPTS_JM} "${DEFAULT_ENV_JAVA_OPTS_JM}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_JM="$( echo "${FLINK_ENV_JAVA_OPTS_JM}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_TM}" ]; then
    FLINK_ENV_JAVA_OPTS_TM=$(readFromConfig ${KEY_ENV_JAVA_OPTS_TM} "${DEFAULT_ENV_JAVA_OPTS_TM}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_TM="$( echo "${FLINK_ENV_JAVA_OPTS_TM}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_HS}" ]; then
    FLINK_ENV_JAVA_OPTS_HS=$(readFromConfig ${KEY_ENV_JAVA_OPTS_HS} "${DEFAULT_ENV_JAVA_OPTS_HS}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_HS="$( echo "${FLINK_ENV_JAVA_OPTS_HS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_CLI}" ]; then
    FLINK_ENV_JAVA_OPTS_CLI=$(readFromConfig ${KEY_ENV_JAVA_OPTS_CLI} "${DEFAULT_ENV_JAVA_OPTS_CLI}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_CLI="$( echo "${FLINK_ENV_JAVA_OPTS_CLI}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_SSH_OPTS}" ]; then
    FLINK_SSH_OPTS=$(readFromConfig ${KEY_ENV_SSH_OPTS} "${DEFAULT_ENV_SSH_OPTS}" "${YAML_CONF}")
fi

# Define ZK_HEAP if it is not already set
if [ -z "${ZK_HEAP}" ]; then
    ZK_HEAP=$(readFromConfig ${KEY_ZK_HEAP_MB} 0 "${YAML_CONF}")
fi

# High availability
if [ -z "${HIGH_AVAILABILITY}" ]; then
     HIGH_AVAILABILITY=$(readFromConfig ${KEY_HIGH_AVAILABILITY} "" "${YAML_CONF}")
     if [ -z "${HIGH_AVAILABILITY}" ]; then
        # Try deprecated value
        DEPRECATED_HA=$(readFromConfig "recovery.mode" "" "${YAML_CONF}")
        if [ -z "${DEPRECATED_HA}" ]; then
            HIGH_AVAILABILITY="none"
        elif [ ${DEPRECATED_HA} == "standalone" ]; then
            # Standalone is now 'none'
            HIGH_AVAILABILITY="none"
        else
            HIGH_AVAILABILITY=${DEPRECATED_HA}
        fi
     fi
fi

# Arguments for the JVM. Used for job and task manager JVMs.
# DO NOT USE FOR MEMORY SETTINGS! Use conf/flink-conf.yaml with keys
# KEY_JOBM_MEM_SIZE and TaskManagerOptions#TOTAL_PROCESS_MEMORY for that!
if [ -z "${JVM_ARGS}" ]; then
    JVM_ARGS=""
fi

# Check if deprecated HADOOP_HOME is set, and specify config path to HADOOP_CONF_DIR if it's empty.
if [ -z "$HADOOP_CONF_DIR" ]; then
    if [ -n "$HADOOP_HOME" ]; then
        # HADOOP_HOME is set. Check if its a Hadoop 1.x or 2.x HADOOP_HOME path
        if [ -d "$HADOOP_HOME/conf" ]; then
            # its a Hadoop 1.x
            HADOOP_CONF_DIR="$HADOOP_HOME/conf"
        fi
        if [ -d "$HADOOP_HOME/etc/hadoop" ]; then
            # Its Hadoop 2.2+
            HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
        fi
    fi
fi

# try and set HADOOP_CONF_DIR to some common default if it's not set
if [ -z "$HADOOP_CONF_DIR" ]; then
    if [ -d "/etc/hadoop/conf" ]; then
        echo "Setting HADOOP_CONF_DIR=/etc/hadoop/conf because no HADOOP_CONF_DIR was set."
        HADOOP_CONF_DIR="/etc/hadoop/conf"
    fi
fi

INTERNAL_HADOOP_CLASSPATHS="${HADOOP_CLASSPATH}:${HADOOP_CONF_DIR}:${YARN_CONF_DIR}"

if [ -n "${HBASE_CONF_DIR}" ]; then
    INTERNAL_HADOOP_CLASSPATHS="${INTERNAL_HADOOP_CLASSPATHS}:${HBASE_CONF_DIR}"
fi

# Auxilliary function which extracts the name of host from a line which
# also potentially includes topology information and the taskManager type
extractHostName() {
    # handle comments: extract first part of string (before first # character)
    SLAVE=`echo $1 | cut -d'#' -f 1`

    # Extract the hostname from the network hierarchy
    if [[ "$SLAVE" =~ ^.*/([0-9a-zA-Z.-]+)$ ]]; then
            SLAVE=${BASH_REMATCH[1]}
    fi

    echo $SLAVE
}

# Auxilliary functions for log file rotation
rotateLogFilesWithPrefix() {
    dir=$1
    prefix=$2
    while read -r log ; do
        rotateLogFile "$log"
    # find distinct set of log file names, ignoring the rotation number (trailing dot and digit)
    done < <(find "$dir" ! -type d -path "${prefix}*" | sed s/\.[0-9][0-9]*$// | sort | uniq)
}

rotateLogFile() {
    log=$1;
    num=$MAX_LOG_FILE_NUMBER
    if [ -f "$log" -a "$num" -gt 0 ]; then
        while [ $num -gt 1 ]; do
            prev=`expr $num - 1`
            [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
            num=$prev
        done
        mv "$log" "$log.$num";
    fi
}

readMasters() {
    MASTERS_FILE="${FLINK_CONF_DIR}/masters"

    if [[ ! -f "${MASTERS_FILE}" ]]; then
        echo "No masters file. Please specify masters in 'conf/masters'."
        exit 1
    fi

    MASTERS=()
    WEBUIPORTS=()

    MASTERS_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOSTWEBUIPORT=$( extractHostName $line)

        if [ -n "$HOSTWEBUIPORT" ]; then
            HOST=$(echo $HOSTWEBUIPORT | cut -f1 -d:)
            WEBUIPORT=$(echo $HOSTWEBUIPORT | cut -s -f2 -d:)
            MASTERS+=(${HOST})

            if [ -z "$WEBUIPORT" ]; then
                WEBUIPORTS+=(0)
            else
                WEBUIPORTS+=(${WEBUIPORT})
            fi

            if [ "${HOST}" != "localhost" ] && [ "${HOST}" != "127.0.0.1" ] ; then
                MASTERS_ALL_LOCALHOST=false
            fi
        fi
    done < "$MASTERS_FILE"
}

readSlaves() {
    SLAVES_FILE="${FLINK_CONF_DIR}/slaves"

    if [[ ! -f "$SLAVES_FILE" ]]; then
        echo "No slaves file. Please specify slaves in 'conf/slaves'."
        exit 1
    fi

    SLAVES=()

    SLAVES_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOST=$( extractHostName $line)
        if [ -n "$HOST" ] ; then
            SLAVES+=(${HOST})
            if [ "${HOST}" != "localhost" ] && [ "${HOST}" != "127.0.0.1" ] ; then
                SLAVES_ALL_LOCALHOST=false
            fi
        fi
    done < "$SLAVES_FILE"
}

# starts or stops TMs on all slaves
# TMSlaves start|stop
TMSlaves() {
    CMD=$1

    readSlaves

    if [ ${SLAVES_ALL_LOCALHOST} = true ] ; then
        # all-local setup
        for slave in ${SLAVES[@]}; do
            "${FLINK_BIN_DIR}"/taskmanager.sh "${CMD}"
        done
    else
        # non-local setup
        # Stop TaskManager instance(s) using pdsh (Parallel Distributed Shell) when available
        command -v pdsh >/dev/null 2>&1
        if [[ $? -ne 0 ]]; then
            for slave in ${SLAVES[@]}; do
                ssh -n $FLINK_SSH_OPTS $slave -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/taskmanager.sh\" \"${CMD}\" &"
            done
        else
            PDSH_SSH_ARGS="" PDSH_SSH_ARGS_APPEND=$FLINK_SSH_OPTS pdsh -w $(IFS=, ; echo "${SLAVES[*]}") \
                "nohup /bin/bash -l \"${FLINK_BIN_DIR}/taskmanager.sh\" \"${CMD}\""
        fi
    fi
}

runBashJavaUtilsCmd() {
    local cmd=$1
    local conf_dir=$2
    local class_path=$3
    local dynamic_args=${@:4}
    class_path=`manglePathList "${class_path}"`

    local output=`${JAVA_RUN} -classpath "${class_path}" org.apache.flink.runtime.util.BashJavaUtils ${cmd} --configDir "${conf_dir}" $dynamic_args 2>&1 | tail -n 1000`
    if [[ $? -ne 0 ]]; then
        echo "[ERROR] Cannot run BashJavaUtils to execute command ${cmd}." 1>&2
        # Print the output in case the user redirect the log to console.
        echo "$output" 1>&2
        exit 1
    fi

    echo "$output"
}

extractExecutionParams() {
    local output=$1
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"

    local execution_config=`echo "$output" | tail -n 1`
    if ! [[ $execution_config =~ ^${EXECUTION_PREFIX}.* ]]; then
        echo "[ERROR] Unexpected result: $execution_config" 1>&2
        echo "[ERROR] The last line of the BashJavaUtils outputs is expected to be the execution result, following the prefix '${EXECUTION_PREFIX}'" 1>&2
        echo "$output" 1>&2
        exit 1
    fi

    echo ${execution_config} | sed "s/$EXECUTION_PREFIX//"
}
