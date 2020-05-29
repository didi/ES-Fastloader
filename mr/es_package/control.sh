#!/bin/bash
#############################################
## main
## 非托管方式, 启动服务
## control.sh脚本, 必须实现start和stop两个方法
#############################################
#set -x
export PS4='+ [`basename ${BASH_SOURCE[0]}`:$LINENO ${FUNCNAME[0]} \D{%F %T} $$ ] '

workspace=$(cd $(dirname $0) && pwd)
cd ${workspace}

module=elasticsearch
app=${module}

logfile=${workspace}/logs/app.log
pidfile=${workspace}/var/app.pid


#### setup env
source ~/.bashrc

IS_PACKAGED_VERSION='distributions'
if [ "$IS_PACKAGED_VERSION" != "distributions" ]; then
    cat >&2 << EOF
Error: You must build the project with Maven or download a pre-built package
before you can run Elasticsearch. See 'Building from Source' in README.textile
or visit https://www.elastic.co/download to get a pre-built package.
EOF
    exit 1
fi

CDPATH=""
SCRIPT="$0"

## SCRIPT may be an arbitrarily deep series of symlinks. Loop until we have the concrete path.
#while [ -h "$SCRIPT" ] ; do
#  ls=`ls -ld "$SCRIPT"`
#  # Drop everything prior to ->
#  link=`expr "$ls" : '.*-> \(.*\)$'`
#  if expr "$link" : '/.*' > /dev/null; then
#    SCRIPT="$link"
#  else
#    SCRIPT=`dirname "$SCRIPT"`/"$link"
#  fi
#done

# determine elasticsearch home
#ES_HOME=`dirname "$SCRIPT"`/

# make ELASTICSEARCH_HOME absolute
ES_HOME=${workspace}

# now set the classpath
ES_CLASSPATH="$ES_HOME/lib/*"

if [ -x /usr/bin/java ]; then
    JAVA="/usr/bin/java"
elif [ -x "$JAVA_HOME/bin/java" ]; then
#if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=`which java`
fi

if [ ! -x "$JAVA" ]; then
    echo "Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME"
    exit 1
fi

# don't let JAVA_TOOL_OPTIONS slip in (e.g. crazy agents in ubuntu)
# works around https://bugs.launchpad.net/ubuntu/+source/jayatana/+bug/1441487
if [ "x$JAVA_TOOL_OPTIONS" != "x" ]; then
    echo "Warning: Ignoring JAVA_TOOL_OPTIONS=$JAVA_TOOL_OPTIONS"
    echo "Please pass JVM parameters via JAVA_OPTS instead"
    unset JAVA_TOOL_OPTIONS
fi

# JAVA_OPTS is not a built-in JVM mechanism but some people think it is so we
# warn them that we are not observing the value of $JAVA_OPTS
if [ ! -z "$JAVA_OPTS" ]; then
  echo -n "warning: ignoring JAVA_OPTS=$JAVA_OPTS; "
  echo "pass JVM parameters via ES_JAVA_OPTS"
fi

# Special-case path variables.
case `uname` in
    CYGWIN*)
        ES_CLASSPATH=`cygpath -p -w "$ES_CLASSPATH"`
        ES_HOME=`cygpath -p -w "$ES_HOME"`
    ;;
esac

# check the Java version
"$JAVA" -cp "$ES_CLASSPATH" org.elasticsearch.tools.java_version_checker.JavaVersionChecker

if [ -z "$ES_PATH_CONF" ]; then ES_PATH_CONF="$ES_HOME"/config; fi

if [ -z "$ES_PATH_CONF" ]; then
  echo "ES_PATH_CONF must be set to the configuration path"
  exit 1
fi

# now make ES_PATH_CONF absolute
ES_PATH_CONF=`cd "$ES_PATH_CONF"; pwd`

ES_DISTRIBUTION_FLAVOR=default
ES_DISTRIBUTION_TYPE=tar

if [ -z "$ES_TMPDIR" ]; then
  #set +e
  mktemp --version 2>&1 | grep coreutils > /dev/null
  mktemp_coreutils=$?
  #set -e
  if [ $mktemp_coreutils -eq 0 ]; then
    ES_TMPDIR=`mktemp -d --tmpdir "elasticsearch.XXXXXXXX"`
  else
    ES_TMPDIR=`mktemp -d -t elasticsearch`
  fi
fi

# full hostname passed through cut for portability on systems that do not support hostname -s
# export on separate line for shells that do not support combining definition and export
HOSTNAME=`hostname | cut -d. -f1`
export HOSTNAME

deploy_user="arius"
monitor_package="monitor_script_v2"
config_file="${workspace}/config/elasticsearch.yml"

function getRole() {
    if [[ "$workspace" =~ .*trib ]]; then
        echo "tribnode"
        return 0
    fi

    cmd="cat ${config_file} | grep node.master"
    isMaster=$(eval "$cmd")
    if [ $? -ne 0 ]; then
        echo "getRole failed in $cmd"
        exit 1
    elif [ x"$isMaster" != "x" ]; then
        isMaster=`echo $isMaster | awk -F: '{print $NF}'`
        isMaster=${isMaster//[[:space:]]/}
        if [ "x$isMaster" == "xtrue" ]; then
            echo "masternode"
            return 0
        fi
    fi

    cmd="cat ${config_file} | grep node.data"
    isData=$(eval "$cmd")
    if [ $? -ne 0 ]; then
        echo "getRole failed in $cmd"
        exit 1
    elif [ x"$isData" != "x" ]; then
        isData=`echo $isData | awk -F: '{print $NF}'`
        isData=${isData//[[:space:]]/}
        if [ "x$isData" == "xtrue" ]; then
            echo "datanode"
            return 0
        fi
    fi

    echo "clientnode"
    return
}

# add crontab
function addCrontab() {
    role=`getRole`
    monitor="monitor_${role}.sh"

    if [[ "x$monitor" != "x" ]] && [[ -f "/home/${deploy_user}/${monitor_package}/${monitor}" ]]; then
        sh /home/${deploy_user}/${monitor_package}/add_crontab.sh "$monitor"
    else
       echo "add monitor crontab failed."
       return 1
    fi
}

## rm crontab
function rmCrontab() {
    role=`getRole`
    monitor="monitor_${role}.sh"

    if [[ "x$monitor" != "x" ]] && [[ -f "/home/${deploy_user}/${monitor_package}/${monitor}" ]]; then
        sh /home/${deploy_user}/${monitor_package}/rm_crontab.sh "$monitor"
    else
       echo "rm monitor crontab failed."
       return 1
    fi
}

## function
function start() {
    # 创建日志目录
    mkdir -p var &>/dev/null
    # check服务是否存活,如果存在则返回
    check_pid
    if [ $? -ne 0 ];then
        local pid=$(get_pid)
        echo "${app} is started, pid=${pid}"
        return 0
    fi

    # 以后台方式 启动程序
    echo -e "Starting the $module ...\c"

    ES_JVM_OPTIONS="$ES_PATH_CONF"/jvm.options
    JVM_OPTIONS=`"$JAVA" -cp "$ES_CLASSPATH" org.elasticsearch.tools.launchers.JvmOptionsParser "$ES_JVM_OPTIONS"`
    ES_JAVA_OPTS="${JVM_OPTIONS//\$\{ES_TMPDIR\}/$ES_TMPDIR} $ES_JAVA_OPTS"

    cd "$ES_HOME"
    # manual parsing to find out, if process should be detached
    exec \
      "$JAVA" \
      $ES_JAVA_OPTS \
      -Des.path.home="$ES_HOME" \
      -Des.path.conf="$ES_PATH_CONF" \
      -Des.distribution.flavor="$ES_DISTRIBUTION_FLAVOR" \
      -Des.distribution.type="$ES_DISTRIBUTION_TYPE" \
      -cp "$ES_CLASSPATH" \
      org.elasticsearch.bootstrap.Elasticsearch \
      "$@" >> ${logfile} 2>&1 \
      <&- &

    retval=$?
    pid=$!

    [ $retval -eq 0 ] || exit $retval

    if [ ! -z "$ES_STARTUP_SLEEP_TIME" ]; then
      sleep $ES_STARTUP_SLEEP_TIME
    fi

    if [ ! -z "${pidfile}" ]; then
        echo $pid > ${pidfile}
    else
        echo "pidfile must be provided!!!"
        kill -9 $pid
        exit 1
    fi

    # 检查服务是否启动成功
    check_pid
    if [ $? -eq 0 ];then
        echo "${app} start failed, please check"
        exit 1
    fi

    echo "${app} start ok, pid=${pid}"
    # 启动成功, 退出码为 0
    return 0
}

function stop() {
    local timeout=60
    # 循环stop服务, 直至60s超时
    for (( i = 0; i < ${timeout}; i++ )); do
        # 检查服务是否停止,如果停止则直接返回
        check_pid
        if [ $? -eq 0 ];then
           echo "${app} is stopped"
           return 0
        fi
        # 检查pid是否存在
        local pid=$(get_pid)
        if [ ${pid} == "" ];then
           echo "${app} is stopped, can't find pid on ${pidfile}"
           return 0
        fi

        # 停止该服务
        if [ $i -ge $((timeout-3)) ]; then
            kill -9 ${pid} &>/dev/null
        else
            kill ${pid} &>/dev/null
        fi
        # 检查该服务是否停止ok
        check_pid
        if [ $? -eq 0 ];then
            # stop服务成功, 返回码为 0
            echo "${app} stop ok"
            return 0
        fi
        # 服务未停止, 继续循环
        sleep 1
    done
    # stop服务失败, 返回码为 非0
    echo "stop timeout(${timeout}s)"
    exit 1
}

function update() {
    echo "update service"
    exit 0
}

function status(){
    check_pid
    local running=$?
    if [ ${running} -ne 0 ];then
        local pid=$(get_pid)
        echo "${app} is started, pid=${pid}"
    else
        echo "${app} is stopped"
    fi
    exit 0
}

## internals
function get_pid() {
    if [ -f $pidfile ];then
        pid=$(cat $pidfile | sed 's/ //g')
        #(ps -fp $pid | grep -i "bootstrap.Elasticsearch" &>/dev/null) && echo $pid
        echo $pid
    fi
}

function check_pid() {
    pid=$(get_pid)
    if [ "x" != "x${pid}" ]; then
        running=$(ps -fp ${pid} | grep -v 'PID TTY' | grep -i "bootstrap.Elasticsearch" |wc -l)
        return ${running}
    fi
    return 0
}

action=$1
case $action in
    "start" )
        # 启动服务
        start
        #addCrontab
        ;;
    "stop" )
        # 停止服务
        stop
        #rmCrontab
        ;;
    "status" )
        # 检查服务
        status
        ;;
    "update" )
        # 更新操作
        update
        ;;
    * )
        echo "unknown command"
        exit 1
        ;;
esac
