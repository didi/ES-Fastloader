#!/bin/bash

export PS4='+ [`basename ${BASH_SOURCE[0]}`:$LINENO ${FUNCNAME[0]} \D{%F %T} $$ ] '
workspace=$(cd $(dirname $0) && pwd)
cd ${workspace}

ECM_URL="http://10.88.129.71/api/v1/cluster/resource"
JSON_SH="/etc/container/init/json.sh"
HTTP_ADDRESS=""

#输出集群状态颜色
_color(){
    color=$1
    char=$2

    if [ x"$char" == "x" ];then
        char="."
    fi

    case "$color" in
        "green")
            echo -e -n "\033[42;34m${char}\033[0m"
            ;;
        "yellow")
            echo -e -n "\033[43;34m${char}\033[0m"
            ;;
        "red")
            echo -e -n "\033[41;30m${char}\033[0m"
            ;;
        *)
            echo -n "color Error"
    esac
}

function rand(){
    min=$1
    max=$(($2-$min+1))
    num=$(($RANDOM+1000000000)) #增加一个10位的数再求余
    echo $(($num%$max+$min))
}


## 配置文件
config_file="${workspace}/config/elasticsearch.yml"

function getRole() {
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

# get cluster name
function getClusterName() {
    cmd="cat ${config_file} | grep cluster.name"
    clusterName=$(eval "$cmd")
    if [ $? -ne 0 ]; then
        echo "getClusterName failed in $cmd"
        exit 1
    elif [ x"$clusterName" != "x" ]; then
        clusterName=`echo $clusterName | awk -F: '{print $NF}'`
        clusterName=${clusterName//[[:space:]]/}
        if [ "x${clusterName}" != "x" ]; then
            echo "${clusterName}"
            return 0
        fi
    fi

    return 1
}

# check cluster health
function checkClusterStatus() {
    if [ "X$ENABLE_ECM" == "X" ]; then
        return 0
    fi

    clusterName=${CLUSTERNAME}
    nodeRole=${NODEROLE}

    if [ x"$nodeRole" == "x" ];then
        nodeRole=`getRole`
    fi

    if [ x"$clusterName" == "x" ];then
       clusterName=`getClusterName`
    fi

    echo "--------check cluster status start---------"
    echo -e "Start checking the status of $clusterName in $HOSTNAME ...\c"
    sleep 5s

    if [ x"$nodeRole" == "xdatanode" ]; then
        for ((i=0; i<30; i++)); do
            httpAddress=`curl -s -XGET --connect-timeout 3 --max-time 5 "${ECM_URL}?cluster_name=${clusterName}" | bash ${JSON_SH} -l | grep http_address | awk '{print $NF}' | sed 's/"//g'`
            if [ x"$httpAddress" != "x" ]; then
                IFS=',' read -r -a array <<< "$httpAddress"
                if [[ "${#array[@]}" == 0 ]]; then
                    continue
                fi

                num=$(rand 0 ${#array[@]}-1)
                HTTP_ADDRESS=${array[$num]}
                break
            fi
            sleep 5s
        done

        while true; do
            clusterStatus=`curl -s -XGET --connect-timeout 3 --max-time 5 "${HTTP_ADDRESS}/_cluster/health" | bash ${JSON_SH} -l | grep status | awk '{print $NF}' | sed 's/"//g'`
            if [ x"$clusterStatus" != "xgreen" ]; then
                _color "$clusterStatus"
                sleep 10s
            else
                _color "$clusterStatus"
                break
            fi
        done
    fi

    echo -e "\n--------check cluster status done---------"
}

#### check cluster health
checkClusterStatus
