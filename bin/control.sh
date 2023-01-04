#!/usr/bin/env bash

CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export APP_HOME="$(dirname ${CURRENT_DIR})"

export APP_TERMINATE_SIGNAL_FILE=${APP_HOME}/.terminate_signal

get_now_epoch_in_millis() {
# OS Dependent for sunfire - v890
    local epoch_time_in_secs=`truss date 2>&1 | awk '/^time/ {print $NF}'`
    local TSP_MSEC=`perl -MTime::HiRes -e 'print int(1000 * Time::HiRes::gettimeofday),"\n"'`
    local MSEC=`echo $TSP_MSEC | cut -c11-13`
    echo "${epoch_time_in_secs}${MSEC}"

# OS dependent for popular linux flavours
#    echo `date +%s`
}

start_app() {
    target_jar=${APP_HOME}/target/Skybase.jar.original
    lib_dir=${APP_HOME}/target/dependency/*
    MAIN_CLASS="com.gamma.build.skybase.AppBootstrap"
    JAVA_EXEC=`which java`

    MAIN_ARGS="--app.home "${APP_HOME}" \
--app.datasources "Eric-MSC\|Eric-MSC-Stitcher\|Eric-GMSC\|Eric-GMSC-Stitcher\|Huawei-GGSN\|Eric-CCN" \
--app.stitcher.db "h2" \
--app.datasource.gmsc.stitcher "true" \
--app.datasource.gmsc.stitcher.process-partial-records "true" \
--app.datasource.gmsc.stitcher.temp.path "${APP_HOME}/data/gmsc/tmp" \
--app.datasource.gmsc.stitcher.out.path "${APP_HOME}/data/gmsc/stitched" \
--app.datasource.msc.stitcher "true" \
--app.datasource.msc.stitcher.process-partial-records "true" \
--app.datasource.msc.stitcher.temp.path "${APP_HOME}/data/mmsc/tmp" \
--app.datasource.msc.stitcher.out.path "${APP_HOME}/data/gmsc/stitched""

    if [ ! -f "${target_jar}" ] ; then
        echo "Target jar not found : ${target_jar}"
        exit 1
    fi
    if [ ! -f "${JAVA_EXEC}" ] ; then
        echo "Executable java exec not found. Requires minimum JAVA 8 version"
        exit 1
    fi

    CLASS_PATH="${target_jar}:${lib_dir}"
    COMMAND="$JAVA_EXEC -cp ${CLASS_PATH} ${MAIN_CLASS} ${MAIN_ARGS}"
    echo ${COMMAND}
#    ${COMMAND}
    nohup ${COMMAND} &
}

#nohup ${JAVA_EXEC} -jar ${target_jar} &
if [ "$1" = "start" ] ; then
    if [ -f "${APP_TERMINATE_SIGNAL_FILE}" ] ; then
        rm ${APP_TERMINATE_SIGNAL_FILE}
    fi
    start_app
elif [ "$1" = "stop" ] ; then
    if [ -f "${APP_TERMINATE_SIGNAL_FILE}" ] ; then
        echo "Terminate signal file already exists. Please check logs and processes to verify if program running"
    else
        echo "`get_now_epoch_in_millis`" > ${APP_TERMINATE_SIGNAL_FILE}
        echo "Created the terminate file. Please check app logs to confirm"
    fi
else
    echo "Invalid options. Valid options are: start / stop"
    exit 1
fi

exit 0