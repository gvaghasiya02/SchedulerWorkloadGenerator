#!/bin/bash
configFile=$1
workloadFile=$2
if [ -z $BIGFUN_HOME ]
then 
	echo "ERROR: BIGFUN_HOME is not defined."
	exit 1
fi

CONFIGFILE=${BIGFUN_HOME}/conf/${configFile}
if [ ! -f ${CONFIGFILE} ]; then
	echo -e "ERROR: The configuration file for BigFUN (with the name bigfun-conf.json ) can not be found under ${BIGFUN_HOME}/conf directory."
	exit 1
fi

QGENCONFIGFILE=${BIGFUN_HOME}/files/query-params-OLD2.txt
if [ ! -f ${QGENCONFIGFILE} ]; then
        echo -e "ERROR: The query generator config file (with the name query-params.txt ) can not be found under ${BIGFUN_HOME}/files directory."
        exit 1
fi

WORKLOADFILE=${BIGFUN_HOME}/files/workloads/${workloadFile}
if [ ! -f ${WORKLOADFILE} ]; then
	echo -e "ERROR: The workload file (with the name workload.txt ) can not be found under ${BIGFUN_HOME}/files directory."
	exit 1
fi

QUERYINDEXFILE=${BIGFUN_HOME}/files/query-index-OLD2.json
if [ ! -f ${QUERYINDEXFILE} ]; then    
	echo -e "Generating query-index file ..."
	${BIGFUN_HOME}/scripts/qix-gen.sh ${BIGFUN_HOME}/files/queries ${QUERYINDEXFILE}
fi


java -cp ${BIGFUN_HOME}/target/bigfun-driver-jar-with-dependencies.jar driver.Driver ${BIGFUN_HOME} ${WORKLOADFILE}





