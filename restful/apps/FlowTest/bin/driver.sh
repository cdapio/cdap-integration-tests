#!/bin/sh

if [ $# -lt 4 ]
then
    echo "Usage: $0 [jar_file] [hostname] [trials] [events]"
    exit
fi

JAR_FILE=$1
shift
HOSTNAME=$1
shift
TRIALS=$1
shift
SENDS=$1
ARCHIVE_NAME=`basename $JAR_FILE`

curl -X DELETE "http://$HOSTNAME:10000/v2/streams/flowin"

# Loop for testing
TRIAL_COUNT=0
while [ "$TRIAL_COUNT" -lt "$TRIALS" ]
do
    echo "Deploy application '$JAR_FILE' to '$HOSTNAME'"
    curl -s --data-binary @"$JAR_FILE" -H "X-Archive-Name: $ARCHIVE_NAME" "http://$HOSTNAME:10000/v2/apps"

    echo "Sending $SENDS events to stream 'flowin'"
    for i in `seq 1 $SENDS`
    do
        curl -s -d "$i" "http://$HOSTNAME:10000/v2/streams/flowin"
    done

    echo "Start SimpleFlow"
    curl -X POST "http://$HOSTNAME:10000/v2/apps/FlowTestApp/flows/SimpleFlow/start"

    # Wait for RUNNING
    STATUS=`curl -s "http://$HOSTNAME:10000/v2/apps/FlowTestApp/flows/SimpleFlow/status" | sed 's/^{"status":"\([A-Z]*\).*$/\1/'`
    while [ "x$STATUS" != "xRUNNING" ]
    do
        sleep 1
        STATUS=`curl -s "http://$HOSTNAME:10000/v2/apps/FlowTestApp/flows/SimpleFlow/status" | sed 's/^{"status":"\([A-Z]*\).*$/\1/'`
    done

    echo "Flow started"

    # Check for events processed
    COUNT=0
    INSTANCES=1
    INC=1
    EVENTS=`curl -s "http://$HOSTNAME:10000/v2/metrics/reactor/apps/FlowTestApp/flows/SimpleFlow/flowlets/EndFlowlet/process.events.processed?aggregate=true" | sed 's/^{"data":\([0-9]*\).*$/\1/'`
    while [ "$EVENTS" -ne "$SENDS" ]
    do
        COUNT=`expr $COUNT + 1`
        if [ "$COUNT" -gt 300 ]
        then
            echo "Timeout for waiting $SENDS events being processed"
            exit 1
        fi
        
        sleep 2
        # Change instances
        if [ "$INSTANCES" -eq 5 ]
        then
            INC=-1
        elif [ "$INSTANCES" -eq 1 ]
        then
            INC=1
        fi
        INSTANCES=`expr $INSTANCES + $INC`
        echo "Change number of instances to $INSTANCES"
        curl -X PUT -d "{ \"instances\" : $INSTANCES }" "http://$HOSTNAME:10000/v2/apps/FlowTestApp/flows/SimpleFlow/flowlets/InputFlowlet/instances"
        curl -X PUT -d "{ \"instances\" : $INSTANCES }" "http://$HOSTNAME:10000/v2/apps/FlowTestApp/flows/SimpleFlow/flowlets/EndFlowlet/instances"

        EVENTS=`curl -s "http://$HOSTNAME:10000/v2/metrics/reactor/apps/FlowTestApp/flows/SimpleFlow/flowlets/EndFlowlet/process.events.processed?aggregate=true" | sed 's/^{"data":\([0-9]*\).*$/\1/'`
    done

    echo "Stop SimpleFlow"
    curl -X POST "http://$HOSTNAME:10000/v2/apps/FlowTestApp/flows/SimpleFlow/stop"

    # Query procedure
    echo "Start CheckProcedure"
    curl -X POST "http://$HOSTNAME:10000/v2/apps/FlowTestApp/procedures/CheckProcedure/start"

    # Wait for RUNNING
    STATUS=`curl -s "http://$HOSTNAME:10000/v2/apps/FlowTestApp/procedures/CheckProcedure/status" | sed 's/^{"status":"\([A-Z]*\).*$/\1/'`
    while [ "x$STATUS" != "xRUNNING" ]
    do
        sleep 1
        STATUS=`curl -s "http://$HOSTNAME:10000/v2/apps/FlowTestApp/procedures/CheckProcedure/status" | sed 's/^{"status":"\([A-Z]*\).*$/\1/'`
    done
    echo "CheckProcedure running"

    COUNT=1
    RESULT=`curl -s -d "{\"size\":$SENDS}" "http://$HOSTNAME:10000/v2/apps/FlowTestApp/procedures/CheckProcedure/methods/check"`
    while [ "x$RESULT" != "xOK" ]
    do
        COUNT=`expr $COUNT + 1`
        if [ "$COUNT" -gt 10 ]
        then
            echo "Incorrect event counts. $RESULT."
            exit 1
        fi
        echo "Wrong event counts. Retry."
        sleep 1
        RESULT=`curl -s -d "{\"size\":$SENDS}" "http://$HOSTNAME:10000/v2/apps/FlowTestApp/procedures/CheckProcedure/methods/check"`
    done

    echo "Stop CheckProcedure"
    curl -X POST "http://$HOSTNAME:10000/v2/apps/FlowTestApp/procedures/CheckProcedure/stop"

    echo "Truncate counter table"
    curl -X POST "http://$HOSTNAME:10000/v2/datasets/counter/truncate"

    echo "Delete app"
    curl -X DELETE "http://$HOSTNAME:10000/v2/apps/FlowTestApp"

    TRIAL_COUNT=`expr $TRIAL_COUNT + 1`
done
