#!/bin/bash

read -p "Enter first date in format yyyy-MM-dd-HH:mm:ss "  FIRSTDATE
read -p "Enter second date in format yyyy-MM-dd-HH:mm:ss "  SECONDDATE
read -p "Enter error type you want to find 1 for 5XXerror or 2 for long queries " ERRTYPE
CURDIR=$(pwd)

finder_state () {
    #find state of the query succeeded/failed
    STATE=$( aws athena get-query-execution \
        --query-execution-id $ID | grep -e "State\"" \
        | awk '{for (i=2; i<NF; i++) printf $i " "; print $NF}'  | tr -d '"' | tr -d ',' )  
    echo "$STATE" 
}

#find location in the S3 bucket
finder_location () {
    LOCATION=$( aws athena get-query-execution \
            --query-execution-id $ID | grep -e "OutputLocation\"" \
            | awk '{for (i=2; i<NF; i++) printf $i " "; print $NF}'  | tr -d '"' | tr -d ',' )
}

# if state "Succeeded" continue
state_check () {
    if [ "$STATE" == "SUCCEEDED" ]; then
        finder_location
        FILELOC="$CURDIR/athena-5xx-log-utc-$FIRSTDATE.csv"
        aws s3 cp $LOCATION $FILELOC
        COUNTROW="$(wc -l $FILELOC | awk '{print $1}')"
        if [ "$(($COUNTROW + 1))" -gt "2" ]; then
            if [ $ERRTYPE -eq 1 ]; then
                for (( I=2; I<=$COUNTROW; I++ ))
                do  
                    sed -n "$I p" $FILELOC | cut -d , -f 12,15,16
                done
            fi
            if [ $ERRTYPE -eq 2 ]; then
                for (( I=2; I<=$COUNTROW; I++ ))
                do  
                    sed -n "$I p" $FILELOC | cut -d , -f 9,15,16
                done
            fi
        else
            echo "Nothing found on this date"
        fi
    else
        echo "STATE: FAILED (format date or aws error look above)"
    fi 
}

finder () {
    finder_state
    while [ "$STATE" == "RUNNING" ]
    do
        #sleep for long queries
        sleep 3
        finder_state
    done
    state_check
    exit 0
}

if [ "$ERRTYPE" -gt "0" ]; then
    if [ $ERRTYPE -eq 1 ]; then
        # start athena query and find query id
        ID=$( aws athena start-query-execution \
        --query-string "SELECT * FROM alb_logs \
        WHERE parse_datetime(time,'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z') \
        BETWEEN parse_datetime('$FIRSTDATE','yyyy-MM-dd-HH:mm:ss') \
            AND parse_datetime('$SECONDDATE','yyyy-MM-dd-HH:mm:ss') \
            AND elb_status_code BETWEEN 500 AND 599"  \
            --work-group primary  \
            --query-execution-context Database=default,Catalog=AwsDataCatalog | grep QueryExecutionId \
            | awk '{for (i=2; i<NF; i++) printf $i " "; print $NF}' \
            | tr -d '"' )
        finder
    fi

    if [ $ERRTYPE -eq 2 ]; then
        read -p "Enter target_processing_time " TIME
        ID=$( aws athena start-query-execution \
        --query-string "SELECT * FROM alb_logs \
        WHERE parse_datetime(time,'yyyy-MM-dd''T''HH:mm:ss.SSSSSS''Z') \
        BETWEEN parse_datetime('$FIRSTDATE','yyyy-MM-dd-HH:mm:ss') \
            AND parse_datetime('$SECONDDATE','yyyy-MM-dd-HH:mm:ss') \
            AND (target_processing_time > $TIME)"  \
            --work-group primary  \
            --query-execution-context Database=default,Catalog=AwsDataCatalog | grep QueryExecutionId \
            | awk '{for (i=2; i<NF; i++) printf $i " "; print $NF}' \
            | tr -d '"' )
        finder
    fi
    echo "Wrong enter of error type"
else
    echo "Wrong enter of error type"
fi