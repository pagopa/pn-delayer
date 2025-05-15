#!/bin/bash
if [ $# -eq 0 ]; then
  INPUT_FILE="default.json"
  echo "Nessun file specificato, uso $INPUT_FILE"
elif [ $# -eq 1 ]; then
  INPUT_FILE=$1
else
  echo "Utilizzo: $0 [<default.json>]"
  exit 1
fi

if [ ! -f "$INPUT_FILE" ]; then
  echo "File non trovato: $INPUT_FILE"
  exit 1
fi

echo "{" > kinesis.event.example.json
echo "  \"Records\": [" >> kinesis.event.example.json

NUM_ELEMENTS=$(jq '. | length' "$INPUT_FILE")

for (( i=0; i<$NUM_ELEMENTS; i++ )); do
    REQUEST_ID=$(jq -r ".[$i].requestId" "$INPUT_FILE")
    IUN=$(jq -r ".[$i].iun" "$INPUT_FILE")
    PRODUCT_TYPE=$(jq -r ".[$i].productType" "$INPUT_FILE")
    SENDER_PA_ID=$(jq -r ".[$i].senderPaId" "$INPUT_FILE")
    RECIPIENT_ID=$(jq -r ".[$i].recipientId" "$INPUT_FILE")
    UNIFIED_DELIVERY_DRIVER=$(jq -r ".[$i].unifiedDeliveryDriver" "$INPUT_FILE")
    TENDER_ID=$(jq -r ".[$i].tenderId" "$INPUT_FILE")
    CAP=$(jq -r ".[$i].recipientNormalizedAddress.cap" "$INPUT_FILE")
    PR=$(jq -r ".[$i].recipientNormalizedAddress.pr" "$INPUT_FILE")

    UUID1=$(uuidgen | tr '[:upper:]' '[:lower:]')
    UUID2=$(uuidgen | tr '[:upper:]' '[:lower:]')

    JSON_TO_ENCODE=$(cat <<EOF
{
      "version": "0",
      "id": "$UUID1",
      "detail-type": "PreparePhaseOneOutcomeEvent",
      "source": "Pipe pn-paper-channel-to-delayer-pipe",
      "account": "830192246553",
      "time": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
      "region": "eu-south-1",
      "resources": [],
      "detail": {
        "source": "pn-paperchannel_to_delayer",
        "body": {
          "requestId": "$REQUEST_ID",
          "unifiedDeliveryDriver": "$UNIFIED_DELIVERY_DRIVER",
          "senderPaId": "$SENDER_PA_ID",
          "recipientId": "$RECIPIENT_ID",
          "tenderId": "$TENDER_ID",
          "iun": "$IUN",
          "recipientNormalizedAddress": {
            "cap": "$CAP",
            "pr": "$PR"
          }
        }
      }
    }
EOF
)

    ENCODED_DATA=$(echo -n "$JSON_TO_ENCODE" | base64 -w 0)

    SEQUENCE_NUMBER=$(printf "%056d" $RANDOM$RANDOM$RANDOM$RANDOM)

    TIMESTAMP=$(date +%s.%3N)

    cat <<EOF >> kinesis.event.example.json
      {
          "kinesis": {
              "kinesisSchemaVersion": "1.0",
              "partitionKey": "${UUID1}_${UUID2}",
              "sequenceNumber": "$SEQUENCE_NUMBER",
              "data":"$ENCODED_DATA",
              "approximateArrivalTimestamp": $TIMESTAMP
          },
          "eventSource": "aws:kinesis",
          "eventVersion": "1.0",
          "eventID": "shardId-000000000002:$SEQUENCE_NUMBER",
          "eventName": "aws:kinesis:record",
          "invokeIdentityArn": "arn:aws:iam::830192246553:role/pn-kinesisPaperDeliveryLambdaRole",
          "awsRegion": "eu-south-1",
          "eventSourceARN": "arn:aws:kinesis:eu-south-1:830192246553:stream/pn-delayer_inputs"
      }
EOF

    if [ $i -lt $((NUM_ELEMENTS-1)) ]; then
        echo "," >> kinesis.event.example.json
    fi
done

echo "" >> kinesis.event.example.json
echo "  ]" >> kinesis.event.example.json
echo "}" >> kinesis.event.example.json

echo "File kinesis.event.example.json generato con successo!"