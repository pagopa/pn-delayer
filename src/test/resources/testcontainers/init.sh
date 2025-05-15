echo " - Create pn-delayer TABLES"

echo "### CREATE QUEUES ###"
queues="pn-delayer_to_paperchannel"
for qn in  $( echo $queues | tr " " "\n" ) ; do
    echo creating queue $qn ...
    aws --profile default --region us-east-1 --endpoint-url http://localstack:4566 \
        sqs create-queue \
        --attributes '{"DelaySeconds":"2"}' \
        --queue-name $qn
    echo ending create queue
done

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryDriverCapacities  \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=activationDateFrom,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=activationDateFrom,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryDriverUsedCapacities\
    --attribute-definitions \
        AttributeName=unifiedDeliveryDriverGeokey,AttributeType=S \
        AttributeName=deliveryDate,AttributeType=S \
    --key-schema \
        AttributeName=unifiedDeliveryDriverGeokey,KeyType=HASH \
        AttributeName=deliveryDate,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryHighPriority  \
    --attribute-definitions \
        AttributeName=unifiedDeliveryDriverGeokey,AttributeType=S \
        AttributeName=createdAt,AttributeType=S \
    --key-schema \
        AttributeName=unifiedDeliveryDriverGeokey,KeyType=HASH \
        AttributeName=createdAt,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryReadyToSend  \
    --attribute-definitions \
        AttributeName=deliveryDate,AttributeType=S \
        AttributeName=requestId,AttributeType=S \
    --key-schema \
        AttributeName=deliveryDate,KeyType=HASH \
        AttributeName=requestId,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5


echo "Initialization terminated"
