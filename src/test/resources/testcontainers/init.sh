echo " - Create pn-delayer TABLES"

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

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveriesReadyToSend  \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=sk,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryKinesisEvent  \
    --attribute-definitions \
        AttributeName=sequenceNumber,AttributeType=S \
    --key-schema \
        AttributeName=sequenceNumber,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5


echo "Initialization terminated"
