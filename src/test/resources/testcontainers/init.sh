echo " - Create pn-delayer TABLES"

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveriesCounter  \
    --attribute-definitions \
        AttributeName=deliveryDate,AttributeType=S \
        AttributeName=sk,AttributeType=S \
    --key-schema \
        AttributeName=deliveryDate,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveriesIncoming  \
    --attribute-definitions \
        AttributeName=province,AttributeType=S \
        AttributeName=sk,AttributeType=S \
        AttributeName=unifiedDeliveryDriverProvince,AttributeType=S \
    --key-schema \
        AttributeName=province,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5 \
    --global-secondary-indexes \
                "[
                    {
                        \"IndexName\": \"unifiedDeliveryDriverProvince-index\",
                        \"KeySchema\": [{\"AttributeName\":\"unifiedDeliveryDriverProvince\",\"KeyType\":\"HASH\"},{\"AttributeName\":\"sk\",\"KeyType\":\"RANGE\"}],
                        \"Projection\":{
                            \"ProjectionType\":\"ALL\"
                        },
                         \"ProvisionedThroughput\": {
                             \"ReadCapacityUnits\": 10,
                             \"WriteCapacityUnits\": 5
                         }
                    }
                ]"

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-DelayerPaperDelivery  \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=sk,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5 \


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
    --table-name pn-PaperDeliveriesHighPriority  \
    --attribute-definitions \
        AttributeName=unifiedDeliveryDriverGeokey,AttributeType=S \
        AttributeName=sk,AttributeType=S \
    --key-schema \
        AttributeName=unifiedDeliveryDriverGeokey,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
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

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveriesKinesisEvent  \
    --attribute-definitions \
        AttributeName=requestId,AttributeType=S \
    --key-schema \
        AttributeName=requestId,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveriesPrintCapacity  \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=startDate,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=startDate,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveriesPrintCapacityCounter  \
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
    --table-name pn-PaperDeliverySenderLimit  \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=deliveryDate,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=deliveryDate,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryUsedSenderLimit  \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=deliveryDate,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=deliveryDate,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

echo "Initialization terminated"
