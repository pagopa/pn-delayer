echo " - Create pn-delayer TABLES"

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryCounters \
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
        AttributeName=tenderIdGeoKey,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=activationDateFrom,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5 \
    --global-secondary-indexes \
        '[
            {
                "IndexName": "tenderIdGeoKey-index",
                "KeySchema": [
                    {"AttributeName":"tenderIdGeoKey","KeyType":"HASH"},
                    {"AttributeName":"activationDateFrom","KeyType":"RANGE"}
                ],
                "Projection":{
                    "ProjectionType":"ALL"
                },
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 5
                }
            }
        ]'

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
    --table-name pn-PaperDeliveryKinesisEvent  \
    --attribute-definitions \
        AttributeName=sequenceNumber,AttributeType=S \
    --key-schema \
        AttributeName=sequenceNumber,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryPrintCapacity  \
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
    --table-name pn-PaperDeliveryPrintCapacityCounter  \
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

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryCountersMock \
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
    --table-name pn-DelayerPaperDeliveryMock \
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
    --table-name pn-PaperDeliveryDriverUsedCapacitiesMock\
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
    --table-name pn-PaperDeliveryDriverCapacitiesMock \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=activationDateFrom,AttributeType=S \
        AttributeName=tenderIdGeoKey,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=activationDateFrom,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5 \
    --global-secondary-indexes \
        '[
            {
                "IndexName": "tenderIdGeoKey-index",
                "KeySchema": [
                    {"AttributeName":"tenderIdGeoKey","KeyType":"HASH"},
                    {"AttributeName":"activationDateFrom","KeyType":"RANGE"}
                ],
                "Projection":{
                    "ProjectionType":"ALL"
                },
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 5
                }
            }
        ]'

aws --profile default --region us-east-1 --endpoint-url=http://localstack:4566 \
    dynamodb create-table \
    --table-name pn-PaperDeliveryUsedSenderLimitMock \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=deliveryDate,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=deliveryDate,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=5

echo "Initialization terminated"
