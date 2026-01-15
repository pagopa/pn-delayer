import sys
import time
import random
import json
import boto3

from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ----------------------------
# Args
# ----------------------------
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'delivery-date',
        'used-capacities-table-name',
        'used-limit-table-name',
        'counter-table-name',
        'province-table-name',
        'driver-province-parameter',
        'parallelism',
        'log-every',
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DELIVERY_DATE = args['delivery-date']
USED_CAPACITIES_TABLE_NAME = args['used-capacities-table-name']
USED_LIMIT_TABLE_NAME = args['used-limit-table-name']
COUNTER_TABLE_NAME = args['counter-table-name']
PROVINCE_TABLE_NAME = args['province-table-name']
DRIVER_PROVINCE_PARAMETER = args['driver-province-parameter']

PARALLELISM = int(args.get("parallelism", "20"))
LOG_EVERY = int(args.get("log-every", "5000"))

# ----------------------------
# Parse driver parameter
# ----------------------------
if DRIVER_PROVINCE_PARAMETER is None:
    raise RuntimeError("driver-parameter not found")

DRIVER_PROVINCE_PARAMETER_OBJ = DRIVER_PROVINCE_PARAMETER if isinstance(DRIVER_PROVINCE_PARAMETER, dict) else json.loads(DRIVER_PROVINCE_PARAMETER)

# Broadcast (Spark-safe)
BC_DRIVER_PROVINCE_PARAMETER_OBJ = sc.broadcast(DRIVER_PROVINCE_PARAMETER_OBJ)

dynamodb = boto3.resource("dynamodb")

# ----------------------------
# Utility functions
# ----------------------------
def load_provinces() -> list[str]:
    province_table = dynamodb.Table(PROVINCE_TABLE_NAME)
    resp = province_table.scan(ProjectionExpression="province")
    provinces_set = {it.get("province") for it in resp.get("Items", []) if it.get("province")}
    if not provinces_set:
        raise RuntimeError("Province not found in province table")
    return list(provinces_set)

def load_unified_delivery_drivers() -> list[str]:
    obj = BC_DRIVER_PROVINCE_PARAMETER_OBJ.value

    drivers_set = set()

    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, str) and item:
                drivers_set.add(item)
            elif isinstance(item, dict):
                d = item.get("driver")
                if isinstance(d, str) and d:
                    drivers_set.add(d)
    elif isinstance(obj, dict):
        for item in obj.values():
            if isinstance(item, str) and item:
                drivers_set.add(item)
            elif isinstance(item, dict):
                d = item.get("driver")
                if isinstance(d, str) and d:
                    drivers_set.add(d)
    else:
        raise RuntimeError(f"Unsupported driver-province-parameter type: {type(obj)}")

    drivers = list(drivers_set)

    if not drivers:
        raise RuntimeError(f"Drivers not found {DRIVER_PROVINCE_PARAMETER}")

    print(f"Load {len(drivers)} drivers")
    return drivers

def chunked(lst, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

def batch_delete_with_retry(
        dynamo_client,
        table_name: str,
        keys_ddb_typed: list[dict],
        max_attempts: int = 10,
        base: float = 0.2,
):
    request_items = {table_name: [{"DeleteRequest": {"Key": key}} for key in keys_ddb_typed]}

    attempt = 0
    while True:
        attempt += 1
        resp = dynamo_client.batch_write_item(RequestItems=request_items)

        unprocessed = resp.get("UnprocessedItems", {})
        remaining = unprocessed.get(table_name, [])

        if not remaining:
            return

        if attempt >= max_attempts:
            raise RuntimeError(
                f"[DYNAMO BATCH DELETE] Max attempts reached ({max_attempts}) "
                f"with unprocessed items: {len(remaining)}"
            )

        request_items = unprocessed
        sleep = base * (2 ** (attempt - 1))
        sleep += random.uniform(0, sleep / 2)
        print(f"[BATCH RETRY] unprocessed={len(remaining)} attempt={attempt} sleep={sleep:.2f}s")
        time.sleep(sleep)

def process_partition_delete(
        item_iter,
        table_name: str,
        sort_key_attr: str,
        projection_expression: str,
        key_builder,
        progress_prefix: str,
        done_prefix: str,
):
    import boto3
    from boto3.dynamodb.conditions import Key

    dynamodb_local = boto3.resource("dynamodb")
    table = dynamodb_local.Table(table_name)

    dynamo_client = boto3.client("dynamodb")

    deleted = 0

    for item_value in item_iter:
        lek = None
        local_count = 0

        while True:
            q = {
                "IndexName": "deliveryDate-index",
                "KeyConditionExpression": Key("deliveryDate").eq(DELIVERY_DATE) & Key(sort_key_attr).eq(item_value),
                "ProjectionExpression": projection_expression,
            }
            if lek:
                q["ExclusiveStartKey"] = lek

            resp = table.query(**q)

            page_items = resp.get("Items", [])
            if page_items:
                for chunk in chunked(page_items, 25):
                    ddb_keys = [key_builder(it) for it in chunk]
                    batch_delete_with_retry(dynamo_client, table_name, ddb_keys)

                    deleted += len(chunk)
                    local_count += len(chunk)
                    if deleted % LOG_EVERY == 0:
                        print(f"[{progress_prefix} PROGRESS] deleted={deleted}")

            lek = resp.get("LastEvaluatedKey")
            if not lek:
                break

        if local_count > 0:
            print(f"[{done_prefix} DONE] {item_value}: {local_count} items deleted")

    print(f"[{progress_prefix} PARTITION DONE] deleted total in this partition: {deleted}")
    yield deleted

# ----------------------------
# Pipeline: UsedSenderLimit per province
# ----------------------------
def process_partition_provinces(province_iter):
    from boto3.dynamodb.types import TypeSerializer

    serializer = TypeSerializer()

    def to_ddb_key(it: dict) -> dict:
        return {
            "pk": serializer.serialize(it["pk"]),
            "deliveryDate": serializer.serialize(it["deliveryDate"]),
        }

    yield from process_partition_delete(
        province_iter,
        table_name=USED_LIMIT_TABLE_NAME,
        sort_key_attr="province",
        projection_expression="pk, deliveryDate",
        key_builder=to_ddb_key,
        progress_prefix="USED_SENDER_LIMIT",
        done_prefix="USED_SENDER_LIMIT province",
    )

# ----------------------------
# Pipeline: UsedCapacities per unifiedDeliveryDriver
# ----------------------------
def process_partition_drivers(driver_iter):
    """Processa una partizione di drivers per la tabella UsedCapacities"""
    from boto3.dynamodb.types import TypeSerializer

    serializer = TypeSerializer()

    def to_ddb_key(it: dict) -> dict:
        return {
            "unifiedDeliveryDriverGeokey": serializer.serialize(it["unifiedDeliveryDriverGeokey"]),
            "deliveryDate": serializer.serialize(it["deliveryDate"]),
        }

    yield from process_partition_delete(
        driver_iter,
        table_name=USED_CAPACITIES_TABLE_NAME,
        sort_key_attr="unifiedDeliveryDriver",
        projection_expression="unifiedDeliveryDriverGeokey, deliveryDate",
        key_builder=to_ddb_key,
        progress_prefix="USED_CAPACITIES",
        done_prefix="USED_CAPACITIES driver",
    )

# ----------------------------
# Delete print counter
# ----------------------------
def delete_print_counter_for_delivery_date(delivery_date: str):
    table = dynamodb.Table(COUNTER_TABLE_NAME)
    table.delete_item(
        Key={
            "pk": "PRINT",
            "sk": delivery_date,
        }
    )

    print( f"[COUNTER] Deleted counter record pk=PRINT sk={delivery_date}")

# ----------------------------
# Main execution
# ----------------------------
print(f"Start reset data for deliveryDate: {DELIVERY_DATE}\n")

# --------------------------------------------------
# UsedSenderLimit deletion pipeline (by province)
# --------------------------------------------------
used_sender_limit_provinces = load_provinces()
print(f"[USED_SENDER_LIMIT] Found {len(used_sender_limit_provinces)} provinces")

used_sender_limit_parallelism = min(max(1, PARALLELISM),len(used_sender_limit_provinces))

used_sender_limit_rdd = spark.sparkContext.parallelize(
    used_sender_limit_provinces,
    numSlices=used_sender_limit_parallelism,
)

print(
    f"[USED_SENDER_LIMIT] Starting deletion "
    f"with numSlices={used_sender_limit_parallelism}"
)

used_sender_limit_deleted_counts = (
    used_sender_limit_rdd
    .mapPartitions(process_partition_provinces)
    .collect()
)

used_sender_limit_deleted_total = sum(used_sender_limit_deleted_counts)

print(
    f"[USED_SENDER_LIMIT] Deletion completed. "
    f"Total items deleted={used_sender_limit_deleted_total}\n"
)

# --------------------------------------------------
# UsedCapacities deletion pipeline (by driver)
# --------------------------------------------------
used_capacities_drivers = load_unified_delivery_drivers()

used_capacities_parallelism = min(
    max(1, PARALLELISM),
    len(used_capacities_drivers),
)

used_capacities_rdd = spark.sparkContext.parallelize(
    used_capacities_drivers,
    numSlices=used_capacities_parallelism,
)

print(
    f"[USED_CAPACITIES] Starting deletion "
    f"with numSlices={used_capacities_parallelism}"
)

used_capacities_deleted_counts = (
    used_capacities_rdd
    .mapPartitions(process_partition_drivers)
    .collect()
)

used_capacities_deleted_total = sum(used_capacities_deleted_counts)

print(
    f"[USED_CAPACITIES] Deletion completed. "
    f"Total items deleted={used_capacities_deleted_total}\n"
)

# --------------------------------------------------
# Delete PRINT counter
# --------------------------------------------------
delete_print_counter_for_delivery_date(DELIVERY_DATE)

print("Cleanup completed successfully.")
job.commit()
