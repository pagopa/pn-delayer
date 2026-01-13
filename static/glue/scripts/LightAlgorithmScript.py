import sys
import time
import random
import json
import boto3

from datetime import date, datetime, timezone, timedelta

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
        "JOB_NAME",
        "delivery-date",
        "paper-delivery-table-name",
        "province-table-name",
        "counter-table-name",
        "priority-parameter",
        "counter-ttl-days",
        "counter-working-days",
        "print-capacity",
        "parallelism",
        "log-every",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DELIVERY_DATE = args["delivery-date"]
PAPER_DELIVERY_TABLE_NAME = args["paper-delivery-table-name"]
PROVINCE_TABLE_NAME = args["province-table-name"]
COUNTER_TABLE_NAME = args["counter-table-name"]

PRIORITY_PARAMETER = args["priority-parameter"]
PRINT_COUNTER_TTL_DURATION_DAYS = int(args["counter-ttl-days"])
PRINT_CAPACITY_WEEKLY_WORKING_DAYS = int(args["counter-working-days"])
PRINT_CAPACITY_CONFIG = args["print-capacity"]

PARALLELISM = int(args.get("parallelism", "20"))
LOG_EVERY = int(args.get("log-every", "20000"))

SOURCE_PK_VALUE = f"{DELIVERY_DATE}~EVALUATE_SENDER_LIMIT"

# ----------------------------
# Parse priority parameter once
# ----------------------------
if PRIORITY_PARAMETER is None:
    raise RuntimeError("priority-parameter not found")

PRIORITY_OBJ = PRIORITY_PARAMETER if isinstance(PRIORITY_PARAMETER, dict) else json.loads(PRIORITY_PARAMETER)

# Broadcast (Spark-safe)
BC_PRIORITY_OBJ = sc.broadcast(PRIORITY_OBJ)

# ----------------------------
# Driver-side helpers
# ----------------------------
def load_provinces(dynamodb_resource, province_table_name: str) -> list[str]:
    province_table = dynamodb_resource.Table(province_table_name)
    resp = province_table.scan(ProjectionExpression="province")
    provinces_set = {it.get("province") for it in resp.get("Items", []) if it.get("province")}
    if not provinces_set:
        raise RuntimeError("Nessuna provincia trovata: controlla ProjectionExpression/attributo.")
    return list(provinces_set)

def calculate_print_counter_ttl(days: int) -> int:
    expire_at = datetime.now(timezone.utc) + timedelta(days=days)
    return int(expire_at.timestamp())

def compute_daily_print_capacity(delivery_date: date, print_capacity_config: str) -> int:
    return next(
        cap
        for start, cap in sorted(
            (
                (date.fromisoformat(d.split(";")[0]), int(d.split(";")[1]))
                for d in print_capacity_config.split(",")
            ),
            reverse=True,
        )
        if delivery_date >= start
    )

def remap_items(priority_obj: dict, items: list[dict], delivery_date: str) -> list[dict]:
    return [build_paper_delivery_record(priority_obj, it, delivery_date) for it in items]

def build_paper_delivery_record(priority_obj: dict, payload: dict, delivery_date: str) -> dict:
    d = retrieve_date(payload)
    priority = calculate_priority(priority_obj, payload.get("productType"), payload.get("attempt"))
    return {
        "pk": build_pk(delivery_date),
        "sk": build_sk(priority, d, payload["requestId"]),
        "attempt": payload.get("attempt"),
        "cap": (payload.get("recipientNormalizedAddress") or {}).get("cap"),
        "createdAt": datetime.now(timezone.utc).isoformat(timespec="milliseconds") + "Z",
        "iun": payload.get("iun"),
        "notificationSentAt": payload.get("notificationSentAt"),
        "prepareRequestDate": payload.get("prepareRequestDate"),
        "priority": priority,
        "productType": payload.get("productType"),
        "province": (payload.get("recipientNormalizedAddress") or {}).get("pr"),
        "requestId": payload["requestId"],
        "senderPaId": payload.get("senderPaId"),
        "tenderId": payload.get("tenderId"),
        "unifiedDeliveryDriver": payload.get("unifiedDeliveryDriver"),
        "recipientId": payload.get("recipientId"),
        "workflowStep": "EVALUATE_PRINT_CAPACITY",
    }

def retrieve_date(payload: dict) -> str:
    attempt_norm = normalize_attempt(payload.get("attempt"))

    if payload.get("productType") == "RS" or attempt_norm == 1:
        d = payload.get("prepareRequestDate")
        if not d:
            raise RuntimeError(
                f"Missing required field 'prepareRequestDate' for requestId={payload.get('requestId')} "
                f"(productType={payload.get('productType')}, attempt={attempt_norm})"
            )
        return d

    d = payload.get("notificationSentAt")
    if not d:
        raise RuntimeError(
            f"Missing required field 'notificationSentAt' for requestId={payload.get('requestId')} "
            f"(productType={payload.get('productType')}, attempt={attempt_norm})"
        )
    return d

def normalize_attempt(attempt) -> int:
    if attempt is None:
        raise RuntimeError("Missing required field 'attempt'")
    if isinstance(attempt, bool):
        raise RuntimeError(f"Invalid attempt value (bool): {attempt!r} (expected 0 or 1)")
    if isinstance(attempt, int):
        attempt_i = attempt
    elif isinstance(attempt, str):
        s = attempt.strip()
        if s not in ("0", "1"):
            raise RuntimeError(f"Invalid attempt string: {attempt!r} (expected '0' or '1')")
        attempt_i = int(s)
    else:
        raise RuntimeError(f"Invalid attempt type: {type(attempt).__name__} value={attempt!r} (expected 0/1)")

    if attempt_i not in (0, 1):
        raise RuntimeError(f"Invalid attempt value: {attempt_i} (expected 0 or 1)")

    return attempt_i

def build_pk(delivery_date: str) -> str:
    return f"{delivery_date}~EVALUATE_PRINT_CAPACITY"

def build_sk(priority: int, date_str: str, request_id: str) -> str:
    return f"{priority}~{date_str}~{request_id}"

def calculate_priority(priority_obj: dict, product_type: str, attempt) -> int:
    attempt_norm = normalize_attempt(attempt)
    target_key = f"PRODUCT_{product_type}.ATTEMPT_{attempt_norm}"
    for priority, values in priority_obj.items():
        if target_key in values:
            return int(priority)
    raise RuntimeError(f"Priority not found for {target_key}")

def chunked(lst, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

def put_print_capacity_counter(
    dynamodb_resource,
    counter_table_name: str,
    number_of_shipments: int,
    delivery_date_str: str,
    ttl_days: int,
    weekly_working_days: int,
    print_capacity_config: str,
):
    counters_table = dynamodb_resource.Table(counter_table_name)

    ttl_value = calculate_print_counter_ttl(ttl_days)
    daily_capacity = compute_daily_print_capacity(date.fromisoformat(delivery_date_str), print_capacity_config)
    weekly_capacity = daily_capacity * weekly_working_days

    item = {
        "pk": "PRINT",
        "sk": delivery_date_str,
        "dailyExecutionCounter": 0,
        "dailyExecutionNumber": 0,
        "dailyPrintCapacity": daily_capacity,
        "weeklyPrintCapacity": weekly_capacity,
        "numberOfShipments": number_of_shipments,
        "sentToNextWeek": 0,
        "sentToPhaseTwo": 0,
        "stopSendToPhaseTwo": False,
        "lastEvaluatedKeyNextWeek": {},
        "lastEvaluatedKeyPhase2": {},
        "ttl": ttl_value,
    }

    counters_table.put_item(Item=item)

def batch_write_item_with_retry(
    dynamo_client,
    table_name: str,
    items_ddb_typed: list[dict],
    max_attempts: int = 10,
    base: float = 0.2,
):
    request_items = {table_name: [{"PutRequest": {"Item": it}} for it in items_ddb_typed]}

    attempt = 0
    while True:
        attempt += 1
        resp = dynamo_client.batch_write_item(RequestItems=request_items)

        unprocessed = resp.get("UnprocessedItems", {})
        remaining = unprocessed.get(table_name, [])

        if not remaining:
            return

        if attempt >= max_attempts:
            raise RuntimeError(f"[DYNAMO BATCH WRITE] Max attempts reached ({max_attempts}) with unprocessed items: {len(remaining)}")

        request_items = unprocessed
        sleep = base * (2 ** (attempt - 1))
        sleep += random.uniform(0, sleep / 2)
        print(f"[BATCH RETRY] unprocessed={len(remaining)} attempt={attempt} sleep={sleep:.2f}s")
        time.sleep(sleep)


# ----------------------------
# Executor function factory
# ----------------------------
def make_process_partition(config: dict, bc_priority_obj):
    def process_partition(province_iter):
        import boto3
        from boto3.dynamodb.conditions import Key
        from boto3.dynamodb.types import TypeSerializer

        priority_obj = bc_priority_obj.value

        dynamodb_local = boto3.resource("dynamodb")
        table = dynamodb_local.Table(config["paper_delivery_table_name"])

        dynamo_client = boto3.client("dynamodb")
        serializer = TypeSerializer()

        def to_ddb_item(py_item: dict) -> dict:
            return {k: serializer.serialize(v) for k, v in py_item.items()}

        processed = 0

        for prov in province_iter:
            lek = None
            local_count = 0

            while True:
                q = {
                    "KeyConditionExpression": Key("pk").eq(config["source_pk_value"]) & Key("sk").begins_with(prov),
                }
                if lek:
                    q["ExclusiveStartKey"] = lek

                resp = table.query(**q)

                page_items = resp.get("Items", [])
                if page_items:
                    remapped = remap_items(priority_obj, page_items, config["delivery_date"])

                    for chunk in chunked(remapped, 25):
                        ddb_chunk = [to_ddb_item(it) for it in chunk]
                        batch_write_item_with_retry(dynamo_client, config["paper_delivery_table_name"], ddb_chunk)

                        processed += len(chunk)
                        local_count += len(chunk)
                        if config["log_every"] > 0 and processed % config["log_every"] == 0:
                            print(f"[PROGRESS] processed={processed}")

                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break

            print(f"[DONE province] {prov}: {local_count} items")

        print(f"[PARTITION DONE] processed total in this partition: {processed}")
        yield processed

    return process_partition


# ----------------------------
# Driver
# ----------------------------
dynamodb_driver = boto3.resource("dynamodb")

provinces = load_provinces(dynamodb_driver, PROVINCE_TABLE_NAME)
print(f"Found {len(provinces)} provinces")

num_slices = min(max(1, PARALLELISM), len(provinces))
rdd = spark.sparkContext.parallelize(provinces, numSlices=num_slices)

config = {
    "delivery_date": DELIVERY_DATE,
    "paper_delivery_table_name": PAPER_DELIVERY_TABLE_NAME,
    "source_pk_value": SOURCE_PK_VALUE,
    "log_every": LOG_EVERY,
}

process_partition_fn = make_process_partition(config, BC_PRIORITY_OBJ)

print(f"Start processing pk={SOURCE_PK_VALUE} on {len(provinces)} provinces with numSlices={num_slices}")
partition_counts = rdd.mapPartitions(process_partition_fn).collect()
processed_total = sum(partition_counts)
print(f"[TOTAL] processed_total={processed_total}")

put_print_capacity_counter(
    dynamodb_resource=dynamodb_driver,
    counter_table_name=COUNTER_TABLE_NAME,
    number_of_shipments=processed_total,
    delivery_date_str=DELIVERY_DATE,
    ttl_days=PRINT_COUNTER_TTL_DURATION_DAYS,
    weekly_working_days=PRINT_CAPACITY_WEEKLY_WORKING_DAYS,
    print_capacity_config=PRINT_CAPACITY_CONFIG,
)

print(f"Inserted print capacity counter for deliveryDate {DELIVERY_DATE} with {processed_total} shipments.")
print("Completed")
job.commit()
