import sys
import time
import random
import boto3
import json
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer

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

dynamodb = boto3.resource("dynamodb")

def load_provinces() -> list[str]:
    province_table = dynamodb.Table(PROVINCE_TABLE_NAME)
    resp = province_table.scan(ProjectionExpression="province")
    provinces_set = {it.get("province") for it in resp.get("Items", []) if it.get("province")}
    if not provinces_set:
        raise RuntimeError("Nessuna provincia trovata: controlla ProjectionExpression/attributo.")
    return list(provinces_set)

def calculate_print_counter_ttl(days: int) -> int:
    expire_at = datetime.now(timezone.utc) + timedelta(days=days)
    return int(expire_at.timestamp())

def compute_daily_print_capacity(delivery_date: date) -> int:
    return next(
        cap
        for start, cap in sorted(
            (
                (date.fromisoformat(d.split(";")[0]), int(d.split(";")[1]))
                for d in PRINT_CAPACITY_CONFIG.split(",")
            ),
            reverse=True,
        )
        if delivery_date >= start
    )

def remap_items(items: list[dict], delivery_week: str) -> list[dict]:
    return [build_paper_delivery_record(it, delivery_week) for it in items]

def build_paper_delivery_record(payload: dict, delivery_week: str) -> dict:
    d = retrieve_date(payload)
    priority = calculate_priority(payload.get("productType"), payload.get("attempt"))
    return {
        "pk": build_pk(delivery_week),
        "sk": build_sk(priority, d, payload["requestId"]),
        "attempt": payload.get("attempt"),
        "cap": (payload.get("recipientNormalizedAddress") or {}).get("cap"),
        "createdAt": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
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
        "workflowStep": "SENT_TO_PREPARE_PHASE_2",
    }

def retrieve_date(payload: dict) -> str:
    attempt = payload.get("attempt")
    try:
        attempt_i = int(attempt) if attempt is not None else None
    except (TypeError, ValueError):
        attempt_i = None

    if payload.get("productType") == "RS" or attempt_i == 1:
        return payload["prepareRequestDate"]
    return payload["notificationSentAt"]

def build_pk(delivery_week: str) -> str:
    return f"{delivery_week}~EVALUATE_PRINT_CAPACITY"

def build_sk(priority: int, date_str: str, request_id: str) -> str:
    return f"{priority}~{date_str}~{request_id}"

def calculate_priority(product_type: str, attempt) -> int:
    if PRIORITY_PARAMETER is None:
        raise RuntimeError("priority-parameter not found")

    priority_obj = PRIORITY_PARAMETER if isinstance(PRIORITY_PARAMETER, dict) else json.loads(PRIORITY_PARAMETER)

    target_key = f"PRODUCT_{product_type}.ATTEMPT_{attempt}"
    for priority, values in priority_obj.items():
        if target_key in values:
            return int(priority)
    raise RuntimeError(f"Priority not found for {target_key}")


def chunked(lst, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def put_print_capacity_counter(
    number_of_shipments: int,
    delivery_date: str,
):
    counters_table = dynamodb.Table(COUNTER_TABLE_NAME)

    ttl_value = calculate_print_counter_ttl(PRINT_COUNTER_TTL_DURATION_DAYS)

    daily_capacity = compute_daily_print_capacity(date.fromisoformat(delivery_date))
    weekly_capacity = daily_capacity * PRINT_CAPACITY_WEEKLY_WORKING_DAYS

    item = {
        "pk": "PRINT",
        "sk": delivery_date,
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
# Executor function
# ----------------------------
def process_partition(province_iter):
    import boto3
    from boto3.dynamodb.conditions import Key
    from boto3.dynamodb.types import TypeSerializer

    dynamodb_local = boto3.resource("dynamodb")
    table = dynamodb_local.Table(PAPER_DELIVERY_TABLE_NAME)

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
                "KeyConditionExpression": Key("pk").eq(SOURCE_PK_VALUE) & Key("sk").begins_with(prov),
            }
            if lek:
                q["ExclusiveStartKey"] = lek

            resp = table.query(**q)

            page_items = resp.get("Items", [])
            if page_items:
                remapped = remap_items(page_items, DELIVERY_DATE)

                for chunk in chunked(remapped, 25):
                    ddb_chunk = [to_ddb_item(it) for it in chunk]
                    batch_write_item_with_retry(dynamo_client, PAPER_DELIVERY_TABLE_NAME, ddb_chunk)

                    processed += len(chunk)
                    local_count += len(chunk)
                    if processed % LOG_EVERY == 0:
                        print(f"[PROGRESS] processed={processed}")

            lek = resp.get("LastEvaluatedKey")
            if not lek:
                break

        print(f"[DONE province] {prov}: {local_count} items")

    print(f"[PARTITION DONE] processed total in this partition: {processed}")
    yield processed


# ----------------------------
# Driver
# ----------------------------
provinces = load_provinces()
print(f"Found {len(provinces)} provinces")

num_slices = min(max(1, PARALLELISM), len(provinces))
rdd = spark.sparkContext.parallelize(provinces, numSlices=num_slices)

print(f"Start processing pk={SOURCE_PK_VALUE} on {len(provinces)} provinces with numSlices={num_slices}")
partition_counts = rdd.mapPartitions(process_partition).collect()
processed_total = sum(partition_counts)
print(f"[TOTAL] processed_total={processed_total}")

put_print_capacity_counter(number_of_shipments=processed_total, delivery_date=DELIVERY_DATE,)
print(f"Inserted print capacity counter for deliveryDate {DELIVERY_DATE} with {processed_total} shipments.")

print("Completed")
job.commit()

