import sys
import time
import random
import json
import boto3

from datetime import date, datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

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
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DELIVERY_DATE = args["delivery_date"]
PAPER_DELIVERY_TABLE_NAME = args["paper_delivery_table_name"]
PROVINCE_TABLE_NAME = args["province_table_name"]
COUNTER_TABLE_NAME = args["counter_table_name"]

PRIORITY_PARAMETER = args["priority_parameter"]
PRINT_COUNTER_TTL_DURATION_DAYS = int(args["counter_ttl_days"])
PRINT_CAPACITY_WEEKLY_WORKING_DAYS = int(args["counter_working_days"])
PRINT_CAPACITY_CONFIG = args["print_capacity"]

PARALLELISM = int(args.get("parallelism", "20"))
LOG_EVERY = int(args.get("log-every", "20000"))

SOURCE_PK_VALUE = f"{DELIVERY_DATE}~EVALUATE_SENDER_LIMIT"

# ----------------------------
# Parse priority parameter
# ----------------------------
if PRIORITY_PARAMETER is None:
    raise RuntimeError("priority-parameter not found")

PRIORITY_OBJ = (
    PRIORITY_PARAMETER
    if isinstance(PRIORITY_PARAMETER, dict)
    else json.loads(PRIORITY_PARAMETER)
)

# ----------------------------
# Dynamo (driver scoped)
# ----------------------------
dynamodb = boto3.resource("dynamodb")
dynamo_client = boto3.client("dynamodb")
serializer = TypeSerializer()

# ----------------------------
# Utility functions (IDENTICHE)
# ----------------------------
def load_provinces():
    province_table = dynamodb.Table(PROVINCE_TABLE_NAME)
    resp = province_table.scan(ProjectionExpression="province")
    provinces = {it.get("province") for it in resp.get("Items", []) if it.get("province")}
    if not provinces:
        raise RuntimeError("No provinces found")
    return list(provinces)

def calculate_print_counter_ttl(days: int) -> int:
    expire_at = datetime.now(timezone.utc) + timedelta(days=days)
    return int(expire_at.timestamp())

def compute_daily_print_capacity(delivery_date: date, print_capacity_config: str) -> int:
    capacities = sorted(
        (
            (date.fromisoformat(d.split(";")[0]), int(d.split(";")[1]))
            for d in print_capacity_config.split(",")
        ),
        reverse=True,
    )
    for start, cap in capacities:
        if delivery_date >= start:
            return cap
    raise RuntimeError("No print capacity configured")

def normalize_attempt(attempt) -> int:
    if attempt is None:
        raise RuntimeError("Missing required field 'attempt'")
    if isinstance(attempt, bool):
        raise RuntimeError("Invalid attempt value (bool)")
    if isinstance(attempt, Decimal):
        if attempt % 1 != 0:
            raise RuntimeError(f"Invalid attempt value (non-integer): {attempt}")
        attempt_i = int(attempt)
    elif isinstance(attempt, int):
        attempt_i = attempt
    elif isinstance(attempt, str) and attempt.strip() in ("0", "1"):
        attempt_i = int(attempt.strip())
    else:
        raise RuntimeError(f"Invalid attempt value: {attempt!r}")
    if attempt_i not in (0, 1):
        raise RuntimeError(f"Invalid attempt value: {attempt_i}")

    return attempt_i

def calculate_priority(priority_obj, product_type, attempt) -> int:
    attempt_norm = normalize_attempt(attempt)
    target_key = f"PRODUCT_{product_type}.ATTEMPT_{attempt_norm}"
    for priority, values in priority_obj.items():
        if target_key in values:
            return int(priority)
    raise RuntimeError(f"Priority not found for {target_key}")

def retrieve_date(payload: dict) -> str:
    attempt_norm = normalize_attempt(payload.get("attempt"))

    if payload.get("productType") == "RS" or attempt_norm == 1:
        d = payload.get("prepareRequestDate")
        if not d:
            raise RuntimeError("Missing prepareRequestDate")
        return d

    d = payload.get("notificationSentAt")
    if not d:
        raise RuntimeError("Missing notificationSentAt")
    return d

def build_pk(delivery_date: str) -> str:
    return f"{delivery_date}~EVALUATE_PRINT_CAPACITY"

def build_sk(priority: int, date_str: str, request_id: str) -> str:
    return f"{priority}~{date_str}~{request_id}"

def build_paper_delivery_record(priority_obj, payload, delivery_date):
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

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

def batch_write_item_with_retry(table_name, items_ddb):
    req = {table_name: [{"PutRequest": {"Item": it}} for it in items_ddb]}
    while True:
        resp = dynamo_client.batch_write_item(RequestItems=req)
        unp = resp.get("UnprocessedItems", {})
        if not unp:
            return
        req = unp
        time.sleep(random.uniform(0.2, 1.0))

# ----------------------------
# CORE: process one province (PARALLEL)
# ----------------------------
def process_province(province: str) -> int:
    table = dynamodb.Table(PAPER_DELIVERY_TABLE_NAME)
    processed = 0
    lek = None

    while True:
        q = {
            "KeyConditionExpression": Key("pk").eq(SOURCE_PK_VALUE) & Key("sk").begins_with(province),
        }
        if lek:
            q["ExclusiveStartKey"] = lek

        resp = table.query(**q)
        items = resp.get("Items", [])

        if items:
            remapped = [
                build_paper_delivery_record(PRIORITY_OBJ, it, DELIVERY_DATE)
                for it in items
            ]
            for chunk in chunked(remapped, 25):
                ddb_items = [
                    {k: serializer.serialize(v) for k, v in it.items()}
                    for it in chunk
                ]
                batch_write_item_with_retry(PAPER_DELIVERY_TABLE_NAME, ddb_items)
                processed += len(chunk)
                if processed % LOG_EVERY == 0:
                    print(f"[{province}] processed={processed}")

        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break

    print(f"[DONE] province={province} processed={processed}")
    return processed

# ----------------------------
# MAIN
# ----------------------------
provinces = load_provinces()
print(f"Found {len(provinces)} provinces")

total = 0
with ThreadPoolExecutor(max_workers=PARALLELISM) as executor:
    futures = [executor.submit(process_province, p) for p in provinces]
    for f in as_completed(futures):
        total += f.result()

print(f"[TOTAL] processed_total={total}")

# ----------------------------
# Counter (IDENTICO)
# ----------------------------
ttl_value = calculate_print_counter_ttl(PRINT_COUNTER_TTL_DURATION_DAYS)
daily_capacity = compute_daily_print_capacity(
    date.fromisoformat(DELIVERY_DATE),
    PRINT_CAPACITY_CONFIG,
)

dynamodb.Table(COUNTER_TABLE_NAME).put_item(
    Item={
        "pk": "PRINT",
        "sk": DELIVERY_DATE,
        "dailyExecutionCounter": 0,
        "dailyExecutionNumber": 0,
        "dailyPrintCapacity": daily_capacity,
        "weeklyPrintCapacity": daily_capacity * PRINT_CAPACITY_WEEKLY_WORKING_DAYS,
        "numberOfShipments": total,
        "sentToNextWeek": 0,
        "sentToPhaseTwo": 0,
        "stopSendToPhaseTwo": False,
        "lastEvaluatedKeyNextWeek": {},
        "lastEvaluatedKeyPhase2": {},
        "ttl": ttl_value,
    }
)

print("Completed")
job.commit()
