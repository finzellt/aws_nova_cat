# tools/seed_workitems.py (updated)
import os
import sys
from datetime import UTC, datetime

import boto3

table = boto3.resource("dynamodb").Table(os.environ["NOVACAT_TABLE_NAME"])

response = table.scan(
    FilterExpression="entity_type = :et AND #s = :status",
    ExpressionAttributeNames={"#s": "status"},
    ExpressionAttributeValues={":et": "Nova", ":status": "ACTIVE"},
    ProjectionExpression="nova_id, primary_name",
)

novae = response["Items"]
now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

# Optional: pass --phase2 to seed only the photometry WorkItem for V1324 Sco
phase2 = "--phase2" in sys.argv

count = 0
if phase2:
    # Phase 2: seed only photometry for V1324 Sco
    for item in novae:
        if "v1324" in item.get("primary_name", "").lower():
            table.put_item(
                Item={
                    "PK": "WORKQUEUE",
                    "SK": f"{item['nova_id']}#photometry#{now}",
                    "entity_type": "WorkItem",
                    "nova_id": item["nova_id"],
                    "dirty_type": "photometry",
                    "created_at": now,
                    "source": "manual_seed_phase2",
                }
            )
            count = 1
            print(f"Seeded photometry WorkItem for {item['primary_name']}")
            break
    if count == 0:
        print("ERROR: Could not find V1324 Sco!")
        sys.exit(1)
else:
    # Phase 1: seed all dirty types EXCEPT photometry for V1324 Sco
    for item in novae:
        nova_id = item["nova_id"]
        name = item.get("primary_name", "")
        is_v1324 = "v1324" in name.lower()

        dirty_types = ["spectra", "references"]
        if not is_v1324:
            dirty_types.append("photometry")
        else:
            print(f"Skipping photometry for {name} (will add in phase 2)")

        for dirty_type in dirty_types:
            table.put_item(
                Item={
                    "PK": "WORKQUEUE",
                    "SK": f"{nova_id}#{dirty_type}#{now}",
                    "entity_type": "WorkItem",
                    "nova_id": nova_id,
                    "dirty_type": dirty_type,
                    "created_at": now,
                    "source": "manual_seed",
                }
            )
            count += 1

print(f"Seeded {count} WorkItems for {len(novae)} novae")
