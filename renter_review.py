from pymongo import MongoClient
import mysql.connector
from datetime import datetime
import json

# ====== STEP 1: Connect to MongoDB ======
mongo_client = MongoClient("")  # แก้ URL ตามจริง
mongo_db = mongo_client[""]                  # แก้ชื่อ DB
collection = mongo_db["renter_reviews"]            # แก้ชื่อ Collection

# ====== STEP 2: Connect to MySQL ======
mysql_conn = mysql.connector.connect(
    host="", # แก้ตาม MySQL host ของคุณ
    user="",      # แก้ตาม user ของคุณ
    password="",  # แก้รหัสผ่าน
    database="", # ใช้ฐานข้อมูล haupcar ตามตัวอย่าง
    port=
)
cursor = mysql_conn.cursor()

# ====== STEP 3: Get latest created_at ======
cursor.execute("SELECT MAX(created_at) FROM renter_reviews;")
result = cursor.fetchone()
latest_created_at = result[0] if result and result[0] else datetime(2022, 1, 1)

print(f"📌 Fetching renter reviews after: {latest_created_at}")

# ====== STEP 4: Query MongoDB for new documents ======
query = { "createdAt": { "$gt": latest_created_at } }
current_year = None
insert_count = 0

for doc in collection.find(query).sort("createdAt", 1):
    created_at = doc.get("createdAt")
    if created_at:
        year = created_at.year
        if year != current_year:
            current_year = year
            print(f"📅 Processing year: {year}")

    doc_id = str(doc.get("_id"))
    reservation_no = doc.get("reservationNo")
    updated_at = doc.get("updatedAt")
    renter_user_id = doc.get("renterUserId")
    rating = doc.get("rating")
    text = doc.get("text", "")

    # Convert tags to JSON string
    tags = doc.get("tags", [])
    tag_list = []
    for tag in tags:
        title = tag.get("title", {})
        tag_list.append({
            "en": title.get("en", ""),
            "th": title.get("th", "")
        })
    tags_json = json.dumps(tag_list, ensure_ascii=False)

    # user info
    user = doc.get("user", {})
    user_id = user.get("userId")
    user_name = user.get("name", "")
    vehicle_id = doc.get("vehicleId")

    insert_sql = """
    INSERT INTO renter_reviews (
        id, reservation_no, created_at, updated_at, renter_user_id,
        rating, text, tags, user_id, user_name, vehicle_id, logtime
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON DUPLICATE KEY UPDATE reservation_no=VALUES(reservation_no);
    """

    try:
        cursor.execute(insert_sql, (
            doc_id, reservation_no, created_at, updated_at, renter_user_id,
            rating, text, tags_json, user_id, user_name, vehicle_id
        ))
        insert_count += 1
    except Exception as e:
        print(f"❌ Failed to insert reservation_no {reservation_no} | Error: {e}")

# ====== STEP 5: Done ======
mysql_conn.commit()
cursor.close()
mysql_conn.close()

print(f"✅ Import completed successfully. Inserted {insert_count} new records.")
