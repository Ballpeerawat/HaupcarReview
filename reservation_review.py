from pymongo import MongoClient
import mysql.connector
from datetime import datetime
import json

# ====== STEP 1: Connect to MongoDB ======
mongo_client = MongoClient("mongodb://peerawat.s:7voRMq5NZpcGfDUAY58LYwhntMt96Q@mongo.prod.k8s.haupcar.com:27017/?authSource=admin&readPreference=primary&directConnection=true&ssl=false")  # แก้ URL ตามจริง
mongo_db = mongo_client["ts-restful-api"]                  # แก้ชื่อ DB
collection = mongo_db["reservation_reviews"]            # แก้ชื่อ Collection

# ====== STEP 2: Connect to MySQL ======
mysql_conn = mysql.connector.connect(
    host="data.db.haupcar.com", # แก้ตาม MySQL host ของคุณ
    user="peerawat.s@haupcar.com",      # แก้ตาม user ของคุณ
    password="ds_QN53yeaF6L1}5",  # แก้รหัสผ่าน
    database="haupcar", # ใช้ฐานข้อมูล haupcar ตามตัวอย่าง
    port=25060
)
cursor = mysql_conn.cursor()

# ====== STEP 3: Get latest created_at ======
cursor.execute("SELECT MAX(created_at) FROM reservation_reviews;")
result = cursor.fetchone()
latest_created_at = result[0] if result and result[0] else datetime(2022, 1, 1)

print(f"📌 Fetching new data from MongoDB after: {latest_created_at}")

# ====== STEP 4: Query MongoDB ======
query = { "createdAt": { "$gt": latest_created_at } }

insert_count = 0
current_year = None

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
    host_user_id = doc.get("hostUserId")
    rating = doc.get("rating")

    # === tags ===
    tags = doc.get("tags", [])
    tag_list = []
    for tag in tags:
        title = tag.get("title", {})
        tag_list.append({
            "en": title.get("en", ""),
            "th": title.get("th", "")
        })
    tags_json = json.dumps(tag_list, ensure_ascii=False)

    text = doc.get("text", "")

    user = doc.get("user", {})
    user_id = user.get("userId")
    user_name = user.get("name", "")
    vehicle_id = doc.get("vehicleId")

    insert_sql = """
    INSERT INTO reservation_reviews (
        id, reservation_no, created_at, updated_at, host_user_id,
        rating, tags, text, user_id, user_name, vehicle_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        reservation_no = VALUES(reservation_no),
        created_at = VALUES(created_at),
        updated_at = VALUES(updated_at),
        host_user_id = VALUES(host_user_id),
        rating = VALUES(rating),
        tags = VALUES(tags),
        text = VALUES(text),
        user_id = VALUES(user_id),
        user_name = VALUES(user_name),
        vehicle_id = VALUES(vehicle_id);
    """

    try:
        cursor.execute(insert_sql, (
            doc_id, reservation_no, created_at, updated_at, host_user_id,
            rating, tags_json, text, user_id, user_name, vehicle_id
        ))
        insert_count += 1
    except Exception as e:
        print(f"❌ Failed to insert reservation_no {reservation_no} | Error: {e}")

# ====== STEP 5: Finish ======
mysql_conn.commit()
cursor.close()
mysql_conn.close()

print(f"✅ Import completed. Inserted/updated {insert_count} records.")