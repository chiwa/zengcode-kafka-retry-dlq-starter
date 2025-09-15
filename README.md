# Zengcode Kafka Retry/DLQ Starter

Spring Boot Starter สำหรับ Kafka ที่ช่วยจัดการ **Retry + Dead Letter Queue (DLQ)** อัตโนมัติ  
✅ Plug-and-Play ใช้กับ `@KafkaListener` ได้ทันที  
✅ รองรับการดีเลย์แบบ fixed ตาม `initial-interval-ms`  
✅ ใส่ header `x-retry-attempt` ให้ทุกครั้งที่วนกลับไป retry  
✅ ตก DLQ อัตโนมัติเมื่อครบจำนวนรอบที่กำหนด

---

## ทำงานยังไง (สรุปสั้น)

* คุณเขียน `@KafkaListener` ที่อ่านจาก **topic หลัก** ปกติ
* ถ้า listener โยน exception:

   1. Starter จะ **publish ไปที่ `<base>.retry`** พร้อมแนบ `x-retry-attempt` (เริ่มจาก 0)
   2. **Retry Bridge** (ที่ starter สร้างให้) จะ consume จาก `<base>.retry` → **หน่วงเวลา**ตาม `initial-interval-ms` → **ส่งกลับ** `<base>` พร้อมเพิ่ม `x-retry-attempt` +1
   3. ถ้า fail ซ้ำ วนข้อ 1–2 ไปเรื่อยๆ จน `x-retry-attempt + 1 >= max-attempts` → **ส่งเข้า `<base>.dlq`**

> ตอนนี้เราใช้ **fixed delay** ด้วย `initial-interval-ms` (ค่าพื้นฐาน). พารามิเตอร์อย่าง `multiplier`/`max-interval-ms` ถูกกันที่ไว้ก่อน—ยังไม่เปิดใช้แบบ exponential ในเวอร์ชันนี้

---

## โฟลว์ของข้อความ

```
<base> (เช่น orders)
  ├─ ✅ success → done
  └─ ❌ fail → <base>.retry
          (bridge: sleep initial-interval-ms → ส่งกลับ <base> พร้อม x-retry-attempt+1)
          └─ ❌ fail → <base>.retry → ... (วนจน attempt ครบ)
                    └─ ❌ ครบ attempts → <base>.dlq
```

* **ไม่มี** topic รูปแบบ `.retry.1000ms`, `.retry.2000ms` อีกต่อไป
* ใช้เพียง 3 topics ต่อหนึ่ง base:

   * `<base>` (เช่น `orders`)
   * `<base>.retry` (เช่น `orders.retry`)
   * `<base>.dlq` (เช่น `orders.dlq`)

---

## การใช้งาน

### 1) เพิ่ม properties (ตัวอย่าง)

```properties
# consumer ปกติของแอป
spring.kafka.consumer.group-id=app-consumer
spring.kafka.consumer.auto-offset-reset=earliest

# แนะนำให้ตั้งเป็น byte[] ให้ตรงกับสตาร์เตอร์ (จะใช้ได้กับ String ได้ถ้าตั้ง serializer/deserializer ให้ตรง)
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.ByteArraySerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer

# เปิดสตาร์เตอร์
kafka.retry.enabled=true
kafka.retry.retry-suffix=.retry
kafka.retry.dlq-suffix=.dlq
kafka.retry.initial-interval-ms=1000     # หน่วงคงที่ 1s ก่อนส่งกลับ base
kafka.retry.max-attempts=2               # รวม main+retry (0→1) แล้วตก DLQ เมื่อเกิน
```

> หมายเหตุ: `max-attempts=2` = ลองที่ main 1 ครั้ง (attempt=0) ถ้าพัง → ไป retry 1 ครั้ง (attempt=1) ถ้ายังพัง → DLQ

### 2) สร้าง topic (3 อัน)

* `orders`
* `orders.retry`
* `orders.dlq`

จะสร้างเองด้วย `KafkaAdmin`/`NewTopic` หรือจะให้แอปคุณสร้างตอนบูตก็ได้ (ขึ้นกับ org ของคุณ)

### 3) เขียน `@KafkaListener` ปกติ

```java
@KafkaListener(topics = "orders")
public void handle(byte[] message) {
  var text = new String(message, StandardCharsets.UTF_8);
  if (text.contains("FAIL")) {
    throw new RuntimeException("boom!");
  }
  // process success
}
```

**ไม่ต้อง**ประกาศ ErrorHandler เอง, **ไม่ต้อง**ไป subscribe `<base>.retry` เอง
สตาร์เตอร์จะผูก `DefaultErrorHandler` และลง `Retry Bridge` ให้โดยอัตโนมัติ

---

## Header ที่เกี่ยวข้อง

* `x-retry-attempt` (string → เลขจำนวนครั้งที่เคย retry)

   * main ล้มครั้งแรก → โดนส่งเข้า `<base>.retry` ด้วย `x-retry-attempt=0`
   * bridge ส่งกลับ `<base>` จะเพิ่มเป็น 1, 2, …
   * เมื่อตก `<base>.dlq` จะติดค่านี้มาด้วย (debug/audit ได้)

> (ถ้าต้องการ header อื่นๆ เช่น exception class/message, message-id, first-failure-ts สามารถต่อยอดในอนาคตได้)

---

## คำถามที่พบบ่อย

* **ต้องไปแก้อะไรใน client มั้ย?**
  แทบไม่ต้อง — เขียน listener จาก `<base>` ตามเดิม สตาร์เตอร์จัดการ retry/dlq ให้เอง

* **ใช้กับ String message ได้มั้ย?**
  ได้ ถ้าตั้ง serializer/deserializer ให้ตรงกับของคุณ (ตัวอย่างด้านบนเป็น byte\[] เพื่อความกลาง)

* **ดีเลย์ทำที่ไหน?**
  ทำที่ **Retry Bridge** (ตัว consumer ภายในสตาร์เตอร์) โดย `Thread.sleep(initial-interval-ms)` ก่อนส่งกลับ `<base>`

---

แค่นี้ก็พร้อมใช้งานในโปรดักชันแบบแก้ของเดิมน้อยที่สุด และไม่ต้องให้ทีม client ไปวุ่นกับ error handler รายตัวครับ 🚀
