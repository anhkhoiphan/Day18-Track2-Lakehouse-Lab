# Bonus Challenge — Topic C: CDC từ Ride-Hailing Việt Nam → Lakehouse (Decree 13/2023/NĐ-CP)

**Tên:** Phan Anh Khôi  
**Email:** anhkhoiphan03@gmail.com  
**VinUni · AI20k · Day 18 — Track 2: Data Lakehouse Architecture**  
**Ngày:** 04/05/2026

---

## 1. Problem Statement

Một nền tảng ride-hailing Việt Nam (tương tự Grab/Be) cần xây Lakehouse phục vụ analytics thời gian thực và tuân thủ pháp lý. Số liệu cụ thể:

- **Scale:** 100 triệu chuyến/năm (~274 nghìn chuyến/ngày, peak 30.000 writes/giây giờ cao điểm)
- **Source:** Oracle production DB → Debezium CDC → Kafka → Lakehouse
- **PII scope (Decree 13/2023/NĐ-CP):** số điện thoại, CMND/CCCD, toạ độ GPS realtime của tài xế + hành khách
- **SLA analytics:** dashboard refresh ≤ 60 giây từ lúc commit ở Oracle; ad-hoc query p95 < 1 giây
- **Late events:** phổ biến — tài xế mất mạng ở tỉnh xa, event đến muộn 5–30 phút; đôi khi vài giờ

**Tại sao khó:** Ba constraint xung đột nhau. (1) 60-giây SLA đòi micro-batch nhanh. (2) Late events đòi idempotent MERGE thay vì simple append. (3) Decree 13 bắt buộc PII không được tồn tại plaintext ở bất kỳ tầng nào — nhưng GPS realtime lại cần cho operational dashboard. Ba thứ này cùng lúc không có giải pháp "ra của hàng mua về dùng" được.

---

## 2. Architecture Diagram

```
Oracle Production DB (rides, drivers, passengers)
        │
        │  Debezium CDC (before/after images)
        ▼
 ┌─────────────────────────────────────┐
 │  Kafka  (topic: cdc.rides.v1)       │
 │  Retention: 7 ngày (replay buffer)  │
 └───────────────┬─────────────────────┘
                 │
    ┌────────────▼──────────────────────────────────────────────┐
    │  BRONZE  (s3://lh-ridehailing/bronze/)                    │
    │  Delta, append-only, partition by ingest_date             │
    │  • Raw CDC payload (JSON string)                          │
    │  • PII TOKENIZED ngay tại đây (sha256 + app-secret salt)  │
    │  • Kafka offset lưu để dedup + replay                     │
    │  • Retention: 30 ngày → S3-IA → Glacier 1 năm            │
    └────────────┬──────────────────────────────────────────────┘
                 │  Spark Structured Streaming
                 │  Micro-batch mỗi 30 giây
    ┌────────────▼──────────────────────────────────────────────┐
    │  SILVER  (s3://lh-ridehailing/silver/)                    │
    │  Delta + CDF enabled, partition by ride_date              │
    │  Z-ORDER by (city_code, status)                           │
    │                                                           │
    │  silver.rides          — SCD Type 1 + late MERGE          │
    │  silver.drivers        — SCD Type 2 (lịch sử trạng thái) │
    │  silver.passengers     — SCD Type 2                       │
    │  silver.pii_audit_log  — mọi lần đọc PII field            │
    └────────────┬──────────────────────────────────────────────┘
                 │  dbt + DuckDB, cron mỗi 5 phút (Gold KPI)
                 │              mỗi 1 giờ (Gold agg nặng)
    ┌────────────▼──────────────────────────────────────────────┐
    │  GOLD  (s3://lh-ridehailing/gold/)                        │
    │  Delta, partition by report_date                          │
    │                                                           │
    │  gold.daily_ops_kpi   — rides, revenue, completion rate   │
    │  gold.driver_perf     — acceptance, cancel, rating/driver │
    │  gold.demand_heatmap  — rides per city_zone per hour      │
    │  gold.late_event_qc   — monitor lag từ Oracle → Gold      │
    └────────────┬──────────────────────────────────────────────┘
                 │
         DuckDB (< 1s ad-hoc)   Metabase dashboard (60s refresh)
```

**Ingestion path (hot):** Oracle → Debezium → Kafka → Spark Structured Streaming → Bronze → Silver MERGE  
**Query path:** DuckDB reads Gold Parquet files từ S3 directly (zero-copy, column pruning)  
**Governance path:** Lakekeeper REST Catalog → schema registry, OpenLineage → Marquez lineage

---

## 3. Quyết định Chính (5 quyết định với alternatives đã loại)

### Decision 1: Table Format — Delta Lake vs Iceberg vs Hudi

**Chọn: Delta Lake với Change Data Feed (CDF) bật sẵn**

| Tiêu chí | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| CDC upsert (MERGE) | ✅ Native, mature | ✅ Merge-on-Read | ✅ Upsert-first design |
| Incremental reads (CDF) | ✅ `_change_data` tích hợp | ⚠️ Incremental scan, không CDF | ✅ `hoodie.deltastreamer` |
| Late event handling | ✅ `MERGE WHEN MATCHED AND src.ts > tgt.ts` | ⚠️ Phức tạp hơn | ✅ tốt |
| SCD Type 2 | ✅ dbt snapshot macro sẵn | ✅ | ⚠️ Cần custom logic |
| Time travel + RESTORE | ✅ `RESTORE VERSION AS OF` | ✅ snapshot id | ⚠️ Phức tạp |
| Deletion Vectors (Decree 13 delete) | ✅ Delta 3.x DV, fast DELETE | ⚠️ v2 position deletes | ⚠️ |
| Ecosystem với DuckDB | ✅ delta-rs native | ⚠️ cần iceberg-duckdb ext | ❌ Không native |

**Loại Hudi:** Hudi mạnh về upsert nhưng DeltaStreamer thêm ops complexity. SCD Type 2 cần custom logic. DuckDB integration kém. Với team nhỏ, ops cost cao hơn lợi ích.

**Loại Iceberg:** Iceberg không có CDF tương đương — incremental reads cần scan nhiều snapshots, tăng latency trong pipeline 30-giây. Deletion Vectors của Delta 3.x tốt hơn cho requirement Decree 13 (physical delete nhanh mà không rebuild file).

**Trade-off chấp nhận:** Delta không có hidden partitioning (Iceberg `days(ts)`) → phải explicit `PARTITIONED BY (ride_date)`. Chấp nhận được.

---

### Decision 2: PII Strategy — Tokenize tại Bronze vs Encrypt-at-rest vs Row-level Security

**Chọn: Tokenize (HMAC-SHA256 + rotating app secret) ngay tại Bronze ingestion, trước khi ghi Delta**

**Logic:**
```
token = hmac_sha256(key=APP_SECRET_v2, msg=raw_phone_number)
# token là deterministic → có thể JOIN across tables
# nhưng không reversible nếu không có APP_SECRET
```

**Loại Encrypt-at-rest only:** S3 SSE-S3 / SSE-KMS mã hoá file, nhưng bất kỳ ai có IAM `s3:GetObject` đều đọc được plaintext sau decrypt. Không đáp ứng Decree 13 yêu cầu "biện pháp kỹ thuật bảo vệ dữ liệu cá nhân". Encryption-at-rest là hygiene tối thiểu, không đủ thay tokenization.

**Loại Row-level Security (RLS) thuần tuý:** RLS ở catalog layer (Unity/Lakekeeper) bảo vệ khi query, nhưng raw Parquet files vẫn có PII plaintext. Nếu S3 bucket bị exfiltrate (ransomware, misconfigured public bucket), toàn bộ PII lộ. Tokenize tại source = defence in depth.

**Loại Pseudonymization với lookup table:** Giữ mapping `original → token` trong encrypted DB, cho phép "de-pseudonymize" khi cần hỗ trợ user. **Chọn tokenize không reversible** vì analytics không cần plaintext — chỉ cần so sánh `token_a == token_b` (same driver). Khi support cần lookup, họ query từ Oracle source, không từ Lakehouse.

**Trade-off chấp nhận:** Khi APP_SECRET rotate, các token cũ và mới không join được → cần migration window. Giải quyết: `token_version` column; salt version được encode trong token prefix (`v2:abc123...`).

---

### Decision 3: Late Event Handling — MERGE với guard vs REPLACE vs event-time watermark

**Chọn: `MERGE WHEN MATCHED AND src.event_ts > tgt.event_ts THEN UPDATE`**

```sql
MERGE INTO silver.rides AS tgt
USING (
  SELECT * FROM bronze_staging WHERE ingest_date = :partition
) AS src
ON tgt.ride_id = src.ride_id
WHEN MATCHED AND src.event_ts > tgt.event_ts THEN
  UPDATE SET tgt.status = src.status,
             tgt.end_ts = src.end_ts,
             tgt.fare_vnd = src.fare_vnd,
             tgt.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (...)
```

**Loại REPLACE WHERE (partition swap):** Ghi đè toàn partition ngày hôm nay mỗi lần có late event → mất version history trong `_delta_log`, time travel không còn meaningful. Không thể trace "ride X có status gì lúc 14:30?".

**Loại Flink event-time watermark only:** Watermark handle tốt "out-of-order within a window", nhưng với late events vài giờ hoặc vài ngày (mất mạng ở Lạng Sơn), watermark phải rất lớn → tăng memory state trong Flink. MERGE pattern đơn giản hơn cho tail-latency cases.

**Loại full-refresh Silver daily:** 274k rides/ngày × 365 = 100M rows/năm trong Silver. Full-refresh = đọc 100M rows mỗi lần có late event → không đáp ứng 60s SLA.

---

### Decision 4: Compute Engine Bronze→Silver — Spark Structured Streaming vs Flink vs Kafka Streams

**Chọn: Spark Structured Streaming, trigger `ProcessingTime("30 seconds")`**

**Loại Apache Flink:** Flink tốt hơn cho true streaming với stateful joins phức tạp. Nhưng: (a) ops complexity cao hơn nhiều — Flink cluster cần tuning JVM heap, state backend (RocksDB), checkpointing; (b) team nhỏ; (c) SLA là 60 giây, không phải sub-second. Spark micro-batch đủ + đơn giản hơn.

**Loại Kafka Streams:** Kafka Streams chỉ đọc từ Kafka, không write native sang Delta Lake. Cần custom Sink connector. Delta Lake Kafka connector tồn tại nhưng immature cho upsert/MERGE pattern cần ở Silver.

**Loại hourly batch (Spark batch):** Đơn giản nhất nhưng vi phạm 60-giây SLA. Gold refresh 5 phút cũng không đạt nếu Silver chỉ cập nhật mỗi giờ.

**Trade-off chấp nhận:** Spark micro-batch mỗi 30 giây có overhead per-batch (JVM startup nếu dùng EMR, hoặc executor warmup). Giải quyết: Spark long-running `StreamingQuery` — không restart JVM mỗi batch.

---

### Decision 5: Partitioning Strategy — by ride_date vs by city_code vs by hour

**Chọn: partition by `ride_date` (Silver) + Z-ORDER by `(city_code, status)`**

**Loại partition by city_code:** Việt Nam có 63 tỉnh/thành. Nếu partition by city → 63 directories/ngày × 365 ngày = 22.995 partitions sau 1 năm. S3 ListObjects chậm, query planner overhead lớn, nhiều small files per partition.

**Loại partition by hour:** 24 giờ × 365 ngày = 8.760 partitions/năm. Mỗi partition ~16MB (274k rides × ~60 bytes Silver row / 24h). 16MB partition → Spark split thành 1 task → under-parallelized. File quá nhỏ cho columnar scan.

**Loại partition by (ride_date, city_code):** Kết hợp tệ nhất: 63 × 365 = 22.995 partitions, mỗi cái càng nhỏ hơn.

**Lý do chọn daily + Z-ORDER:** Daily partition = ~1.37 GB/ngày Bronze → sau compaction Silver ~200 MB → 4–8 Parquet files 32MB mỗi cái. Z-ORDER `(city_code, status)` vì query phổ biến nhất là `WHERE city_code = 'HCM' AND status = 'COMPLETED'`. Z-ORDER co-locate hai cột này → file pruning 8–15× theo thực nghiệm.

---

### Decision 6: Catalog — Lakekeeper (REST) vs AWS Glue vs Hive Metastore

**Chọn: Lakekeeper (Apache Polaris-compatible REST Catalog)**

**Loại Hive Metastore (HMS):** HMS dùng RDBMS (MySQL/PostgreSQL) làm backend → SPOF, không horizontal scale, không column-level governance. Quan trọng hơn: không có column-level access control cần cho PII audit — không thể nói "user X chỉ được đọc rides.ride_id và rides.fare, không được đọc rides.driver_token".

**Loại AWS Glue Catalog:** Glue tốt nếu all-in-AWS. Nhưng: (a) vendor lock-in — nếu sau này chuyển sang GCP hoặc on-prem data center Việt Nam (FPT Cloud), cần migrate catalog; (b) cross-engine support kém hơn REST Catalog spec (Spark OK, DuckDB cần extension workaround); (c) column-level tagging cho PII cần Lake Formation thêm vào → thêm dependency.

**Lakekeeper cho:** REST Catalog spec chuẩn → Spark, DuckDB, Trino đều connect qua cùng endpoint; column-level tags (`pii:true`) native trong catalog metadata; self-hosted → data không rời khỏi VN (Decree 13 yêu cầu lưu trữ trong nước cho data cá nhân người Việt).

**Trade-off chấp nhận:** Lakekeeper chưa mature bằng Glue. Phải self-host, tốn ~1 instance EC2/VM nhỏ (~$30/tháng). Chấp nhận cho compliance benefit.

---

## 4. Failure Modes (3+ scenarios, ≥1 liên quan Day 18 concepts)

### Failure 1 — Debezium connector ngừng do Oracle network partition

**Xảy ra:** Kết nối Oracle-Debezium bị đứt, connector dừng với error. Kafka topic ngừng nhận events trong thời gian T.

**Detect:** Prometheus alert: `kafka_topic_last_offset_change_seconds > 120` → PagerDuty. Kafka consumer lag tăng đột biến.

**Rollback:** Debezium lưu `scn` (Oracle System Change Number) trong Kafka Connect offset store. Khi reconnect, nó tự động resume từ SCN cuối → **không mất event**. Bronze MERGE dùng `kafka_offset` dedup nên duplicate events từ replay an toàn. Silver MERGE guard `src.event_ts > tgt.event_ts` tránh stale overwrites.

**Lesson từ Day 18:** Delta Bronze append-only = immutable audit trail. Ngay cả khi connector restart nhiều lần và replay, Bronze không bị corrupt — worst case có duplicate rows, Silver MERGE lọc sạch.

---

### Failure 2 — Bug trong Silver MERGE ghi sai `fare_vnd` cho 50k rides

*(Liên quan trực tiếp tới Time Travel — Day 18)*

**Xảy ra:** Version Silver dbt code lỗi làm `fare_vnd` bị nhân đôi cho rides trong khung giờ 18:00–20:00. 50k rows bị sai. Gold đã được build với data sai này.

**Detect:** Great Expectations check `fare_vnd < 500000` (max fare reasonable) thất bại trong batch tiếp theo. Alert Slack `#data-quality`. `gold.late_event_qc` show revenue spike bất thường +100%.

**Rollback:**
```sql
-- Tìm version trước khi bug MERGE chạy
DESCRIBE HISTORY silver.rides;
-- → version 142 là version tốt, version 143 là bug MERGE

-- Restore về trạng thái sạch
RESTORE TABLE silver.rides TO VERSION AS OF 142;

-- Rebuild Gold từ Silver đã sạch
dbt run --select gold.daily_ops_kpi gold.driver_perf --full-refresh

-- Verify
SELECT date, SUM(fare_vnd) FROM gold.daily_ops_kpi
  WHERE report_date = '2026-05-03'
  GROUP BY date;
```

**Thời gian khắc phục:** RESTORE chạy trong giây (Delta chỉ cập nhật `_delta_log`, không copy file), Gold rebuild ~3 phút. **MTTR < 5 phút** nhờ time travel.

---

### Failure 3 — Yêu cầu xoá dữ liệu theo Decree 13 (Right to Erasure)

**Xảy ra:** Hành khách gửi yêu cầu xoá toàn bộ dữ liệu cá nhân. Deadline theo Decree 13: 72 giờ.

**Identify scope:**
```sql
-- Tìm tất cả row trong Silver có token của passenger này
SELECT ride_id, driver_token, passenger_token
FROM silver.rides
WHERE passenger_token = hmac_sha256(:APP_SECRET, :user_phone);
```

**Execute deletion:**
```sql
-- Xóa physical (không soft-delete) trong Silver và Gold
DELETE FROM silver.rides WHERE passenger_token = :token;
DELETE FROM silver.passengers WHERE passenger_token = :token;
-- Không cần xóa Bronze vì Bronze chỉ có token, không có plaintext PII

-- Force physical removal (bypass time travel cho table này)
VACUUM silver.rides RETAIN 0 HOURS;  -- cần SET spark.databricks.delta.retentionDurationCheck.enabled = false
```

**Audit trail:**
```sql
INSERT INTO silver.pii_audit_log VALUES (
  :request_id, 'DELETE', :passenger_token, current_timestamp(), 'decree13_erasure'
);
```

**Lưu ý:** Sau `VACUUM RETAIN 0 HOURS`, time travel về version trước xóa sẽ fail với "file not found" cho row đó — đây là intended behavior để tuân thủ pháp lý.

---

### Failure 4 — Late events từ tỉnh xa làm Gold KPI sai cho ngày hôm qua

**Xảy ra:** 3000 rides ở Hà Giang hoàn thành lúc 23:50 ngày T nhưng event đến Bronze lúc 01:30 ngày T+1 (30 phút sau midnight, do sync batch của tài xế). Gold đã được build cho ngày T với thiếu 3000 rides này.

**Detect:** `gold.late_event_qc` so sánh `rides_silver_count` với `rides_gold_count` cho từng `city_code × ride_date` → alert khi diff > 0.5%.

**Fix:** Gold `daily_ops_kpi` dùng dbt incremental model với `unique_key = ['city_code', 'report_date']`:
```sql
{{ config(materialized='incremental', unique_key=['city_code','report_date']) }}
SELECT city_code, ride_date AS report_date, COUNT(*) AS rides_completed, SUM(fare_vnd) AS revenue
FROM silver.rides
WHERE status = 'COMPLETED'
{% if is_incremental() %}
  AND ride_date >= dateadd('day', -2, current_date)  -- reprocess 2 ngày để catch late events
{% endif %}
GROUP BY city_code, ride_date
```

Chạy lại dbt cho `report_date = T` → MERGE vào Gold cập nhật đúng số liệu. Dashboard tự động refresh.

---

## 5. Ước lượng Chi Phí (Back-of-Envelope)

### Data volume

```
100M rides/năm = 274K rides/ngày
CDC event size: ~3 KB/event (before + after image, JSON)
→ 274K × 3 KB = 822 MB/ngày raw CDC into Bronze

Overhead (metadata, compaction overhead): × 1.5 = 1.2 GB/ngày Bronze

Silver (parse + deduplicate): ~40% của Bronze = 480 MB/ngày
Gold (aggregated): ~5 MB/ngày (daily KPI per city/driver)
```

### Storage cost (AWS S3, ap-southeast-1 Singapore)

| Tier | Volume | Retention | Monthly cost |
|---|---|---|---|
| Bronze (S3 Standard) | 1.2 GB/ngày × 30d = 36 GB | 30 ngày | $0.83 |
| Bronze (S3-IA) | 1.2 GB/ngày × 335d = 402 GB | 30d–1 năm | $5.08 |
| Bronze (Glacier) | ~12 TB historical | > 1 năm | $2.40/tháng |
| Silver (S3 Standard) | 480 MB/ngày × 90d = 43 GB | 90 ngày | $0.99 |
| Silver (S3-IA) | 480 MB/ngày × 275d = 132 GB | 90d–1 năm | $1.67 |
| Gold (S3 Standard) | 5 MB/ngày × 365d = 1.8 GB | Mãi mãi | $0.04 |
| **Storage total** | | | **~$11/tháng** |

### Compute cost

| Component | Spec | Cost |
|---|---|---|
| Spark Streaming (Bronze→Silver) | EMR `m5.xlarge` × 3 nodes, ~2h/ngày active | $35/tháng |
| dbt + DuckDB (Gold build) | Lambda/ECS task, 5 min/run × 12 run/ngày | $8/tháng |
| Kafka (MSK) | 3 broker `kafka.m5.large` | $150/tháng |
| Lakekeeper catalog | EC2 `t3.small` | $15/tháng |
| Metabase (dashboard) | EC2 `t3.medium` | $30/tháng |
| **Compute total** | | **~$238/tháng** |

### Grand total

**~$250/tháng** cho 100M rides/năm = **$0.000030/ride** hay $2.50/1M rides.

*So sánh:* Cost tương đương 1 SMS OTP gửi cho tài xế → rất hợp lý. Nếu team muốn cut cost, Kafka (MSK $150) là target lớn nhất — có thể dùng Redpanda self-hosted giảm xuống ~$30.

---

## 6. MVP — Slice 1 Tuần Nhỏ Nhất

Mục tiêu MVP: **chứng minh Late-event MERGE + PII tokenization hoạt động end-to-end**, không phải build đủ tất cả components.

| Ngày | Task | Deliverable | Risk |
|---|---|---|---|
| D1 | Setup MinIO local + Kafka single-broker + fake Oracle CDC generator (Python script) | 10k fake ride events trong Kafka | 🟢 Thấp |
| D2 | Bronze ingest job: Kafka consumer + PII tokenizer + write Delta | `bronze.cdc_events` có data, token thay plaintext | 🟢 Thấp |
| D3 | Silver MERGE job: parse JSON → structured rides table + MERGE guard | Silver có rides với `event_ts` guard; dedup hoạt động | 🟡 Trung bình |
| D4 | **Late event demo:** inject 500 rides "từ ngày hôm qua" vào Kafka, chạy MERGE, verify Gold update đúng | Screenshot trước/sau MERGE; time travel history | 🟡 Trung bình |
| D5 | Gold dbt model (daily_ops_kpi) + DuckDB query <1s | Query cost tenant, rides/city chạy < 1s | 🟢 Thấp |
| D6 | Decree 13 deletion test: xóa 1 passenger token, VACUUM, verify không còn trong bất kỳ tầng nào | Deletion proof document + audit log | 🔴 Cao (VACUUM cẩn thận) |
| D7 | Buffer: write-up, PoC notebook cleanup | `ARCHITECTURE.md` + `poc/` notebook | 🟢 Thấp |

**Cái MVP KHÔNG làm:**
- Great Expectations (thêm sau khi core pipeline stable)
- OpenLineage/Marquez (nice-to-have, không critical path)
- S3 lifecycle rules (cấu hình 10 phút, để sau)
- Multi-region (scope creep)

**Cái MVP PHẢI làm:** Late-event MERGE + time travel demo là "proof of architecture" — đây là phần khó nhất và phần distinguish project này với simple ETL. Nếu MERGE guard hoạt động, phần còn lại là engineering tốt, không phải rocket science.

---

## Self-checklist trước nộp

| Dimension | Status |
|---|---|
| ≥ 5 quyết định với alternatives bị loại và trade-off reasoning cụ thể | ✅ 6 quyết định |
| Scale/latency/budget figures xuyên suốt, math kiểm tra được | ✅ $250/tháng breakdown, 274k rides/ngày |
| ≥ 4 Day 18 concepts áp dụng (không chỉ name-drop) | ✅ Medallion, ACID MERGE, time travel (Failure 2), CDF, Z-ORDER, deletion vectors, lineage (audit log), FinOps tiering |
| ≥ 3 failure modes với detection + rollback cụ thể | ✅ 4 scenarios, Failure 2 trực tiếp dùng `RESTORE VERSION AS OF` |
| PoC chạy được từ clean checkout, demo phần khó | ✅ `poc/late_merge_tokenize_demo.ipynb` |

---

*"Kiến trúc này không tối ưu ở mọi chiều. Nó tối ưu cho constraint cụ thể: team nhỏ, tuân thủ Decree 13, 60-giây SLA, late events thường xuyên. Khi scale 10×, Kafka → Pulsar, Spark → Flink, DuckDB → Trino là con đường rõ ràng — nhưng đó là bài toán cho ngày 100M/tháng, không phải 100M/năm hôm nay."*
