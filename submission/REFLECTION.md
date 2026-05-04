# Reflection — Day 18 Lab

## Anti-pattern rủi ro nhất: #3 — Small-file problem

**NB2 đo được:** 200 files trước OPTIMIZE, query time 215.8 ms → sau `compact()` + `z_order(["user_id"])` còn 55 files, query time 27.4 ms — **7.9× nhanh hơn**, files-pruned ratio **55×** (1 trong 55 files chứa `user_id=4242`).

Production mới thấy hết nguy cơ. NB4 nhận LLM inference logs qua Bronze → Silver. Nếu streaming ghi micro-batch mỗi 30 giây — pattern hợp lý để đạt 60-giây SLA — thì mỗi giờ sinh 120 files, mỗi ngày 2.880 files. Sau 1 tháng: **86.400 files** trong Silver. Không có OPTIMIZE cron, query "cost/latency 7 ngày" mở hàng nghìn file nhỏ phần lớn không liên quan — đúng lúc sếp hỏi dashboard.

Anti-pattern #1 (data swamp) cũng tiềm ẩn — Bronze lưu `raw_json` string, dễ bị giữ nguyên đến Silver. NB4 đã xử lý đúng: DuckDB parse + dedup (200.000 → 190.052 rows, drop 9.948 duplicate).

**Fix:** `OPTIMIZE` daily 03:00 (off-peak), `target_size=256MB`, Z-ORDER theo `model` và `user_id`.
