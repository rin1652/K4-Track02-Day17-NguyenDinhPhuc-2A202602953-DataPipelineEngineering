# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:**
**Repo:**
**Commit bài nộp:**
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):**
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` chứa nhiều hàng cho cùng `ticket_id` thay vì một trạng thái duy nhất. Test `test_silver_tickets_one_row_per_ticket` và `test_silver_tickets_latest_state_wins` bị fail. | `gold_feature_daily` tính thiếu event đến muộn (ví dụ event xảy ra ngày 12 nhưng được ingest ngày 15). Giá trị tính theo ngày `event_date` không khớp với full-recompute. | T-97 đã bị xoá ở nguồn nhưng vẫn xuất hiện trong snapshot và chunks (vẫn còn nguyên). Test `test_cdc_delete_becomes_tombstone` và các test liên quan bị fail. |
| **Nguyên nhân gốc** | Hàm `upsert_silver_tickets` chỉ dùng `INSERT INTO` (append-only) thay vì UPSERT/MERGE, và chưa có cơ chế kiểm tra thứ tự theo `_lsn` để ngăn thay đổi cũ ghi đè thay đổi mới. | Cấu hình `LOOKBACK_DAYS = 0` nên khi xử lý một batch của một ngày, pipeline chỉ recompute partition đúng ngày đó, bỏ qua các event đến trễ thuộc về các ngày trước. | Do thao tác delete (`op = 'd'`), trường `after` bị rỗng (null), làm biểu thức lấy `ticket_id` từ `after` bị null. Bộ lọc `WHERE ticket_id IS NOT NULL` do đó vô tình bỏ qua toàn bộ sự kiện xoá CDC này. |
| **Cách sửa** (file, vài dòng) | Sửa `pipeline/silver.py`: Dùng `DELETE FROM` để xoá trạng thái cũ, theo sau là `INSERT INTO` kèm điều kiện `WHERE NOT EXISTS` kiểm tra `_lsn` để luôn lấy bản ghi mới nhất. | Đổi `LOOKBACK_DAYS = 3` trong `pipeline/config.py` để bao phủ đủ P99 lateness (3.0 ngày). Cửa sổ recompute giờ sẽ lui lại đủ số ngày để gán event muộn về đúng `event_date`. | Sửa `pipeline/staging.py`: Đổi cách lấy `ticket_id` thành `COALESCE(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id')` để nhặt được ID cả khi `after` là null. |
| **Khái niệm trên slide** | Idempotency / Upsert / CDC deduplication | Lateness (Late arriving data) / Event time vs Processing Time / Overwrite partition | CDC Delete vs Kafka Tombstone / Log compaction |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Silver chứa state hiện tại của object nên cần MERGE LSN lớn nhất để update; Gold tính tổng hợp (aggregation) theo thời gian sự kiện, nên overwrite-partition (cộng thêm lookback_days) giúp cập nhật chính xác và quét lại cả các late events.
- Tombstone thay vì xoá hẳn hàng trong Silver: Lưu giữ được LSN của lệnh xóa, đóng vai trò như một "chốt chặn" để ngăn những thay đổi bị trễ (có LSN nhỏ hơn) làm "hồi sinh" lại ticket.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo khả năng tái tạo (reproducibility) của mô hình ML (có thể quay lại lịch sử huấn luyện bất biến), khác với tính toàn vẹn mới nhất cần thiết cho hệ thống RAG.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu đang ở quy mô nhỏ đến vừa, việc xử lý in-process với DuckDB gọn nhẹ, nhanh chóng, không gặp độ trễ (overhead) khởi tạo phân tán quá lớn như Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   **Trả lời:** Chúng ta có thể dùng tính năng Data Masking thông qua Data Catalog / Views để chặn việc query ra dữ liệu cũ của user đã xoá, hoặc chạy một tiến trình compaction/hard-delete ngầm định kỳ (VD: 30 ngày) để xoá hẳn dữ liệu PII ra khỏi lịch sử (để vẫn giữ tính chất reproducible ngắn hạn nhưng tuân thủ quyền xoá).
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   **Trả lời:** Mình sẽ đặt chốt kiểm tra PII (DLP - Data Loss Prevention hoặc chạy mô hình NER nhận dạng tên riêng) ở ngay bước Ingest từ Bronze sang Silver. Đo lường bằng cách viết Data Test (báo cáo % số lượng ticket bị dò rỉ PII) hoặc cách ly (quarantine) chúng để review thủ công trước khi đẩy vào Gold.

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 1.09s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
...                                                                                                                                                                                             [100%]
3 passed, 10 deselected in 0.60s

$ make dbt
19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]

Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.81 seconds (0.81s).

Completed successfully

Done. PASS=19 WARN=0 ERROR=0 SKIP=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.

```text
$ make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
