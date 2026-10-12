# REPORT — Lab Day 9

| | |
|---|---|
| Họ và tên | Đặng Hữu Tâm |
| MSSV | `2A202602940` |
| GitHub | `tam253211-a11y` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-06667` |


## Kết quả cuối

- `dataset_version` của bạn: `a55001de6327`  (khớp `report/evidence/run_summary.json`, lượt `step all --reset` cuối)
- Số khung hình train / val / test của bạn: `654 / 196 / 169` (tổng 1019; số trip 18 / 4 / 4)
- `report/verify.txt`: `27/27 checks passed`. Trên Windows, `check_submission.py --save-verify` không gọi được `scripts/lab.sh` (Python của Windows không chạy được file `.sh`), nên file được tạo bằng chính lệnh mà script đó gọi: `bash scripts/lab.sh verify > report/verify.txt`.
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, ảnh bài tập sập `fail_task_log.png` (log task đỏ thấy `trip-06667`), `airflow_fail_graph.png`, `airflow_retry_graph.png` (xanh sau khi clear), cùng ảnh thêm `minio_gold_dataset.png`. Log từng bước nằm trong `report/run_logs/`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt thấy trong 2 dòng đầu của `bronze_raw_telemetry_a` (3021 dòng) và `bronze_raw_telemetry_b` (3234 dòng):

| | A | B |
|---|---|---|
| Thời gian | `ts_utc` chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`) | `timestamp_ms` epoch mili giây (`1759933801509`) |
| Tốc độ | `speed_mps` (m/s, `1.213`) | `spd_kmh` (km/h, `55.11`) |
| Bàn đạp | `gas`, `brake` tỷ lệ 0–1 (`0.028`) | `throttle_pct`, `brake_pct` theo % (`2.0`) |
| Tên cột khác | `heading_deg`, `lat/lon/alt_m`, `acc_x/acc_y`, `steer` | `hdg`, `latitude/longitude/altitude`, `ax/ay`, `steering` |
| Đồng hồ | `telemetry_clock_offset_ms = 0` | `telemetry_clock_offset_ms = 1500`: mẫu đầu `1759933801509` = 14:30:01.509 UTC, chậm 1,5 s so với video bắt đầu 14:30:00 |

Ở bronze, `typeof(speed_mps)` và `typeof(spd_kmh)` đều là `VARCHAR`, và mỗi bảng có 30 ô tốc độ trống vẫn được giữ nguyên. Lý do không gộp và ép kiểu ngay:
- Hai schema khác nhau. Gộp ở bronze buộc phải chọn luật chuyển đổi ngay lúc nạp; nếu luật sai (quên chia 3,6, quên trừ 1500 ms) thì bản gốc mất luôn. Giữ nguyên bản cho phép sửa luật ở silver rồi chạy lại mà không phải ingest lại.
- Ép kiểu lúc nạp sẽ làm hỏng hoặc rơi mất dòng xấu (ô trống, giá trị lạ) trước khi ai kịp đếm. Để dạng chữ thì mọi dòng đều vào bronze (đúng 3021/3234 = số dòng CSV), còn việc phân loại lỗi để silver làm và ghi lý do vào `rejects`.
- Bronze chỉ thêm, có `_source_file`, `_batch_id`, `_row_no`, nên mỗi dòng silver truy ngược được về đúng dòng CSV gốc.

Chạy lại: lần nạp thứ hai báo `video: already present, telemetry_rows: already present` vì file được nhận diện bằng sha256 (`636ba1a9…` cho A, `bfa62703…` cho B). `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (`version 0, WRITE, rows_added 3021`), nên nạp hai lần không nhân đôi dữ liệu.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai model staging đưa cả hai nguồn về cùng các cột `ts` (timestamptz UTC), `speed_mps`, `heading_deg`, `lat/lon/alt_m`, `acc_x/acc_y`, `gas/brake` (0–1), `steer`:
- `stg_telemetry_a.sql` chỉ ép kiểu, vì A đã dùng m/s, ISO UTC và lệch 0.
- `stg_telemetry_b.sql` làm 4 việc: đổi tên cột; `spd_kmh / 3.6` để ra m/s; `throttle_pct / 100` và `brake_pct / 100`; `to_timestamp((timestamp_ms - offset_ms) / 1000.0)`, trong đó `offset_ms` được join từ `bronze.video_files` (1500 ms, khai báo trong `sources.json`) chứ không viết cứng.

Bằng chứng trong đầu ra bước 2:
- `bat_dau` của B là `2025-10-08 14:30:00.009`, khớp video bắt đầu lúc 14:30:00. Trước khi trừ, mẫu đầu là 14:30:01.509.
- Tốc độ tối đa sau chuẩn hóa: A 71 km/h, B 100 km/h. Cả hai đều hợp lý; nếu B quên chia 3,6 thì sẽ ra khoảng 360 km/h.
- Kết quả verify module 2: 7/7.

| drive | duplicate | speed_missing | speed_out_of_range | tổng cách ly | dòng sạch | sạch + cách ly |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 = số dòng bronze |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 = số dòng bronze |

Tổng cộng `silver.telemetry` có 6149 dòng và `silver.telemetry_rejects` có 106 dòng. Dòng lỗi vào `rejects` thay vì bị xóa vì:
1. Phải đối soát được: sạch + cách ly = nhận về. Nếu xóa im lặng thì không ai biết đã mất 53 dòng mỗi drive.
2. Mỗi dòng giữ lý do, `_batch_id` và `_row_no`. Ví dụ 3 dòng `speed_out_of_range` của A đều có giá trị `250.0` m/s ở `_row_no` 400, 1700, 2500, nên truy ngược được về đúng dòng CSV để báo cho đội phần cứng.
3. Nếu luật làm sạch sai (ngưỡng quá chặt), vẫn còn dữ liệu để phục hồi mà không phải ingest lại.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

**`max_gap_ms` là warn.** Trong `schema.yml` test này có `severity: warn` kèm comment *"sensor dropouts are reported, not fatal"*. Khoảng trống hơn 1 s là cảm biến mất tín hiệu thật: mỗi drive có đúng 1 khoảng như vậy (A từ 08:01:59.116 đến 08:02:02.189, dài 3073 ms; B dài 3124 ms). Khoảng này trùng khít với 30 dòng `speed_missing` liên tiếp đã bị cách ly. Dữ liệu còn lại không sai, chỉ thiếu một đoạn. Bước 3 đã tự xử lý: 6 frame mỗi drive rơi vào khoảng trống có `sync_gap_ms` từ 384 đến 2884 ms (A), vượt `sync_tol_ms = 150`, nên bị đánh mất đồng bộ và loại khỏi gold. Dừng cả pipeline vì một lần mất tín hiệu là quá tay, nên chỉ cần báo để người vận hành theo dõi.

**`in_range` (0..70 m/s) là error** (mặc định của dbt). Tốc độ ngoài khoảng này là **sai giá trị**, không phải thiếu. Nó sẽ thành nhãn sai trong dataset huấn luyện, nên bắt buộc phải chặn.

**Thí nghiệm `break-gate`** (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 ở bước làm sạch, nên 6 dòng `250.0` m/s (3 dòng ở mỗi drive) không còn bị cách ly mà lọt vào `silver.telemetry`. Kết quả:
- Khi nới luật: `dbt test` ra `PASS=5 WARN=1 ERROR=1`, `gate_failed: true`. Test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**.
- Sau khi khôi phục: `PASS=6 WARN=1 ERROR=0`.

Ý nghĩa: luật làm sạch và cổng chất lượng là **hai lớp độc lập**. Khi một người sửa luật làm sạch sai, test `error` trên silver vẫn chặn trước khi frames và gold dùng tới dữ liệu đó. Trong Airflow, task `dbt_test` sẽ đỏ và các task phía sau không chạy. Nếu `in_range` chỉ là `warn` thì 6 dòng 900 km/h đã đi thẳng vào dataset.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm chạy lại partition `trip-00003` (60 khung) hai lần:

| | số dòng `silver.frames` | dấu vân tay |
|---|---|---|
| Trước | 1246 | `6c7480dfde7e` |
| Sau 2 lần chạy lại (overwrite theo `trip_id`) | **1246** | **`6c7480dfde7e`** (`unchanged: true`) |
| Mô phỏng `append` chạy 2 lần | trip có 60 khung thành **120 dòng** (gấp đôi) | — |

`cli history silver.frames` cho thấy 2 commit mới (version 26 và 27), mỗi commit `rows_added: 60` với `predicate: trip_id = 'trip-00003'`. Mỗi lần ghi **thay thế** đúng 60 dòng của trip đó, không cộng thêm.

Vì sao overwrite theo partition an toàn hơn "xóa rồi ghi lại":
- **Nguyên tử.** Delta ghi file Parquet mới trước, sau đó một commit duy nhất vào `_delta_log` vừa đánh dấu file cũ của trip là đã bỏ, vừa thêm file mới. Nếu tiến trình chết giữa chừng (như bài tập sập ở bước 5) thì commit chưa xảy ra: người đọc vẫn thấy đủ 60 dòng cũ, file dở dang chỉ là rác, không ai đọc tới.
- Với "DELETE rồi INSERT" (hai giao dịch), nếu sập sau DELETE thì trip đó **mất trắng**, và bước gold chạy lúc đó sẽ ra dataset thiếu trip mà không ai biết.
- **Phạm vi hẹp.** Predicate `trip_id = ...` chỉ đụng tới một trip, 25 trip còn lại không bị ghi lại. Vì vậy Airflow chạy lại được một task `silver_frames` riêng lẻ, và chạy bao nhiêu lần kết quả cũng như nhau.
- **Time travel.** Version cũ vẫn còn, nên có thể so sánh hoặc quay lại nếu lần ghi mới sai.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 / 602 | 8 / 644 |
| 2 (đang dùng) | 149 / 602 | 53 / 644 |
| 6 | 363 / 602 | 244 / 644 |

**Đen và tối khác nhau** (bước 3, ngưỡng `dark_luma = 12`, `dark_soft = 40` trong `dbt_project.yml`):

| | drive A | drive B | độ sáng trung bình |
|---|---|---|---|
| Khung đen (`is_black`) | 6 | 4 | 0,0 (min = max = 0; độ nét cũng bằng 0) |
| Khung tối được giữ (`is_dark`) | 4 | **61** | A 37,4; B 30,8 (khoảng 12,8–39,6) |

Khung đen là **lỗi thiết bị**: cả ảnh bằng 0, không có thông tin gì, nên bị loại khỏi gold. Khung tối là **điều kiện thật** (đường hầm, bóng râm ở B): ảnh vẫn có nội dung (blur_score trung bình 333, so với 0 của khung đen), chỉ thiếu sáng. Pipeline gắn cờ `is_dark` nhưng **giữ** khung tối. Mình đồng ý: nếu bỏ khung tối thì model không bao giờ được học cảnh hầm hay ban đêm, đúng những lúc xe cần nó nhất. Cờ `is_dark` vẫn cho phép lọc hoặc cân bằng lại khi train nếu cần.

**Ngưỡng trùng.** Mình chọn **2** (giữ nguyên ngưỡng của pipeline):
- Ngưỡng 0 chỉ bắt khung giống hệt từng bit: 61 khung ở A, mới khoảng 10%. Nó bỏ sót camera đứng hình vì nhiễu nén vẫn làm lệch vài bit (verify chỉ bắt 11/12 khung đứng hình ở ngưỡng 2).
- Ngưỡng 6 loại 363/602 khung ở A (60%) và 244/644 ở B (38%). Khi đó bắt đầu loại cả những khung khác nhau thật, chẳng hạn cảnh đường cao tốc ít chi tiết. dHash chỉ có 64 bit, nên 6 bit là gần 10% ảnh.
- Ngưỡng 2 bắt hết phần lớn khung đứng hình, chỉ loại khoảng 25% (A) và 8% (B).

**Phụ thuộc vào tốc độ** (đo trên `silver.frame_telemetry`, ngưỡng 2):

| tốc độ | drive A: tỷ lệ khung trùng | drive B |
|---|---|---|
| < 2 m/s (gần dừng) | **59,3%** (54/91) | không có |
| 2–8 m/s | 11,3% | 0% |
| 8–15 m/s | 14,5% | 2,5% |
| ≥ 15 m/s | 23,3% | 10,0% |

Xe gần dừng thì 2 khung cách nhau 0,5 s gần như giống hệt, nên trùng nhiều là đúng. Hai chuỗi trùng dài nhất ở A là `trip-08888` (23 khung liên tiếp, 0,1 m/s) và `trip-02086` (16 khung, 0,2 m/s), cả hai đều là xe đứng yên. Khi xe chạy nhanh, khung trùng là dấu hiệu lỗi: `trip-03904` có 11 khung trùng liên tiếp trong khi xe chạy 12,3 m/s, chính là camera **đứng hình** khoảng 6 s mà verify bắt được 11/12. Ngoài ra `trip-04622` có 9 khung trùng ở 17,9 m/s, nhiều khả năng là đoạn đường cao tốc ít chi tiết. Ngưỡng tốt nên **phụ thuộc tốc độ**: chặt hơn (0–1 bit) khi xe chạy nhanh, vì cảnh phải thay đổi nhiều, khung nào trùng gần như chắc chắn là lỗi; nới hơn (2–4 bit) khi xe dừng, để bớt các khung thừa không thêm thông tin. Một cách khác là lấy mẫu theo quãng đường thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Có 1019 khung đáng dùng, thuộc 26 trip.

| Cách chia | số trip nằm ở hơn 1 split | số trip có khung ở **cả train lẫn test** |
|---|---|---|
| Theo khung hình (ngẫu nhiên 70/15/15) | **26/26** | **26/26** |
| Theo trip (gold của pipeline) | **0** | **0** |

Chia ngẫu nhiên theo khung thì *mọi* trip đều bị xé ra: mỗi trip có khoảng 40–60 khung, nên gần như chắc chắn có vài khung rơi vào test. Pipeline của mình chia nguyên trip: test gồm `trip-00003`, `trip-02134` (drive A) và `trip-05297`, `trip-09209` (drive B); val gồm `trip-04868`, `trip-09608`, `trip-03365`, `trip-05905`. Không trip nào xuất hiện ở 2 split, và test dbt chống rò rỉ đều PASS.

Vì sao điểm đẹp giả tạo: hai khung liền nhau chỉ cách 0,5 s, cùng con đường, cùng ánh sáng, cùng xe phía trước. Ở bước 3, có tới 149/602 khung của A lệch dHash ≤ 2 bit so với khung trước. Nếu khung t nằm ở train và khung t+1 nằm ở test thì model chỉ cần **nhớ** cảnh, không cần **tổng quát hóa**, vẫn đoán đúng. Điểm test khi đó đo khả năng nhận lại cảnh đã thấy, chứ không đo khả năng lái ở con đường mới. Ra đường thật, độ chính xác sẽ tụt so với con số báo cáo. Chia theo trip (theo cảnh) giữ cho test là những cảnh model chưa từng thấy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Trong `pipeline/publish.py`, version là 12 ký tự đầu của `sha256` trên danh sách `frame_id|split` của mọi khung trong gold. Nó chỉ phụ thuộc vào **khung nào được dùng và nằm ở split nào**.

- **Lý do không dùng ngày giờ hay số đếm:** cùng một dataset chạy lại sẽ ra version mới, dù nội dung không đổi. Khi đó không phân biệt được "dữ liệu đổi thật" với "chỉ chạy lại". Ngược lại, hai máy cùng chạy có thể ra cùng một số đếm cho hai nội dung khác nhau. Băm nội dung thì **cùng nội dung ra cùng version, khác nội dung ra khác version**. Nhờ đó so sánh được kết quả giữa hai mô hình, và giảng viên chạy lại có thể đối chiếu.
- **Hai lần chạy liên tiếp:** `step 4` ra `a55001de6327`, chạy `cli export-dataset` lại vẫn ra `a55001de6327` (1019 khung: train 654, val 196, test 169), dù `created_at` trong manifest đã đổi. Sau bài tập sập ở bước 5 và lần chạy `step all --reset` cuối, version cũng vẫn giữ nguyên.
- **Liên hệ MSSV:** `gold_dataset_frames.sql` xếp trip trong mỗi drive theo `md5(trip_id || LAB_STUDENT_ID)`, rồi lấy 70% đầu làm train, 15% làm val, phần còn lại làm test. Mình thử đổi MSSV thành `2A202602941`: version thành `a3eff0b109cd`, số khung train/val/test thành 671/154/194 (thay vì 654/196/169), trong khi số trip mỗi split vẫn là 18/4/4 và tổng vẫn 1019 khung. Khôi phục `2A202602940` thì version về lại `a55001de6327`. Vậy dữ liệu giống nhau nhưng MSSV khác dẫn tới thứ tự trip khác, split khác, version khác. Đây là cách giảng viên kiểm tra bài có đúng do MSSV này chạy hay không.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/a55001de6327/manifest.json` có:

```json
"lineage": {
  "bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
  "silver.frames": 27, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc" } },
"rules": {"drop": ["is_black","is_blurry","is_near_dup","not sync_ok"], "split": "per trip, stratified by drive, md5 order"}
```

Dataset `a55001de6327` (1019 khung: drive A 436, drive B 583) được tạo từ:
- Hai video gốc có đúng hai sha256 ở trên.
- Telemetry thô A và B ở **version 0** của bảng bronze.
- Bảng `video_files` ở **version 1** (chứa offset 1500 ms của B).
- `silver.frames` ở **version 27**. Số version cao vì 26 lần ghi theo partition trip, cộng thêm 2 lần chạy lại `trip-00003` trong thí nghiệm idempotent, nên version của bảng **khác** với dữ liệu: nội dung vẫn y như version 25, chỉ lịch sử dài hơn. Ở lượt `step all --reset` cuối (`report/run_logs/manifest_final.txt`), lineage ghi `silver.frames: 25`, còn các mục khác giữ nguyên. `dataset_version` vẫn là `a55001de6327`, và `manifest_sha256` vẫn là `bb6993ca…5135`. Điều này cho thấy version của bảng ghi *lịch sử thao tác*, còn `dataset_version` ghi *nội dung*.
- Các bảng silver dbt ở version 0.
- Luật loại khung: đen, mờ, trùng, mất đồng bộ.

3 tháng sau, để tái tạo đúng dataset này cần:
1. Hai file video khớp sha256, cùng `telemetry_a.csv`/`telemetry_b.csv`/`trips.csv`/`sources.json` (đã kiểm bằng `data/MANIFEST.json`).
2. Mã pipeline ở cùng commit git, gồm cả các ngưỡng trong `dbt_project.yml`: `dark_luma=12`, `blur_var=40`, `dup_hamming=2`, `sync_tol_ms=150`, `split 0.70/0.15`.
3. **`LAB_STUDENT_ID = 2A202602940`**, vì nó quyết định split.

Chạy lại sẽ phải ra đúng `a55001de6327`. Nếu ra version khác thì biết ngay là một trong ba thứ trên đã đổi. Ngoài ra, các version Delta trong lineage cho phép đọc lại bảng silver đúng thời điểm (time travel), miễn là chưa `VACUUM`.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Cách làm: đặt `LAB_FAIL_ONCE=trip-06667` trong `.env`, chạy `docker compose up -d`, trigger `day09_reset` rồi trigger `day09_video_pipeline` (run `run2_sap`).

**Khi sập** (37 task instance trong run):
- **1 task đỏ**: `silver_frames` có map_index **18**. Log `attempt=1` có dòng `RuntimeError: simulated crash while extracting trip-06667; run it again`.
- **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`.
- **29 task success**: `bronze_drive_a`, `bronze_drive_b`, `bronze_trips`, `trip_commands` và 25 task `silver_frames` của các trip khác.

Như vậy dbt và gold **không chạy** trên silver thiếu 1 trip, nên không sinh ra dataset thiếu dữ liệu mà không ai biết.

**Chạy lại:** chỉ clear task đỏ cùng các task phía sau của nó (`airflow tasks clear -t '^silver_frames$' -d --only-failed`, tương đương nút *Clear task* trên giao diện). Kết quả:
- Chỉ `silver_frames[18]` có log `attempt=2`.
- 25 trip đã thành công **không chạy lại**.
- 7 task phía sau chạy lần đầu.
- Run chuyển xanh: **37/37 success**.

**Dữ liệu:**
- `lake.silver_frames` có **1246 dòng / 1246 frame_id khác nhau**, bằng đúng lúc chưa sập, không nhân đôi.
- `trip-06667` có 29 khung, ghi bằng đúng một commit Delta (`version 25, rows_added 29, predicate trip_id = 'trip-06667'`).
- Lần sập xảy ra *trước* khi ghi, nên không để lại dữ liệu dở dang.
- `scripts/lab.sh verify` cho **27/27**, `dataset_version` vẫn là **`a55001de6327`** (1019 khung, 654/196/169).

Bằng chứng gồm:
- Ảnh `report/screenshots/fail_task_log.png` (log đỏ thấy `trip-06667`).
- Ảnh `airflow_fail_graph.png` (silver_frames đỏ, các task sau màu cam).
- Ảnh `airflow_retry_graph.png` (xanh sau khi clear).
- Các log `report/run_logs/fail_task_attempt1.log` và `verify_after_retry.log`.

Sau bài tập mình đã xóa `LAB_FAIL_ONCE` khỏi `.env` và áp dụng lại Compose.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

**Quy mô:** lab hiện có 2 drive × khoảng 5 phút = 10 phút video, 1 camera, ra 1246 khung. Slide 38 dùng đúng kịch bản "500 xe, 8 camera mỗi xe, lô + thời gian thực". Theo slide 9, video đã mã hóa khoảng 36 GB/giờ cho mỗi xe. Vậy 500 xe × 8 giờ/ngày là khoảng **144 TB video mỗi ngày**. Chỉ tính 1 camera thì cũng đã gấp khoảng **24 000 lần** lab, tức khoảng 29 triệu khung/ngày ở 2 fps. Ba thành phần vỡ trước:

1. **Decode video bằng FFmpeg trong một container Airflow** (`silver_frames`, 1 task cho mỗi trip, CPU của một máy, `mem_limit` 2,5 GB). Slide 24 chỉ ra decode là nút cổ chai của xử lý khung hình. Ở lab, cả DAG cho 10 phút video đã mất khoảng 2 phút 48 giây (run `run1_xanh`); nhân 24 000 lần là hàng chục ngày máy cho mỗi ngày dữ liệu. **Thay bằng:** xử lý video phân tán bằng **Ray** (ô 7 "Xử lý video nặng" ở slide 38; Ray Data ở slide 24), decode trên GPU bằng **NVIDIA DALI**, chỉ decode các khung cần lấy mẫu. Kết hợp với **trigger lọc ngay trên xe** (slide 31) để chỉ đoạn đáng xem mới được tải lên.
2. **DuckDB + dbt trên một file `lakehouse.duckdb`.** Slide 22 nói ELT nạp trước rồi biến đổi trong lakehouse, video nằm yên và chỉ metadata đi qua bảng. Hướng đó vẫn đúng, nhưng engine một máy, một tiến trình ghi của lab không chạy nổi join ASOF trên hàng tỷ dòng telemetry mỗi ngày. Slide 23 chọn engine theo độ trễ: **Spark** cho batch hoặc micro-batch "tính lại cả ngày", **Flink** cho cảnh báo telemetry per-event trong vài giây, **dbt** cho SQL theo lịch. **Thay bằng:** Spark (ô 5 "Batch/ELT", slide 38) đọc thẳng bảng **Delta** trên S3; Flink (ô 6) cho luồng telemetry thời gian thực; dbt vẫn giữ làm lớp mô hình và test, nhưng chạy trên Spark. Xử lý **tăng dần** theo partition `ngày/xe` thay vì `--reset` toàn bộ.
3. **Ingest, điều phối và lưu trữ một node.** Lab đọc CSV và MP4 từ thư mục `data/`. Airflow LocalExecutor với dynamic mapping, nên 26 task hôm nay sẽ thành hàng chục nghìn mapped task mỗi ngày. MinIO một node phải chứa hàng chục triệu file JPEG nhỏ. **Thay bằng:**
   - Telemetry liên tục đi qua **Kafka** (slide 13–14: streaming, độ trễ cỡ giây).
   - Video upload thẳng **S3**, queue chỉ mang sự kiện nhỏ (slide 14).
   - **Airflow** chỉ điều phối ở mức `ngày × xe`, chạy trên executor phân tán.
   - Object storage là S3/GCS, khung ảnh gom thành shard thay vì mỗi khung một file.

**Slide 12 (vòng lặp hỏng ở bốn chỗ)** cũng áp dụng khi lên quy mô:
- "Không đo được": các bộ đếm của lab (quarantine, frame funnel, verify 27/27) phải chuyển thành kiểm tra tự động trên mọi partition, bằng **Great Expectations** (ô 9 ở slide 38) hoặc dbt tests.
- "Leakage": vẫn chia theo trip/cảnh như gold hiện tại.
- "Không ai sở hữu": cần **catalog và lineage** (DataHub, ô 10) để biết bảng nào thuộc ai.
- Version dataset: chuyển từ `manifest.json` sang **lakeFS** (ô 11).

Những gì **giữ nguyên được**: medallion, cách ly có lý do, overwrite theo partition (idempotent, slide 26), chia theo cảnh (slide 33), `dataset_version` bằng băm nội dung và lineage (slide 29–30). Đó là các nguyên tắc, không phụ thuộc quy mô.

## Metabase

Câu SQL thêm vào dashboard và điều nó cho biết:

Hai câu hỏi đã được thêm vào cuối dashboard **"Day 9: video data pipeline"** (`http://localhost:3000/dashboard/2`), xem `report/screenshots/metabase_dashboard.png`.

**Câu hỏi riêng 1: tỷ lệ khung bị loại theo trip** (trip nào hao hụt nhiều nhất, và vì sao)

```sql
select trip_id, drive_id, split, frames as khung_trich, frames_kept as khung_giu,
       frames - frames_kept as khung_loai,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_loai,
       round(avg_speed_kmh::numeric, 1) as toc_do_tb_kmh
from marts.mart_trip_summary
order by pct_loai desc
```

Kết quả: 5 trip hao hụt nhiều nhất **đều thuộc drive A**:
- `trip-04622`: 56,7% (34/60)
- `trip-03904`: 50,0%
- `trip-02086`: 46,7%
- `trip-08888`: 40,0%
- `trip-07774`: 38,2%

Đối chiếu với bước 3, có hai nguyên nhân khác nhau:
1. **Xe đứng yên.** `trip-08888` chạy trung bình 6,5 km/h, `trip-02086` 17,6 km/h, đều có chuỗi khung trùng dài (23 và 16 khung) khi xe dừng. Loại các khung này là **đúng**, vì chúng không thêm thông tin.
2. **Lỗi camera.** `trip-03904` chạy 46 km/h nhưng có 11 khung trùng liên tiếp, đó là đoạn **camera đứng hình**. Loại các khung này cũng đúng, nhưng là dấu hiệu cần báo cho đội phần cứng.

`trip-04622` (67 km/h, bị loại 57%) là trường hợp đáng xem lại: xe chạy nhanh mà vẫn mất hơn nửa số khung, nhiều khả năng do cảnh cao tốc ít chi tiết khiến dHash ở ngưỡng 2 coi các khung là trùng. Đây là ứng viên tốt để thử ngưỡng trùng phụ thuộc tốc độ như đề xuất ở Câu 5.

Ngược lại, trip của bài tập sập `trip-06667` chỉ bị loại 6,9% (2/29): chạy lại không làm mất hay nhân đôi khung nào.

**Câu hỏi riêng 2 (bonus, câu hỏi vận hành): lý do loại khung theo drive, tính theo %**

```sql
select drive_id, frames_total as tong,
       round(100.0 * dropped_near_dup    / frames_total, 1) as pct_trung,
       round(100.0 * dropped_blurry      / frames_total, 1) as pct_mo,
       round(100.0 * dropped_black       / frames_total, 1) as pct_den,
       round(100.0 * dropped_out_of_sync / frames_total, 1) as pct_mat_dong_bo,
       round(100.0 * flagged_dark_kept   / frames_total, 1) as pct_toi_giu_lai,
       frames_kept as giu_lai, round(100.0 * frames_kept / frames_total, 1) as pct_giu_lai
from marts.mart_frame_funnel order by drive_id
```

| drive | tổng | trùng | mờ | đen | mất đồng bộ | tối (giữ lại) | giữ lại |
|---|---|---|---|---|---|---|---|
| drive_a | 602 | **24,8%** | 1,5% | 1,0% | 1,2% | 0,7% | 436 (**72,4%**) |
| drive_b | 644 | 8,2% | 0% | 0,6% | 1,1% | **9,5%** | 583 (**90,5%**) |

Ý nghĩa vận hành: hao hụt của A gấp khoảng 3 lần B, và gần như toàn bộ là do **khung trùng**, không phải do lỗi cảm biến (đen, mờ, mất đồng bộ chỉ khoảng 1% mỗi loại). Muốn tăng dữ liệu cho A, nên đổi cách lấy mẫu (theo quãng đường thay vì 2 khung/giây) thay vì sửa phần cứng. B có 9,5% khung tối được giữ lại (đường hầm), nên cần theo dõi xem các khung tối có rơi đều vào cả train, val và test hay không.

## Nhìn lại

- **Bất ngờ nhất:** cảnh báo `max_gap_ms` (bước 2) và 6 khung `sync_lost` của mỗi drive (bước 3) là *cùng một sự cố*: 30 dòng `speed_missing` liên tiếp, khoảng 3 s mất tín hiệu. Một lỗi nguồn hiện ra ở ba nơi (rejects, test warn, cờ khung); lineage giúp nối chúng lại.
- **Chưa rõ:** khung **đầu tiên** của mỗi drive (`08:00:00.000`, `14:30:00.000`) luôn bị loại vì ASOF join chỉ nhìn **ngược** thời gian (`event_time >= ts`), mà mẫu telemetry đầu đến sau 9–14 ms. Khung đó hợp lệ, chỉ cần join "gần nhất hai phía". Mình chưa rõ việc chỉ nhìn ngược là cố ý (không dùng dữ liệu "tương lai") hay là một hạn chế.
- **Lỗi pipeline chưa bắt:** độ lệch đồng hồ của B chỉ là **một hằng số khai báo** (1500 ms), và verify chỉ kiểm mẫu đầu trong 0,5 s. Nếu đồng hồ thiết bị **trôi dần** (mỗi phút lệch thêm vài chục ms) hoặc khai báo sai, thì `sync_gap_ms` vẫn nhỏ, vì nó chỉ đo khoảng cách tới mẫu telemetry gần nhất, không đo độ đúng của đồng hồ. Không test nào bắt được. Muốn bắt cần đối chiếu một tín hiệu độc lập, ví dụ chuyển động ước lượng từ video (optical flow) so với `speed_mps`.
- Tương tự, `in_range` chỉ bắt giá trị **vô lý** (250 m/s). Giá trị **hợp lý nhưng sai**, như tốc độ đứng yên một con số suốt 20 s khi video cho thấy xe đang chạy, vẫn lọt qua.

## Thí nghiệm riêng (bonus): đổi `sync_tol_ms`

Không sửa file cấu hình: mình chạy `scripts/lab.sh dbt run --select silver_frame_telemetry gold_dataset_frames --vars '{sync_tol_ms: X}'`, đếm gold, rồi chạy lại không có `--vars` để khôi phục mặc định 150 ms (gold về 1019). Log ở `report/run_logs/bonus_sync_tol.log`.

| `sync_tol_ms` | khung `sync_lost` | khung gold | train / val / test | gap lớn nhất trong gold (ms) | gap trung bình (ms) |
|---|---|---|---|---|---|
| 50 | 600 | **538** | 351 / 94 / 93 | 20 | 9,7 |
| 100 | 324 | 762 | 488 / 144 / 130 | 100 | 33,3 |
| **150 (mặc định)** | 14 | **1019** | 654 / 196 / 169 | 120 | 52,9 |
| 300 | 14 | 1019 | 654 / 196 / 169 | 120 | 52,9 |
| 1000 | 11 | 1022 | 654 / 199 / 169 | **884** | 54,6 |
| 3000 | 3 | 1030 | 654 / 207 / 169 | **2884** | 69,7 |

Nhận xét:
- Telemetry khoảng 10 Hz, tức mỗi mẫu cách nhau khoảng 100 ms, và khung lấy ở thời điểm bất kỳ. Vì vậy dung sai < 100 ms loại cả những khung bình thường: ở 50 ms mất **47%** dataset (1019 xuống 538) mà không vì lỗi gì.
- Từ 150 đến 300 ms kết quả **không đổi**: mọi khung bình thường đều có telemetry trong vòng 120 ms, còn 14 khung `sync_lost` là 12 khung trong 2 lần mất tín hiệu cộng 2 khung đầu không có mẫu phía trước. 150 ms nằm đúng trong "vùng ổn định" này.
- Nới lên 1000 hoặc 3000 ms thì gold tăng thêm 3 và 11 khung, nhưng đó chính là các khung trong lúc mất tín hiệu, được gắn telemetry **cũ tới 2,9 s**. Ở tốc độ 20 m/s, như vậy là lệch khoảng 58 m: nhãn tốc độ, góc lái **sai** mà không ai biết.

Kết luận: `sync_tol_ms` nên lớn hơn chu kỳ lấy mẫu telemetry một chút (khoảng 1,5 lần chu kỳ), nhưng nhỏ hơn nhiều so với độ dài một lần mất tín hiệu. 150 ms là hợp lý cho telemetry 10 Hz.
