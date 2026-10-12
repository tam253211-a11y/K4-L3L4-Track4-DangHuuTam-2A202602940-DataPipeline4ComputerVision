# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Đặng Hữu Tâm |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602940` |
| GitHub | `tam253211-a11y` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

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

**`max_gap_ms` là warn.** Trong `schema.yml` test này có `severity: warn` kèm comment *"sensor dropouts are reported, not fatal"*. Khoảng trống hơn 1 s là cảm biến mất tín hiệu thật: mỗi drive có đúng 1 khoảng như vậy (A dài 3073 ms, B dài 3124 ms). Dữ liệu ở đó không sai, chỉ thiếu. Bước sau đã tự xử lý: frame rơi vào khoảng trống không ghép được telemetry trong dung sai nên bị đánh `sync_lost` và loại khỏi gold. Dừng cả pipeline vì một lần mất tín hiệu là quá tay, nên chỉ cần báo để người vận hành theo dõi.

**`in_range` (0..70 m/s) là error** (mặc định của dbt). Tốc độ ngoài khoảng này là **sai giá trị**, không phải thiếu. Nó sẽ thành nhãn sai trong dataset huấn luyện, nên bắt buộc phải chặn.

**Thí nghiệm `break-gate`** (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 ở bước làm sạch, nên 6 dòng `250.0` m/s (3 dòng ở mỗi drive) không còn bị cách ly mà lọt vào `silver.telemetry`. Kết quả:
- Khi nới luật: `dbt test` ra `PASS=5 WARN=1 ERROR=1`, `gate_failed: true`. Test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**.
- Sau khi khôi phục: `PASS=6 WARN=1 ERROR=0`.

Ý nghĩa: luật làm sạch và cổng chất lượng là **hai lớp độc lập**. Khi một người sửa luật làm sạch sai, test `error` trên silver vẫn chặn trước khi frames và gold dùng tới dữ liệu đó. Trong Airflow, task `dbt_test` sẽ đỏ và các task phía sau không chạy. Nếu `in_range` chỉ là `warn` thì 6 dòng 900 km/h đã đi thẳng vào dataset.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
