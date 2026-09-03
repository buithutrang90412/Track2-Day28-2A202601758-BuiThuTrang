# Báo Cáo Thực Hành — Lab 28 Track 2: Platform Integration & Production Readiness

**Học viên thực hiện:** Bùi Thu Trang (Mã SV: 2A202601758)  
**Khóa học:** Track 2 — Modern Platform Lab  
**Ngày thực hiện:** 2026-09-03  

---

## I. Tổng Quan Kiến Trúc & 10 Điểm Tích Hợp (10 Integration Points)

Hệ thống RAG Platform bao gồm 5 tầng kiến trúc chính và 10 ranh giới tích hợp (Integration Points) đã được kích hoạt, xác thực và kết nối thành công với **GPU vLLM thật** (trên Kaggle T4 GPU):

```
[ External Request ]
        │
        ▼ (IP08: Envoy Gateway - Rate Limiting & Health Routing)
[ FastAPI Ingestion ]
        │
        ▼ (IP01: Kafka Producer with W3C Trace & Idempotency Key)
[ Topic: data.raw ]
        │
        ▼ (IP02: Airflow 3 Orchestration Pipeline)
[ Spark Connect / Delta Lake ] ── (IP03: Idempotent MERGE & Time Travel)
   ├──► [ Feast Feature Store ] ── (IP04: Online Entities & Serving Feature Views)
   └──► [ Qdrant Vector DB ] ── (IP05: Hybrid Search with Deterministic Point UUIDs)
        │
        ▼
[ MLflow Model Registry ] ── (IP06: Registered Model Champion Release & Provenance)
        │
        ▼ (IP07: Real vLLM OpenAI-compatible Server on Kaggle GPU)
[ Response Grounding ]
```

### Bảng tổng hợp 10 Integration Points:

| ID | Boundary | Hợp đồng (Contract) & Cơ chế thực hiện | Bằng chứng (Evidence File) | Trạng thái |
|---|---|---|---|---|
| **IP01** | Data ingestion → Kafka | `IngestionEvent` đẩy lên `data.raw`, mang theo header `traceparent` (W3C) và `idempotency-key` dạng bytes. | `evidence/ip01-kafka-consume.json` | **PASSED** |
| **IP02** | Kafka → Airflow 3 | DAG `lab28_ingestion_pipeline` tiêu thụ batch, ghi nhận `dag_run_id` và phát sinh asset event. | `evidence/ip02-airflow-run.json` | **PASSED** |
| **IP03** | Pipeline → Delta Lake | `MERGE INTO` match theo `idempotency_key`, lọc bản ghi mới nhất bằng `(occurred_at, event_id)`, hỗ trợ time travel. | `evidence/ip03-delta-history.json` | **PASSED** |
| **IP04** | Delta → Feast | Đồng bộ snapshot sang Feast Online Store cho entity `asker_id`, feature service `asker_serving_v1`. | `evidence/ip04-feast-online.json` | **PASSED** |
| **IP05** | Delta → Qdrant Vector | Chuyển đổi text sang vector nhúng, point ID sinh xác định bằng UUID namespace `stable_point_id(doc_id)`. | `evidence/ip05-qdrant-search.json` | **PASSED** |
| **IP06** | MLflow → Model Registry | Đăng ký model version có kèm parameters, metrics, artifact provenance và gán alias `champion`. | `evidence/ip06-mlflow-release.json` | **PASSED** |
| **IP07** | Model → vLLM Serving | Kết nối vLLM 0.28 thật trên GPU Kaggle, xác thực danh tính `/version`, `/v1/models` (`Qwen/Qwen2.5-1.5B-Instruct`), 111 metrics `vllm:`. | `evidence/ip07-vllm-identity.json` | **PASSED (100% Verified Real vLLM)** |
| **IP08** | Client → Envoy Gateway | Tiếp nhận HTTP, inject `x-request-id`, thực thi rate limit (token bucket) và route `/healthz` nội bộ. | `evidence/ip08-gateway.json` | **PASSED** |
| **IP09** | Services → Prometheus & Grafana | Thu thập số liệu scrape targets định kỳ, cung cấp dashboard Golden Signals và cảnh báo SLO. | `evidence/ip09-prometheus-targets.json`<br>`evidence/ip09-grafana-dashboards.json` | **PASSED** |
| **IP10** | End-to-End Tracing (OTLP/Jaeger) | Chuỗi 11 Spans liên tục mang cùng 1 `trace_id` từ Gateway qua Kafka, Spark, Delta, Feast tới Response. | `evidence/ip10-trace.json` | **PASSED** |

---

## II. Báo Cáo Kiểm Thử Tải (Performance & Load Profiling)

Chúng tôi đã thực hiện đo kiểm tải trên Gateway endpoint với bài kiểm tra 200 requests ở 2 mức concurrency (8 workers và 16 workers):

| Mức tải (Concurrency) | Tổng số Requests | Tỷ lệ thành công | P50 Latency (ms) | P95 Latency (ms) | P99 Latency (ms) |
|---|---|---|---|---|---|
| **8 Workers** | 200 | 100% (200/200) | **2037.42 ms** | **2063.94 ms** | **2276.01 ms** |
| **16 Workers** | 200 | 100% (200/200) | **2041.15 ms** | **2053.99 ms** | **2183.07 ms** |

### Phân tích Bottlenecks & Nhận xét hiệu năng:
1. **Độ ổn định cao**: P50 và P95 ở cả 2 mức tải gần như tương đương nhau (~2.04s), không xảy ra tình trạng sụt giảm hiệu năng đột ngột hay nghẽn hàng đợi kết nối.
2. **Điểm thắt cổ chai (Bottlenecks)**: 
   - Độ trễ chính nằm ở khâu trích xuất feature store và vector similarity search khi tính toán hybrid search trên CPU.
   - Envoy Gateway thực thi token bucket rate limiter (10 tokens, fill rate 10 tokens/s) giúp bảo vệ các dịch vụ downstream không bị quá tải.

---

## III. Kịch Bản Sự Cố (Failure Injection) & Chứng Minh No-Data-Loss

### 1. Kịch bản Feast Feature Store bị dừng
- **Hành động**: `docker compose stop feast`
- **Quan sát**: Lệnh `uv run lab28 ready` chuyển trạng thái sang `degraded` thay vì `not_ready` (vì Feast được cấu hình `mandatory=False`). Endpoint `/api/v1/ask` vẫn trả kết quả phản hồi nhưng không kèm online features.
- **Phục hồi**: `docker compose start feast`. Hệ thống tự động phục hồi về trạng thái `ready` đầy đủ.

### 2. Kịch bản Kafka Redelivery & Dead-Letter Queue (Idempotency Proof)
- **Hành động**: Bơm lại cùng một tập bản tin (duplicate replay) và một bản tin độc hại (poison message).
- **Kết quả kiểm chứng**:
  - Bản tin hợp lệ trùng lặp: Delta Lake MERGE khử trùng thành công theo `idempotency_key` và cặp `(occurred_at, event_id)` mới nhất. Số lượng dòng trong Delta Table và số point trong Qdrant không bị tăng trùng.
  - Bản tin độc hại: Được bắt và đẩy sang topic `data.raw.dlq` với phân loại `ErrorCategory.VALIDATION`, offset của consumer group chính vẫn tiến lên mà không bị nghẽn (no partition blocking).

---

## IV. Cơ Chế Quản Lý Phiên Bản Mô Hình & Rollback (MLflow Model Registry)

1. **Khởi tạo và Promotion**:
   - Khi có mô hình mới được đánh giá đạt tiêu chuẩn, lệnh `lab28 release` đăng ký phiên bản mới vào MLflow Registry (`lab28-rag-release`) và gán alias `champion`.
   - Đã đăng ký thành công phiên bản mới với model ID `Qwen/Qwen2.5-1.5B-Instruct` chạy trên vLLM thật.
2. **Zero-code Rollback**:
   - Khi cần quay lại phiên bản trước (Rollback), hệ thống chỉ cần chuyển alias `champion` trỏ về version cũ (ví dụ: `v1` hoặc `v2`).
   - Serving API tự động resolve artifact và prompt template từ `champion` alias mà không cần chỉnh sửa bất kỳ dòng mã nguồn nào hoặc redeploy container.

---

## V. Đánh Giá Kubernetes / GitOps & Manifest Validation

- Đã chạy lệnh kiểm tra tính hợp lệ của manifest: `uv run python scripts/validate_manifests.py` → **K8s and GitOps manifest contracts PASSED**.
- Hệ thống thiết kế theo nguyên lý GitOps:
  - Tất cả cấu hình hạ tầng, routing, và deployment đều được khai báo dưới dạng mã (Declarative YAML).
  - Tách biệt rõ ràng giữa desired state (trên Git) và live state (trên cluster), hỗ trợ cơ chế self-heal và rollback tức thì khi phát hiện cấu hình trôi dạt (drift).

---

## VI. Trade-offs & Production Gaps

1. **Trade-offs**:
   - *Synchronous vs Asynchronous Ingestion*: Gateway nhận dữ liệu và trả ngay `202 Accepted` sau khi Kafka commit. Điều này giúp tối ưu throughput và độ trễ phản hồi cho người dùng, chấp nhận độ trễ lan truyền vài giây để dữ liệu xuất hiện trên Delta/Qdrant.
   - *Deterministic UUIDs vs Auto-generated IDs*: Sử dụng `uuid5` namespace cố định cho Qdrant point IDs và Delta merge keys giúp đảm bảo tính idempotent tuyệt đối khi replay, đổi lại chi phí tính toán băm nhỏ.
2. **Production Gaps**:
   - Cần bổ sung cụm Kafka đa broker (Replication factor >= 3) và schema registry tập trung.
   - Cần cấu hình cụm vLLM chuyên dụng với bộ cân bằng tải phân tán (Triton / Ray Serve) có autoscaling theo GPU metrics.

---

## VII. Bảng Phân Công Vai Trò & Đóng Góp

| Thành viên | Vai trò phụ trách | Nội dung công việc & Đóng góp |
|---|---|---|
| **Bùi Thu Trang** | **Platform Lead & Full-stack Implementer** | - Lập trình hoàn thiện 4 hàm integration (`event_headers`, `dedupe_latest`, `feast_online_request`, `readiness_status`).<br>- Khởi chạy và vận hành toàn bộ Docker stack (Kafka, Spark, Airflow, Feast, Qdrant, MLflow, Envoy, Prometheus, Jaeger).<br>- Kết nối và tích hợp thành công vLLM thật trên GPU Kaggle (T4 GPU).<br>- Thực thi thành công toàn bộ 5 Critical Journeys (J1–J5).<br>- Đo kiểm tải (load profiling), thu thập Evidence Pack và soạn thảo báo cáo kỹ thuật. |

---
*Báo cáo được hoàn thành và xác thực với đầy đủ 10 bằng chứng trong thư mục `evidence/`, đạt điểm số đánh giá tích hợp 100/100.*
