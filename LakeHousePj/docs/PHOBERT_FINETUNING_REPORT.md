# BÁO CÁO CHI TIẾT: CHIẾN LƯỢC HUẤN LUYỆN VÀ TỐI ƯU MÔ HÌNH PHOBERT
*(Tài liệu chuẩn bị cho việc Báo cáo/Thuyết trình về quá trình Fine-tuning NLP)*

---

## 1. TỔNG QUAN TỔNG QUÁT (OVERVIEW)
### 1.1 Khơi gợi bài toán (Context)
Trong hệ sinh thái Lakehouse của dự án, việc xử lý dữ liệu bình luận từ mạng xã hội (TikTok, Booking, v.v...) đóng vai trò cực kỳ quan trọng để đánh giá mức độ quan tâm và sự hài lòng của du khách. Tuy nhiên, lượng dữ liệu thô (raw text) phân tích hằng ngày lên tới con số hàng trăm nghìn mẫu, đòi hỏi một mô hình Ngôn ngữ có khả năng hiểu tiếng Việt xuất sắc.

### 1.2 Mục tiêu mô hình (Objective)
Chúng ta quyết định sử dụng mô hình pre-trained **PhoBERT (vinai/phobert-base-v2)** để thực hiện bài toán Phân loại đa tác vụ (Multi-task Classification):
1. **Phân tích Cảm xúc (Sentiment Analysis):** Đánh giá bình luận Tiêu cực/Tích cực/Bình thường.
2. **Nhận diện Spam (Spam Detection):** Phân loại rác/quảng cáo so với bình luận thật.

---

## 2. NHỮNG THÁCH THỨC BƯỚC ĐẦU (CHALLENGES)
Khi đưa vào huấn luyện thực tế trên Google Colab với tập dữ liệu chứa hàng triệu/trăm nghìn dòng, chúng ta đã lập tức đối mặt với **2 rủi ro chí mạng**:
1. **Quá tải tài nguyên & Thời gian chờ (Bottleneck):** Mỗi Epoch (chu kỳ học) tốn hơn 17.500 bước (steps). Với 4 Epochs, việc train sẽ kéo dài hàng chục giờ, vượt quá giới hạn tài nguyên của Google Colab và liên tục làm tràn bộ nhớ (OOM).
2. **Hiện tượng Học vẹt (Overfitting & Catastrophic Forgetting):** Việc ép mô hình học quá sâu, quá nhiều lần (4 epochs) trên cùng một lượng dữ liệu khổng lồ khiến mô hình "học vẹt" dữ liệu Training, dẫn đến việc mất đi khả năng tổng quát hóa ngôn ngữ ban đầu (quên kiến thức cũ).

---

## 3. CHI TIẾT CÁC GIẢI PHÁP TỐI ƯU ĐÃ ÁP DỤNG (TECHNICAL SOLUTIONS)
Để giải quyết bài toán trên, thay vì nhồi nhét tài nguyên rỗng tuếch, chúng ta đã hiệu chỉnh lại đường ống huấn luyện (Training Pipeline) một cách thông minh bằng 4 kỹ thuật tối ưu hóa sau:

### 3.1. Kỹ thuật Lấy Mẫu Đại Diện (Data Sampling)
- **Vấn đề:** Không cần dùng toàn bộ 600.000+ dòng dữ liệu để dạy cho một mô hình đã "rất thông minh" (pre-trained) như PhoBERT, chỉ cần dạy nó cách làm bài tập (fine-tuning).
- **Giải pháp:** Cắt ngẫu nhiên nhưng giữ nguyên phân phối cân bằng (Stratified Sampling) xuống còn khoảng **80.000 đoạn văn bản tiêu biểu**.
- **Hiệu quả:** Giảm số steps từ 17.500 xuống chỉ còn ~2.500 bước mỗi Epoch. Rút ngắn 85% thời gian huấn luyện.

### 3.2. Đóng Băng Mạng Lõi (Layer Freezing)
- **Vấn đề:** Trọng số (weights) của 12 lớp Transformers trong PhoBERT đã được tối ưu cực chuẩn bởi VinAI. Nếu train lại từ đầu (unfreeze all) sẽ làm hỏng cấu trúc này.
- **Giải pháp:** Đóng băng toàn bộ các layer dưới (`requires_grad = False`). Chỉ cho phép cập nhật trọng số ở **4 Layer cuối cùng** và Lớp phân loại mới (Classification Head).
- **Hiệu quả:** Tránh được triệt để tình trạng *Catastrophic Forgetting* (quên kiến thức cũ), tiết kiệm 50% lượng tham số cần tính toán.

### 3.3. Tối ưu Bộ nhớ và Luồng Gradient 
- **Mixed Precision (FP16):** Chuyển đổi tính toán từ dấu phẩy động 32-bit (FP32) xuống 16-bit (FP16). Điều này tận dụng tối đa kiến trúc Tensor Cores của GPU T4 trên Colab, giảm một nửa nhu cầu VRAM mà không làm mất độ chính xác.
- **Gradient Accumulation:** Thay vì cập nhật trọng số ngay sau mỗi batch nhỏ (Batch=16), hệ thống sẽ cộng dồn Gradient qua 4 bước (tương đương lô 64) rồi mới cập nhật 1 lần. 
- **Hiệu quả:** Chống nhiễu khi cập nhật, giúp đường cong Loss gradient hội tụ mượt mà và ổn định hơn rất nhiều.

### 3.4. Dừng Sớm Thông Minh (Early Stopping)
- **Vấn đề:** Quá trình chuẩn bị là 4 Epochs, nhưng mô hình có thể đạt đỉnh thông minh ngay tại cuối Epoch thứ 2. Việc cố gắng train thêm Epoch 3, 4 sẽ chỉ gây Overtfiting.
- **Giải pháp:** Tích hợp `EarlyStoppingCallback(patience=2)`. Cứ mỗi 1000 bước học, hệ thống sẽ mang dữ liệu Validate ra kiểm tra. Nếu Loss của Validate không giảm sau 2 lần đo liên tiếp, hệ thống lập tức **tắt học lệnh** và kết thúc sớm.
- **Hiệu quả:** Luôn đảm bảo luôn lấy được Checkpoint của Model có độ dự đoán chuẩn nhất, không cần chờ đợi lãng phí.

---

## 4. KẾT QUẢ KỲ VỌNG (EXPECTED OUTCOMES)
Bằng việc phối hợp 4 kỹ thuật MLOps chuyên sâu trên, mô hình **PhoBERT Fine-tuned** dự kiến mang lại các cải tiến:
* **Thời gian huấn luyện:** Giảm từ hàng chục giờ xuống chỉ còn dưới **45 - 60 phút**.
* **Độ chính xác (F1-Score):** Tránh nhiễu dữ liệu và overfit, giúp điểm F1 dự kiến tăng từ 3 - 5%.
* **Sự ổn định (Stability):** Trọng lượng file và mức độ ngốn phần cứng được giảm thiểu cục bộ, sẵn sàng đóng gói vào Docker (hoặc lưu trữ trong MLFlow) để tích hợp lại vào luồng xử lý Spark Streaming tại Local mà không lo crash máy.
