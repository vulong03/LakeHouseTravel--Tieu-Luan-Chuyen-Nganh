# KỊCH BẢN THUYẾT TRÌNH BẢO VỆ KHÓA LUẬN - PHẦN MÔ HÌNH HỌC SÂU
## PHÂN TÍCH CẢM XÚC PHOBERT & DỰ BÁO LƯỢNG LƯU TRÚ LSTM/GRU

Tài liệu này tổng hợp cấu trúc slide, lời thoại thuyết trình (ngắn gọn trong 3-5 phút) và bộ câu hỏi phản biện tiềm năng từ hội đồng cho phần mô hình học sâu.

---

## I. CẤU TRÚC SLIDE VÀ LỜI THOẠI THUYẾT TRÌNH

### Slide 1: Quy trình gán nhãn yếu và PhoBERT đa nhiệm
* **Nội dung hiển thị trên Slide:**
  * **Thách thức:** 860.000 bình luận TikTok thô hoàn toàn chưa có nhãn.
  * **Giải pháp Gán nhãn yếu (Weak Labeling):**
    * Tận dụng `underthesea`, bộ từ khóa và biểu tượng cảm xúc (Emojis).
    * Triển khai song song hóa qua Spark Pandas UDF.
    * Ngưỡng tự tin lọc nhiễu: $Sentiment \ge 0.6$, $Intent \ge 0.5$ + Giới hạn 30.000 mẫu trung tính $[0.35, 0.6)$.
    * Tập dữ liệu tinh chỉnh (Fine-tuning set): ~81.000 bình luận.
  * **Kiến trúc PhoBERT đa nhiệm (Multi-task Learning):**
    * Mạng nền: `vinai/phobert-base-v2`.
    * Đầu ra song song: Nhánh Cảm xúc (3 lớp), Nhánh Ý định (4 lớp), Nhánh Khía cạnh (18 đầu ra đa nhãn Sigmoid).
    * Tối ưu hóa: Đóng băng 8 tầng Transformer đầu, học vi sai (backbone $2 \times 10^{-5}$, heads $1 \times 10^{-4}$), làm mịn nhãn (Label Smoothing = 0.1).
    * Hiệu năng: Suy diễn 860k bình luận trên Google Colab GPU T4 trong 11 phút (~1.340 dòng/giây).
* **Lời thoại thuyết trình (Lời nói):**
  > *"Kính thưa Hội đồng, thách thức lớn nhất đối với tập dữ liệu 860.000 bình luận TikTok thu thập được là hoàn toàn chưa có nhãn phục vụ phân tích. Nhóm đã xây dựng quy trình gán nhãn yếu (Weak Labeling) dựa trên hệ luật ngôn ngữ, phân tách câu cấp phân cú và chạy song song phân tán bằng Spark Pandas UDF. Để đảm bảo dữ liệu huấn luyện sạch nhiễu, nhóm áp dụng cơ chế lọc theo độ tin cậy và cân bằng mẫu trung tính, thu được tập dữ liệu tinh chỉnh chất lượng cao gồm khoảng 81.000 mẫu.*
  > 
  > *Nhóm đề xuất mô hình PhoBERT đa nhiệm (Multi-task Learning) trên nền `vinai/phobert-base-v2` để dự báo đồng thời cả ba nhiệm vụ: cảm xúc, ý định và khía cạnh du lịch chỉ với một lần lan truyền xuôi, giúp giảm thiểu tài nguyên tính toán. Mô hình được tối ưu bằng kỹ thuật đóng băng 8 tầng Transformer dưới, áp dụng tốc độ học vi sai và làm mịn nhãn để chống nhiễu. Quá trình suy diễn toàn bộ 860k bình luận trên GPU T4 của Google Colab đạt tốc độ ấn tượng hơn 1.300 bình luận/giây và hoàn tất chỉ trong 11 phút, kết quả được lưu trữ tại tầng Gold."*

---

### Slide 2: Kỹ nghệ đặc trưng và Tiền xử lý chuỗi thời gian
* **Nội dung hiển thị trên Slide:**
  * **Tập đặc trưng tích hợp:** 50 đặc trưng tổng hợp theo hạt **Tỉnh/Tháng** tại bảng `fact_province_month_dl_features` (TikTok + Booking.com + NLP PhoBERT).
  * **Ngăn ngừa rò rỉ dữ liệu (Data Leakage Prevention):**
    * Biến đổi logarit target: $y_{scaled} = \log(1+y)$ nhằm ổn định phương sai chuỗi thời gian.
    * Chia dữ liệu Train (75%) / Val (12.5%) / Test (12.5%) theo mốc thời gian trước khi xử lý.
    * Ngưỡng cắt ngoại lệ ($p_{99}$) và bộ tham số `RobustScaler` chỉ được tính (fit) trên tập Train, sau đó áp dụng (transform) sang Val và Test.
  * **Tạo chuỗi dữ liệu cửa sổ trượt:** 
    * Kích thước chuỗi đầu vào: $3 \text{ tháng} \times 50 \text{ đặc trưng}$.
    * Giải pháp xử lý biên: Chuẩn hóa toàn bộ trước, tạo chuỗi trượt liên tục theo từng tỉnh để tránh mất dữ liệu tại ranh giới phân chia, rồi mới phân chia tập dựa trên nhãn thời gian `year_month` của target.
* **Lời thoại thuyết trình (Lời nói):**
  > *"Từ kết quả NLP của PhoBERT, nhóm đã thực hiện kỹ nghệ đặc trưng, kết hợp với chỉ số tương tác TikTok và dữ liệu đặt phòng Booking.com để xây dựng bảng đặc trưng tích hợp Gold gồm 50 đặc trưng với hạt dữ liệu là Tỉnh thành theo Tháng.*
  > 
  > *Để chuẩn bị dữ liệu cho mô hình chuỗi thời gian, nhóm thiết kế quy trình tiền xử lý nghiêm ngặt nhằm chống rò rỉ thông tin từ tương lai. Biến mục tiêu được biến đổi logarit $\log(1+y)$ để ổn định phương sai. Tập dữ liệu được phân chia theo trình tự thời gian trước, sau đó các thông số chuẩn hóa RobustScaler và ngưỡng cắt ngoại lệ chỉ được tính toán trên tập Train và áp dụng nhất quán sang tập Val và Test. Cuối cùng, dữ liệu được chuyển đổi thành chuỗi trượt 3 tháng liên tiếp ($L=3$) theo từng tỉnh để làm đầu vào cho mô hình hồi quy."*

---

### Slide 3: Kiến trúc mô hình dự báo LSTM và GRU
* **Nội dung hiển thị trên Slide:**
  * **Kiến trúc song song:** Mô hình chính `LSTMForecaster` và đối chứng `GRUForecaster` đồng nhất cấu trúc đầu vào và đầu ra.
  * **Cấu trúc chi tiết:**
    * 2 tầng hồi quy tuần tự (ẩn 48, dropout 0.43) $\rightarrow$ Chuẩn hóa tầng `LayerNorm(48)`.
    * Cơ chế chú ý theo thời gian `Temporal Attention` (tính trọng số $\alpha_t$ đóng góp của từng bước thời gian).
    * Đầu MLP 3 tầng dự báo tuyến tính: $\text{Linear}(48\rightarrow48) \rightarrow \text{GELU} \rightarrow \text{Linear}(48\rightarrow16) \rightarrow \text{ReLU} \rightarrow \text{Linear}(16\rightarrow1)$.
    * Output dự báo: $\hat{y}$ đại diện cho lượng đánh giá trên thang logarit $\log(1+y)$.
  * **Hàm mất mát tích hợp Hybrid Loss:** 
    * $$\mathcal{L}_{\text{Total}} = 0.7 \times \mathcal{L}_{\text{Huber}}(\delta=0.5) + 0.3 \times \mathcal{L}_{\text{SMAPE}}$$
    * Huber Loss ($\delta=0.5$): Ổn định đạo hàm khi sai số nhỏ (MSE) và chống nhiễu ngoại lai khi sai số lớn (MAE).
    * SMAPE Loss: Tránh thiên lệch quy mô du lịch, bảo vệ hiệu năng dự báo ở các tỉnh nhỏ.
* **Lời thoại thuyết trình (Lời nói):**
  > *"Về kiến trúc mô hình dự báo, nhóm xây dựng mô hình chính LSTM và mô hình đối chứng GRU với cấu trúc hoàn toàn đồng nhất để so sánh công bằng. Mô hình gồm 2 tầng hồi quy tuần tự sâu xếp chồng, theo sau bởi lớp LayerNorm để hỗ trợ dự báo đệ quy từng tỉnh với batch size bằng 1.*
  > 
  > *Điểm cải tiến ở đây là nhóm tích hợp cơ chế chú ý theo thời gian (Temporal Attention) giúp mô hình tự học trọng số đóng góp của từng tháng lịch sử thay vì coi chúng như nhau. Vectơ ngữ cảnh sau đó đi qua đầu MLP 3 tầng sâu với các hàm kích hoạt GELU và ReLU để dự báo lượng lưu trú trên thang log. Nhóm thiết kế hàm mất mát tích hợp Hybrid Loss gồm 70% Huber Loss nhằm ổn định gradient, chống nhiễu từ các video viral đột biến, và 30% SMAPE Loss để tối ưu hóa sai số tương đối, đặc biệt hiệu quả với các địa phương có lượng phòng thấp."*

---

### Slide 4: Kết quả thực nghiệm và Ứng dụng dự báo
* **Nội dung hiển thị trên Slide:**
  * **Hiệu năng trên tập Test độc lập:** Cả hai mô hình đạt hiệu năng xuất sắc với $R^2 > 0.95$.
    * Mô hình LSTM: $R^2 = 0.9542$, $\text{RMSE} = 0.4213$, $\text{MAE} = 0.3251$, $\text{MAPE} = 37.91\%$.
    * Mô hình GRU: $R^2 = 0.9564$, $\text{RMSE} = 0.4110$, $\text{MAE} = 0.3104$, $\text{MAPE} = 37.87\%$.
  * **Kích thước tham số:** GRU có $31.810$ tham số (tiết kiệm 23% so với $41.314$ tham số của LSTM), chứng minh sự tối ưu của cấu trúc cổng giản lược trên chuỗi ngắn.
  * **Dự báo và Triển khai:**
    * Dự báo tự hồi quy 12 tháng kế tiếp cho 63 tỉnh (756 điểm dữ liệu).
    * Quản lý thực nghiệm tập trung bằng MLflow Tracking & Model Registry.
    * Triển khai dashboard **Gradio** kết nối MinIO truy xuất nhanh qua cơ chế bộ đệm.
    * Đóng gói bằng Docker Compose và chạy thử nghiệm trên đám mây GCP.
* **Lời thoại thuyết trình (Lời nói):**
  > *"Cuối cùng là kết quả thực nghiệm trên tập Test độc lập. Cả hai mô hình đều đạt hiệu năng xuất sắc với hệ số xác định R2 vượt 0.95. Mô hình GRU cho kết quả nhỉnh hơn một lượng nhỏ với R2 đạt 0.9564 và MAPE đạt 37.87%, đồng thời số lượng tham số của GRU ít hơn 23% so với LSTM. Điều này chứng minh cấu trúc cổng đơn giản của GRU là tối ưu và hiệu quả hơn đối với chuỗi lịch sử ngắn 3 tháng.*
  > 
  > *Toàn bộ kết quả dự báo tự hồi quy 12 tháng tiếp theo của 63 tỉnh thành được nhóm lưu trữ vào bảng Iceberg Gold và xuất bản lên MinIO. Nhóm đã đóng gói toàn bộ hệ thống bằng Docker Compose, triển khai trên đám mây GCP và xây dựng ứng dụng Gradio Dashboard liên kết trực tiếp với MLflow. Dashboard cung cấp các tính năng thiết thực như xếp hạng tỉnh thành, so sánh xu hướng du lịch tương lai và phân tích cơ cấu du khách theo vùng miền, chứng minh khả năng áp dụng thực tế cao của đề tài. Em xin chân thành cảm ơn Hội đồng đã lắng nghe!"*

---

## II. BỘ CÂU HỎI PHẢN BIỆN TIỀN NĂNG & GỢI Ý TRẢ LỜI (FOR THESIS DEFENSE)

### Câu hỏi 1: Tại sao em lại lựa chọn chuẩn hóa LayerNorm (Layer Normalization) thay vì BatchNorm (Batch Normalization) trong kiến trúc LSTM/GRU?
* **Gợi ý trả lời:** 
  > *"Thưa thầy/cô, nhóm lựa chọn LayerNorm thay vì BatchNorm vì đặc thù của quy trình suy diễn thực tế. Khi đưa mô hình vào chạy dự báo tương lai cho 12 tháng tiếp theo, quá trình dự báo tự hồi quy được thực hiện tuần tự và đệ quy riêng lẻ cho từng tỉnh/thành phố một. Tại thời điểm đó, kích thước lô (batch size) của dữ liệu đưa vào mô hình bắt buộc phải giảm xuống bằng 1. 
  > 
  > BatchNorm phụ thuộc vào việc tính toán trung bình và phương sai trên toàn bộ lô dữ liệu (batch dimension), nên khi batch size bằng 1, các chỉ số thống kê này sẽ bị méo mó và gây sai lệch nghiêm trọng cho kết quả dự báo. Ngược lại, LayerNorm thực hiện chuẩn hóa độc lập trên chiều đặc trưng (feature dimension) của từng mẫu dữ liệu riêng biệt tại mỗi bước thời gian, hoàn toàn không phụ thuộc vào kích thước lô. Do đó, LayerNorm giúp mô hình hoạt động ổn định và chính xác ở cả giai đoạn huấn luyện batch lớn lẫn giai đoạn suy diễn thực tế với batch bằng 1."*

### Câu hỏi 2: Tại sao em lại chọn giá trị $\delta = 0.5$ cho hàm mất mát Huber Loss? Giá trị này đại diện cho điều gì?
* **Gợi ý trả lời:**
  > *"Thưa thầy/cô, trong hàm mất mát Huber Loss, tham số $\delta$ đóng vai trò là ngưỡng chuyển đổi giữa hàm bậc hai (MSE - dùng cho sai số nhỏ) và hàm bậc nhất (MAE - dùng cho sai số lớn/outliers).
  > 
  > Sở dĩ nhóm lựa chọn $\delta = 0.5$ vì biến mục tiêu `hotel_review_volume` đã được biến đổi logarit $\log(1+y)$ trước khi đưa vào mô hình, khiến tầm giá trị bị nén lại trong khoảng hẹp từ 0 đến 8. Trên thang logarit này, một sai lệch tuyệt đối bằng $0.5$ tương đương với tỷ lệ chênh lệch giá trị thực tế ở thang đo ban đầu là khoảng $e^{0.5} \approx 1.65$ lần. Thiết lập ngưỡng chuyển đổi tại mốc $0.5$ giúp mô hình phân tách hợp lý giữa:
  > * Những biến động tự nhiên nhỏ trong phạm vi cho phép (dưới $1.65$ lần) để mô hình hội tụ mượt mà bằng MSE.
  > * Những sai số đột biến lớn (trên $1.65$ lần) do các đợt cao điểm hoặc video siêu viral gây ra để mô hình chuyển sang MAE nhằm giới hạn hình phạt, tránh làm lệch hướng học của toàn bộ mạng."*

### Câu hỏi 3: Việc kết hợp Huber Loss với SMAPE Loss nhằm mục đích gì? Tại sao không dùng hoàn toàn MSE hay MAE truyền thống?
* **Gợi ý trả lời:**
  > *"Thưa thầy/cô, nếu chỉ sử dụng MSE hoặc MAE truyền thống, mô hình sẽ gặp khó khăn lớn do sự chênh lệch quy mô du lịch cực kỳ lớn giữa các địa phương ở Việt Nam (ví dụ: Hà Nội, TP.HCM có hàng nghìn lượt đánh giá lưu trú mỗi tháng, trong khi các tỉnh miền núi nhỏ chỉ có vài chục lượt). MSE và MAE là các chỉ số đo lường sai số tuyệt đối, mô hình sẽ có xu hướng tập trung tối ưu hóa cho các trung tâm lớn vì sai số tuyệt đối ở đó lớn hơn rất nhiều, dẫn đến việc dự báo ở các tỉnh nhỏ bị sai lệch nghiêm trọng.
  > 
  > Để giải quyết, nhóm xây dựng hàm mất mát kết hợp Hybrid Loss:
  > * **Huber Loss (70%):** Đóng vai trò hạt nhân giúp ổn định đạo hàm, chống bùng nổ gradient trước các giá trị ngoại lai thô.
  > * **SMAPE Loss (30%):** Đo lường sai số phần trăm đối xứng. Vì SMAPE tính tỷ lệ phần trăm sai số so với quy mô của chính tỉnh đó, nó buộc mô hình phải quan tâm công bằng đến sai số tương đối của cả tỉnh nhỏ lẫn tỉnh lớn.
  > 
  > Sự kết hợp này mang lại sự cân bằng tối ưu giữa tính ổn định gradient và khả năng kiểm soát sai số theo tỷ lệ phần trăm trên toàn quốc."*

### Câu hỏi 4: Em đã thực hiện những biện pháp cụ thể nào để ngăn chặn hiện tượng rò rỉ dữ liệu (Data Leakage) khi chuẩn hóa RobustScaler và cắt ngoại lệ?
* **Gợi ý trả lời:**
  > *"Thưa thầy/cô, rò rỉ dữ liệu là lỗi rất dễ mắc phải trong các mô hình chuỗi thời gian nếu chuẩn hóa trên toàn bộ tập dữ liệu trước khi chia. Để ngăn chặn tuyệt đối lỗi này, nhóm đã thiết kế quy trình xử lý độc lập như sau:
  > 
  > 1. **Phân chia dữ liệu trước:** Nhóm thực hiện phân tách tập dữ liệu thành các tập Train, Validation và Test theo mốc thời gian tăng dần của biến `year_month` trước khi thực hiện bất kỳ bước tính toán thống kê nào.
  > 2. **Tính toán (Fit) chỉ trên tập Train:** Chỉ số trung vị (Median), khoảng tứ phân vị (IQR) để phục vụ `RobustScaler`, cũng như ngưỡng phân vị 99th ($p_{99}$) để giới hạn ngoại lệ chỉ được tính toán duy nhất trên dữ liệu của tập Train.
  > 3. **Áp dụng (Transform) sang các tập còn lại:** Các tham số tĩnh thu được từ tập Train này sau đó mới được áp dụng để cắt cụt ngoại lệ và chuẩn hóa cho cả tập Validation và Test. Điều này đảm bảo mô hình hoàn toàn không "nhìn thấy" trước bất kỳ thông tin phân phối nào của tập Validation và Test trong suốt quá trình chuẩn hóa."*

### Câu hỏi 5: Tại sao việc tạo chuỗi thời gian cửa sổ trượt lại dễ gây mất dữ liệu ở vùng biên phân cắt (Boundary Sequence Loss)? Nhóm đã giải quyết thế nào?
* **Gợi ý trả lời:**
  > *"Thưa thầy/cô, nếu chúng ta cắt DataFrame thành ba tập độc lập trước khi tạo chuỗi, ta sẽ gặp hiện tượng mất dữ liệu biên. Ví dụ, với độ dài chuỗi $L=3$, để tạo ra mẫu dự báo cho tháng đầu tiên của tập Test (ví dụ: tháng 11/2024), mô hình cần dữ liệu đặc trưng của 3 tháng liền trước là tháng 8, 9 và 10/2024. Tuy nhiên, do tập Test bị cắt độc lập, dữ liệu của tháng 8, 9, 10 nằm ở tập Train/Val nên tập Test không có đủ dữ liệu lịch sử để ghép chuỗi, dẫn đến việc điểm dữ liệu tháng 11/2024 bị loại bỏ một cách đáng tiếc.
  > 
  > Để khắc phục, nhóm thực hiện chuẩn hóa dữ liệu trên toàn bảng DataFrame trước. Sau đó, hàm tạo chuỗi `prepare_sequences` sẽ duyệt qua toàn bộ lịch sử thời gian liên tục của từng tỉnh để tạo ra tất cả các ma trận chuỗi $3 \times 50$. Cuối cùng, các ma trận chuỗi này mới được phân phối vào các tập Train, Val hoặc Test dựa trên nhãn thời gian `year_month` của biến mục tiêu cần dự báo. Giải pháp này giúp duy trì tính liên tục của chuỗi dữ liệu tại vùng biên phân cắt mà vẫn bảo vệ tuyệt đối nguyên tắc chống rò rỉ dữ liệu."*

### Câu hỏi 6: Tại sao mô hình GRU lại đạt kết quả tốt hơn một chút và có số lượng tham số ít hơn so với LSTM trong thực nghiệm của em?
* **Gợi ý trả lời:**
  > *"Thưa thầy/cô, về mặt lý thuyết kiến trúc:
  > * **LSTM** duy trì riêng biệt trạng thái ô nhớ (Cell State) và trạng thái ẩn (Hidden State), sử dụng 3 cổng điều hướng: cổng vào (Input), cổng quên (Forget) và cổng ra (Output).
  > * **GRU** đã tối giản hóa bằng cách hợp nhất trạng thái ô nhớ và trạng thái ẩn thành một, đồng thời chỉ sử dụng 2 cổng: cổng cập nhật (Update Gate) và cổng thiết lập lại (Reset Gate).
  > 
  > Sự rút gọn này giúp GRU chỉ tiêu tốn $31.810$ tham số, ít hơn khoảng $23\%$ so với $41.314$ tham số của LSTM. Trong thực nghiệm của đề tài, độ dài chuỗi lịch sử đầu vào khá ngắn ($L=3$ tháng). Với chuỗi dữ liệu ngắn và hạt dữ liệu nhỏ, cấu trúc cổng tinh giản của GRU đã đủ khả năng để học toàn bộ các mối quan hệ tuần tự mà không cần đến cơ chế lưu trữ dài hạn phức tạp của LSTM. Ít tham số hơn cũng giúp GRU kiểm soát tốt hơn hiện tượng quá khớp (overfitting) trên tập dữ liệu quy mô vừa phải, dẫn đến chỉ số hiệu năng thực nghiệm trên tập Test của GRU ($R^2 = 0.9564$) nhỉnh hơn một mức nhỏ so với LSTM ($R^2 = 0.9542$)."*
