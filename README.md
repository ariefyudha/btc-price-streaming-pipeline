# 🚀 Building Real-Time Data Pipelines with Kafka & Spark Streaming

Dokumen ini adalah panduan lengkap (*end-to-end*) untuk mempelajari dan menjalankan arsitektur *real-time data pipeline*. 

Panduan ini disusun secara berurutan (*step-by-step*), mulai dari memahami struktur folder, membedah alur kode (*Producer*, *Spark Streaming*, dan *Dashboard*), hingga mengeksekusi dan mematikan infrastruktur.

---

## 🛠️ Persiapan Sebelum Memulai

Pastikan komputer yang digunakan telah meng-install perangkat lunak berikut:
1.  **Docker Desktop** (Pastikan dalam keadaan menyala / *running*).
2.  **IDE / Code Editor** (Disarankan VS Code untuk membaca kode).
3.  **Git** (Untuk mengunduh / *clone* repositori ini).

---

## 🏗️ Langkah 1: Eksplorasi Struktur Proyek

Buka IDE Anda dan arahkan ke folder utama proyek ini (`kafka-spark-streaming-bootcamp`). Struktur repositori ini sudah dibagi dengan rapi menjadi beberapa komponen utama:

-   📄 **`docker-compose.yml`**: Ini adalah "resep" infrastruktur kita. Daripada meng-install Kafka dan Spark secara manual yang rumit, file ini menyuruh Docker menyiapkan server Kafka, Spark Master, Spark Worker, serta menjalankan *Producer* dan *Dashboard* secara otomatis dalam satu jaringan virtual (*network*).
-   📁 **`src/producer/`**: Berisi script Python untuk menyedot data transaksi nyata dari Binance WebSocket.
-   📁 **`src/spark/`**: Berisi script PySpark sebagai "otak" pemroses data utama.
-   📁 **`src/dashboard/`**: Berisi aplikasi Web (Flask + HTML) untuk visualisasi grafik secara *real-time*.

---

## 📡 Langkah 2: Membedah Data Producer (Kafka Collection)

Langkah pertama dalam *pipeline* adalah mengumpulkan data. Buka file **`src/producer/market_data_generator.py`**. 

**Highlight Logika Script:**
1.  **Simulasi Data Bitcoin Asli (Yahoo Finance)**: Script ini menggunakan API publik dari **Yahoo Finance** untuk mengunduh sejarah pergerakan harga Bitcoin 1 hari terakhir. Data asli tersebut kemudian "diputar ulang" (*replay*) satu per satu setiap detik untuk mensimulasikan aliran data (*streaming*) secara *real-time*. Ini adalah teknik *Data Engineering* yang sangat umum digunakan untuk *backtesting pipeline* tanpa takut terkena blokir (*rate-limit*) atau kendala jaringan!
2.  **Injeksi Data Kotor (*Dirty Data*)**: Perhatikan baris kode `is_dirty_data = random.random() < 0.05`. Terdapat probabilitas 5% sistem sengaja mengubah harga dari Yahoo Finance menjadi harga negatif atau volume nol. Ini sangat penting untuk mensimulasikan data rusak/anomali untuk menguji fitur *Data Validation* ETL kita nanti!
3.  **Pengiriman ke Kafka**: Menggunakan *library* `kafka-python`, script ini secara konstan mengirim (Produce) data yang sudah distandarisasi ke dalam *Topic* Kafka yang bernama **`btc-trades`**.

---

## ⚙️ Langkah 3: Membedah Pipeline ETL (Spark Streaming)

Data mentah dari Producer akan diolah dan divalidasi secara *real-time*. Buka file **`src/spark/streaming_pipeline.py`**. Ini adalah inti dari materi pembelajaran kita.

**Highlight Logika Script:**
1.  **EXTRACT (Membaca Data)**: Perhatikan blok `spark.readStream`. Spark terus-menerus mendengarkan topik `btc-trades` dari server Kafka untuk menangkap data mentah yang baru masuk.
2.  **TRANSFORM (Parsing JSON)**: Data dari Kafka awalnya berformat binari string. Spark mengekstrak JSON tersebut dan memetakannya (*mapping*) ke dalam bentuk kolom tabular terstruktur dengan tipe data yang benar (Double, Integer, Timestamp).
3.  **DATA VALIDATION & DLQ**: Perhatikan variabel `valid_data` dan `invalid_data`. Spark menggunakan `.filter()` untuk memastikan hanya data dengan harga dan volume positif (> 0) yang diproses lebih lanjut. Data kotor (invalid) tidak dibuang, melainkan bisa diarahkan ke sistem peringatan lain (teknik *Dead Letter Queue*).
4.  **STATEFUL AGGREGATION (Tumbling Window)**: Inilah kehebatan Spark Streaming. Spark "menahan" memori data selama jendela waktu tertentu (misalnya 10 detik), lalu menghitung *Open, High, Low, Close* (OHLC) beserta *Total Volume*. Proses merangkum ribuan *tick data* menjadi satu *candle* per 10 detik ini disebut operasi *Stateful*.
5.  **DATA ROUTING (Sink)**: Terakhir, hasil agregasi yang sudah bersih ditulis kembali (Sink) ke topik Kafka yang baru bernama **`btc-candles`**. Hasil ini juga dicetak ke layar *console* agar bisa kita lihat secara langsung.

---

## 🌟 Langkah 4: Membedah Dashboard Real-Time (WOW Factor)

Untuk memvisualisasikan hasil hitungan Spark di atas, buka folder **`src/dashboard/`**.

**Highlight Logika Script:**
1.  **Backend (`app.py`)**: Merupakan server web Flask sederhana yang bertindak sebagai *Kafka Consumer*. Ia mendengarkan topik `btc-candles` (hasil olahan Spark) dan meneruskannya ke halaman web melalui jalur koneksi terbuka bernama *Server-Sent Events* (SSE).
2.  **Frontend (`templates/index.html`)**: Memuat pustaka *TradingView Lightweight Charts*. Script Javascript di dalamnya terus menerima *event* SSE dari Flask dan langsung menggambar pergerakan *candle* hijau/merah di layar tanpa memerlukan *refresh* halaman!

---

## 🚀 Langkah 5: Eksekusi End-to-End

Semua teori dan kode sudah dipahami, sekarang saatnya membuktikan arsitektur kita berjalan dengan lancar.

1.  **Menyalakan Seluruh Infrastruktur:**
    Buka terminal di dalam folder utama proyek, lalu eksekusi perintah:
    ```bash
    docker-compose up -d --build
    ```
    *Tunggu prosesnya selesai. Perintah ini secara ajaib menyalakan server Kafka, Spark Master/Worker, menjalankan Data Producer di latar belakang, dan menghidupkan server Web Dashboard.*

2.  **Menjalankan Spark Streaming ETL Job:**
    Masuk ke dalam *container* `spark-master` untuk memulai pemrosesan aliran data dengan menjalankan perintah ini di terminal:
    ```bash
    docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/src/spark/streaming_pipeline.py
    ```
    *Setelah proses unduh dependensi selesai (beberapa detik), Anda akan melihat tabel (Batch) muncul perlahan-lahan di layar terminal Anda yang berisi rekap agregasi 10-detikan.*

3.  **Visualisasi Real-Time (Dashboard):**
    Sekarang, buka browser Anda dan ketikkan alamat berikut:
    **`http://localhost:5001`**
    
    *Boom! Anda akan melihat grafik lilin (Candlestick) khas TradingView yang terbentuk dan bergerak otomatis secara real-time setiap kali Spark selesai menghitung 10-detik kumpulan data!*

---

## 🛑 Langkah 6: Mematikan Proyek (Teardown)

Jika Anda sudah selesai melakukan demo atau praktik, matikan semua sistem agar memori komputer Anda tidak terkuras.

1.  Hentikan proses `spark-submit` di terminal yang sedang berjalan dengan menekan tombol `CTRL + C`.
2.  Lalu, jalankan perintah *teardown* berikut di terminal:
    ```bash
    docker-compose down
    ```
    *(Tunggu hingga indikator status "Removed" muncul pada layar untuk memastikan semua komponen infrastruktur telah dimatikan).*
