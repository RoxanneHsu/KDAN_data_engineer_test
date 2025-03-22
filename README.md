# KDAN_data_engineer_test
## 系統架構概覽

#### 目標

- 自動化爬取指定台股股票之每日收盤價
- 透過 GCP 服務建立
- 並可於 Looker Studio 顯示折線圖 / 表格

#### 系統架構與運作流程
```
Cloud Scheduler → Cloud Functions → Python 爬蟲程式 → BigQuery → Looker Studio
```
#### 運作說明
- Cloud Scheduler 每日 18:00 定時觸發 Cloud Function
- Cloud Function 執行 Python 爬蟲，抓取 TWSE 官方 API 當日股價
- 抓取結果寫入 BigQuery，自動去除重複資料
- Looker Studio 連接 BigQuery，自動更新收盤價資料，以折線圖或表格呈現

預期每日僅執行一次，符合 GCP 免費額度，具成本效益。

#### Docker 支援 - 本地端開發測試友善
- 支援本地 Docker 環境測試，開發效率更高
- 同一套程式碼可兼容：
    - 本地 docker run
    - 雲端 Cloud Function

- 減少維護成本，無需開發兩套版本


## 部署與使用流程
#### 先決要件
- GCP 專案 (Project): 需擁有一個 GCP 專案，並取得 PROJECT_ID,
- BigQuery Dataset & Table:	須先建立 Dataset，或程式內自動建立
- Cloud Function: Python 爬蟲程式打包為 Cloud Function(可手動或自動部署)
- Cloud Scheduler	排程器設定好 CRON 表達式，指定 HTTP 觸發 Cloud Function(可手動或自動部署)
- Looker Studio	連接 BigQuery，設計圖表與報表

#### 雲端運行（排程自動觸發）
- 完整串接後，排程自動運行 ETL 流程無需手動執行

#### 本地端運行
`docker build -t local-stock-crawler . `    

`docker run --rm -it local-stock-crawler`


### Looker Studio
- [link](https://lookerstudio.google.com/reporting/a620b954-92ae-4fc5-b7c8-01e46be1f8f7)