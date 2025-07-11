# Proyecto Data Engineering

## 🚀 Quick Start
Levantar con docker con el siguiente comando:
```bash
docker compose up --build -d
```

## 📊 Dashboard
Access the analytics dashboard at: **http://localhost:8501**

Or run locally:
```bash
python run_dashboard.py
```

## 🔄 Manual Pipeline Execution
Correr los archivos en el siguiente orden:
```bash
python ingest.py
python flatten.py
python silver.py
python load.py
python gold.py
```

## 📈 Services
- **Mage AI Pipeline**: http://localhost:6789
- **Analytics Dashboard**: http://localhost:8501
- **Slack Notifications**: Configured for pipeline monitoring
