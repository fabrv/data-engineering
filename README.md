# Proyecto Data Engineering
Levantar con docker con el siguiente comando:
```bash
docker compose up --build -d
```


Correr los archivos en el siguiente orden:
```bash
python ingest.py
python flatten.py
python silver.py
python load.py
python gold.py
```
