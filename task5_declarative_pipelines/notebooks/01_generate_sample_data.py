import json

dbutils.widgets.text("landing_path", "/Volumes/dbr_dev_ua5816bd/lena066636_bronze/landing/cities/")
dbutils.widgets.dropdown("batch", "1", ["1", "2"])

landing_path = dbutils.widgets.get("landing_path")
batch = dbutils.widgets.get("batch")

batches = {
    # initial load
    "1": [
        {"city_id": 1, "city": "Kyiv",    "population": 2950000, "effective_date": "2026-01-01"},
        {"city_id": 2, "city": "Lviv",    "population": 720000,  "effective_date": "2026-01-01"},
        {"city_id": 3, "city": "Odesa",   "population": 1010000, "effective_date": "2026-01-01"},
        {"city_id": 4, "city": "Kharkiv", "population": 1420000, "effective_date": "2026-01-01"},
    ],
    # Kyiv: population changed (new SCD2 version); Odesa: unchanged (no new version);
    # Dnipro: new city (first version)
    "2": [
        {"city_id": 1, "city": "Kyiv",   "population": 3000000, "effective_date": "2026-02-02"},
        {"city_id": 3, "city": "Odesa",  "population": 1010000, "effective_date": "2026-02-02"},
        {"city_id": 5, "city": "Dnipro", "population": 980000,  "effective_date": "2026-02-02"},
    ],
}

content = "\n".join(json.dumps(row) for row in batches[batch])
dbutils.fs.put(f"{landing_path}cities_batch_{batch}.json", content, overwrite=True)
