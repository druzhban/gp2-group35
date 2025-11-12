# 🚀 UNIVERSAL STEAM SCRAPER (multi-device, resilient, staged)
# Поддерживает: многопоточность, несколько устройств, логирование, восстановление прогресса

import os
import csv
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# ---------------- CONFIG ----------------
DEVICE_ID = 1          # номер устройства (1, 2, 3)
DEVICE_COUNT = 3       # общее количество устройств
MAX_THREADS = 5        # потоков
BATCH_SIZE = 40        # сколько ссылок парсить перед сохранением
OUTPUT_DIR = "results"
os.makedirs(OUTPUT_DIR, exist_ok=True)
PROGRESS_FILE = f"progress_{DEVICE_ID}.txt"
ERROR_LOG = f"errors_{DEVICE_ID}.log"
# ----------------------------------------

def init_driver():
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=opts)

# ======================================================
#  ЭТАП 1: СБОР ССЫЛОК (разделяется между устройствами)
# ======================================================
def get_search_pages():
    """Генерация страниц поиска"""
    base = "https://store.steampowered.com/search/?category1=998&page="
    pages = [f"{base}{i}" for i in range(1, 401)]  # пример: 300 страниц
    return pages[DEVICE_ID-1::DEVICE_COUNT]

def collect_links_from_page(url):
    driver = init_driver()
    links = set()
    try:
        driver.get(url)
        time.sleep(random.uniform(1.5, 2.5))
        for it in driver.find_elements(By.CSS_SELECTOR, ".search_result_row"):
            href = it.get_attribute("href")
            if href:
                links.add(href.split("?")[0])
    except Exception as e:
        print(f"⚠️ Ошибка страницы {url}: {e}")
    finally:
        driver.quit()
    return list(links)

def collect_links():
    pages = get_search_pages()
    print(f"🧩 Устройство {DEVICE_ID}: {len(pages)} страниц поиска")
    all_links = []
    with ThreadPoolExecutor(MAX_THREADS) as ex:
        futures = [ex.submit(collect_links_from_page, p) for p in pages]
        for f in as_completed(futures):
            all_links.extend(f.result())
    filename = f"links_part_{DEVICE_ID}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([[x] for x in sorted(set(all_links))])
    print(f"💾 Устройство {DEVICE_ID}: сохранено {len(all_links)} ссылок в {filename}")

# ======================================================
#  ЭТАП 2: ПАРСИНГ ИГР С ВОЗОБНОВЛЕНИЕМ
# ======================================================
def parse_game(link):
    driver = init_driver()
    try:
        driver.get(link)
        time.sleep(random.uniform(1.5, 3))
        title = driver.find_element(By.CSS_SELECTOR, ".apphub_AppName").text
        release = driver.find_element(By.CSS_SELECTOR, ".date").text
        price_el = driver.find_elements(By.CSS_SELECTOR, ".game_purchase_price, .discount_final_price")
        price = price_el[0].text if price_el else "N/A"
        return {"title": title, "release": release, "price": price, "url": link}
    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as ef:
            ef.write(f"{link} | {e}\n")
        return {"url": link, "error": str(e)}
    finally:
        driver.quit()

def get_completed_links():
    """Считывает список уже обработанных ссылок"""
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, encoding="utf-8") as f:
        return set(x.strip() for x in f if x.strip())

def update_progress(link):
    """Добавляет ссылку в лог прогресса"""
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(link + "\n")

def parse_links():
    filename = f"links_part_{DEVICE_ID}.csv"
    if not os.path.exists(filename):
        print(f"❌ Нет файла {filename}. Сначала собери ссылки.")
        return

    with open(filename, encoding="utf-8") as f:
        links = [row[0] for row in csv.reader(f)]

    done = get_completed_links()
    pending = [x for x in links if x not in done]
    print(f"🧩 Устройство {DEVICE_ID}: {len(done)} готово, {len(pending)} осталось.")

    batch_num = 0
    for i in range(0, len(pending), BATCH_SIZE):
        batch = pending[i:i+BATCH_SIZE]
        results = []
        with ThreadPoolExecutor(MAX_THREADS) as ex:
            futures = [ex.submit(parse_game, link) for link in batch]
            for f in as_completed(futures):
                res = f.result()
                results.append(res)
                update_progress(res["url"])

        batch_num += 1
        out_file = os.path.join(OUTPUT_DIR, f"data_part_{DEVICE_ID}_{batch_num}.csv")
        with open(out_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["title", "release", "price", "url", "error"])
            writer.writeheader()
            writer.writerows(results)
        print(f"💾 {DEVICE_ID}: Сохранен файл {out_file} ({len(results)} записей)")
        time.sleep(random.uniform(2, 5))

# ======================================================
#  ДОП: Объединение ссылок и результатов
# ======================================================
def merge_links():
    """Объединяет все links_part_X.csv в один steam_links.csv"""
    all_links = set()
    for i in range(1, DEVICE_COUNT + 1):
        f = f"links_part_{i}.csv"
        if os.path.exists(f):
            with open(f, encoding="utf-8") as ff:
                all_links.update(row[0] for row in csv.reader(ff))
    with open("steam_links.csv", "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([[x] for x in sorted(all_links)])
    print(f"✅ Объединено {len(all_links)} ссылок в steam_links.csv")

def merge_results():
    """Объединяет все CSV с результатами"""
    all_rows = []
    for root, _, files in os.walk(OUTPUT_DIR):
        for file in files:
            if file.endswith(".csv"):
                path = os.path.join(root, file)
                with open(path, encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    all_rows.extend(list(reader))
    with open("steam_full.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "release", "price", "url", "error"])
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"✅ Все части объединены в steam_full.csv ({len(all_rows)} записей)")

# ======================================================
#  ОСНОВНОЙ ВХОД
# ======================================================
if __name__ == "__main__":
    # --- Этап 1: сбор ссылок ---
    # collect_links()

    # --- Этап 2: парсинг (с возобновлением) ---
    parse_links()

    # --- (опционально после всех устройств) ---
    # merge_links()
    # merge_results()
