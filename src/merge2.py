import pandas as pd
import glob

def merge_api_results():
    # Ищем все CSV-файлы в папке API
    files = sorted(glob.glob("API/*.csv"))
    print(f" Найдено {len(files)} файлов для объединения")

    all_data = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_data.append(df)
            print(f" Добавлен {f} ({len(df)} строк)")
        except Exception as e:
            print(f" Ошибка при чтении {f}: {e}")

    result = pd.concat(all_data, ignore_index=True)

    result.to_csv("API_merged.csv", index=False, encoding="utf-8-sig")
    print(f"\n Сохранено в API_merged.csv ({len(result)} строк)")

if __name__ == "__main__":
    merge_api_results()
