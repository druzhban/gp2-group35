import pandas as pd
import glob

def merge_results():
    # Собираем все CSV из папки results
    files = sorted(glob.glob("results/*.csv"))
    print(f" Найдено {len(files)} файлов для объединения")

    # Читаем и добавляем в список
    all_data = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_data.append(df)
            print(f" Добавлен {f} ({len(df)} строк)")
        except Exception as e:
            print(f" Ошибка при чтении {f}: {e}")

    # Объединяем в один DataFrame
    result = pd.concat(all_data, ignore_index=True)

    result.to_csv("steam_final_dataset.csv", index=False, encoding="utf-8-sig")
    print(f"\n Сохранено в steam_final_dataset.csv ({len(result)} строк)")

if __name__ == "__main__":
    merge_results()

