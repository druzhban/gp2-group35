import requests
import pandas as pd
import time
import json
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

class RAWGDataCollector:
    def __init__(self, api_key):
        self.base_url = "https://api.rawg.io/api"
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        # Создание директории для данных
        self.data_dir = "rawg_data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def make_request(self, endpoint, params=None, max_retries=3):
        """Базовый метод для выполнения API запросов с повторными попытками"""
        if params is None:
            params = {}
        
        params['key'] = self.api_key
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=15)
                # Проверка на лимит запросов
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"Достигнут лимит запросов. Ожидание {retry_after} секунд...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Попытка {attempt + 1}/{max_retries} не удалась для {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Экспоненциальная задержка
                else:
                    print(f"Все попытки исчерпаны для запроса {url}")
                    return None
        return None
    
    def safe_get(self, data, key, default=None):
        """Безопасное получение значения из словаря"""
        if data is None:
            return default
        if isinstance(data, dict):
            return data.get(key, default)
        return default
    
    def safe_get_list(self, data, key, default=None):
        """Безопасное получение списка, гарантирует возврат списка"""
        if default is None:
            default = []
        if data is None:
            return default
        value = data.get(key, default) if isinstance(data, dict) else default
        return value if isinstance(value, list) else default
    
    def save_to_file(self, data, filename):
        """Сохранение данных в файл JSON"""
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Данные сохранены в: {filepath}")
        return filepath
    
    def create_unified_dataset(self, results):
        """Создание единого датасета в формате CSV из всех собранных данных"""
        if not results['games']:
            print("Нет данных об играх для объединения")
            return None
        
        try:
            print("СОЗДАНИЕ ЕДИНОГО ДАТАСЕТА В ФОРМАТЕ CSV")

            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unified_files = []
            
            # 1. создание csv файла для игр 
            if results.get('games'):
                games_filename = f"unified_games_{timestamp}.csv"
                games_filepath = os.path.join(self.data_dir, games_filename)
                df_games = pd.DataFrame(results['games'])
                df_games.to_csv(games_filepath, index=False, encoding='utf-8-sig')
                print(f"Сохранено: {len(df_games)} игр в файле {games_filename}")
                unified_files.append(games_filename)
            
            # 2. cоздание csv файла для жанров
            if results.get('genres'):
                genres_filename = f"unified_genres_{timestamp}.csv"
                genres_filepath = os.path.join(self.data_dir, genres_filename)
                df_genres = pd.DataFrame(results['genres'])
                df_genres.to_csv(genres_filepath, index=False, encoding='utf-8-sig')
                print(f"Сохранено: {len(df_genres)} жанров в файле {genres_filename}")
                unified_files.append(genres_filename)
            
            # 3. создание csv файла для платформ
            if results.get('platforms'):
                platforms_filename = f"unified_platforms_{timestamp}.csv"
                platforms_filepath = os.path.join(self.data_dir, platforms_filename)
                df_platforms = pd.DataFrame(results['platforms'])
                df_platforms.to_csv(platforms_filepath, index=False, encoding='utf-8-sig')
                print(f"Сохранено: {len(df_platforms)} платформ в файле {platforms_filename}")
                unified_files.append(platforms_filename)
            
            # 4. создание csv файла для разработчиков
            if results.get('developers'):
                developers_filename = f"unified_developers_{timestamp}.csv"
                developers_filepath = os.path.join(self.data_dir, developers_filename)
                df_developers = pd.DataFrame(results['developers'])
                df_developers.to_csv(developers_filepath, index=False, encoding='utf-8-sig')
                print(f"Сохранено: {len(df_developers)} разработчиков в файле {developers_filename}")
                unified_files.append(developers_filename)
            
            # 5. создание csv файла для магазинов
            if results.get('stores'):
                stores_filename = f"unified_stores_{timestamp}.csv"
                stores_filepath = os.path.join(self.data_dir, stores_filename)
                df_stores = pd.DataFrame(results['stores'])
                df_stores.to_csv(stores_filepath, index=False, encoding='utf-8-sig')
                print(f" Сохранено: {len(df_stores)} магазинов в файле {stores_filename}")
                unified_files.append(stores_filename)
            
            print(f"\n Все CSV файлы успешно сохранены в директорию: {self.data_dir}")
            for file in unified_files:
                print(f"- {file}")
            
            return {
                'games_file': games_filename if results.get('games') else None,
                'genres_file': genres_filename if results.get('genres') else None,
                'platforms_file': platforms_filename if results.get('platforms') else None,
                'developers_file': developers_filename if results.get('developers') else None,
                'stores_file': stores_filename if results.get('stores') else None,
                'file_paths': [os.path.join(self.data_dir, f) for f in unified_files]
            }
            
        except Exception as e:
            print(f"Ошибка при создании единого датасета: {e}")
            return None
    
    # ЗАПРОС 1: Сбор игр
    def collect_5k_games(self):
        """Сбор 5,000 игр с пагинацией и фильтрацией тестовых данных"""
        print("\n ЗАПРОС 1: СБОР ИГР ")
        all_games = []
        current_page = 1
        target_count = 5000
        games_per_page = 40
        
        # Рассчитываем количество страниц для 5,000 игр
        total_pages = target_count // games_per_page
        if target_count % games_per_page > 0:
            total_pages += 1
        
        print(f"Будет собрано {target_count} игр с {total_pages} страниц")
        print("Используем фильтры для исключения тестовых данных:")
        print("- Диапазон дат: 1970-01-01 до 2024-12-31")
        print("- Минимум 5 оценок")
        print("- Игнорируем игры с подозрительными датами")
        
        while current_page <= total_pages and len(all_games) < target_count:
            print(f"Запрос страницы {current_page}/{total_pages}... Собрано игр: {len(all_games)}/{target_count}")
            
            # Параметры запроса с фильтрами
            params = {
                'page_size': games_per_page,
                'page': current_page,
                'ordering': '-metacritic,-rating',  # сначала игры с высоким рейтингом
                'dates': '1970-01-01,2024-12-31',  # диапазон дат
                'ratings_count': '5,'  # минимум 5 оценок
            }
            
            data = self.make_request("games", params)
            if not data:
                print(f"Ошибка при получении страницы {current_page}. Пропускаем...")
                current_page += 1
                time.sleep(1)
                continue
            
            results = data.get('results', [])
            if not results:
                print("Получены пустые результаты, стоп сбор.")
                break
            
            for game in results:
                try:
                    # Проверяем, является ли игра тестовой
                    game_name = game.get('name', '').lower()
                    game_slug = game.get('slug', '').lower()
                    
                    # Фильтрация тестовых игр
                    test_keywords = ['test', 'demo', 'prototype', 'sample', 'template', 'debug', 'trash', 'junk', 'example']
                    if any(keyword in game_name or keyword in game_slug for keyword in test_keywords):
                        continue
                    
                    # Пропускаем игры с подозрительными датами (в будущем)
                    released = game.get('released', '')
                    if released and (released.startswith(('203', '204', '205', '206', '207', '208', '209'))):
                        continue
                    
                    # Пропускаем игры без необходимых данных
                    if not game.get('id') or not game.get('name') or not game.get('released'):
                        continue
                    
                    # Пропускаем игры с нулевыми рейтингами
                    if game.get('rating', 0) == 0 and game.get('metacritic', 0) == 0:
                        continue
                    
                    # Пропускаем игры с очень низким количеством оценок
                    if game.get('ratings_count', 0) < 5:
                        continue
                    
                    # Собираем информацию об игре
                    genres = self.safe_get_list(game, 'genres')
                    platforms = self.safe_get_list(game, 'platforms')
                    stores = self.safe_get_list(game, 'stores')
                    tags = self.safe_get_list(game, 'tags')
                    esrb_rating = self.safe_get(game, 'esrb_rating', {})
                    
                    game_info = {
                        'id': game.get('id'),
                        'name': game.get('name'),
                        'slug': game.get('slug'),
                        'released': game.get('released'),
                        'tba': game.get('tba', False),
                        'rating': game.get('rating', 0),
                        'rating_top': game.get('rating_top', 0),
                        'ratings_count': game.get('ratings_count', 0),
                        'metacritic': game.get('metacritic'),
                        'playtime': game.get('playtime', 0),
                        'platforms_count': len(platforms),
                        'genres': ', '.join([self.safe_get(genre, 'name', '') for genre in genres if self.safe_get(genre, 'name', '')]),
                        'stores': ', '.join([self.safe_get(store.get('store', {}), 'name', '') for store in stores if self.safe_get(store.get('store', {}), 'name', '')]),
                        'tags': ', '.join([self.safe_get(tag, 'name', '') for tag in tags[:5] if self.safe_get(tag, 'name', '')]),
                        'esrb_rating': self.safe_get(esrb_rating, 'name', ''),
                        'background_image': game.get('background_image', ''),
                        'added': game.get('added', 0),
                        'suggestions_count': game.get('suggestions_count', 0),
                        'updated': datetime.now().isoformat()
                    }
                    
                    # Удаляем пустые значения для лучшей читаемости
                    for key, value in list(game_info.items()):
                        if value in ['', None, [], {}]:
                            game_info[key] = 'N/A'
                    
                    all_games.append(game_info)
                    
                    # проверка  цели
                    if len(all_games) >= target_count:
                        break
                except Exception as e:
                    print(f"Ошибка при обработке игры: {e}")
                    continue
            
            current_page += 1
            
            # сохранение промежуточные результаты каждые 5 страниц
            if current_page % 5 == 0:
                self.save_to_file(all_games, f"games_partial_{len(all_games)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            # задержка для соблюдения лимитов API
            time.sleep(0.7)
        
        # сохранение всех собранных игр
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.save_to_file(all_games, f"games_5k_{timestamp}.json")
        
        # также сохраняем в csv 
        csv_filename = f"games_5k_{timestamp}.csv"
        df = pd.DataFrame(all_games)
        df.to_csv(os.path.join(self.data_dir, csv_filename), index=False, encoding='utf-8-sig')
        print(f"Данные также сохранены в CSV: {csv_filename}")
        
        # вывод статистики по собранным данным
        if all_games:
            filled_fields = {}
            for field in all_games[0].keys():
                filled_count = sum(1 for game in all_games if game.get(field) not in [None, '', 'N/A', 0])
                filled_fields[field] = filled_count
            
            print("\nСтатистика заполненности полей:")
            for field, count in filled_fields.items():
                percentage = (count / len(all_games)) * 100
                print(f"- {field}: {count}/{len(all_games)} ({percentage:.1f}%)")
        
        print(f"\nСбор завершен! Всего собрано игр: {len(all_games)}")
        return all_games
    
    # ЗАПРОС 2: Данные о жанрах
    def collect_genres(self):
        """Сбор данных о жанрах"""
        print("\n ЗАПРОС 2: СБОР ДАННЫХ О ЖАНРАХ ")
        
        params = {
            'page_size': 20,  # Максимальное количество жанров
            'ordering': '-games_count'  # Сортировка по популярности
        }
        
        data = self.make_request("genres", params)
        if not data:
            return []
        
        genres_data = []
        for genre in data.get('results', []):
            try:
                genre_info = {
                    'id': genre.get('id'),
                    'name': genre.get('name'),
                    'slug': genre.get('slug'),
                    'games_count': genre.get('games_count', 0),
                    'image_background': genre.get('image_background', '')
                }
                genres_data.append(genre_info)
            except Exception as e:
                print(f"Ошибка при обработке жанра: {e}")
                continue
        
        self.save_to_file(genres_data, f"genres_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        return genres_data
    
    # ЗАПРОС 3: Данные о платформах
    def collect_platforms(self):
        """Сбор данных о платформах"""
        print("\n ЗАПРОС 3: СБОР ДАННЫХ О ПЛАТФОРМАХ ")
        
        params = {
            'page_size': 20,  # Максимальное количество платформ
            'ordering': '-games_count'  # Сортировка по популярности
        }
        
        data = self.make_request("platforms", params)
        if not data:
            return []
        
        platforms_data = []
        for platform in data.get('results', []):
            try:
                platform_info = {
                    'id': platform.get('id'),
                    'name': platform.get('name'),
                    'slug': platform.get('slug'),
                    'platforms_count': platform.get('platforms_count', 0),
                    'games_count': platform.get('games_count', 0)
                }
                platforms_data.append(platform_info)
            except Exception as e:
                print(f"Ошибка при обработке платформы: {e}")
                continue
        
        self.save_to_file(platforms_data, f"platforms_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        return platforms_data
    
    # ЗАПРОС 4: Данные о разработчиках
    def collect_developers(self):
        """Сбор данных о разработчиках"""
        print("\n  ЗАПРОС 4: СБОР ДАННЫХ О РАЗРАБОТЧИКАХ ")
        
        params = {
            'page_size': 20,  # Максимальное количество разработчиков
            'ordering': '-games_count'  # Сортировка по популярности
        }
        
        data = self.make_request("developers", params)
        if not data:
            return []
        
        developers_data = []
        for developer in data.get('results', []):
            try:
                dev_info = {
                    'id': developer.get('id'),
                    'name': developer.get('name'),
                    'slug': developer.get('slug'),
                    'games_count': developer.get('games_count', 0),
                    'image_background': developer.get('image_background', '')
                }
                developers_data.append(dev_info)
            except Exception as e:
                print(f"Ошибка при обработке разработчика: {e}")
                continue
        
        self.save_to_file(developers_data, f"developers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        return developers_data
    
    # ЗАПРОС 5: Данные о магазинах
    def collect_stores(self):
        """Сбор данных о магазинах"""
        print("\n ЗАПРОС 5: СБОР ДАННЫХ О МАГАЗИНАХ ")
        
        params = {
            'page_size': 20  # Максимальное количество магазинов
        }
        
        data = self.make_request("stores", params)
        if not data:
            return []
        
        stores_data = []
        for store in data.get('results', []):
            try:
                store_info = {
                    'id': store.get('id'),
                    'name': store.get('name'),
                    'slug': store.get('slug'),
                    'domain': store.get('domain', ''),
                    'games_count': store.get('games_count', 0)
                }
                stores_data.append(store_info)
            except Exception as e:
                print(f"Ошибка при обработке магазина: {e}")
                continue
        
        self.save_to_file(stores_data, f"stores_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        return stores_data
    
    def execute_all_requests(self):
        """Выполнение всех 5 запросов к API"""
        print("СБОР ИГР И ДАННЫХ С RAWG API")
        
        # Запрос 1: Сбор игр
        games = self.collect_5k_games()
        
        # Остальные запросы (выполняются после сбора игр)
        print("\nВыполнение остальных 4 запросов к разным endpoint...")
        
        # Запрос 2: Жанры
        genres = self.collect_genres()
        
        # Запрос 3: Платформы
        platforms = self.collect_platforms()
        
        # Запрос 4: Разработчики
        developers = self.collect_developers()
        
        # Запрос 5: Магазины
        stores = self.collect_stores()

        #  вызов метода создания единого датасета
        unified_dataset_info = self.create_unified_dataset({
            'games': games,
            'genres': genres,
            'platforms': platforms,
            'developers': developers,
            'stores': stores
        })
        
        
        print("ВСЕ ЗАПРОСЫ ВЫПОЛНЕНЫ УСПЕШНО")
        print(f"Результаты сохранены в директорию: {self.data_dir}")
        print(f"- Игр собрано: {len(games)}")
        print(f"- Жанров: {len(genres)}")
        print(f"- Платформ: {len(platforms)}")
        print(f"- Разработчиков: {len(developers)}")
        print(f"- Магазинов: {len(stores)}")
        if unified_dataset_info:
            print("- Единый датасет создан в формате csv")
            for file in unified_dataset_info['file_paths']:
                print(f"  • {os.path.basename(file)}")
        
        
        return {
            'games': games,
            'genres': genres,
            'platforms': platforms,
            'developers': developers,
            'stores': stores,
            'unified_dataset': unified_dataset_info
        }
    
def main():
    API_KEY = "a511d306d1ac4d909376d273baa02a42"  
    
    if not API_KEY:
        print("Ошибка: Не указан API ключ!")
        return
    
    collector = RAWGDataCollector(API_KEY)
    results = collector.execute_all_requests()
    
    # Вывод краткой статистики
    print("\nКРАТКАЯ СТАТИСТИКА ПО СОБРАННЫМ ДАННЫМ:")
    print(f" Всего игр: {len(results['games'])}")
    print(f" Уникальных жанров: {len(results['genres'])}")
    print(f" Платформ: {len(results['platforms'])}")
    print(f" Разработчиков: {len(results['developers'])}")
    print(f" Магазинов: {len(results['stores'])}")
    
    if results['games']:
        ratings = [game['rating'] for game in results['games'] if game.get('rating', 0) > 0 and game.get('rating') != 'N/A']
        if ratings:
            print(f"- Средний рейтинг собранных игр: {sum(ratings)/len(ratings):.2f}")
        
        playtimes = [game['playtime'] for game in results['games'] if game.get('playtime', 0) > 0 and game.get('playtime') != 'N/A']
        if playtimes:
            print(f"- Среднее время игры: {sum(playtimes)/len(playtimes):.1f} часов")
            
    if results.get('unified_dataset'):
        print(f"\n Единый датасет готов ")
        print("Файлы находятся в директории:", collector.data_dir)

if __name__ == "__main__":
    main()