import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from scipy.sparse import csr_matrix, save_npz, load_npz, hstack
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
import pickle
import re
import hashlib
from datetime import datetime
import os
import logging
from typing import Dict, List, Tuple, Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class DataPipeline:
    """Модуль сбора и предобработки данных"""

    def __init__(self, db_config: dict, models_path: str):
        self.db_config = db_config
        self.models_path = models_path
        self.engine = None
        self.connection = None

        os.makedirs(models_path, exist_ok=True)

        # Данные
        self.movies_df = None
        self.reviews_df = None
        self.genres_df = None
        self.subgenres_df = None
        self.user_genres_df = None
        self.user_years_df = None
        self.user_main_df = None
        self.countries_df = None

        # Векторизаторы
        self.tfidf_vectorizer = None
        self.tfidf_matrix = None
        self.genre_vectors = None
        self.actor_vectors = None
        self.director_vectors = None

        # Метаданные
        self.current_year = datetime.now().year
        self.data_hash = None

    def _create_connection(self):
        """Создает подключение к БД"""
        try:
            db_url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@" \
                     f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(db_url)
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("Подключение к БД установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return False

    def _execute_query(self, query: str, params: tuple = None) -> List[dict]:
        """Выполняет SQL запрос"""
        if not self.connection:
            return []
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return cursor.fetchall()
                return []
        except Exception as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            return []

    def _load_table(self, table_name: str, columns: List[str] = None) -> pd.DataFrame:
        """Загружает таблицу из БД"""
        if not self.engine:
            return pd.DataFrame()
        try:
            if columns:
                query = f"SELECT {', '.join(columns)} FROM db.{table_name}"
            else:
                query = f"SELECT * FROM db.{table_name}"
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logger.error(f"Ошибка загрузки {table_name}: {e}")
            return pd.DataFrame()

    def get_data_hash(self) -> str:
        """Вычисляет хеш текущего состояния данных"""
        try:
            query = """
                SELECT 
                    COALESCE(MAX(pg_stat_all_tables.last_vacuum), '1900-01-01') as last_change
                FROM pg_stat_all_tables
                WHERE schemaname = 'db'
            """
            result = self._execute_query(query)
            last_change = result[0]['last_change'] if result else datetime.now()

            tables = ['movies', 'reviews', 'users', 'genres']
            hash_str = str(last_change)
            for table in tables:
                count_query = f"SELECT COUNT(*) as cnt FROM db.{table}"
                count_result = self._execute_query(count_query)
                hash_str += f":{table}:{count_result[0]['cnt'] if count_result else 0}"

            return hashlib.md5(hash_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Ошибка вычисления хеша: {e}")
            return hashlib.md5(str(datetime.now()).encode()).hexdigest()

    async def load_data(self) -> bool:
        """Асинхронная загрузка данных"""
        logger.info("Начало загрузки данных...")

        if not self._create_connection():
            return False

        # Загружаем таблицы параллельно
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                'movies': executor.submit(self._load_table, 'movies'),
                'reviews': executor.submit(self._load_table, 'reviews'),
                'users': executor.submit(self._load_table, 'users'),
                'genres': executor.submit(self._load_table, 'genres'),
                'subgenres': executor.submit(self._load_table, 'subgenres'),
                'user_genres': executor.submit(self._load_table, 'user_interests_genres'),
                'user_years': executor.submit(self._load_table, 'user_interests_years'),
                'countries': executor.submit(self._load_table, 'countries')
            }

            self.movies_df = futures['movies'].result()
            self.reviews_df = futures['reviews'].result()
            self.user_main_df = futures['users'].result()
            self.genres_df = futures['genres'].result()
            self.subgenres_df = futures['subgenres'].result()
            self.user_genres_df = futures['user_genres'].result()
            self.user_years_df = futures['user_years'].result()
            self.countries_df = futures['countries'].result()

        logger.info(f"Загружено: {len(self.movies_df)} фильмов, {len(self.reviews_df)} ревью")

        # Извлечение movie_id
        if 'movie_url' in self.movies_df.columns and 'movie_id' not in self.movies_df.columns:
            self.movies_df['movie_id'] = self.movies_df['movie_url'].str.extract(r'/title/(tt\d+)/')

        if len(self.reviews_df) > 0 and 'movie_review_url' in self.reviews_df.columns:
            self.reviews_df['movie_id'] = self.reviews_df['movie_review_url'].str.extract(r'/title/(tt\d+)/')
            self.reviews_df['user_url_clean'] = self.reviews_df['user_url'].str.split('?').str[0]
            self.reviews_df['user_url_clean'] = self.reviews_df['user_url_clean'].str.replace(
                'https://www.imdb.com', '').str.replace('http://www.imdb.com', '').str.strip('/')
            self.reviews_df['rating'] = pd.to_numeric(self.reviews_df['rating'], errors='coerce')

        # После загрузки reviews_df
        if len(self.reviews_df) > 0 and 'user_url' in self.reviews_df.columns:
            # Нормализуем user_url
            def normalize_user_url(url):
                if pd.isna(url):
                    return ''
                url = str(url).strip()
                url = url.replace('https://www.imdb.com', '')
                url = url.replace('http://www.imdb.com', '')
                url = url.rstrip('/')
                url = url.split('?')[0]
                return url

            self.reviews_df['user_url_normalized'] = self.reviews_df['user_url'].apply(normalize_user_url)
            self.reviews_df['user_url_clean'] = self.reviews_df['user_url_normalized']

            logger.info(f"Создана нормализованная колонка user_url_normalized")

        self.data_hash = self.get_data_hash()
        return True

    def preprocess_data(self):
        """Предобработка данных"""
        logger.info("Предобработка данных...")

        # Заполнение пропусков
        text_fields = ['genre', 'plot', 'directors', 'actors', 'country',
                       'title_ru', 'description_ru', 'directors_ru', 'actors_ru']
        for field in text_fields:
            if field in self.movies_df.columns:
                self.movies_df[field] = self.movies_df[field].fillna('').astype(str)
            else:
                self.movies_df[field] = ''

        # Извлечение года
        def extract_year(year_val):
            if pd.isna(year_val):
                return None
            match = re.search(r'(\d{4})', str(year_val))
            return int(match.group(1)) if match else None

        self.movies_df['year_num'] = self.movies_df['year'].apply(extract_year)

        # Нормализация рейтингов
        imdb_series = pd.to_numeric(self.movies_df.get('imdb', 0), errors='coerce')
        imdb_min, imdb_max = imdb_series.min(), imdb_series.max()
        if pd.notna(imdb_min) and pd.notna(imdb_max) and imdb_max > imdb_min:
            self.movies_df['imdb_norm'] = (imdb_series - imdb_min) / (imdb_max - imdb_min)
        else:
            self.movies_df['imdb_norm'] = 0.5
        self.movies_df['imdb_norm'] = self.movies_df['imdb_norm'].fillna(0.5)

        # Количество голосов
        self.movies_df['votes'] = pd.to_numeric(self.movies_df.get('number_of_imdb_votes', 0),
                                                errors='coerce').fillna(0)
        self.movies_df['votes_log'] = np.log1p(self.movies_df['votes'].values)

        # Свежесть
        def safe_age(year_val):
            if pd.isna(year_val):
                return 50
            try:
                return max(0, self.current_year - int(year_val))
            except:
                return 50

        self.movies_df['age_years'] = self.movies_df['year_num'].apply(safe_age)
        self.movies_df['recency_score'] = np.exp(-self.movies_df['age_years'] / 20)

        # Комбинированные признаки
        self.movies_df['combined_features'] = (
                self.movies_df['genre'] + ' ' +
                self.movies_df['plot'] + ' ' +
                self.movies_df['directors'] + ' ' +
                self.movies_df['actors'] + ' ' +
                self.movies_df['country']
        )

    def create_feature_vectors(self):
        """Создание векторов признаков"""
        logger.info("Создание векторов признаков...")

        # TF-IDF
        self.tfidf_vectorizer = TfidfVectorizer(
            stop_words='english',
            max_features=15000,
            max_df=0.8,
            min_df=3,
            ngram_range=(1, 2),
            sublinear_tf=True
        )
        self.tfidf_matrix = self.tfidf_vectorizer.fit_transform(
            self.movies_df['combined_features'].fillna('')
        )
        logger.info(f"TF-IDF матрица: {self.tfidf_matrix.shape}")

        # Жанровые векторы
        all_genres = set()
        for genres in self.movies_df['genre'].str.split(','):
            if isinstance(genres, list):
                all_genres.update([g.strip() for g in genres if g.strip()])

        self.genre_list = sorted(list(all_genres))
        genre_to_idx = {g: i for i, g in enumerate(self.genre_list)}

        genre_matrix = np.zeros((len(self.movies_df), len(self.genre_list)))
        for idx, row in self.movies_df.iterrows():
            genres = [g.strip() for g in str(row['genre']).split(',') if g.strip()]
            for genre in genres:
                if genre in genre_to_idx:
                    genre_matrix[idx, genre_to_idx[genre]] = 1

        self.genre_vectors = csr_matrix(genre_matrix)
        logger.info(f"Жанровые векторы: {self.genre_vectors.shape}")

        # Актеры (топ-500)
        all_actors = set()
        for actors in self.movies_df['actors'].str.split(','):
            if isinstance(actors, list):
                all_actors.update([a.strip() for a in actors if a.strip()])

        self.top_actors = sorted(list(all_actors))[:500]
        actor_to_idx = {a: i for i, a in enumerate(self.top_actors)}

        actor_matrix = np.zeros((len(self.movies_df), len(self.top_actors)))
        for idx, row in self.movies_df.iterrows():
            actors = [a.strip() for a in str(row['actors']).split(',') if a.strip()]
            for actor in actors[:10]:
                if actor in actor_to_idx:
                    actor_matrix[idx, actor_to_idx[actor]] = 1

        self.actor_vectors = csr_matrix(actor_matrix)
        logger.info(f"Актеры: {self.actor_vectors.shape}")

        # Режиссеры (топ-200)
        all_directors = set()
        for directors in self.movies_df['directors'].str.split(','):
            if isinstance(directors, list):
                all_directors.update([d.strip() for d in directors if d.strip()])

        self.top_directors = sorted(list(all_directors))[:200]
        director_to_idx = {d: i for i, d in enumerate(self.top_directors)}

        director_matrix = np.zeros((len(self.movies_df), len(self.top_directors)))
        for idx, row in self.movies_df.iterrows():
            directors = [d.strip() for d in str(row['directors']).split(',') if d.strip()]
            for director in directors[:5]:
                if director in director_to_idx:
                    director_matrix[idx, director_to_idx[director]] = 1

        self.director_vectors = csr_matrix(director_matrix)
        logger.info(f"Режиссеры: {self.director_vectors.shape}")

    def create_user_item_matrix(self) -> csr_matrix:
        """Создание матрицы пользователь-фильм"""
        if self.reviews_df is None or len(self.reviews_df) == 0:
            return None

        logger.info("Создание user-item матрицы...")

        # Очистка данных
        reviews_clean = self.reviews_df.dropna(subset=['user_url_clean', 'movie_id', 'rating'])
        reviews_clean['rating'] = pd.to_numeric(reviews_clean['rating'], errors='coerce')
        reviews_clean = reviews_clean.dropna(subset=['rating'])

        if len(reviews_clean) == 0:
            return None

        # Фильтрация
        user_counts = reviews_clean.groupby('user_url_clean').size()
        movie_counts = reviews_clean.groupby('movie_id').size()

        active_users = user_counts[user_counts >= 5].index.tolist()
        popular_movies = movie_counts[movie_counts >= 3].index.tolist()

        filtered_reviews = reviews_clean[
            reviews_clean['user_url_clean'].isin(active_users) &
            reviews_clean['movie_id'].isin(popular_movies)
            ]

        if len(filtered_reviews) == 0:
            return None

        # Создание индексов
        self.user_indices = {user: i for i, user in enumerate(active_users)}
        self.movie_indices = {movie: i for i, movie in enumerate(popular_movies)}
        self.user_list = active_users
        self.movie_list = popular_movies

        # Создание матрицы
        rows, cols, values = [], [], []
        for _, row in filtered_reviews.iterrows():
            if row['user_url_clean'] in self.user_indices and row['movie_id'] in self.movie_indices:
                rows.append(self.user_indices[row['user_url_clean']])
                cols.append(self.movie_indices[row['movie_id']])
                rating = max(0, min(10, float(row['rating'])))
                values.append(rating / 10.0)

        if len(rows) == 0:
            return None

        matrix = csr_matrix((values, (rows, cols)),
                            shape=(len(active_users), len(popular_movies)))

        logger.info(f"User-item матрица: {matrix.shape}, ненулевых: {matrix.nnz}")
        return matrix

    def compute_popularity_scores(self) -> np.ndarray:
        """Вычисление популярности фильмов"""
        logger.info("Вычисление популярности...")

        def safe_numeric(series, default=0):
            numeric = pd.to_numeric(series, errors='coerce').fillna(default)
            return numeric.values

        # IMDb рейтинг
        imdb_scores = safe_numeric(self.movies_df['imdb'], 0)
        imdb_min, imdb_max = imdb_scores.min(), imdb_scores.max()
        imdb_norm = (imdb_scores - imdb_min) / (imdb_max - imdb_min) if imdb_max > imdb_min else np.zeros_like(
            imdb_scores)

        # Голоса
        votes = np.maximum(safe_numeric(self.movies_df['number_of_imdb_votes'], 0), 0)
        votes_log = np.log1p(votes)
        votes_max = votes_log.max()
        votes_norm = votes_log / votes_max if votes_max > 0 else np.zeros_like(votes_log)

        # Рецензии
        user_reviews = np.maximum(safe_numeric(self.movies_df['number_of_user_reviews'], 0), 0)
        reviews_log = np.log1p(user_reviews)
        reviews_max = reviews_log.max()
        reviews_norm = reviews_log / reviews_max if reviews_max > 0 else np.zeros_like(reviews_log)

        # Взвешенная популярность
        scores = 0.5 * imdb_norm + 0.25 * votes_norm + 0.25 * reviews_norm
        return np.nan_to_num(scores, nan=0.0)

    def compute_recency_scores(self) -> np.ndarray:
        """Вычисление свежести фильмов"""
        logger.info("Вычисление свежести...")

        def safe_year(year_val):
            if pd.isna(year_val):
                return self.current_year
            try:
                match = re.search(r'(\d{4})', str(year_val))
                return int(match.group(1)) if match else self.current_year
            except:
                return self.current_year

        years = self.movies_df['year'].apply(safe_year).values
        age_years = np.maximum(self.current_year - years, 0)
        scores = np.exp(-age_years / 20)
        return np.nan_to_num(scores, nan=0.5)

    async def run_pipeline(self) -> Dict:
        """Запуск полного пайплайна"""
        logger.info("Запуск пайплайна обработки данных...")

        # Загрузка
        if not await self.load_data():
            raise Exception("Не удалось загрузить данные")

        # Предобработка
        self.preprocess_data()

        # Создание векторов
        self.create_feature_vectors()

        # User-item матрица
        user_item_matrix = self.create_user_item_matrix()

        # Популярность и свежесть
        popularity_scores = self.compute_popularity_scores()
        recency_scores = self.compute_recency_scores()

        # Комбинированные признаки для схожести
        from sklearn.preprocessing import normalize

        tfidf_norm = normalize(self.tfidf_matrix, norm='l2')
        genre_norm = normalize(self.genre_vectors, norm='l2')
        actor_norm = normalize(self.actor_vectors, norm='l2')
        director_norm = normalize(self.director_vectors, norm='l2')

        combined_features = hstack([
            tfidf_norm * 0.4,
            genre_norm * 0.3,
            actor_norm * 0.2,
            director_norm * 0.1
        ])

        logger.info("Пайплайн обработки завершен")

        return {
            'movies_df': self.movies_df,
            'reviews_df': self.reviews_df,
            'user_main_df': self.user_main_df,
            'genres_df': self.genres_df,
            'subgenres_df': self.subgenres_df,
            'user_genres_df': self.user_genres_df,
            'user_years_df': self.user_years_df,
            'countries_df': self.countries_df,
            'tfidf_vectorizer': self.tfidf_vectorizer,
            'tfidf_matrix': self.tfidf_matrix,
            'genre_vectors': self.genre_vectors,
            'actor_vectors': self.actor_vectors,
            'director_vectors': self.director_vectors,
            'user_item_matrix': user_item_matrix,
            'popularity_scores': popularity_scores,
            'recency_scores': recency_scores,
            'combined_features': combined_features,
            'user_indices': getattr(self, 'user_indices', None),
            'movie_indices': getattr(self, 'movie_indices', None),
            'user_list': getattr(self, 'user_list', None),
            'movie_list': getattr(self, 'movie_list', None),
            'genre_list': self.genre_list,
            'top_actors': self.top_actors,
            'top_directors': self.top_directors,
            'data_hash': self.data_hash
        }

    def save_data(self, data: Dict):
        """Сохраняет обработанные данные"""
        logger.info("Сохранение обработанных данных...")

        # Сохранение DataFrame'ов
        data['movies_df'].to_pickle(f'{self.models_path}movies_df.pkl')
        if data['reviews_df'] is not None:
            data['reviews_df'].to_pickle(f'{self.models_path}reviews_df.pkl')
        if data['user_main_df'] is not None:
            data['user_main_df'].to_pickle(f'{self.models_path}user_main_df.pkl')
        if data['genres_df'] is not None:
            data['genres_df'].to_pickle(f'{self.models_path}genres_df.pkl')

        # Сохранение векторизаторов и матриц
        with open(f'{self.models_path}tfidf_vectorizer.pkl', 'wb') as f:
            pickle.dump(data['tfidf_vectorizer'], f)

        save_npz(f'{self.models_path}tfidf_matrix.npz', data['tfidf_matrix'])
        save_npz(f'{self.models_path}genre_vectors.npz', data['genre_vectors'])
        save_npz(f'{self.models_path}actor_vectors.npz', data['actor_vectors'])
        save_npz(f'{self.models_path}director_vectors.npz', data['director_vectors'])
        save_npz(f'{self.models_path}combined_features.npz', data['combined_features'])

        if data['user_item_matrix'] is not None:
            save_npz(f'{self.models_path}user_item_matrix.npz', data['user_item_matrix'])

        # Сохранение массивов
        np.save(f'{self.models_path}popularity_scores.npy', data['popularity_scores'])
        np.save(f'{self.models_path}recency_scores.npy', data['recency_scores'])

        # Сохранение списков
        with open(f'{self.models_path}movie_ids.pkl', 'wb') as f:
            pickle.dump(data['movies_df']['movie_id'].dropna().tolist(), f)

        if data['user_indices'] is not None:
            with open(f'{self.models_path}user_indices.pkl', 'wb') as f:
                pickle.dump(data['user_indices'], f)
            with open(f'{self.models_path}movie_indices.pkl', 'wb') as f:
                pickle.dump(data['movie_indices'], f)
            with open(f'{self.models_path}user_list.pkl', 'wb') as f:
                pickle.dump(data['user_list'], f)
            with open(f'{self.models_path}movie_list.pkl', 'wb') as f:
                pickle.dump(data['movie_list'], f)

        # Сохранение хеша
        with open(f'{self.models_path}data_hash.txt', 'w') as f:
            f.write(data['data_hash'])

        # Сохранение метаданных
        metadata = {
            'genre_list': data['genre_list'],
            'top_actors': data['top_actors'],
            'top_directors': data['top_directors'],
            'num_movies': len(data['movies_df']),
            'num_users': len(data['user_list']) if data['user_list'] else 0,
            'num_reviews': len(data['reviews_df']) if data['reviews_df'] is not None else 0
        }

        with open(f'{self.models_path}metadata.pkl', 'wb') as f:
            pickle.dump(metadata, f)

        logger.info("Данные сохранены")
