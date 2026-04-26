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
        try:
            self.connection.rollback()
        except:
            pass
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

    @staticmethod
    def _normalize_user_url(user_url: str) -> str:
        """Приводит user_url к единому виду для сравнения."""
        if not user_url:
            return ''

        normalized = str(user_url).strip()
        normalized = normalized.split('?', 1)[0]
        normalized = normalized.replace('https://www.imdb.com', '')
        normalized = normalized.replace('http://www.imdb.com', '')
        normalized = re.sub(r'/+', '/', normalized)
        if normalized and not normalized.startswith('/'):
            normalized = f'/{normalized}'
        normalized = normalized.rstrip('/')
        return normalized

    @staticmethod
    def _extract_movie_id(movie_url: str) -> Optional[str]:
        """Извлекает movie_id (tt...) из разных форматов URL."""
        if not movie_url:
            return None

        value = str(movie_url).strip()
        # Частый формат: /title/tt1234567/
        match = re.search(r'/title/([^/?#]+)/?', value)
        if match:
            movie_id = match.group(1).strip()
            if movie_id:
                return movie_id

        # Fallback: ищем tt-id в любом месте строки
        match = re.search(r'(tt\d{5,12})', value)
        if match:
            return match.group(1).strip()

        return None

    def _load_table(self, table_name: str, columns: List[str] = None) -> pd.DataFrame:
        """Загружает таблицу из БД"""
        try:
            self.connection.rollback()
        except:
            pass
        if not self.engine:
            return pd.DataFrame()
        try:
            if columns:
                query = f"SELECT {', '.join(columns)} FROM db.{table_name}"
            else:
                query = f"SELECT * FROM db.{table_name}"

            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            logger.error(f"Ошибка загрузки {table_name}: {e}")
            return pd.DataFrame()

    def get_data_hash(self) -> str:
        """Вычисляет хеш текущего состояния данных"""
        try:
            self.connection.rollback()
        except:
            pass
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
        try:
            self.connection.rollback()
        except:
            pass

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
        if len(self.reviews_df) > 0 and 'movie_review_url' in self.reviews_df.columns:
            self.reviews_df['movie_id'] = self.reviews_df['movie_review_url'].str.extract(r'/title/(tt\d+)/')
            self.reviews_df['user_url_clean'] = self.reviews_df['user_url'].str.split('?').str[0]
            self.reviews_df['user_url_clean'] = self.reviews_df['user_url_clean'].str.replace(
                'https://www.imdb.com', '').str.replace('http://www.imdb.com', '').str.strip('/')
            self.reviews_df['rating'] = pd.to_numeric(self.reviews_df['rating'], errors='coerce')

            # Удаляем строки с отсутствующим movie_id
            self.reviews_df = self.reviews_df.dropna(subset=['movie_id'])

            logger.info(f"Извлечено {len(self.reviews_df)} ревью с movie_id")

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

            if len(self.reviews_df) > 0 and 'user_url' in self.reviews_df.columns:
                # Создаем копию, чтобы избежать предупреждений
                self.reviews_df = self.reviews_df.copy()

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

                self.reviews_df.loc[:, 'user_url_normalized'] = self.reviews_df['user_url'].apply(normalize_user_url)
                self.reviews_df.loc[:, 'user_url_clean'] = self.reviews_df['user_url_normalized']

                logger.info(f"Создана нормализованная колонка user_url_normalized")

            logger.info(f"Создана нормализованная колонка user_url_normalized")

        self.data_hash = self.get_data_hash()
        return True

    def preprocess_data(self):
        """Предобработка данных"""
        logger.info("Предобработка данных...")

        # Создаем копию, чтобы избежать предупреждений
        if self.movies_df is not None:
            self.movies_df = self.movies_df.copy()

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

        # Создаем копию для безопасной работы
        reviews_clean = self.reviews_df.copy()

        # Очистка данных
        reviews_clean = reviews_clean.dropna(subset=['user_url_clean', 'movie_id', 'rating'])
        reviews_clean.loc[:, 'rating'] = pd.to_numeric(reviews_clean['rating'], errors='coerce')
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

        # Проверяем, что все компоненты существуют
        if self.tfidf_matrix is not None and self.genre_vectors is not None and \
                self.actor_vectors is not None and self.director_vectors is not None:

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
        else:
            logger.warning("Не все компоненты доступны для создания combined_features")
            combined_features = None

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
            'genre_list': getattr(self, 'genre_list', None),
            'top_actors': getattr(self, 'top_actors', None),
            'top_directors': getattr(self, 'top_directors', None),
            'data_hash': self.data_hash
        }

    def save_data(self, data: Dict):
        """Сохраняет обработанные данные"""
        logger.info("Сохранение обработанных данных...")

        # Сохранение DataFrame
        data['movies_df'].to_pickle(os.path.join(self.models_path, 'movies_df.pkl'))
        if data['reviews_df'] is not None:
            data['reviews_df'].to_pickle(os.path.join(self.models_path, 'reviews_df.pkl'))
        if data['user_main_df'] is not None:
            data['user_main_df'].to_pickle(os.path.join(self.models_path, 'user_main_df.pkl'))
        if data['genres_df'] is not None:
            data['genres_df'].to_pickle(os.path.join(self.models_path, 'genres_df.pkl'))

        # Сохранение векторизаторов и матриц
        if data['tfidf_vectorizer'] is not None:
            with open(os.path.join(self.models_path, 'tfidf_vectorizer.pkl'), 'wb') as f:
                pickle.dump(data['tfidf_vectorizer'], f)

        if data['tfidf_matrix'] is not None:
            save_npz(os.path.join(self.models_path, 'tfidf_matrix.npz'), data['tfidf_matrix'])

        if data['genre_vectors'] is not None:
            save_npz(os.path.join(self.models_path, 'genre_vectors.npz'), data['genre_vectors'])

        if data['actor_vectors'] is not None:
            save_npz(os.path.join(self.models_path, 'actor_vectors.npz'), data['actor_vectors'])

        if data['director_vectors'] is not None:
            save_npz(os.path.join(self.models_path, 'director_vectors.npz'), data['director_vectors'])

        # ВАЖНО: Проверяем, что combined_features не None перед сохранением
        if data.get('combined_features') is not None:
            save_npz(os.path.join(self.models_path, 'combined_features.npz'), data['combined_features'])
        else:
            logger.warning("combined_features is None, пропускаем сохранение")

        if data.get('user_item_matrix') is not None:
            save_npz(os.path.join(self.models_path, 'user_item_matrix.npz'), data['user_item_matrix'])

        # Сохранение массивов
        if data.get('popularity_scores') is not None:
            np.save(os.path.join(self.models_path, 'popularity_scores.npy'), data['popularity_scores'])

        if data.get('recency_scores') is not None:
            np.save(os.path.join(self.models_path, 'recency_scores.npy'), data['recency_scores'])

        # Сохранение списков
        if 'movies_df' in data and data['movies_df'] is not None and 'movie_id' in data['movies_df'].columns:
            with open(os.path.join(self.models_path, 'movie_ids.pkl'), 'wb') as f:
                pickle.dump(data['movies_df']['movie_id'].dropna().tolist(), f)

        if data.get('user_indices') is not None:
            with open(os.path.join(self.models_path, 'user_indices.pkl'), 'wb') as f:
                pickle.dump(data['user_indices'], f)

        if data.get('movie_indices') is not None:
            with open(os.path.join(self.models_path, 'movie_indices.pkl'), 'wb') as f:
                pickle.dump(data['movie_indices'], f)

        if data.get('user_list') is not None:
            with open(os.path.join(self.models_path, 'user_list.pkl'), 'wb') as f:
                pickle.dump(data['user_list'], f)

        if data.get('movie_list') is not None:
            with open(os.path.join(self.models_path, 'movie_list.pkl'), 'wb') as f:
                pickle.dump(data['movie_list'], f)

        # Сохранение хеша
        if data.get('data_hash') is not None:
            with open(os.path.join(self.models_path, 'data_hash.txt'), 'w') as f:
                f.write(data['data_hash'])

        # Сохранение метаданных
        metadata = {
            'genre_list': data.get('genre_list', []),
            'top_actors': data.get('top_actors', []),
            'top_directors': data.get('top_directors', []),
            'num_movies': len(data['movies_df']) if data['movies_df'] is not None else 0,
            'num_users': len(data.get('user_list', [])),
            'num_reviews': len(data['reviews_df']) if data['reviews_df'] is not None else 0
        }

        with open(os.path.join(self.models_path, 'metadata.pkl'), 'wb') as f:
            pickle.dump(metadata, f)

        logger.info("Данные сохранены")

    def get_data_hashes(self) -> Dict[str, str]:
        """Вычисляет хеши для всех источников данных"""
        try:
            self.connection.rollback()
        except:
            pass

        hashes = {}

        # Хеш таблицы movies
        if self.movies_df is not None and len(self.movies_df) > 0:
            movies_hash = hashlib.md5(
                (str(len(self.movies_df)) +
                 str(self.movies_df['movie_id'].nunique()) +
                 str(self.movies_df['year'].nunique())).encode()
            ).hexdigest()
            hashes['movies'] = movies_hash

        # Хеш таблицы reviews
        if self.reviews_df is not None and len(self.reviews_df) > 0:
            reviews_hash = hashlib.md5(
                (str(len(self.reviews_df)) +
                 str(self.reviews_df['user_url'].nunique()) +
                 str(self.reviews_df['movie_id'].nunique())).encode()
            ).hexdigest()
            hashes['reviews'] = reviews_hash

        # Хеш таблицы users
        if self.user_main_df is not None and len(self.user_main_df) > 0:
            users_hash = hashlib.md5(
                (str(len(self.user_main_df)) +
                 str(self.user_main_df['user_url'].nunique())).encode()
            ).hexdigest()
            hashes['users'] = users_hash

        # Хеш таблицы genres
        if self.genres_df is not None and len(self.genres_df) > 0:
            genres_hash = hashlib.md5(
                (str(len(self.genres_df)) +
                 str(self.genres_df['genre_en'].nunique())).encode()
            ).hexdigest()
            hashes['genres'] = genres_hash

        # Добавляем timestamp последнего обновления БД
        try:
            if self.connection:
                with self.connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT MAX(COALESCE(pg_stat_all_tables.last_vacuum, '1900-01-01')) as last_change
                        FROM pg_stat_all_tables
                        WHERE schemaname = 'db'
                    """)
                    result = cursor.fetchone()
                    if result and result[0]:
                        hashes['db_last_change'] = hashlib.md5(str(result[0]).encode()).hexdigest()
        except Exception as e:
            logger.error(f"Ошибка получения timestamp БД: {e}")

        return hashes

    async def load_user_data(self) -> bool:
        """Загрузка данных пользователей"""
        logger.info("Загрузка данных пользователей...")

        if not self._create_connection():
            return False

        # Загружаем пользователей
        self.user_main_df = self._load_table('users')

        # Загружаем отзывы
        self.reviews_df = self._load_table('reviews')

        # Загружаем просмотренные фильмы
        watched_df = self._load_table('user_watched')
        if not watched_df.empty:
            self.user_watched_df = watched_df

        # Загружаем избранное
        favorites_df = self._load_table('user_favorites')
        if not favorites_df.empty:
            self.user_favorites_df = favorites_df

        logger.info(f"Загружено: {len(self.user_main_df)} пользователей, "
                    f"{len(self.reviews_df)} отзывов, "
                    f"{len(watched_df) if hasattr(self, 'user_watched_df') else 0} просмотренных, "
                    f"{len(favorites_df) if hasattr(self, 'user_favorites_df') else 0} в избранном")

        return True

    async def save_user_rating(self, user_url: str, movie_id: str, rating: float,
                               review_text: str = None, review_title: str = None) -> bool:
        """Сохранение оценки пользователя в БД (rating сохраняется как TEXT)"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return False

            import hashlib
            from datetime import datetime

            # Генерируем review_url
            review_hash = hashlib.md5(f"{user_url}_{movie_id}_{datetime.now().isoformat()}".encode()).hexdigest()[:16]
            review_url = f"/review/{review_hash}"
            movie_review_url = f"/title/{movie_id}/"

            # Преобразуем rating в строку (сохраняем как TEXT)
            rating_str = f"{rating:.1f}" if rating else "0"

            with self.connection.cursor() as cursor:
                # Проверяем, существует ли уже оценка
                cursor.execute("""
                    SELECT review_url FROM db.reviews 
                    WHERE user_url = %s AND movie_review_url = %s
                """, (user_url, movie_review_url))

                existing = cursor.fetchone()

                if existing:
                    # Обновляем существующую оценку
                    cursor.execute("""
                        UPDATE db.reviews 
                        SET rating = %s, review_text = %s, review_title = %s, date = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_url = %s AND movie_review_url = %s
                        RETURNING review_url
                    """, (rating_str, review_text, review_title, datetime.now().date(), user_url, movie_review_url))
                else:
                    # Вставляем новую оценку
                    cursor.execute("""
                        INSERT INTO db.reviews (review_url, movie_review_url, user_url, review_title, rating, date, review_text)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (review_url, movie_review_url, user_url, review_title, rating_str, datetime.now().date(),
                          review_text))

                self.connection.commit()

                # Обновляем статистику пользователя
                await self._update_user_stats(user_url)

                logger.info(f"Сохранена оценка {rating} для пользователя {user_url} к фильму {movie_id}")
                return True

        except Exception as e:
            logger.error(f"Ошибка сохранения оценки: {e}")
            self.connection.rollback()
            return False

    async def add_to_watched(self, user_url: str, movie_id: str) -> bool:
        """Добавление фильма в просмотренные"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return False

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO db.user_watched (user_url, movie_id, added_date)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_url, movie_id) DO NOTHING
                """, (user_url, movie_id))

                self.connection.commit()
                logger.info(f"Фильм {movie_id} добавлен в просмотренные для {user_url}")
                return True

        except Exception as e:
            logger.error(f"Ошибка добавления в просмотренные: {e}")
            self.connection.rollback()
            return False

    async def add_to_favorites(self, user_url: str, movie_id: str) -> bool:
        """Добавление фильма в избранное"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return False

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO db.user_favorites (user_url, movie_id, added_date)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_url, movie_id) DO NOTHING
                """, (user_url, movie_id))

                self.connection.commit()
                logger.info(f"Фильм {movie_id} добавлен в избранное для {user_url}")
                return True

        except Exception as e:
            logger.error(f"Ошибка добавления в избранное: {e}")
            self.connection.rollback()
            return False

    async def remove_from_favorites(self, user_url: str, movie_id: str) -> bool:
        """Удаление фильма из избранного"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return False

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM db.user_favorites 
                    WHERE user_url = %s AND movie_id = %s
                """, (user_url, movie_id))

                self.connection.commit()
                logger.info(f"Фильм {movie_id} удален из избранного для {user_url}")
                return True

        except Exception as e:
            logger.error(f"Ошибка удаления из избранного: {e}")
            self.connection.rollback()
            return False

    async def get_user_favorites(self, user_url: str) -> List[str]:
        """Получение списка избранных фильмов пользователя"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return []

            normalized_user_url = self._normalize_user_url(user_url)

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT movie_id FROM db.user_favorites 
                    WHERE user_url = %s
                       OR regexp_replace(
                            replace(replace(split_part(user_url, '?', 1), 'https://www.imdb.com', ''), 'http://www.imdb.com', ''),
                            '/+$',
                            ''
                        ) = %s
                    ORDER BY added_date DESC
                """, (user_url, normalized_user_url))

                return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Ошибка получения избранного: {e}")
            if self.connection:
                self.connection.rollback()
            return []

    async def get_user_watched(self, user_url: str) -> List[str]:
        """Получение списка просмотренных фильмов пользователя"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return []

            normalized_user_url = self._normalize_user_url(user_url)

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT movie_id FROM db.user_watched 
                    WHERE user_url = %s
                       OR regexp_replace(
                            replace(replace(split_part(user_url, '?', 1), 'https://www.imdb.com', ''), 'http://www.imdb.com', ''),
                            '/+$',
                            ''
                        ) = %s
                    ORDER BY added_date DESC
                """, (user_url, normalized_user_url))

                return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Ошибка получения просмотренных: {e}")
            if self.connection:
                self.connection.rollback()
            return []

    async def get_user_reviewed_movies(self, user_url: str) -> List[Dict]:
        """Получение просмотренных фильмов из отзывов пользователя."""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return []

            normalized_user_url = self._normalize_user_url(user_url)

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT
                        r.movie_review_url,
                        r.rating,
                        r.review_text,
                        r.date,
                        m.title,
                        m.title_ru,
                        m.year
                    FROM db.reviews r
                    LEFT JOIN db.movies m ON m.movie_url = r.movie_review_url
                    WHERE user_url = %s
                       OR regexp_replace(
                            replace(replace(split_part(user_url, '?', 1), 'https://www.imdb.com', ''), 'http://www.imdb.com', ''),
                            '/+$',
                            ''
                        ) = %s
                    ORDER BY r.date DESC NULLS LAST
                """, (user_url, normalized_user_url))
                rows = cursor.fetchall()

            reviewed_movies = []

            for row in rows:
                movie_review_url = row.get('movie_review_url') if row else None
                if not movie_review_url:
                    continue

                movie_id = self._extract_movie_id(movie_review_url)
                if not movie_id:
                    continue

                reviewed_movies.append({
                    'movie_id': movie_id,
                    'rating': row.get('rating'),
                    'review_text': row.get('review_text', ''),
                    'date': row.get('date'),
                    'title': row.get('title', ''),
                    'title_ru': row.get('title_ru', ''),
                    'year': row.get('year')
                })

            return reviewed_movies

        except Exception as e:
            logger.error(f"Ошибка получения просмотренных из отзывов: {e}")
            if self.connection:
                self.connection.rollback()
            return []

    async def get_user_rating(self, user_url: str, movie_id: str) -> Optional[Dict]:
        """Получение оценки пользователя для фильма (rating как TEXT)"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return None

            movie_review_url = f"/title/{movie_id}/"

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT rating, review_text, review_title, date 
                    FROM db.reviews 
                    WHERE user_url = %s AND movie_review_url = %s
                """, (user_url, movie_review_url))

                result = cursor.fetchone()
                if result:
                    rating_value = None
                    rating_raw = result[0]
                    if rating_raw:
                        try:
                            rating_str = str(rating_raw).strip()
                            if '/' in rating_str:
                                rating_str = rating_str.split('/')[0].strip()
                            rating_value = float(rating_str)
                        except:
                            rating_value = None

                    return {
                        'rating': rating_value,
                        'review_text': result[1] if result[1] else '',
                        'review_title': result[2] if result[2] else '',
                        'date': result[3]
                    }
                return None

        except Exception as e:
            logger.error(f"Ошибка получения оценки: {e}")
            return None

    async def check_movie_watched(self, user_url: str, movie_id: str) -> bool:
        """Проверка, добавлен ли фильм в просмотренные"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return False

            normalized_user_url = user_url.replace('/user/', '')

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM db.user_watched 
                    WHERE (user_url = %s OR user_url = %s OR user_url = %s)
                      AND movie_id = %s
                """, (user_url, normalized_user_url, f"/user/{normalized_user_url}", movie_id))

                return cursor.fetchone() is not None

        except Exception as e:
            logger.error(f"Ошибка проверки просмотра: {e}")
            self.connection.rollback()
            return False

    async def check_movie_favorite(self, user_url: str, movie_id: str) -> bool:
        """Проверка, добавлен ли фильм в избранное"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return False

            normalized_user_url = self._normalize_user_url(user_url)

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM db.user_favorites 
                    WHERE (user_url = %s
                           OR regexp_replace(
                                replace(replace(split_part(user_url, '?', 1), 'https://www.imdb.com', ''), 'http://www.imdb.com', ''),
                                '/+$',
                                ''
                           ) = %s)
                      AND movie_id = %s
                """, (user_url, normalized_user_url, movie_id))

                return cursor.fetchone() is not None

        except Exception as e:
            logger.error(f"Ошибка проверки избранного: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    async def create_user(self, username: str, user_url: str = None) -> Optional[Dict]:
        """Создание нового пользователя"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            self.connection.rollback()
        except:
            pass
        try:
            if not self.connection:
                if not self._create_connection():
                    return None

            import re
            from datetime import datetime

            if not user_url:
                user_url_base = re.sub(r'[^a-zA-Z0-9]', '_', username.lower())
                user_url = f"/user/{user_url_base}"

                # Проверяем уникальность
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT user_url FROM db.users WHERE user_url = %s", (user_url,))
                    if cursor.fetchone():
                        import time
                        user_url = f"/user/{user_url_base}_{int(time.time())}"

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO db.users (user_url, username, joined, ratings_count, created_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING user_url, username, joined, ratings_count
                """, (user_url, username, datetime.now().date(), 0))

                self.connection.commit()
                result = cursor.fetchone()

                logger.info(f"Создан новый пользователь: {username} ({user_url})")
                return dict(result) if result else None

        except Exception as e:
            logger.error(f"Ошибка создания пользователя: {e}")
            self.connection.rollback()
            return None

    async def _update_user_stats(self, user_url: str):
        """Обновление статистики пользователя"""
        try:
            self.connection.rollback()
        except:
            pass
        try:
            with self.connection.cursor() as cursor:
                # Подсчитываем количество оценок (игнорируем пустые или невалидные)
                cursor.execute("""
                    SELECT COUNT(*) as ratings_count
                    FROM db.reviews 
                    WHERE user_url = %s AND rating IS NOT NULL AND rating != ''
                """, (user_url,))

                result = cursor.fetchone()
                ratings_count = result[0] if result else 0

                # Обновляем пользователя
                cursor.execute("""
                    UPDATE db.users 
                    SET ratings_count = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE user_url = %s
                """, (ratings_count, user_url))

                self.connection.commit()
                logger.debug(f"Статистика обновлена для {user_url}: {ratings_count} оценок")

        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")
            self.connection.rollback()
