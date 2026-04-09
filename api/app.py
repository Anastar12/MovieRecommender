from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_from_directory
import asyncio
import logging
from typing import Dict, List
import pandas as pd
import numpy as np
import os
from urllib.parse import unquote

from core.config import AppConfig
from offline.data_pipeline import DataPipeline
from offline.model_trainer import ModelTrainer
from offline.cache_manager import CacheManager
from online.context_handler import ContextHandler
from online.candidate_generator import CandidateGenerator
from online.ranker import Ranker
from online.postprocessor import Postprocessor
from online.feedback_logger import FeedbackLogger

import warnings
warnings.filterwarnings('ignore')

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация компонентов
config = AppConfig()

# Офлайн-компоненты
data_pipeline = DataPipeline(
    db_config={
        'host': config.db.host,
        'port': config.db.port,
        'database': config.db.database,
        'user': config.db.user,
        'password': config.db.password
    },
    models_path=config.offline.models_path
)

model_trainer = ModelTrainer(models_path=config.offline.models_path)
cache_manager = CacheManager(
    redis_config={
        'host': config.redis.host,
        'port': config.redis.port,
        'db': config.redis.db,
        'password': config.redis.password
    },
    cache_ttl=config.offline.cache_ttl_seconds,
    top_n_cached=config.offline.top_n_cached
)

# Онлайн-компоненты (будут инициализированы после загрузки моделей)
context_handler = None
candidate_generator = None
ranker = None
postprocessor = None
feedback_logger = FeedbackLogger(log_path='logs/')

# Глобальные переменные для совместимости со старыми эндпоинтами
recommender = None
reviews_df = None


class ModelsProvider:
    """Провайдер моделей для онлайн-компонентов"""

    def __init__(self, trainer: ModelTrainer, data_pipeline: DataPipeline):
        self.trainer = trainer
        self.data = data_pipeline

        # Загрузка моделей
        self._load_models()

        # Загрузка данных для обратной совместимости
        self._load_compatibility_data()

    def _load_models(self):
        """Загрузка моделей из файлов"""
        self.trainer.load_models()

        # Загрузка данных
        try:
            self.movies_df = pd.read_pickle(f'{self.trainer.models_path}movies_df.pkl')
            self.user_main_df = pd.read_pickle(f'{self.trainer.models_path}user_main_df.pkl')
            self.genres_df = pd.read_pickle(f'{self.trainer.models_path}genres_df.pkl')
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            self.movies_df = None

    def _load_compatibility_data(self):
        """Загрузка данных для обратной совместимости со старыми эндпоинтами"""
        global recommender, reviews_df

        try:
            # Создаем объект-обертку для совместимости
            class RecommenderWrapper:
                def __init__(self, provider):
                    self.provider = provider
                    self.movies_df = provider.movies_df
                    self.user_main_df = provider.user_main_df
                    self.genres_df = provider.genres_df
                    self.subgenres_df = None
                    self.countries_df = None

                    # Загрузка subgenres
                    try:
                        self.subgenres_df = pd.read_pickle(f'{provider.trainer.models_path}subgenres_df.pkl')
                    except:
                        pass

                def get_movie_details(self, movie_id):
                    """Получение деталей фильма"""
                    if self.movies_df is None:
                        return None

                    movie = self.movies_df[self.movies_df['movie_id'] == movie_id]
                    if len(movie) == 0:
                        return None

                    movie = movie.iloc[0]

                    # Безопасное получение значений
                    def safe_get(col_name, default=''):
                        if col_name in movie.index:
                            val = movie[col_name]
                            if val is not None:
                                if isinstance(val, (list, tuple, np.ndarray)):
                                    if len(val) > 0:
                                        return ', '.join(str(v) for v in val if v)
                                    return default
                                elif pd.notna(val):
                                    return val
                        return default

                    # Получаем русские названия жанров
                    genres_ru = []
                    genre_val = safe_get('genre', '')
                    if genre_val and isinstance(genre_val, str):
                        genres_en = [g.strip() for g in str(genre_val).split(',') if g.strip()]
                        for genre_en in genres_en:
                            genre_ru = genre_en
                            if self.genres_df is not None:
                                if 'title_ru' in self.genres_df.columns:
                                    match = self.genres_df[self.genres_df['title'] == genre_en]
                                    if len(match) > 0:
                                        genre_ru = match.iloc[0]['title_ru']
                                elif 'genre_ru' in self.genres_df.columns:
                                    match = self.genres_df[self.genres_df['genre_en'] == genre_en]
                                    if len(match) > 0:
                                        genre_ru = match.iloc[0]['genre_ru']
                            genres_ru.append(genre_ru)

                    # Получаем русские имена режиссёров
                    directors_ru = []
                    directors_val = safe_get('directors_ru', '')
                    if not directors_val or (
                            isinstance(directors_val, str) and (directors_val == '' or directors_val == 'nan')):
                        directors_val = safe_get('directors', '')

                    if directors_val and isinstance(directors_val, str) and directors_val != 'nan':
                        directors_ru = [d.strip() for d in str(directors_val).split(',') if d.strip()]

                    # Получаем русские имена актёров
                    actors_ru = []
                    actors_val = safe_get('actors_ru', '')
                    if not actors_val or (isinstance(actors_val, str) and (actors_val == '' or actors_val == 'nan')):
                        actors_val = safe_get('actors', '')

                    if actors_val and isinstance(actors_val, str) and actors_val != 'nan':
                        actors_ru = [a.strip() for a in str(actors_val).split(',') if a.strip()]

                    # ========== НОВЫЙ КОД: Получаем русские названия стран ==========
                    countries_ru = []

                    # Пробуем получить country_ru
                    country_ru_val = safe_get('country_ru', '')
                    if country_ru_val and isinstance(country_ru_val,
                                                     str) and country_ru_val != 'nan' and country_ru_val != '':
                        # Разделяем по запятой, если несколько стран
                        countries_ru = [c.strip() for c in str(country_ru_val).split(',') if
                                        c.strip() and c.strip() != 'nan']

                    # Если нет country_ru, пробуем country (английское название)
                    if len(countries_ru) == 0:
                        country_val = safe_get('country', '')
                        if country_val and isinstance(country_val, str) and country_val != 'nan' and country_val != '':
                            # Пробуем найти русское название через таблицу countries
                            countries_en = [c.strip() for c in str(country_val).split(',') if
                                            c.strip() and c.strip() != 'nan']

                            # Загружаем таблицу стран, если ещё не загружена
                            countries_df = None
                            countries_path = f'{self.provider.trainer.models_path}countries_df.pkl' if hasattr(self,
                                                                                                               'provider') else None
                            if countries_path and os.path.exists(countries_path):
                                try:
                                    countries_df = pd.read_pickle(countries_path)
                                except:
                                    pass

                            # Если не загрузили через models_path, пробуем другой путь
                            if countries_df is None:
                                countries_csv_path = 'data/countries.csv'
                                if os.path.exists(countries_csv_path):
                                    try:
                                        countries_df = pd.read_csv(countries_csv_path, encoding='utf-8')
                                    except:
                                        pass

                            # Для каждой страны ищем русское название
                            for country_en in countries_en:
                                country_ru = country_en  # по умолчанию английское
                                if countries_df is not None:
                                    # Пробуем разные варианты названий колонок
                                    if 'country_name_en' in countries_df.columns and 'country_name_ru' in countries_df.columns:
                                        match = countries_df[
                                            countries_df['country_name_en'].str.lower() == country_en.lower()]
                                        if len(match) > 0:
                                            country_ru = match.iloc[0]['country_name_ru']
                                    elif 'name_en' in countries_df.columns and 'name_ru' in countries_df.columns:
                                        match = countries_df[countries_df['name_en'].str.lower() == country_en.lower()]
                                        if len(match) > 0:
                                            country_ru = match.iloc[0]['name_ru']
                                    elif 'en' in countries_df.columns and 'ru' in countries_df.columns:
                                        match = countries_df[countries_df['en'].str.lower() == country_en.lower()]
                                        if len(match) > 0:
                                            country_ru = match.iloc[0]['ru']
                                countries_ru.append(country_ru)

                    # Если всё ещё нет стран, пробуем просто country_ru из safe_get
                    if len(countries_ru) == 0 and country_ru_val and isinstance(country_ru_val,
                                                                                str) and country_ru_val != 'nan':
                        countries_ru = [country_ru_val]

                    # Если совсем ничего нет, оставляем пустой массив
                    # ========== КОНЕЦ НОВОГО КОДА ==========

                    return {
                        'movie_id': str(movie_id),
                        'title': str(safe_get('title', '')),
                        'title_ru': str(safe_get('title_ru', safe_get('title', ''))),
                        'year': str(safe_get('year', '')),
                        'genre': str(safe_get('genre', '')),
                        'genres': genres_ru,
                        'imdb': safe_get('imdb', None),
                        'kinopoisk': safe_get('kinopoisk', None),  # Добавляем Кинопоиск рейтинг
                        'plot': str(safe_get('plot', '')),
                        'plot_ru': str(safe_get('description_ru', safe_get('plot', ''))),
                        'directors': directors_ru,
                        'directors_ru': directors_ru,
                        'actors': actors_ru,
                        'actors_ru': actors_ru,
                        'country': str(safe_get('country', '')),
                        'country_ru': ', '.join(countries_ru) if countries_ru else str(
                            safe_get('country_ru', safe_get('country', ''))),
                        'countries': countries_ru,  # Добавляем массив стран для модального окна
                        'type': str(safe_get('type', '')),
                        'type_ru': str(safe_get('type_ru', safe_get('type', ''))),
                        'age_limit': str(safe_get('age_limit', '')),
                        'age_limit_ru': str(safe_get('age_limit_ru', safe_get('age_limit', ''))),
                        'imdb_rating': safe_get('imdb', None),  # Добавляем для совместимости с шаблоном
                    }

                def get_user_stats(self, user_url):
                    """Получение статистики пользователя"""
                    if self.user_main_df is None:
                        return None

                    user_data = self.user_main_df[self.user_main_df['user_url'] == user_url]
                    if len(user_data) == 0:
                        return None

                    return {
                        'username': user_data.iloc[0].get('username', ''),
                        'total_ratings': user_data.iloc[0].get('ratings_count', 0),
                        'joined': user_data.iloc[0].get('joined', '')
                    }

                def hybrid_recommendations(self, user_url, movie_id=None, top_n=20, **kwargs):
                    """Гибридные рекомендации (использует новую систему)"""
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        recommendations = loop.run_until_complete(
                            get_recommendations_for_user(user_url, {})
                        )
                        return recommendations[:top_n]
                    except Exception as e:
                        logger.error(f"Ошибка в hybrid_recommendations: {e}")
                        return []

                def get_genres_tree(self):
                    """Получение дерева жанров"""
                    if self.genres_df is None:
                        return []

                    genres_tree = []
                    for _, genre in self.genres_df.iterrows():
                        genre_name = genre.get('title_ru', genre.get('genre_ru', ''))
                        if not genre_name:
                            continue

                        genres_tree.append({
                            'name': str(genre_name),
                            'name_en': genre.get('title', genre.get('genre_en', '')),
                            'subgenres': []
                        })

                    return genres_tree

            recommender = RecommenderWrapper(self)

            # Загрузка reviews_df для совместимости
            try:
                reviews_path = f'{self.trainer.models_path}reviews_df.pkl'
                if os.path.exists(reviews_path):
                    reviews_df = pd.read_pickle(reviews_path)
                else:
                    reviews_df = None
            except:
                reviews_df = None

        except Exception as e:
            logger.error(f"Ошибка загрузки совместимых данных: {e}")

    def get_similar_movies(self, movie_id: str, n: int = 50) -> List[Dict]:
        """Получение похожих фильмов"""
        if self.trainer.nn_model is None:
            return []

        try:
            movie_idx = self.data.movie_ids.index(movie_id) if hasattr(self.data, 'movie_ids') else None
            if movie_idx is None:
                return []

            distances, indices = self.trainer.nn_model.kneighbors(
                self.data.combined_features[movie_idx],
                n_neighbors=n + 1
            )

            recommendations = []
            for i, idx in enumerate(indices[0][1:]):
                movie = self.movies_df.iloc[idx]
                recommendations.append({
                    'movie_id': movie['movie_id'],
                    'title': movie['title'],
                    'year': movie['year'],
                    'similarity': 1 - distances[0][i + 1]
                })

            return recommendations
        except Exception as e:
            logger.error(f"Ошибка поиска похожих: {e}")
            return []

    def get_svd_recommendations(self, user_id: str, n: int = 50) -> List[Dict]:
        """SVD рекомендации"""
        if self.trainer.svd_model is None or not hasattr(self.data, 'user_indices'):
            return []

        try:
            if user_id not in self.data.user_indices:
                return []

            user_idx = self.data.user_indices[user_id]
            user_vector = self.trainer.user_factors[user_idx]
            predicted = user_vector @ self.trainer.item_factors.T

            top_indices = np.argsort(predicted)[::-1][:n]

            recommendations = []
            for idx in top_indices:
                if idx < len(self.data.movie_list):
                    movie_id = self.data.movie_list[idx]
                    movie = self.movies_df[self.movies_df['movie_id'] == movie_id]
                    if len(movie) > 0:
                        recommendations.append({
                            'movie_id': movie_id,
                            'title': movie.iloc[0]['title'],
                            'score': float(predicted[idx])
                        })

            return recommendations
        except Exception as e:
            logger.error(f"Ошибка SVD: {e}")
            return []

    def get_als_recommendations(self, user_id: str, n: int = 50) -> List[Dict]:
        """ALS рекомендации"""
        if self.trainer.als_model is None or not hasattr(self.data, 'user_indices'):
            return []

        try:
            if user_id not in self.data.user_indices:
                return []

            user_idx = self.data.user_indices[user_id]
            recommendations = self.trainer.als_model.recommend(
                user_idx,
                self.data.user_item_matrix.T,
                N=n
            )

            result = []
            for movie_idx, score in recommendations:
                if movie_idx < len(self.data.movie_list):
                    movie_id = self.data.movie_list[movie_idx]
                    movie = self.movies_df[self.movies_df['movie_id'] == movie_id]
                    if len(movie) > 0:
                        result.append({
                            'movie_id': movie_id,
                            'title': movie.iloc[0]['title'],
                            'score': float(score)
                        })

            return result
        except Exception as e:
            logger.error(f"Ошибка ALS: {e}")
            return []

    def predict_rating(self, user_id: str, movie_id: str) -> float:
        """Предсказание оценки"""
        if self.trainer.rating_predictor is None:
            return 0.5

        try:
            if user_id not in self.data.user_indices or movie_id not in self.data.movie_indices:
                return 0.5

            user_idx = self.data.user_indices[user_id]
            movie_idx = self.data.movie_indices[movie_id]

            features = np.concatenate([
                self.trainer.user_factors[user_idx][:20],
                self.trainer.item_factors[movie_idx][:20],
                [self.data.popularity_scores[movie_idx]],
                [self.data.recency_scores[movie_idx]]
            ])

            prediction = self.trainer.rating_predictor.predict([features])[0]
            return max(0, min(1, prediction))
        except Exception as e:
            logger.error(f"Ошибка предсказания: {e}")
            return 0.5

    def get_russian_genre(self, genre_en):
        """Получение русского названия жанра"""
        if self.genres_df is None:
            return genre_en

        try:
            if 'title_ru' in self.genres_df.columns:
                match = self.genres_df[self.genres_df['title'] == genre_en]
                if len(match) > 0:
                    return match.iloc[0]['title_ru']
            elif 'genre_ru' in self.genres_df.columns:
                match = self.genres_df[self.genres_df['genre_en'] == genre_en]
                if len(match) > 0:
                    return match.iloc[0]['genre_ru']
        except Exception as e:
            logger.error(f"Ошибка получения русского жанра: {e}")

        return genre_en


class DataProvider:
    """Провайдер данных для онлайн-компонентов"""

    def __init__(self, data_pipeline: DataPipeline):
        self.data = data_pipeline

        # Загрузка данных
        movies_path = f'{self.data.models_path}movies_df.pkl'
        self.movies_df = pd.read_pickle(movies_path) if os.path.exists(movies_path) else None
        self.user_main_df = pd.read_pickle(f'{self.data.models_path}user_main_df.pkl') if os.path.exists(
            f'{self.data.models_path}user_main_df.pkl') else None

    def get_user_stats(self, user_id: str) -> Dict:
        """Получение статистики пользователя"""
        if self.user_main_df is None:
            return None

        user_data = self.user_main_df[self.user_main_df['user_url'] == user_id]
        if len(user_data) == 0:
            return None

        return {
            'username': user_data.iloc[0].get('username', ''),
            'total_ratings': user_data.iloc[0].get('ratings_count', 0),
            'joined': user_data.iloc[0].get('joined', '')
        }

    def get_popular_movies(self, limit: int = 100) -> List[Dict]:
        """Получение популярных фильмов"""
        if self.movies_df is None:
            return []

        if 'imdb_norm' in self.movies_df.columns:
            sorted_df = self.movies_df.nlargest(limit, 'imdb_norm')
        else:
            sorted_df = self.movies_df.head(limit)

        return [
            {
                'movie_id': row['movie_id'],
                'title': row['title'],
                'year': row.get('year', ''),
                'popularity': row.get('imdb_norm', 0.5)
            }
            for _, row in sorted_df.iterrows()
        ]

    def get_movies_by_genre(self, genre: str, limit: int = 50) -> List[Dict]:
        """Получение фильмов по жанру"""
        if self.movies_df is None:
            return []

        if 'genre' in self.movies_df.columns:
            genre_movies = self.movies_df[
                self.movies_df['genre'].str.contains(genre, case=False, na=False)
            ].head(limit)
        else:
            genre_movies = self.movies_df.head(limit)

        return [
            {
                'movie_id': row['movie_id'],
                'title': row['title'],
                'year': row.get('year', '')
            }
            for _, row in genre_movies.iterrows()
        ]

    def get_user_watched_movies(self, user_id: str) -> List[Dict]:
        """Получение просмотренных фильмов пользователя"""
        global reviews_df

        if reviews_df is None:
            return []

        # Нормализуем user_id для сравнения
        def normalize_url(url):
            if not url:
                return ''
            url = str(url).strip()
            # Убираем префиксы
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            # Убираем trailing slash
            url = url.rstrip('/')
            # Убираем параметры запроса
            url = url.split('?')[0]
            return url

        normalized_user_id = normalize_url(user_id)

        # Пробуем разные варианты поиска
        user_reviews = None

        # Вариант 1: по user_url_clean
        if 'user_url_clean' in reviews_df.columns:
            user_reviews = reviews_df[reviews_df['user_url_clean'] == normalized_user_id]

        # Вариант 2: по user_url_normalized
        if (user_reviews is None or len(user_reviews) == 0) and 'user_url_normalized' in reviews_df.columns:
            user_reviews = reviews_df[reviews_df['user_url_normalized'] == normalized_user_id]

        # Вариант 3: по user_url (содержит)
        if (user_reviews is None or len(user_reviews) == 0) and 'user_url' in reviews_df.columns:
            user_reviews = reviews_df[reviews_df['user_url'].str.contains(normalized_user_id, na=False)]

        # Вариант 4: по части URL
        if (user_reviews is None or len(user_reviews) == 0):
            # Извлекаем ID пользователя
            user_id_part = normalized_user_id.split('/')[-1] if '/' in normalized_user_id else normalized_user_id
            if 'user_url' in reviews_df.columns:
                user_reviews = reviews_df[reviews_df['user_url'].str.contains(user_id_part, na=False)]
            elif 'user_url_clean' in reviews_df.columns:
                user_reviews = reviews_df[reviews_df['user_url_clean'].str.contains(user_id_part, na=False)]

        if user_reviews is None or len(user_reviews) == 0:
            return []

        watched = []
        for _, review in user_reviews.iterrows():
            movie_id = review.get('movie_id')
            if pd.notna(movie_id):
                watched.append({
                    'movie_id': str(movie_id),
                    'rating': review.get('rating'),
                    'date': review.get('date'),
                    'review_text': review.get('review_text')
                })

        return watched


# Инициализация глобальных объектов
models_provider = None
data_provider = None
loop = None

# Флаг для отслеживания инициализации
_initialized = False


def init_online_components():
    """Инициализация онлайн-компонентов"""
    global models_provider, data_provider, context_handler, candidate_generator, ranker, postprocessor, loop, _initialized

    if _initialized:
        return

    logger.info("Инициализация онлайн-компонентов...")

    models_provider = ModelsProvider(model_trainer, data_pipeline)
    data_provider = DataProvider(data_pipeline)

    context_handler = ContextHandler(data_provider)
    candidate_generator = CandidateGenerator(models_provider, data_provider, {
        'weights': config.online.weights,
        'candidate_limit': config.online.candidate_limit
    })
    ranker = Ranker(models_provider, data_provider, {
        'final_top_n': config.online.final_top_n
    })
    postprocessor = Postprocessor(data_provider)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Запуск логгера
    try:
        loop.run_until_complete(feedback_logger.start())
    except RuntimeError:
        asyncio.create_task(feedback_logger.start())

    _initialized = True
    logger.info("Онлайн-компоненты инициализированы")


async def get_recommendations_for_user(user_id: str, context_params: Dict = None) -> List[Dict]:
    """Получение рекомендаций для пользователя"""
    # Убеждаемся, что компоненты инициализированы
    if not _initialized:
        init_online_components()

    # Проверка кэша
    cached = cache_manager.get_cached_top_n(user_id)
    if cached:
        logger.info(f"Возвращены кэшированные рекомендации для {user_id}")
        return cached

    # Получение контекста
    context = context_handler.get_user_context(user_id, context_params)
    context['user_rated_movies'] = context_handler.get_user_rated_movies(user_id)
    context['user_genre_preferences'] = context_handler.get_user_genre_preferences(user_id)

    # Генерация кандидатов
    candidates = await candidate_generator.generate_candidates(context)

    if not candidates:
        logger.warning(f"Нет кандидатов для пользователя {user_id}")
        # Возвращаем популярные фильмы как fallback
        popular = data_provider.get_popular_movies(20) if data_provider else []
        return [{'movie_id': p['movie_id'], 'final_score': 0.5, 'title': p['title']} for p in popular]

    # Ранжирование
    ranked = await ranker.rank_candidates(candidates, context)

    # Постобработка
    recommendations = postprocessor.process(ranked, context)

    # Кэширование
    cache_manager.cache_top_n_recommendations(user_id, recommendations)

    # Логирование
    await feedback_logger.log_recommendations_served(user_id, recommendations)

    return recommendations


def run_offline_pipeline():
    """Запуск офлайн-пайплайна"""

    async def pipeline():
        # Сначала инициализируем онлайн-компоненты
        init_online_components()

        # Загрузка и обработка данных
        logger.info("Запуск офлайн-пайплайна...")
        data = await data_pipeline.run_pipeline()

        # Сохранение данных
        logger.info("Сохранение обработанных данных...")
        data_pipeline.save_data(data)

        # Обучение моделей
        logger.info("Обучение моделей...")
        await model_trainer.train_all_models(data)

        # Сохранение моделей
        logger.info("Сохранение моделей...")
        model_trainer.save_models()

        # Прогрев кэша (только если есть пользователи)
        if data.get('user_list') and len(data['user_list']) > 0:
            logger.info(f"Прогрев кэша для {min(100, len(data['user_list']))} пользователей...")
            warmed = 0
            for user_id in data['user_list'][:100]:
                try:
                    recs = await get_recommendations_for_user(user_id, {})
                    if recs:
                        warmed += 1
                except Exception as e:
                    logger.error(f"Ошибка прогрева для {user_id}: {e}")
            logger.info(f"Прогрев кэша завершен. Успешно: {warmed}")

    # Запускаем пайплайн
    offline_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(offline_loop)
    offline_loop.run_until_complete(pipeline())
    offline_loop.close()


# Инициализация приложения (выполняется при старте)
def setup_app():
    """Настройка приложения"""
    global loop

    logger.info("Настройка приложения...")

    # Проверка наличия моделей
    if not model_trainer.load_models():
        logger.info("Модели не найдены, запуск офлайн-обучения...")
        run_offline_pipeline()
    else:
        # Модели есть, просто инициализируем онлайн-компоненты
        init_online_components()

    logger.info("Приложение настроено")


# Вызываем инициализацию при старте
setup_app()


# ==================== API ЭНДПОИНТЫ ====================

@app.route('/api/recommendations', methods=['POST'])
def get_recommendations_api():
    """API для получения рекомендаций"""
    global loop

    data = request.json
    user_id = data.get('user_url') or session.get('user_url')

    if not user_id:
        return jsonify({'error': 'Пользователь не авторизован'}), 401

    top_n = data.get('top_n', 50)

    # Нормализуем URL пользователя
    def normalize_url(url):
        url = str(url).strip()
        url = url.replace('https://www.imdb.com', '')
        url = url.replace('http://www.imdb.com', '')
        url = url.rstrip('/')
        url = url.split('?')[0]
        return url

    normalized_user_id = normalize_url(user_id)

    logger.info(f"Получение рекомендаций для пользователя: {normalized_user_id}")

    try:
        # Запускаем асинхронную функцию в синхронном контексте
        if not _initialized:
            init_online_components()

        # Создаем новый цикл для этого запроса, если текущий закрыт
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        recommendations = loop.run_until_complete(
            get_recommendations_for_user(normalized_user_id, {})
        )

        # Обогащаем рекомендации данными
        enriched = []
        for rec in recommendations[:top_n]:
            movie_id = rec.get('movie_id')
            if movie_id and recommender:
                details = recommender.get_movie_details(movie_id)
                if details:
                    details['score'] = rec.get('final_score', rec.get('score', 0))
                    details['poster'] = get_poster_filename(details.get('title', ''), details.get('year', ''))

                    # Конвертируем жанры в русские
                    genres_ru = []
                    genre_val = details.get('genre', '')
                    if genre_val and pd.notna(genre_val) and not isinstance(genre_val, (list, np.ndarray)):
                        genres_en = [g.strip() for g in str(genre_val).split(',') if g.strip()]
                        for genre_en in genres_en:
                            genre_ru = genre_en
                            if models_provider and models_provider.genres_df is not None:
                                if 'title_ru' in models_provider.genres_df.columns:
                                    match = models_provider.genres_df[models_provider.genres_df['title'] == genre_en]
                                    if len(match) > 0:
                                        genre_ru = match.iloc[0]['title_ru']
                                elif 'genre_ru' in models_provider.genres_df.columns:
                                    match = models_provider.genres_df[models_provider.genres_df['genre_en'] == genre_en]
                                    if len(match) > 0:
                                        genre_ru = match.iloc[0]['genre_ru']
                            genres_ru.append(genre_ru)
                    details['genres'] = genres_ru
                    enriched.append(details)

        logger.info(f"Возвращено {len(enriched)} рекомендаций для пользователя {normalized_user_id}")
        return jsonify({'recommendations': enriched})

    except Exception as e:
        logger.error(f"Ошибка получения рекомендаций: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/debug/user/<path:user_url>', methods=['GET'])
def debug_user(user_url):
    """Диагностический эндпоинт для проверки пользователя"""
    user_url = unquote(user_url)

    def normalize_url(url):
        url = str(url).strip()
        url = url.replace('https://www.imdb.com', '')
        url = url.replace('http://www.imdb.com', '')
        url = url.rstrip('/')
        url = url.split('?')[0]
        return url

    normalized_user_url = normalize_url(user_url)

    debug_info = {
        'original_url': user_url,
        'normalized_url': normalized_user_url,
        'has_recommender': recommender is not None,
        'has_models_provider': models_provider is not None,
        'has_data_provider': data_provider is not None,
        'online_components_initialized': _initialized
    }

    # Проверяем, есть ли пользователь в индексах
    if data_provider and hasattr(data_provider, 'user_main_df') and data_provider.user_main_df is not None:
        user_data = data_provider.user_main_df[data_provider.user_main_df['user_url'] == normalized_user_url]
        if len(user_data) > 0:
            debug_info['user_in_db'] = True
            debug_info['total_ratings'] = int(user_data.iloc[0].get('ratings_count', 0))
            debug_info['username'] = user_data.iloc[0].get('username', '')
        else:
            debug_info['user_in_db'] = False

    # Проверяем watched movies
    if data_provider:
        watched = data_provider.get_user_watched_movies(normalized_user_url)
        debug_info['watched_count'] = len(watched)

    return jsonify(debug_info)


@app.route('/api/feedback/rating', methods=['POST'])
def log_rating():
    """Логирование оценки пользователя"""
    data = request.json
    user_id = data.get('user_id') or session.get('user_url')
    movie_id = data.get('movie_id')
    rating = data.get('rating')

    if not user_id or not movie_id or rating is None:
        return jsonify({'error': 'Missing parameters'}), 400

    loop.run_until_complete(
        feedback_logger.log_rating(user_id, movie_id, rating)
    )

    cache_manager.invalidate_user_cache(user_id)
    return jsonify({'success': True})


@app.route('/api/feedback/view', methods=['POST'])
def log_view():
    """Логирование просмотра"""
    data = request.json
    user_id = data.get('user_id') or session.get('user_url')
    movie_id = data.get('movie_id')
    duration = data.get('duration_seconds')

    if not user_id or not movie_id:
        return jsonify({'error': 'Missing parameters'}), 400

    loop.run_until_complete(
        feedback_logger.log_view(user_id, movie_id, duration)
    )
    return jsonify({'success': True})


@app.route('/api/feedback/click', methods=['POST'])
def log_click():
    """Логирование клика"""
    data = request.json
    user_id = data.get('user_id') or session.get('user_url')
    movie_id = data.get('movie_id')
    position = data.get('position')

    if not user_id or not movie_id:
        return jsonify({'error': 'Missing parameters'}), 400

    loop.run_until_complete(
        feedback_logger.log_click(user_id, movie_id, position)
    )
    return jsonify({'success': True})


@app.route('/api/cache/stats', methods=['GET'])
def get_cache_stats():
    """Получение статистики кэша"""
    stats = cache_manager.get_cache_stats()
    return jsonify(stats)


@app.route('/api/feedback/stats', methods=['GET'])
def get_feedback_stats():
    """Получение статистики обратной связи"""
    stats = feedback_logger.get_stats()
    return jsonify(stats)


# ==================== СТРАНИЦЫ ====================

@app.route('/')
def index():
    """Главная страница - перенаправление на логин"""
    if 'user_url' in session and session['user_url']:
        return redirect(url_for('main_page'))
    return redirect(url_for('login_page'))


@app.route('/login')
def login_page():
    """Страница входа с выбором пользователя"""
    return render_template('login.html')


@app.route('/api/users/list')
def get_users_list():
    """Получить список всех пользователей для входа"""
    try:
        if data_provider is None or data_provider.user_main_df is None:
            return jsonify({'error': 'Данные пользователей не загружены'}), 500

        if len(data_provider.user_main_df) == 0:
            return jsonify({'error': 'Нет данных о пользователях'}), 500

        users = data_provider.user_main_df[['user_url', 'username']].drop_duplicates(
            subset=['user_url']
        ).to_dict('records')

        logger.info(f"Успешно загружено {len(users)} пользователей")
        return jsonify({'users': users})

    except Exception as e:
        logger.error(f"Ошибка при получении пользователей: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/login', methods=['POST'])
def login():
    """Авторизация пользователя"""
    data = request.json
    user_url = data.get('user_url')

    if not user_url:
        return jsonify({'error': 'Не выбран пользователь'}), 400

    username = user_url
    if data_provider and data_provider.user_main_df is not None:
        user_data = data_provider.user_main_df[data_provider.user_main_df['user_url'] == user_url]
        if len(user_data) > 0:
            username = user_data.iloc[0]['username']

    session['user_url'] = user_url
    session['username'] = username

    return jsonify({
        'success': True,
        'user_url': user_url,
        'username': username
    })


@app.route('/api/logout', methods=['POST'])
def logout():
    """Выход из системы"""
    session.pop('user_url', None)
    session.pop('username', None)
    return jsonify({'success': True})


@app.route('/main')
def main_page():
    """Главная страница приложения"""
    if 'user_url' not in session or not session['user_url']:
        return redirect(url_for('login_page'))
    return render_template('main.html',
                           current_user=session.get('username'),
                           current_user_url=session.get('user_url'))


@app.route('/search')
def search_page():
    """Страница поиска"""
    if 'user_url' not in session or not session['user_url']:
        return redirect(url_for('login_page'))
    return render_template('search.html',
                           current_user=session.get('username'),
                           current_user_url=session.get('user_url'))


@app.route('/recommendations')
def recommendations_page():
    """Страница рекомендаций"""
    if 'user_url' not in session or not session['user_url']:
        return redirect(url_for('login_page'))
    return render_template('recommendations.html',
                           current_user=session.get('username'),
                           current_user_url=session.get('user_url'))


@app.route('/watched')
def watched_page():
    """Страница просмотренных фильмов"""
    if 'user_url' not in session or not session['user_url']:
        return redirect(url_for('login_page'))
    return render_template('watched.html',
                           current_user=session.get('username'),
                           current_user_url=session.get('user_url'))


@app.route('/catalog')
def catalog_page():
    """Страница каталога фильмов"""
    if 'user_url' not in session or not session['user_url']:
        return redirect(url_for('login_page'))
    return render_template('catalog.html',
                           current_user=session.get('username'),
                           current_user_url=session.get('user_url'))


@app.route('/movie/<movie_id>')
def movie_detail_page(movie_id):
    """Страница деталей фильма"""
    if 'user_url' not in session:
        return redirect(url_for('login_page'))
    return render_template('movie_detail.html',
                           movie_id=movie_id,
                           current_user=session.get('username'))


@app.route('/actor/<actor_name>')
def actor_page(actor_name):
    """Страница фильмов с актером"""
    if 'user_url' not in session:
        return redirect(url_for('login_page'))
    actor_name = unquote(actor_name)
    return render_template('category_page.html',
                           title=f"Фильмы с актером: {actor_name}",
                           category_type='actor',
                           category_name=actor_name,
                           current_user=session.get('username'))


@app.route('/director/<director_name>')
def director_page(director_name):
    """Страница фильмов режиссера"""
    if 'user_url' not in session:
        return redirect(url_for('login_page'))
    director_name = unquote(director_name)
    return render_template('category_page.html',
                           title=f"Фильмы режиссера: {director_name}",
                           category_type='director',
                           category_name=director_name,
                           current_user=session.get('username'))


@app.route('/genre/<genre_name>')
def genre_page(genre_name):
    """Страница фильмов жанра"""
    if 'user_url' not in session:
        return redirect(url_for('login_page'))
    genre_name = unquote(genre_name)
    return render_template('category_page.html',
                           title=f"Фильмы жанра: {genre_name}",
                           category_type='genre',
                           category_name=genre_name,
                           current_user=session.get('username'))


@app.route('/year/<int:year>')
def year_page(year):
    """Страница фильмов года"""
    if 'user_url' not in session:
        return redirect(url_for('login_page'))
    return render_template('category_page.html',
                           title=f"Фильмы {year} года",
                           category_type='year',
                           category_name=str(year),
                           current_user=session.get('username'))


@app.route('/country/<country_name>')
def country_page(country_name):
    """Страница фильмов из страны"""
    if 'user_url' not in session:
        return redirect(url_for('login_page'))
    country_name = unquote(country_name)
    return render_template('category_page.html',
                           title=f"Фильмы из страны: {country_name}",
                           category_type='country',
                           category_name=country_name,
                           current_user=session.get('username'))


# ==================== API ДЛЯ СТРАНИЦ ====================

@app.route('/api/user/<path:user_url>/watched')
def get_user_watched_movies_api(user_url):
    """API для получения просмотренных фильмов пользователя"""
    if data_provider is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        user_url = unquote(user_url)
        logger.info(f"Получение просмотренных фильмов для пользователя: {user_url}")

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_url = normalize_url(user_url)
        watched_movies = data_provider.get_user_watched_movies(normalized_user_url)

        if not watched_movies:
            return jsonify({'movies': [], 'total': 0})

        enriched = []
        for movie in watched_movies:
            if recommender:
                details = recommender.get_movie_details(movie['movie_id'])
                if details:
                    details['user_rating'] = movie.get('rating')
                    details['user_review'] = movie.get('review_text')
                    details['review_date'] = movie.get('date')
                    details['poster'] = get_poster_filename(details.get('title', ''), details.get('year', ''))

                    # Конвертируем жанры в русские названия
                    genres_ru = []
                    genre_val = details.get('genre', '')
                    if genre_val and pd.notna(genre_val):
                        # Проверяем, что genre_val не массив
                        if not isinstance(genre_val, (list, np.ndarray)):
                            genres_en = [g.strip() for g in str(genre_val).split(',') if g.strip()]
                            for genre_en in genres_en:
                                genre_ru = genre_en
                                if models_provider and models_provider.genres_df is not None:
                                    if 'title_ru' in models_provider.genres_df.columns:
                                        match = models_provider.genres_df[
                                            models_provider.genres_df['title'] == genre_en]
                                        if len(match) > 0:
                                            genre_ru = match.iloc[0]['title_ru']
                                    elif 'genre_ru' in models_provider.genres_df.columns:
                                        match = models_provider.genres_df[
                                            models_provider.genres_df['genre_en'] == genre_en]
                                        if len(match) > 0:
                                            genre_ru = match.iloc[0]['genre_ru']
                                genres_ru.append(genre_ru)
                    details['genres'] = genres_ru

                    # Режиссеры на русском - исправленная проверка
                    directors_ru = []
                    directors_val = details.get('directors_ru', '')

                    # Проверяем, пустое ли значение и не массив ли это
                    if directors_val is None or (
                            isinstance(directors_val, str) and (not directors_val or directors_val == 'nan')):
                        directors_val = details.get('directors', '')

                    # Обрабатываем значение только если это строка
                    if isinstance(directors_val, str) and directors_val and directors_val != 'nan':
                        directors_ru = [d.strip() for d in str(directors_val).split(',') if d.strip()]
                    elif isinstance(directors_val, (list, tuple, np.ndarray)):
                        # Если это уже массив, используем его
                        directors_ru = [str(d).strip() for d in directors_val if d and str(d) != 'nan']

                    details['directors_ru'] = directors_ru
                    details['directors'] = directors_ru  # Перезаписываем массивом русских имен

                    # Актеры на русском - исправленная проверка
                    actors_ru = []
                    actors_val = details.get('actors_ru', '')

                    # Проверяем, пустое ли значение и не массив ли это
                    if actors_val is None or (isinstance(actors_val, str) and (not actors_val or actors_val == 'nan')):
                        actors_val = details.get('actors', '')

                    # Обрабатываем значение только если это строка
                    if isinstance(actors_val, str) and actors_val and actors_val != 'nan':
                        actors_ru = [a.strip() for a in str(actors_val).split(',') if a.strip()]
                    elif isinstance(actors_val, (list, tuple, np.ndarray)):
                        # Если это уже массив, используем его
                        actors_ru = [str(a).strip() for a in actors_val if a and str(a) != 'nan']

                    details['actors_ru'] = actors_ru
                    details['actors'] = actors_ru  # Перезаписываем массивом русских имен

                    enriched.append(details)

        enriched.sort(key=lambda x: x.get('review_date', ''), reverse=True)
        logger.info(f"Успешно загружено просмотренных фильмов: {len(enriched)}")
        return jsonify({'movies': enriched, 'total': len(enriched)})

    except Exception as e:
        logger.error(f"Ошибка при получении просмотренных фильмов: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/user/<path:user_url>/watched/stats')
def get_user_watched_stats_api(user_url):
    """API для получения статистики просмотренных фильмов"""
    if data_provider is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        # Декодируем URL пользователя
        user_url = unquote(user_url)
        logger.info(f"Получение статистики для пользователя: {user_url}")

        # Нормализуем URL
        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_url = normalize_url(user_url)

        # Получаем просмотренные фильмы
        watched_movies = data_provider.get_user_watched_movies(normalized_user_url)

        if not watched_movies:
            return jsonify({
                'total_watched': 0,
                'avg_rating': 0,
                'genre_distribution': [],
                'year_distribution': [],
                'rating_distribution': []
            })

        # Общая статистика
        total_watched = len(watched_movies)

        # Вычисляем среднюю оценку (игнорируем None)
        ratings = [m.get('rating') for m in watched_movies if m.get('rating') is not None]
        avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else 0

        # Распределение по жанрам (с русскими названиями)
        genre_counts = {}
        for movie in watched_movies:
            movie_id = movie.get('movie_id')
            if movie_id and recommender:
                details = recommender.get_movie_details(movie_id)
                if details and details.get('genre'):
                    # Получаем английские названия жанров
                    genres_en = [g.strip() for g in str(details['genre']).split(',') if g.strip()]
                    for genre_en in genres_en:
                        # Конвертируем в русское название
                        genre_ru = genre_en
                        if models_provider and models_provider.genres_df is not None:
                            if 'title_ru' in models_provider.genres_df.columns:
                                match = models_provider.genres_df[models_provider.genres_df['title'] == genre_en]
                                if len(match) > 0:
                                    genre_ru = match.iloc[0]['title_ru']
                            elif 'genre_ru' in models_provider.genres_df.columns:
                                match = models_provider.genres_df[models_provider.genres_df['genre_en'] == genre_en]
                                if len(match) > 0:
                                    genre_ru = match.iloc[0]['genre_ru']
                        genre_counts[genre_ru] = genre_counts.get(genre_ru, 0) + 1

        genre_distribution = [{'genre': k, 'count': v} for k, v in
                              sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:10]]

        # Распределение по годам
        year_counts = {}
        for movie in watched_movies:
            movie_id = movie.get('movie_id')
            if movie_id and recommender:
                details = recommender.get_movie_details(movie_id)
                if details and details.get('year'):
                    year = str(details['year'])
                    if year and year != 'nan' and year != 'None':
                        try:
                            year_int = int(float(year))
                            year_counts[year_int] = year_counts.get(year_int, 0) + 1
                        except:
                            pass

        year_distribution = [{'year': str(k), 'count': v} for k, v in sorted(year_counts.items())]

        # Распределение оценок
        rating_distribution = []
        for i in range(1, 11):
            count = sum(1 for m in watched_movies if m.get('rating') == i)
            rating_distribution.append({'rating': i, 'count': count})

        result = {
            'total_watched': total_watched,
            'avg_rating': avg_rating,
            'genre_distribution': genre_distribution,
            'year_distribution': year_distribution,
            'rating_distribution': rating_distribution
        }

        return jsonify(result)

    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/catalog')
def get_catalog():
    """API для получения всех фильмов и доступных фильтров"""
    if models_provider is None or models_provider.movies_df is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        movies_df = models_provider.movies_df.copy()

        movies = []
        for _, row in movies_df.iterrows():
            movie_id = row.get('movie_id')
            if pd.isna(movie_id):
                continue

            # Получаем русские названия жанров
            genres_ru = []
            genre_val = row.get('genre', '')
            if genre_val and pd.notna(genre_val):
                if not isinstance(genre_val, (list, np.ndarray)):
                    genres_en = [g.strip() for g in str(genre_val).split(',') if g.strip()]
                    for genre_en in genres_en:
                        genre_ru = genre_en
                        if models_provider and models_provider.genres_df is not None:
                            if 'title_ru' in models_provider.genres_df.columns:
                                match = models_provider.genres_df[models_provider.genres_df['title'] == genre_en]
                                if len(match) > 0:
                                    genre_ru = match.iloc[0]['title_ru']
                            elif 'genre_ru' in models_provider.genres_df.columns:
                                match = models_provider.genres_df[models_provider.genres_df['genre_en'] == genre_en]
                                if len(match) > 0:
                                    genre_ru = match.iloc[0]['genre_ru']
                        genres_ru.append(genre_ru)

            movies.append({
                'movie_id': str(movie_id),
                'title': str(row.get('title', '')),
                'title_ru': str(row.get('title_ru', row.get('title', ''))),
                'year': str(row.get('year', '')) if pd.notna(row.get('year')) else None,
                'imdb_rating': float(row['imdb']) if pd.notna(row.get('imdb')) else None,
                'poster': get_poster_filename(row.get('title', ''), row.get('year', '')),
                'genres': genres_ru  # Добавляем жанры
            })

        # Также возвращаем фильтры для модального окна
        filters_data = {
            'genres_flat': [],
            'years': [],
            'countries': [],
            'actors': [],
            'directors': []
        }

        # Собираем уникальные значения для фильтров
        all_genres = set()
        all_years = set()
        all_countries = set()
        all_actors = set()
        all_directors = set()

        for _, row in movies_df.iterrows():
            # Годы
            year_val = row.get('year')
            if year_val and pd.notna(year_val):
                try:
                    year_int = int(float(year_val))
                    if 1900 <= year_int <= 2030:
                        all_years.add(year_int)
                except:
                    pass

            # Страны
            country_val = row.get('country')
            if country_val and pd.notna(country_val) and isinstance(country_val, str):
                countries = [c.strip() for c in country_val.split(',') if c.strip()]
                all_countries.update(countries)

            # Актеры
            actors_val = row.get('actors_ru', row.get('actors', ''))
            if actors_val and pd.notna(actors_val) and isinstance(actors_val, str) and actors_val != 'nan':
                actors = [a.strip() for a in actors_val.split(',') if a.strip()]
                all_actors.update(actors)

            # Режиссеры
            directors_val = row.get('directors_ru', row.get('directors', ''))
            if directors_val and pd.notna(directors_val) and isinstance(directors_val, str) and directors_val != 'nan':
                directors = [d.strip() for d in directors_val.split(',') if d.strip()]
                all_directors.update(directors)

        # Жанры (уже есть в genres_ru)
        for movie in movies:
            all_genres.update(movie.get('genres', []))

        filters_data['genres_flat'] = sorted(list(all_genres))
        filters_data['years'] = sorted(list(all_years), reverse=True)
        filters_data['countries'] = sorted(list(all_countries))
        filters_data['actors'] = sorted(list(all_actors))[:100]  # Ограничиваем для производительности
        filters_data['directors'] = sorted(list(all_directors))[:100]

        return jsonify({
            'movies': movies,
            'total': len(movies),
            'filters': filters_data
        })

    except Exception as e:
        logger.error(f"Ошибка при получении каталога: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/movies/<movie_id>', methods=['GET'])
def get_movie_details_api(movie_id):
    """API для получения деталей фильма"""
    if recommender is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        movie = recommender.get_movie_details(movie_id)
        if movie is None:
            return jsonify({'error': f'Фильм {movie_id} не найден'}), 404

        movie['poster'] = get_poster_filename(movie.get('title', ''), movie.get('year', ''))
        return jsonify(movie)

    except Exception as e:
        logger.error(f"Ошибка при получении фильма {movie_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/search')
def search_movies_api():
    """API для поиска фильмов"""
    query = request.args.get('q', '')
    limit = int(request.args.get('limit', 20))

    if models_provider is None or models_provider.movies_df is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        movies_df = models_provider.movies_df.copy()

        mask = (
                movies_df['title'].str.contains(query, case=False, na=False) |
                movies_df['title_ru'].str.contains(query, case=False, na=False)
        )

        result_df = movies_df[mask].head(limit)

        movies = []
        for _, row in result_df.iterrows():
            # Получаем русские названия жанров
            genres_ru = []
            genre_val = row.get('genre', '')
            if genre_val and pd.notna(genre_val):
                if not isinstance(genre_val, (list, np.ndarray)):
                    genres_en = [g.strip() for g in str(genre_val).split(',') if g.strip()]
                    for genre_en in genres_en:
                        genre_ru = genre_en
                        if models_provider and models_provider.genres_df is not None:
                            if 'title_ru' in models_provider.genres_df.columns:
                                match = models_provider.genres_df[models_provider.genres_df['title'] == genre_en]
                                if len(match) > 0:
                                    genre_ru = match.iloc[0]['title_ru']
                            elif 'genre_ru' in models_provider.genres_df.columns:
                                match = models_provider.genres_df[models_provider.genres_df['genre_en'] == genre_en]
                                if len(match) > 0:
                                    genre_ru = match.iloc[0]['genre_ru']
                        genres_ru.append(genre_ru)

            movies.append({
                'movie_id': str(row.get('movie_id', '')),
                'title': str(row.get('title', '')),
                'title_ru': str(row.get('title_ru', row.get('title', ''))),
                'year': str(row.get('year', '')) if pd.notna(row.get('year')) else None,
                'poster': get_poster_filename(row.get('title', ''), row.get('year', '')),
                'genres': genres_ru,  # Добавляем массив жанров
                'imdb_rating': float(row['imdb']) if pd.notna(row.get('imdb')) else None,  # Добавляем рейтинг
            })

        return jsonify({'movies': movies})

    except Exception as e:
        logger.error(f"Ошибка при поиске: {e}")
        return jsonify({'error': str(e)}), 500


def get_poster_filename(title, year=None):
    """Генерирует имя файла постера на основе названия и года"""
    if not title or pd.isna(title):
        return 'placeholder.jpg'

    clean_title = str(title).lower()
    clean_title = ''.join(c if c.isalnum() or c == ' ' else '' for c in clean_title)
    clean_title = clean_title.replace(' ', '_').strip('_')

    while '__' in clean_title:
        clean_title = clean_title.replace('__', '_')

    if year and not pd.isna(year) and str(year) != 'nan' and str(year) != 'None':
        year = str(year).replace('-', '–')
        return f"{clean_title}_{year}.jpg"

    return f"{clean_title}.jpg"


@app.route('/img/horizontal/<path:filename>')
def serve_horizontal_image(filename):
    """Сервинг горизонтальных постеров"""
    return send_from_directory('img/horizontal', filename)


@app.route('/img/vertical/<path:filename>')
def serve_vertical_image(filename):
    """Сервинг вертикальных постеров"""
    return send_from_directory('img/vertical', filename)


@app.route('/img/long/<path:filename>')
def serve_long_image(filename):
    """Сервинг длинных постеров"""
    long_path = os.path.join('img', 'long', filename)
    if os.path.exists(long_path):
        return send_from_directory('img/long', filename)

    horizontal_path = os.path.join('img', 'horizontal', filename)
    if os.path.exists(horizontal_path):
        return send_from_directory('img/horizontal', filename)

    return send_from_directory('img/long', 'placeholder.jpg')


# ==================== API ДЛЯ КАТЕГОРИЙ ====================

@app.route('/api/category/<category_type>/<path:category_name>')
def get_category_movies(category_type, category_name):
    """API для получения фильмов по категории (жанр, актер, режиссер, год, страна)"""
    if models_provider is None or models_provider.movies_df is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        category_name = unquote(category_name)
        logger.info(f"Поиск фильмов по {category_type}: {category_name}")

        movies_df = models_provider.movies_df.copy()

        # Для жанров конвертируем русское название в английское
        if category_type == 'genre':
            # Создаем маппинг русских названий жанров в английские
            genre_mapping = {}
            if models_provider.genres_df is not None:
                for _, row in models_provider.genres_df.iterrows():
                    if 'title_ru' in row and 'title' in row:
                        if pd.notna(row['title_ru']) and pd.notna(row['title']):
                            genre_mapping[str(row['title_ru']).lower()] = str(row['title'])
                    elif 'genre_ru' in row and 'genre_en' in row:
                        if pd.notna(row['genre_ru']) and pd.notna(row['genre_en']):
                            genre_mapping[str(row['genre_ru']).lower()] = str(row['genre_en'])

            # Конвертируем русское название в английское
            english_genre = genre_mapping.get(category_name.lower(), category_name)
            logger.info(f"Конвертация жанра: {category_name} -> {english_genre}")
            category_name = english_genre

        # Поиск фильмов в зависимости от типа категории
        if category_type == 'actor':
            # Поиск по русским или английским именам актеров
            mask = pd.Series([False] * len(movies_df))
            if 'actors' in movies_df.columns:
                mask = mask | movies_df['actors'].str.contains(category_name, case=False, na=False)
            if 'actors_ru' in movies_df.columns:
                mask = mask | movies_df['actors_ru'].str.contains(category_name, case=False, na=False)
            movies = movies_df[mask].copy()

        elif category_type == 'director':
            # Поиск по русским или английским именам режиссеров
            mask = pd.Series([False] * len(movies_df))
            if 'directors' in movies_df.columns:
                mask = mask | movies_df['directors'].str.contains(category_name, case=False, na=False)
            if 'directors_ru' in movies_df.columns:
                mask = mask | movies_df['directors_ru'].str.contains(category_name, case=False, na=False)
            movies = movies_df[mask].copy()

        elif category_type == 'genre':
            # Поиск по жанру
            if 'genre' in movies_df.columns:
                movies = movies_df[
                    movies_df['genre'].str.contains(category_name, case=False, na=False, regex=False)
                ].copy()
            else:
                movies = pd.DataFrame()

        elif category_type == 'year':
            # Поиск по году
            if 'year' in movies_df.columns:
                # Пробуем разные форматы года
                movies = movies_df[
                    (movies_df['year'].astype(str).str.contains(str(category_name), na=False)) |
                    (movies_df['year_num'].astype(str).str.contains(str(category_name), na=False))
                ].copy()
            else:
                movies = pd.DataFrame()

        elif category_type == 'country':
            # Поиск по стране
            mask = pd.Series([False] * len(movies_df))
            if 'country' in movies_df.columns:
                mask = mask | movies_df['country'].str.contains(category_name, case=False, na=False)
            if 'country_ru' in movies_df.columns:
                mask = mask | movies_df['country_ru'].str.contains(category_name, case=False, na=False)
            movies = movies_df[mask].copy()

        else:
            return jsonify({'error': 'Неверный тип категории'}), 400

        if len(movies) == 0:
            logger.warning(f"Фильмы не найдены для {category_type}: {category_name}")
            return jsonify({'movies': [], 'total': 0})

        # Сортируем по рейтингу IMDb
        if 'imdb' in movies.columns:
            movies['imdb'] = pd.to_numeric(movies['imdb'], errors='coerce')
            movies = movies.sort_values('imdb', ascending=False)
        else:
            movies = movies.sort_index()

        result = []
        for _, movie in movies.iterrows():
            movie_id = movie.get('movie_id')
            if pd.isna(movie_id) or movie_id is None:
                continue

            movie_id = str(movie_id).strip()
            if not movie_id or movie_id == 'nan':
                continue

            # Название
            title = str(movie.get('title', 'Unknown')) if pd.notna(movie.get('title')) else 'Unknown'
            title_ru = str(movie.get('title_ru', title)) if pd.notna(movie.get('title_ru')) else title

            # Русские названия жанров
            genres_ru = []
            genre_val = movie.get('genre', '')
            if pd.notna(genre_val) and genre_val:
                genres_en = [g.strip() for g in str(genre_val).split(',') if g.strip()]
                for genre_en in genres_en:
                    genre_ru = genre_en
                    if models_provider.genres_df is not None:
                        if 'title_ru' in models_provider.genres_df.columns:
                            match = models_provider.genres_df[models_provider.genres_df['title'] == genre_en]
                            if len(match) > 0:
                                genre_ru = match.iloc[0]['title_ru']
                        elif 'genre_ru' in models_provider.genres_df.columns:
                            match = models_provider.genres_df[models_provider.genres_df['genre_en'] == genre_en]
                            if len(match) > 0:
                                genre_ru = match.iloc[0]['genre_ru']
                    genres_ru.append(genre_ru)

            # Рейтинг
            imdb_rating = None
            if 'imdb' in movie and pd.notna(movie['imdb']):
                try:
                    imdb_rating = float(movie['imdb'])
                except:
                    pass

            # Год
            year = str(movie.get('year', '')) if pd.notna(movie.get('year')) else ''

            result.append({
                'movie_id': movie_id,
                'title': title,
                'title_ru': title_ru,
                'year': year,
                'genre': ', '.join(genres_ru) if genres_ru else '',
                'genres': genres_ru,
                'imdb_rating': imdb_rating,
                'poster': get_poster_filename(title, year)
            })

        logger.info(f"Найдено фильмов: {len(result)}")
        return jsonify({'movies': result, 'total': len(result)})

    except Exception as e:
        logger.error(f"Ошибка при поиске фильмов: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/genre/<path:genre_name>/description')
def get_genre_description(genre_name):
    """API для получения описания жанра на русском"""
    try:
        genre_name = unquote(genre_name)
        logger.info(f"Получение описания жанра: {genre_name}")

        # Загружаем файл с описаниями жанров
        genres_desc_df = None
        genres_file = 'data/imdb_genres.csv'
        if os.path.exists(genres_file):
            try:
                genres_desc_df = pd.read_csv(genres_file, sep=';', encoding='utf-8')
                logger.info(f"Загружен файл с описаниями жанров: {len(genres_desc_df)} записей")
            except Exception as e:
                logger.error(f"Ошибка загрузки imdb_genres.csv: {e}")

        # Если файл не загружен, пробуем другой путь
        if genres_desc_df is None or len(genres_desc_df) == 0:
            genres_file_alt = 'data/imdb_genres.csv'
            if os.path.exists(genres_file_alt):
                try:
                    genres_desc_df = pd.read_csv(genres_file_alt, sep=';', encoding='utf-8')
                except:
                    pass

        if genres_desc_df is not None and len(genres_desc_df) > 0:
            # Определяем колонки в зависимости от структуры
            if 'title_ru' in genres_desc_df.columns:
                # Новая структура
                match = genres_desc_df[
                    (genres_desc_df['title_ru'].str.lower() == genre_name.lower()) |
                    (genres_desc_df['title'].str.lower() == genre_name.lower())
                ]
                if len(match) > 0:
                    row = match.iloc[0]
                    return jsonify({
                        'name_ru': row.get('title_ru', genre_name),
                        'name_en': row.get('title', ''),
                        'description_ru': row.get('description_ru', f'Подборка фильмов в жанре "{genre_name}".'),
                        'description_en': row.get('description_en', ''),
                        'type_ru': row.get('type_ru', 'Жанр'),
                        'type_en': row.get('type_en', 'Genre')
                    })
            elif 'genre_ru' in genres_desc_df.columns:
                # Старая структура
                match = genres_desc_df[
                    (genres_desc_df['genre_ru'].str.lower() == genre_name.lower()) |
                    (genres_desc_df['genre_en'].str.lower() == genre_name.lower())
                ]
                if len(match) > 0:
                    row = match.iloc[0]
                    return jsonify({
                        'name_ru': row.get('genre_ru', genre_name),
                        'name_en': row.get('genre_en', ''),
                        'description_ru': row.get('genre_description_ru', f'Подборка фильмов в жанре "{genre_name}".'),
                        'description_en': row.get('genre_description_en', ''),
                        'type_ru': row.get('type_ru', 'Жанр'),
                        'type_en': row.get('type_en', 'Genre')
                    })

        # Если описание не найдено, возвращаем заглушку
        return jsonify({
            'name_ru': genre_name,
            'name_en': '',
            'description_ru': f'Подборка фильмов в жанре "{genre_name}".',
            'description_en': f'Collection of movies in the "{genre_name}" genre.',
            'type_ru': 'Жанр',
            'type_en': 'Genre'
        })

    except Exception as e:
        logger.error(f"Ошибка получения описания жанра: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
