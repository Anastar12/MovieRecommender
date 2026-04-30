from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_from_directory
import asyncio
import logging
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import os
from urllib.parse import unquote
import math
import json
import re
import pickle
from datetime import datetime
import csv
import traceback

from scipy.sparse import load_npz
import psycopg2
from psycopg2.extras import RealDictCursor

from core.config import config
from offline.data_pipeline import DataPipeline
from offline.evaluation_metrics import RecommendationMetrics
from offline.model_trainer import ModelTrainer
from offline.cache_manager import CacheManager
from online.incremental_updater import IncrementalUpdater, OnlineDataUpdater
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

# Отключаем лишние логи от sklearn и других библиотек
logging.getLogger('sklearn').setLevel(logging.WARNING)
logging.getLogger('matplotlib').setLevel(logging.WARNING)
logging.getLogger('implicit').setLevel(logging.WARNING)

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


# Диагностика путей
def check_models_path():
    """Проверка наличия файлов моделей"""
    models_path = config.offline.models_path
    print(f"\n=== ДИАГНОСТИКА МОДЕЛЕЙ ===")
    print(f"Путь к моделям: {models_path}")
    print(f"Путь существует: {os.path.exists(models_path)}")

    if os.path.exists(models_path):
        files = os.listdir(models_path)
        print(f"Файлов в папке: {len(files)}")

        # Проверяем ключевые файлы
        key_files = [
            'rating_predictor.pkl',
            'user_factors.npy',
            'item_factors.npy',
            'movie_indices.pkl',
            'user_indices.pkl',
            'movies_df.pkl'
        ]

        for key_file in key_files:
            exists = key_file in files
            if exists:
                size = os.path.getsize(os.path.join(models_path, key_file)) / 1024 / 1024
                print(f"  ✓ {key_file} ({size:.2f} MB)")
            else:
                print(f"  ✗ {key_file}")
    else:
        print(f"❌ Путь {models_path} не существует!")

    print("==========================\n")


# Вызываем диагностику при старте
check_models_path()


def run_async(coro):
    """
    Безопасный запуск асинхронной корутины в синхронном контексте
    """
    try:
        # Пытаемся получить текущий event loop
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Нет running loop, создаем новый
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    else:
        # Есть running loop, но мы не можем его использовать
        # Создаем новый loop в отдельном потоке
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, coro)
            return future.result()


def handle_nan(obj):
    """Обработчик NaN значений для JSON сериализации"""
    if isinstance(obj, float):
        if math.isnan(obj):
            return None
    if pd.isna(obj):
        return None
    return obj


class RecommendationMetrics:
    """Метрики качества рекомендаций"""

    @staticmethod
    def precision_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Precision@K"""
        if not recommended or k == 0:
            return 0.0

        recommended_k = recommended[:k]
        relevant_set = set(relevant)
        hits = sum(1 for item in recommended_k if item in relevant_set)
        return hits / k

    @staticmethod
    def recall_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Recall@K"""
        if not relevant:
            return 0.0

        recommended_k = set(recommended[:k])
        relevant_set = set(relevant)
        hits = len(recommended_k & relevant_set)
        return hits / len(relevant_set)

    @staticmethod
    def ndcg_at_k(recommended: List[str], relevant: List[str],
                  relevance_scores: Dict[str, float], k: int) -> float:
        """NDCG@K"""
        if not recommended or not relevance_scores:
            return 0.0

        recommended_k = recommended[:k]

        # DCG
        dcg = 0.0
        for i, item in enumerate(recommended_k):
            rel = relevance_scores.get(item, 0)
            dcg += rel / np.log2(i + 2)

        # IDCG - идеальный порядок
        ideal_relevant = sorted(relevant,
                                key=lambda x: relevance_scores.get(x, 0),
                                reverse=True)[:k]

        idcg = 0.0
        for i, item in enumerate(ideal_relevant):
            rel = relevance_scores.get(item, 0)
            idcg += rel / np.log2(i + 2)

        return dcg / idcg if idcg > 0 else 0.0

    @staticmethod
    def map_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Mean Average Precision@K"""
        if not relevant or not recommended:
            return 0.0

        relevant_set = set(relevant)
        precisions = []
        hits = 0

        for i, item in enumerate(recommended[:k], 1):
            if item in relevant_set:
                hits += 1
                precisions.append(hits / i)

        return np.mean(precisions) if precisions else 0.0

    @staticmethod
    def hit_rate_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Hit Rate@K"""
        if not relevant:
            return 0.0

        recommended_k = set(recommended[:k])
        relevant_set = set(relevant)
        return 1.0 if recommended_k & relevant_set else 0.0

    @staticmethod
    def mrr_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Mean Reciprocal Rank@K"""
        if not relevant:
            return 0.0

        relevant_set = set(relevant)
        for i, item in enumerate(recommended[:k], 1):
            if item in relevant_set:
                return 1.0 / i
        return 0.0


class ModelsProvider:
    """Провайдер моделей для онлайн-компонентов"""

    def __init__(self, trainer: ModelTrainer, data_pipeline: DataPipeline):
        self.trainer = trainer
        self.data = data_pipeline
        self.models_path = trainer.models_path

        # Инициализация недостающих атрибутов
        self.popularity_scores = None
        self.recency_scores = None
        self.combined_features = None
        self.movie_ids = []
        self.movie_indices = {}
        self.user_indices = {}
        self.movie_list = []
        self.user_list = []

        # Загрузка моделей
        self._load_models()

        # Загрузка дополнительных данных
        self._load_additional_data()

        # Загрузка данных для обратной совместимости
        self._load_compatibility_data()

    def _load_additional_data(self):
        """Загрузка дополнительных данных для онлайн-компонентов"""
        try:
            # Убедимся, что путь правильный
            models_path = self.trainer.models_path
            logger.info(f"Загрузка дополнительных данных из: {models_path}")

            # Проверяем существование пути
            if not os.path.exists(models_path):
                logger.error(f"Путь к моделям не существует: {models_path}")
                return

            # Загрузка popularity_scores
            pop_path = os.path.join(models_path, 'popularity_scores.npy')
            if os.path.exists(pop_path):
                self.popularity_scores = np.load(pop_path)
                logger.info(f"popularity_scores загружены: {self.popularity_scores.shape}")
            else:
                logger.warning(f"popularity_scores.npy не найден в {models_path}")

            # Загрузка recency_scores
            rec_path = os.path.join(models_path, 'recency_scores.npy')
            if os.path.exists(rec_path):
                self.recency_scores = np.load(rec_path)
                logger.info(f"recency_scores загружены: {self.recency_scores.shape}")
            else:
                logger.warning(f"recency_scores.npy не найден в {models_path}")

            # Загрузка combined_features
            comb_path = os.path.join(models_path, 'combined_features.npz')
            if os.path.exists(comb_path):
                self.combined_features = load_npz(comb_path)
                logger.info(f"combined_features загружены: {self.combined_features.shape}")
            else:
                logger.warning(f"combined_features.npz не найден в {models_path}")

            # Загрузка movie_ids
            movie_ids_path = os.path.join(models_path, 'movie_ids.pkl')
            if os.path.exists(movie_ids_path):
                with open(movie_ids_path, 'rb') as f:
                    self.movie_ids = pickle.load(f)
                logger.info(f"movie_ids загружены: {len(self.movie_ids)}")
            else:
                logger.warning(f"movie_ids.pkl не найден в {models_path}")
                self.movie_ids = []

            # Загрузка movie_indices
            movie_indices_path = os.path.join(models_path, 'movie_indices.pkl')
            if os.path.exists(movie_indices_path):
                with open(movie_indices_path, 'rb') as f:
                    self.movie_indices = pickle.load(f)
                logger.info(f"movie_indices загружены: {len(self.movie_indices)}")
            else:
                logger.warning(f"movie_indices.pkl не найден в {models_path}")
                self.movie_indices = {}

            # Загрузка user_indices
            user_indices_path = os.path.join(models_path, 'user_indices.pkl')
            if os.path.exists(user_indices_path):
                with open(user_indices_path, 'rb') as f:
                    self.user_indices = pickle.load(f)
                logger.info(f"user_indices загружены: {len(self.user_indices)}")
            else:
                logger.warning(f"user_indices.pkl не найден в {models_path}")
                self.user_indices = {}

            # Загрузка user_list
            user_list_path = os.path.join(models_path, 'user_list.pkl')
            if os.path.exists(user_list_path):
                with open(user_list_path, 'rb') as f:
                    self.user_list = pickle.load(f)
                logger.info(f"user_list загружены: {len(self.user_list)}")
            else:
                logger.warning(f"user_list.pkl не найден в {models_path}")
                self.user_list = []

            # Загрузка movie_list
            movie_list_path = os.path.join(models_path, 'movie_list.pkl')
            if os.path.exists(movie_list_path):
                with open(movie_list_path, 'rb') as f:
                    self.movie_list = pickle.load(f)
                logger.info(f"movie_list загружены: {len(self.movie_list)}")
            else:
                logger.warning(f"movie_list.pkl не найден в {models_path}")
                self.movie_list = []

        except Exception as e:
            logger.error(f"Ошибка загрузки дополнительных данных: {e}")
            import traceback
            traceback.print_exc()

    def _load_models(self):
        """Загрузка моделей из файлов"""

        # Убедимся, что путь правильный
        models_path = self.trainer.models_path
        logger.info(f"Загрузка моделей из: {models_path}")

        # Загружаем модели через trainer
        success = self.trainer.load_models()

        if not success:
            logger.warning("Не удалось загрузить модели через trainer.load_models()")

            # Пробуем загрузить вручную
            try:
                # Загрузка SVD модели
                svd_path = os.path.join(models_path, 'svd_model.pkl')
                if os.path.exists(svd_path):
                    with open(svd_path, 'rb') as f:
                        self.trainer.svd_model = pickle.load(f)
                    logger.info("SVD модель загружена вручную")

                # Загрузка ALS модели
                als_path = os.path.join(models_path, 'als_model.pkl')
                if os.path.exists(als_path):
                    with open(als_path, 'rb') as f:
                        self.trainer.als_model = pickle.load(f)
                    logger.info("ALS модель загружена вручную")

                # Загрузка модели предсказания
                rating_path = os.path.join(models_path, 'rating_predictor.pkl')
                if os.path.exists(rating_path):
                    with open(rating_path, 'rb') as f:
                        self.trainer.rating_predictor = pickle.load(f)
                    logger.info("Rating predictor загружен вручную")

                # Загрузка факторов
                user_factors_path = os.path.join(models_path, 'user_factors.npy')
                if os.path.exists(user_factors_path):
                    self.trainer.user_factors = np.load(user_factors_path)
                    logger.info(f"user_factors загружены: {self.trainer.user_factors.shape}")

                item_factors_path = os.path.join(models_path, 'item_factors.npy')
                if os.path.exists(item_factors_path):
                    self.trainer.item_factors = np.load(item_factors_path)
                    logger.info(f"item_factors загружены: {self.trainer.item_factors.shape}")

                # Загрузка NMF факторов
                user_factors_nmf_path = os.path.join(models_path, 'user_factors_nmf.npy')
                if os.path.exists(user_factors_nmf_path):
                    self.trainer.user_factors_nmf = np.load(user_factors_nmf_path)
                    logger.info(f"user_factors_nmf загружены: {self.trainer.user_factors_nmf.shape}")

                item_factors_nmf_path = os.path.join(models_path, 'item_factors_nmf.npy')
                if os.path.exists(item_factors_nmf_path):
                    self.trainer.item_factors_nmf = np.load(item_factors_nmf_path)
                    logger.info(f"item_factors_nmf загружены: {self.trainer.item_factors_nmf.shape}")

            except Exception as e:
                logger.error(f"Ошибка ручной загрузки моделей: {e}")

        # Загрузка данных
        try:
            movies_path = os.path.join(models_path, 'movies_df.pkl')
            if os.path.exists(movies_path):
                self.movies_df = pd.read_pickle(movies_path)
                logger.info(f"movies_df загружен: {len(self.movies_df)} фильмов")
            else:
                self.movies_df = None
                logger.warning(f"movies_df.pkl не найден в {models_path}")

            user_main_path = os.path.join(models_path, 'user_main_df.pkl')
            if os.path.exists(user_main_path):
                self.user_main_df = pd.read_pickle(user_main_path)
                logger.info(f"user_main_df загружен: {len(self.user_main_df)} пользователей")
            else:
                self.user_main_df = None

            genres_path = os.path.join(models_path, 'genres_df.pkl')
            if os.path.exists(genres_path):
                self.genres_df = pd.read_pickle(genres_path)
                logger.info("genres_df загружен")
            else:
                self.genres_df = None

        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            self.movies_df = None

    def get_svd_recommendations(self, user_id: str, n: int = 50) -> List[Dict]:
        """SVD рекомендации с проверкой наличия данных"""
        if self.trainer.svd_model is None:
            logger.warning("SVD модель не загружена")
            return []

        if not hasattr(self, 'user_indices') or self.user_indices is None:
            logger.warning("user_indices не загружены")
            return []

        if not hasattr(self, 'movie_list') or self.movie_list is None:
            logger.warning("movie_list не загружен")
            return []

        try:
            if user_id not in self.user_indices:
                logger.debug(f"Пользователь {user_id} не найден в индексах SVD")
                return []

            user_idx = self.user_indices[user_id]

            # Проверяем наличие user_factors
            if not hasattr(self.trainer, 'user_factors') or self.trainer.user_factors is None:
                logger.warning("user_factors не загружены")
                return []

            if user_idx >= len(self.trainer.user_factors):
                logger.warning(f"Индекс пользователя {user_idx} вне диапазона")
                return []

            user_vector = self.trainer.user_factors[user_idx]

            # Проверяем наличие item_factors
            if not hasattr(self.trainer, 'item_factors') or self.trainer.item_factors is None:
                logger.warning("item_factors не загружены")
                return []

            predicted = user_vector @ self.trainer.item_factors.T

            # Получаем топ-N индексов
            top_indices = np.argsort(predicted)[::-1][:n]

            recommendations = []
            for idx in top_indices:
                if idx < len(self.movie_list):
                    movie_id = self.movie_list[idx]
                    if self.movies_df is not None:
                        movie = self.movies_df[self.movies_df['movie_id'] == movie_id]
                        if len(movie) > 0:
                            recommendations.append({
                                'movie_id': movie_id,
                                'title': movie.iloc[0].get('title', ''),
                                'score': float(predicted[idx])
                            })

            return recommendations
        except Exception as e:
            logger.error(f"Ошибка SVD: {e}")
            return []

    def get_als_recommendations(self, user_id: str, n: int = 50) -> List[Dict]:
        """ALS рекомендации с проверкой наличия данных"""
        if self.trainer.als_model is None:
            logger.warning("ALS модель не загружена")
            return []

        if not hasattr(self, 'user_indices') or self.user_indices is None:
            logger.warning("user_indices не загружены")
            return []

        if not hasattr(self, 'movie_list') or self.movie_list is None:
            logger.warning("movie_list не загружен")
            return []

        try:
            if user_id not in self.user_indices:
                logger.debug(f"Пользователь {user_id} не найден в индексах ALS")
                return []

            user_idx = self.user_indices[user_id]

            # Создаем фиктивную user-item матрицу для ALS
            # ALS модель требует матрицу для рекомендаций
            if not hasattr(self.data, 'user_item_matrix') or self.data.user_item_matrix is None:
                # Пробуем загрузить user_item_matrix
                matrix_path = os.path.join(self.trainer.models_path, 'user_item_matrix.npz')
                if os.path.exists(matrix_path):
                    self.data.user_item_matrix = load_npz(matrix_path)
                else:
                    logger.warning("user_item_matrix не найдена")
                    return []

            recommendations = self.trainer.als_model.recommend(
                user_idx,
                self.data.user_item_matrix.T,
                N=n
            )

            result = []
            for movie_idx, score in recommendations:
                if movie_idx < len(self.movie_list):
                    movie_id = self.movie_list[movie_idx]
                    if self.movies_df is not None:
                        movie = self.movies_df[self.movies_df['movie_id'] == movie_id]
                        if len(movie) > 0:
                            result.append({
                                'movie_id': movie_id,
                                'title': movie.iloc[0].get('title', ''),
                                'score': float(score)
                            })

            return result
        except Exception as e:
            logger.error(f"Ошибка ALS: {e}")
            return []

    def predict_rating(self, user_id: str, movie_id: str) -> float:
        """Предсказание оценки"""
        if self.trainer.rating_predictor is None:
            logger.debug("Модель предсказания не загружена")
            return 0.5

        try:
            # Используем загруженные индексы из self, а не self.data
            if not hasattr(self, 'user_indices') or self.user_indices is None:
                logger.debug("user_indices не загружены в ModelsProvider")
                return 0.5
            if not hasattr(self, 'movie_indices') or self.movie_indices is None:
                logger.debug("movie_indices не загружены в ModelsProvider")
                return 0.5

            # Приводим ID к строке для сравнения
            user_id_str = str(user_id)
            movie_id_str = str(movie_id)

            if user_id_str not in self.user_indices:
                logger.debug(f"Пользователь {user_id_str} не найден в индексах")
                return 0.5
            if movie_id_str not in self.movie_indices:
                logger.debug(f"Фильм {movie_id_str} не найден в индексах")
                return 0.5

            user_idx = self.user_indices[user_id_str]
            movie_idx = self.movie_indices[movie_id_str]

            # Проверяем наличие факторов
            if not hasattr(self.trainer, 'user_factors') or self.trainer.user_factors is None:
                logger.debug("user_factors не загружены")
                return 0.5
            if not hasattr(self.trainer, 'item_factors') or self.trainer.item_factors is None:
                logger.debug("item_factors не загружены")
                return 0.5

            if user_idx >= len(self.trainer.user_factors):
                logger.debug(f"Индекс пользователя {user_idx} вне диапазона")
                return 0.5
            if movie_idx >= len(self.trainer.item_factors):
                logger.debug(f"Индекс фильма {movie_idx} вне диапазона")
                return 0.5

            # Определяем размерности факторов
            user_factor_dim = min(100, self.trainer.user_factors.shape[1] if len(
                self.trainer.user_factors.shape) > 1 else 100)
            item_factor_dim = min(100, self.trainer.item_factors.shape[1] if len(
                self.trainer.item_factors.shape) > 1 else 100)

            # Берем признаки
            user_vec = self.trainer.user_factors[user_idx][:user_factor_dim] if user_factor_dim > 0 else np.array(
                [0.5] * 100)
            item_vec = self.trainer.item_factors[movie_idx][:item_factor_dim] if item_factor_dim > 0 else np.array(
                [0.5] * 100)

            # Дополняем до 100, если нужно
            if len(user_vec) < 100:
                user_vec = np.pad(user_vec, (0, 100 - len(user_vec)), constant_values=0.5)
            if len(item_vec) < 100:
                item_vec = np.pad(item_vec, (0, 100 - len(item_vec)), constant_values=0.5)

            # Формируем признаки - 310 признаков (как при обучении)
            features_list = []

            # 1. SVD признаки (200 признаков)
            features_list.extend(user_vec[:100])
            features_list.extend(item_vec[:100])

            # 2. NMF признаки (100 признаков)
            if hasattr(self.trainer, 'user_factors_nmf') and self.trainer.user_factors_nmf is not None:
                if user_idx < len(self.trainer.user_factors_nmf):
                    nmf_user = self.trainer.user_factors_nmf[user_idx][:50] if len(
                        self.trainer.user_factors_nmf[user_idx]) >= 50 else self.trainer.user_factors_nmf[user_idx]
                    if len(nmf_user) < 50:
                        nmf_user = np.pad(nmf_user, (0, 50 - len(nmf_user)), constant_values=0.5)
                    features_list.extend(nmf_user[:50])
                else:
                    features_list.extend([0.5] * 50)
            else:
                features_list.extend([0.5] * 50)

            if hasattr(self.trainer, 'item_factors_nmf') and self.trainer.item_factors_nmf is not None:
                if movie_idx < len(self.trainer.item_factors_nmf):
                    nmf_item = self.trainer.item_factors_nmf[movie_idx][:50] if len(
                        self.trainer.item_factors_nmf[movie_idx]) >= 50 else self.trainer.item_factors_nmf[movie_idx]
                    if len(nmf_item) < 50:
                        nmf_item = np.pad(nmf_item, (0, 50 - len(nmf_item)), constant_values=0.5)
                    features_list.extend(nmf_item[:50])
                else:
                    features_list.extend([0.5] * 50)
            else:
                features_list.extend([0.5] * 50)

            # 3. Статистические признаки (5)
            user_rating_count = 0.5
            movie_rating_count = 0.5
            features_list.extend([user_rating_count, movie_rating_count, 0.5, 0.5, 0.25])

            # 4. Популярность и свежесть (2)
            if hasattr(self, 'popularity_scores') and self.popularity_scores is not None and movie_idx < len(
                    self.popularity_scores):
                features_list.append(float(self.popularity_scores[movie_idx]))
            else:
                features_list.append(0.5)

            if hasattr(self, 'recency_scores') and self.recency_scores is not None and movie_idx < len(
                    self.recency_scores):
                features_list.append(float(self.recency_scores[movie_idx]))
            else:
                features_list.append(0.5)

            # 5. Взаимодействие признаков (3)
            features_list.extend([0.25, 0.25, 0.125])

            # Проверяем количество признаков
            expected_features = 310
            current_features = len(features_list)

            if current_features < expected_features:
                features_list.extend([0.5] * (expected_features - current_features))
            elif current_features > expected_features:
                features_list = features_list[:expected_features]

            features = np.array(features_list, dtype=np.float32).reshape(1, -1)

            # Обработка NaN и inf
            features = np.nan_to_num(features, nan=0.5, posinf=0.5, neginf=0.5)

            # Предсказание
            if hasattr(self.trainer.rating_predictor, 'predict'):
                prediction = self.trainer.rating_predictor.predict(features)[0]

                # Если есть трансформер, применяем обратное преобразование
                if hasattr(self.trainer, 'rating_transformer') and self.trainer.rating_transformer is not None:
                    prediction = self.trainer.rating_transformer.inverse_transform([[prediction]])[0, 0]

                # Нормализуем от 0 до 1
                prediction = max(0, min(1, prediction))
                return prediction

            return 0.5

        except Exception as e:
            logger.error(f"Ошибка предсказания: {e}")
            import traceback
            traceback.print_exc()
            return 0.5

    def _get_user_index(self, user_id: str) -> Optional[int]:
        """Получение индекса пользователя с поиском по разным форматам"""
        # Прямой поиск
        if user_id in self.user_indices:
            return self.user_indices[user_id]

        # Поиск по нормализованному URL
        normalized = self._normalize_user_url(user_id)
        if normalized in self.user_indices:
            return self.user_indices[normalized]

        # Поиск по user_list
        if hasattr(self, 'user_list') and self.user_list:
            try:
                return self.user_list.index(normalized)
            except ValueError:
                pass

        return None

    def _normalize_user_url(self, user_url: str) -> str:
        """Нормализация URL пользователя"""
        if not user_url:
            return ''

        normalized = str(user_url).strip()
        normalized = normalized.replace('https://www.imdb.com', '')
        normalized = normalized.replace('http://www.imdb.com', '')
        normalized = normalized.rstrip('/')
        normalized = normalized.split('?')[0]

        # Убираем лишние слеши
        while '//' in normalized:
            normalized = normalized.replace('//', '/')

        return normalized

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
                        self.subgenres_df = pd.read_pickle(
                            os.path.join(provider.trainer.models_path, 'subgenres_df.pkl'))
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
                            countries_path = os.path.join(self.provider.trainer.models_path, 'countries_df.pkl')
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

                def get_similar_movies(self, movie_id, n=20):
                    """Прокси для похожих фильмов через ModelsProvider."""
                    return self.provider.get_similar_movies(movie_id, n)

            recommender = RecommenderWrapper(self)

            # Загрузка reviews_df для совместимости
            try:
                reviews_path = os.path.join(self.trainer.models_path, 'reviews_df.pkl')
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
        self.connection = data_pipeline.connection

        # Загрузка данных
        movies_path = os.path.join(self.data.models_path, 'movies_df.pkl')
        self.movies_df = pd.read_pickle(movies_path) if os.path.exists(movies_path) else None
        self.user_main_df = pd.read_pickle(os.path.join(self.data.models_path, 'user_main_df.pkl')) if os.path.exists(os.path.join(self.data.models_path, 'user_main_df.pkl')) else None

    def _ensure_connection(self):
        """Проверяет и пересоздает соединение с БД при необходимости"""
        try:
            if self.connection is None:
                self._create_connection()
                return

            # Проверяем, живо ли соединение
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except (psycopg2.InterfaceError, psycopg2.OperationalError, AttributeError) as e:
            logger.warning(f"Соединение с БД потеряно, переподключаемся: {e}")
            try:
                self.connection.close()
            except:
                pass
            self._create_connection()

    def _create_connection(self):
        """Создает подключение к БД для DataProvider"""
        try:
            if hasattr(self.data, 'db_config'):
                self.connection = psycopg2.connect(**self.data.db_config)
                logger.info("DataProvider: Подключение к БД установлено")
            else:
                logger.error("Нет конфигурации БД для DataProvider")
                self.connection = None
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            self.connection = None

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
        """Получение просмотренных фильмов пользователя из PostgreSQL"""
        try:
            self._ensure_connection()

            if not self.connection:
                logger.error("Нет подключения к БД")
                return []

            # Извлекаем ID пользователя
            import re
            user_match = re.search(r'ur\d+', user_id)
            if user_match:
                user_login = user_match.group(0)
            else:
                user_login = user_id.strip('/').split('/')[-1]

            logger.info(f"Поиск фильмов для пользователя: {user_login}")

            # Все возможные форматы user_url
            user_variants = [
                user_login,
                f"/user/{user_login}",
                f"/user/{user_login}/",
                f"/user/{user_login}?ref_=tturv_t_usr",
                f"https://www.imdb.com/user/{user_login}",
                f"https://www.imdb.com/user/{user_login}/",
                f"https://www.imdb.com//user/{user_login}/",
                user_id,
                f"/user/ur{user_login}" if not user_login.startswith('ur') else user_login,
            ]
            user_variants = list(set(user_variants))

            logger.debug(f"Варианты поиска: {user_variants[:5]}")

            watched_entries = []

            try:
                self.connection.rollback()
            except:
                pass

            with self.connection.cursor() as cursor:
                # 1. Поиск в user_watched
                for variant in user_variants[:5]:
                    try:
                        cursor.execute("""
                            SELECT DISTINCT movie_id, added_date
                            FROM db.user_watched 
                            WHERE user_url = %s
                        """, (variant,))

                        for row in cursor.fetchall():
                            movie_id = row[0] if row[0] else None
                            if movie_id:
                                watched_entries.append({
                                    'movie_id': movie_id,
                                    'added_date': row[1] if len(row) > 1 else None,
                                    'source': 'user_watched',
                                    'rating': None,
                                    'review_text': '',
                                    'date': None,
                                    'title': '',
                                    'title_ru': '',
                                    'year': '',
                                    'genre': ''
                                })
                                logger.debug(f"Найден фильм {movie_id} в user_watched")
                    except Exception as e:
                        pass

                # 2. Поиск в reviews (основной источник)
                for variant in user_variants:
                    try:
                        cursor.execute("""
                            SELECT DISTINCT 
                                r.movie_review_url,
                                r.rating,
                                r.review_text,
                                r.date,
                                m.title,
                                m.title_ru,
                                m.year,
                                m.genre
                            FROM db.reviews r
                            LEFT JOIN db.movies m ON m.movie_url = r.movie_review_url
                            WHERE r.user_url = %s
                              AND r.rating IS NOT NULL 
                              AND r.rating != ''
                            ORDER BY r.date DESC
                            LIMIT 1000
                        """, (variant,))

                        for row in cursor.fetchall():
                            movie_review_url = row[0] if row[0] else ''
                            if not movie_review_url:
                                continue

                            match = re.search(r'(tt\d+)', movie_review_url)
                            if match:
                                movie_id = match.group(1)

                                rating_value = None
                                rating_raw = row[1] if len(row) > 1 else None
                                if rating_raw:
                                    try:
                                        rating_str = str(rating_raw).strip()
                                        if '/' in rating_str:
                                            rating_str = rating_str.split('/')[0].strip()
                                        rating_value = float(rating_str)
                                    except:
                                        rating_value = None

                                watched_entries.append({
                                    'movie_id': movie_id,
                                    'rating': rating_value,
                                    'review_text': row[2] if len(row) > 2 and row[2] else '',
                                    'date': row[3] if len(row) > 3 else None,
                                    'title': row[4] if len(row) > 4 and row[4] else '',
                                    'title_ru': row[5] if len(row) > 5 and row[5] else '',
                                    'year': row[6] if len(row) > 6 else '',
                                    'genre': row[7] if len(row) > 7 and row[7] else '',
                                    'source': 'reviews',
                                    'added_date': None
                                })
                                logger.debug(f"Найден фильм {movie_id} в reviews")
                    except Exception as e:
                        logger.debug(f"Ошибка поиска по варианту {variant}: {e}")

                # 3. Поиск в reviews через LIKE (для форматов с параметрами)
                try:
                    cursor.execute("""
                        SELECT DISTINCT 
                            r.movie_review_url,
                            r.rating,
                            r.review_text,
                            r.date,
                            m.title,
                            m.title_ru,
                            m.year,
                            m.genre
                        FROM db.reviews r
                        LEFT JOIN db.movies m ON m.movie_url = r.movie_review_url
                        WHERE r.user_url LIKE %s
                          AND r.rating IS NOT NULL 
                          AND r.rating != ''
                        ORDER BY r.date DESC
                        LIMIT 1000
                    """, (f'%{user_login}%',))

                    for row in cursor.fetchall():
                        movie_review_url = row[0] if row[0] else ''
                        if not movie_review_url:
                            continue

                        match = re.search(r'(tt\d+)', movie_review_url)
                        if match:
                            movie_id = match.group(1)

                            # Проверяем, нет ли уже такого фильма
                            if not any(e.get('movie_id') == movie_id for e in watched_entries):
                                rating_value = None
                                rating_raw = row[1] if len(row) > 1 else None
                                if rating_raw:
                                    try:
                                        rating_str = str(rating_raw).strip()
                                        if '/' in rating_str:
                                            rating_str = rating_str.split('/')[0].strip()
                                        rating_value = float(rating_str)
                                    except:
                                        rating_value = None

                                watched_entries.append({
                                    'movie_id': movie_id,
                                    'rating': rating_value,
                                    'review_text': row[2] if len(row) > 2 and row[2] else '',
                                    'date': row[3] if len(row) > 3 else None,
                                    'title': row[4] if len(row) > 4 and row[4] else '',
                                    'title_ru': row[5] if len(row) > 5 and row[5] else '',
                                    'year': row[6] if len(row) > 6 else '',
                                    'genre': row[7] if len(row) > 7 and row[7] else '',
                                    'source': 'reviews_like',
                                    'added_date': None
                                })
                except Exception as e:
                    logger.debug(f"Ошибка LIKE поиска: {e}")

                # Дедупликация по movie_id
                unique_movies = {}
                for entry in watched_entries:
                    movie_id = entry.get('movie_id')
                    if movie_id and movie_id not in unique_movies:
                        unique_movies[movie_id] = entry
                    elif movie_id and unique_movies[movie_id].get('rating') is None and entry.get('rating') is not None:
                        unique_movies[movie_id] = entry

                watched_entries = list(unique_movies.values())
                logger.info(f"Загружено {len(watched_entries)} уникальных фильмов для {user_login}")

            # Обогащаем данными о фильмах
            watched = []
            movies_not_found = []
            movies_without_details = []

            for item in watched_entries:
                movie_id = item.get('movie_id')
                if not movie_id:
                    continue

                # Пытаемся получить детали фильма
                details = self._get_movie_details_simple(movie_id)

                if not details:
                    # Создаем базовые детали из имеющихся данных
                    title = item.get('title', '')
                    title_ru = item.get('title_ru', '')
                    year = str(item.get('year', '')) if item.get('year') else ''
                    genre = item.get('genre', '')

                    # Если нет названия, пробуем получить из movies_df через другой запрос
                    if not title and self.movies_df is not None:
                        movie_row = self.movies_df[self.movies_df['movie_id'] == movie_id]
                        if len(movie_row) > 0:
                            title = movie_row.iloc[0].get('title', '')
                            title_ru = movie_row.iloc[0].get('title_ru', '')
                            year = str(movie_row.iloc[0].get('year', ''))
                            genre = movie_row.iloc[0].get('genre', '')

                    details = {
                        'movie_id': movie_id,
                        'title': title if title else f"Фильм {movie_id}",
                        'title_ru': title_ru if title_ru else (title if title else f"Фильм {movie_id}"),
                        'year': year,
                        'genre': genre,
                        'genres': [g.strip() for g in genre.split(',') if g.strip()],
                        'imdb_rating': None,
                        'plot': '',
                        'plot_ru': '',
                        'directors': [],
                        'directors_ru': [],
                        'actors': [],
                        'actors_ru': []
                    }

                    if not title:
                        movies_without_details.append(movie_id)

                    # Проверяем, есть ли хоть какое-то название (русское или английское)
                    has_title = (
                            details.get('title') and
                            details.get('title') != '' and
                            details.get('title') != 'nan' and
                            not details.get('title', '').startswith('Фильм tt')  # Исключаем заглушки
                    )
                    has_title_ru = (
                            details.get('title_ru') and
                            details.get('title_ru') != '' and
                            details.get('title_ru') != 'nan' and
                            not details.get('title_ru', '').startswith('Фильм tt')
                    )

                    # Если нет названия - пропускаем фильм
                    if not has_title and not has_title_ru:
                        logger.debug(f"Пропущен фильм {movie_id} - нет названия")
                        continue

                # Добавляем пользовательскую информацию
                rating = item.get('rating')
                if rating is not None:
                    try:
                        details['user_rating'] = float(rating)
                        details['rating'] = float(rating)
                    except:
                        details['user_rating'] = None
                        details['rating'] = None
                else:
                    details['user_rating'] = None
                    details['rating'] = None

                details['review_text'] = item.get('review_text', '')
                details['review_date'] = str(item.get('date', '')) if item.get('date') else ''
                details['added_date'] = str(item.get('added_date', '')) if item.get('added_date') else ''

                watched.append(details)

            if movies_without_details:
                logger.warning(f"Фильмы без деталей: {len(movies_without_details)} из {len(watched_entries)}")
                logger.debug(f"Примеры ID без деталей: {movies_without_details[:10]}")

            logger.info(
                f"Итоговое количество фильмов для отображения: {len(watched)} (загружено уникальных: {len(watched_entries)})")

            return watched

        except Exception as e:
            logger.error(f"Ошибка получения просмотренных фильмов: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _get_movie_details_simple(self, movie_id: str) -> Optional[Dict]:
        """Простое получение деталей фильма без зависимостей"""
        try:
            if self.movies_df is None:
                return None

            # Поиск фильма по movie_id (как есть)
            movie = self.movies_df[self.movies_df['movie_id'] == movie_id]

            # Если не нашли, пробуем поискать с преобразованием в строку
            if len(movie) == 0:
                movie = self.movies_df[self.movies_df['movie_id'].astype(str) == str(movie_id)]

            if len(movie) == 0:
                # Пробуем поискать по частичному совпадению
                movie = self.movies_df[self.movies_df['movie_id'].astype(str).str.contains(str(movie_id), na=False)]

            if len(movie) == 0:
                return None

            movie = movie.iloc[0]

            def safe_str(key, default=''):
                val = movie.get(key)
                if val is None or pd.isna(val):
                    return default
                if isinstance(val, (list, tuple, np.ndarray)):
                    if len(val) > 0:
                        return ', '.join(str(v) for v in val if v and str(v) != 'nan')
                    return default
                val_str = str(val)
                if val_str == 'nan' or val_str == 'None' or val_str == '':
                    return default
                return val_str

            def safe_float(key, default=None):
                val = movie.get(key)
                if val is None or pd.isna(val):
                    return default
                try:
                    return float(val)
                except:
                    return default

            return {
                'movie_id': str(movie_id),
                'title': safe_str('title', ''),
                'title_ru': safe_str('title_ru', safe_str('title', '')),
                'year': safe_str('year', ''),
                'genre': safe_str('genre', ''),
                'imdb_rating': safe_float('imdb'),
                'plot': safe_str('plot', ''),
                'plot_ru': safe_str('description_ru', safe_str('plot', '')),
                'directors': [d.strip() for d in safe_str('directors', '').split(',') if
                              d.strip() and d.strip() != 'nan'],
                'directors_ru': [d.strip() for d in safe_str('directors_ru', '').split(',') if
                                 d.strip() and d.strip() != 'nan'],
                'actors': [a.strip() for a in safe_str('actors', '').split(',') if a.strip() and a.strip() != 'nan'],
                'actors_ru': [a.strip() for a in safe_str('actors_ru', '').split(',') if
                              a.strip() and a.strip() != 'nan']
            }
        except Exception as e:
            logger.error(f"Ошибка получения деталей фильма {movie_id}: {e}")
            return None

    def _get_movie_details_from_cache(self, movie_id: str) -> Dict:
        """Получение деталей фильма из локального кэша"""
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

        return {
            'movie_id': str(movie_id),
            'title': str(safe_get('title', '')),
            'title_ru': str(safe_get('title_ru', safe_get('title', ''))),
            'year': str(safe_get('year', '')),
            'genre': str(safe_get('genre', '')),
            'imdb_rating': safe_get('imdb', None),
            'plot': str(safe_get('plot', '')),
            'plot_ru': str(safe_get('description_ru', safe_get('plot', ''))),
            'directors': safe_get('directors', '').split(',') if safe_get('directors', '') else [],
            'directors_ru': safe_get('directors_ru', '').split(',') if safe_get('directors_ru', '') else [],
            'actors': safe_get('actors', '').split(',') if safe_get('actors', '') else [],
            'actors_ru': safe_get('actors_ru', '').split(',') if safe_get('actors_ru', '') else []
        }


# Инициализация глобальных объектов
models_provider = None
data_provider = None
loop = None

# Флаг для отслеживания инициализации
_initialized = False


def init_online_components():
    """Инициализация онлайн-компонентов"""
    global models_provider, data_provider, context_handler, candidate_generator, ranker, postprocessor, loop, _initialized, incremental_updater, online_updater

    if _initialized:
        return

    logger.info("Инициализация онлайн-компонентов...")

    models_provider = ModelsProvider(model_trainer, data_pipeline)
    data_provider = DataProvider(data_pipeline)
    incremental_updater = IncrementalUpdater(models_provider, data_provider, cache_manager)
    online_updater = OnlineDataUpdater(data_pipeline, models_provider)

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

    force_refresh = context_params.get('force_refresh', False) if context_params else False

    # Проверка кэша (только если не принудительное обновление)
    if not force_refresh:
        cached = cache_manager.get_cached_top_n(user_id)
        if cached:
            logger.info(f"Возвращены кэшированные рекомендации для {user_id}")
            return cached

    # Получение контекста
    context = context_handler.get_user_context(user_id, context_params)
    context['user_rated_movies'] = context_handler.get_user_rated_movies(user_id)
    context['user_genre_preferences'] = context_handler.get_user_genre_preferences(user_id)

    logger.info(f"Контекст для {user_id}: оценено фильмов={len(context['user_rated_movies'])}, "
                f"жанров={len(context['user_genre_preferences'])}")

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
    """Настройка приложения с проверкой необходимости переобучения"""
    global loop, models_provider, data_provider, _initialized, recommender, reviews_df

    logger.info("Настройка приложения...")

    # Создаем директорию для моделей
    os.makedirs(config.offline.models_path, exist_ok=True)

    # Проверяем, есть ли уже обученные модели
    model_files_exist = check_models_exist(config.offline.models_path)

    if model_files_exist:
        logger.info("Модели уже существуют, загружаем без переобучения")
        # Только загружаем существующие модели
        if model_trainer.load_models():
            models_provider = ModelsProvider(model_trainer, data_pipeline)
            data_provider = DataProvider(data_pipeline)
            init_online_components()
            logger.info("Модели успешно загружены")
            return
        else:
            logger.warning("Не удалось загрузить модели, потребуется переобучение")

    # Если моделей нет или загрузить не удалось, запускаем обучение в фоне
    logger.info("Запуск обучения моделей в фоновом режиме...")

    # Запускаем обучение в отдельном потоке, чтобы не блокировать запуск сервера
    import threading
    training_thread = threading.Thread(target=run_offline_pipeline_background, daemon=True)
    training_thread.start()

    # Инициализируем базовые компоненты без моделей
    init_online_components_fallback()

    logger.info("Приложение запущено, модели обучаются в фоне")


def check_models_exist(models_path: str) -> bool:
    """Проверяет наличие основных файлов моделей"""
    required_files = [
        'svd_model.pkl',
        'movies_df.pkl',
        'user_main_df.pkl'
    ]

    for file in required_files:
        if not os.path.exists(os.path.join(models_path, file)):
            logger.info(f"Файл {file} отсутствует")
            return False

    logger.info("Основные файлы моделей найдены")
    return True


def run_offline_pipeline_background():
    """Запуск обучения в фоновом режиме"""
    try:
        logger.info("Фоновое обучение моделей начато...")

        # Создаем новый event loop для фонового потока
        background_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(background_loop)

        async def train():
            # Загрузка данных
            await data_pipeline.load_data()
            data_pipeline.preprocess_data()
            data_pipeline.create_feature_vectors()

            # Создание user-item матрицы
            user_item_matrix = data_pipeline.create_user_item_matrix()
            popularity_scores = data_pipeline.compute_popularity_scores()
            recency_scores = data_pipeline.compute_recency_scores()

            # Обучение SVD (быстрая модель)
            await model_trainer.build_svd_model(user_item_matrix)

            # Сохранение моделей
            model_trainer.save_models()

            logger.info("Фоновое обучение завершено")

        background_loop.run_until_complete(train())
        background_loop.close()

        # После обучения обновляем компоненты
        global models_provider, data_provider
        models_provider = ModelsProvider(model_trainer, data_pipeline)
        data_provider = DataProvider(data_pipeline)

    except Exception as e:
        logger.error(f"Ошибка фонового обучения: {e}")
        import traceback
        traceback.print_exc()


def init_online_components_fallback():
    """Инициализация онлайн-компонентов без полноценных моделей (fallback режим)"""
    global models_provider, data_provider, context_handler, candidate_generator, ranker, postprocessor, loop, _initialized

    logger.info("Инициализация онлайн-компонентов в fallback режиме...")

    # Создаем заглушку для моделей
    if models_provider is None:
        models_provider = ModelsProviderFallback()

    if data_provider is None:
        data_provider = DataProviderFallback()

    context_handler = ContextHandler(data_provider)
    candidate_generator = CandidateGenerator(models_provider, data_provider, {
        'weights': config.online.weights,
        'candidate_limit': config.online.candidate_limit
    })
    ranker = Ranker(models_provider, data_provider, {
        'final_top_n': config.online.final_top_n
    })
    postprocessor = Postprocessor(data_provider)

    _initialized = True
    logger.info("Онлайн-компоненты в fallback режиме инициализированы")


class ModelsProviderFallback:
    """Заглушка для ModelsProvider, когда модели еще не обучены"""

    def __init__(self):
        self.movies_df = None
        self.genres_df = None
        logger.info("ModelsProviderFallback инициализирован")

    def get_similar_movies(self, movie_id, n=20):
        return []

    def get_svd_recommendations(self, user_id, n=50):
        return []

    def get_als_recommendations(self, user_id, n=50):
        return []

    def predict_rating(self, user_id, movie_id):
        return 0.5


class DataProviderFallback:
    """Заглушка для DataProvider"""

    def __init__(self):
        self.movies_df = None
        logger.info("DataProviderFallback инициализирован")

    def get_user_watched_movies(self, user_id):
        return []

    def get_popular_movies(self, limit=100):
        return []

# Вызываем инициализацию при старте
setup_app()


# ==================== API ЭНДПОИНТЫ ====================

@app.route('/api/recommendations', methods=['POST'])
def get_recommendations_api():
    """API для получения рекомендаций"""
    global loop

    data = request.json or {}

    # Безопасное получение user_id
    user_id = data.get('user_url')
    if not user_id:
        from flask import session as flask_session
        user_id = flask_session.get('user_url')

    if not user_id:
        return jsonify({'error': 'Пользователь не авторизован'}), 401

    top_n = data.get('top_n', 50)

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
        # Убеждаемся, что компоненты инициализированы
        if not _initialized:
            init_online_components()

        # ВАЖНО: Принудительно загружаем историю пользователя и инвалидируем кэш при первом запросе
        # Получаем историю пользователя (синхронно)
        user_rated_movies = []
        if data_provider:
            user_rated_movies = data_provider.get_user_watched_movies(normalized_user_id)
            logger.info(f"Пользователь {normalized_user_id} оценил {len(user_rated_movies)} фильмов")

        # Получаем жанровые предпочтения
        genre_prefs = {}
        if context_handler:
            # Принудительно обновляем историю в контексте
            context_handler.clear_session_cache(normalized_user_id)
            genre_prefs = context_handler.get_user_genre_preferences(normalized_user_id)

        logger.info(f"Жанровые предпочтения: {genre_prefs}")

        # Если есть оценки, но нет жанровых предпочтений - пересчитываем
        if len(user_rated_movies) > 0 and not genre_prefs:
            logger.info("Пересчет жанровых предпочтений...")
            # Принудительно пересчитываем предпочтения
            if context_handler:
                genre_prefs = context_handler.get_user_genre_preferences(normalized_user_id)
                # Сохраняем в кэш
                if user_id not in context_handler.session_cache:
                    context_handler.session_cache[user_id] = {}
                context_handler.session_cache[user_id]['genre_preferences'] = genre_prefs

        # Инвалидируем кэш если есть оценки, но рекомендации не персонализированы
        force_refresh = len(user_rated_movies) > 0

        # Если есть жанровые предпочтения - обязательно обновляем
        if genre_prefs:
            force_refresh = True
            # Инвалидируем старый кэш
            cache_manager.invalidate_user_cache(normalized_user_id)
            logger.info(f"Кэш инвалидирован для персонализации пользователя {normalized_user_id}")

        # Получаем рекомендации
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        recommendations = loop.run_until_complete(
            get_recommendations_for_user(normalized_user_id, {
                'user_rated_count': len(user_rated_movies),
                'genre_preferences': genre_prefs,
                'force_refresh': force_refresh,
                'user_rated_movies': {m.get('movie_id') for m in user_rated_movies if m.get('movie_id')}
            })
        )

        # Обогащаем рекомендации
        enriched = []
        for rec in recommendations[:top_n]:
            movie_id = rec.get('movie_id')
            if movie_id and recommender:
                details = recommender.get_movie_details(movie_id)
                if details:
                    details['score'] = rec.get('final_score', rec.get('score', 0))
                    details['poster'] = get_poster_filename(details.get('title', ''), details.get('year', ''))

                    # Просто используем уже готовые жанры
                    if 'genres' not in details or not details['genres']:
                        # Fallback: если нет русских жанров, пробуем сконвертировать
                        genre_val = details.get('genre', '')
                        if genre_val and pd.notna(genre_val):
                            genres_ru = []
                            genres_en = [g.strip() for g in str(genre_val).split(',') if g.strip()]
                            for genre_en in genres_en:
                                genre_ru = genre_en
                                if models_provider and models_provider.genres_df is not None:
                                    if 'title_ru' in models_provider.genres_df.columns:
                                        match = models_provider.genres_df[
                                            models_provider.genres_df['title'] == genre_en]
                                        if len(match) > 0:
                                            genre_ru = match.iloc[0]['title_ru']
                                genres_ru.append(genre_ru)
                            details['genres'] = genres_ru
                        else:
                            details['genres'] = []

                    details['recommendation_reason'] = _get_recommendation_reason(details, genre_prefs)
                    enriched.append(details)

        logger.info(f"Возвращено {len(enriched)} персонализированных рекомендаций")
        return jsonify({'recommendations': enriched})

    except Exception as e:
        logger.error(f"Ошибка получения рекомендаций: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/user/<path:user_url>/refresh', methods=['POST'])
def refresh_user_recommendations(user_url):
    """Принудительное обновление рекомендаций пользователя"""
    user_url = unquote(user_url)

    def normalize_url(url):
        url = str(url).strip()
        url = url.replace('https://www.imdb.com', '')
        url = url.replace('http://www.imdb.com', '')
        url = url.rstrip('/')
        url = url.split('?')[0]
        return url

    normalized_user_id = normalize_url(user_url)

    # Инвалидируем кэш
    cache_manager.invalidate_user_cache(normalized_user_id)

    # Очищаем сессионный кэш контекста
    if context_handler:
        context_handler.clear_session_cache(normalized_user_id)

    logger.info(f"Рекомендации для {normalized_user_id} будут пересчитаны")

    return jsonify({
        'success': True,
        'message': 'Кэш очищен, рекомендации будут пересчитаны при следующем запросе'
    })


@app.route('/api/user/<path:user_url>/session', methods=['GET'])
def get_user_session(user_url):
    """Получение информации о сессионных предпочтениях пользователя"""
    user_url = unquote(user_url)

    def normalize_url(url):
        url = str(url).strip()
        url = url.replace('https://www.imdb.com', '')
        url = url.replace('http://www.imdb.com', '')
        url = url.rstrip('/')
        url = url.split('?')[0]
        return url

    normalized_user_id = normalize_url(user_url)

    if incremental_updater and normalized_user_id in incremental_updater.session_preferences:
        session = incremental_updater.session_preferences[normalized_user_id]
        return jsonify({
            'has_recent_ratings': len(session.get('recent_ratings', [])) > 0,
            'recent_ratings_count': len(session.get('recent_ratings', [])),
            'recent_favorites_count': len(session.get('recent_favorites', set())),
            'genre_preferences': incremental_updater.get_user_genre_preferences(normalized_user_id),
            'last_update': session.get('last_update').isoformat() if session.get('last_update') else None
        })
    else:
        return jsonify({
            'has_recent_ratings': False,
            'recent_ratings_count': 0,
            'recent_favorites_count': 0,
            'genre_preferences': {},
            'message': 'Нет сессионных данных для этого пользователя'
        })


def _get_recommendation_reason(movie_details: dict, user_genre_prefs: dict) -> str:
    """Генерирует объяснение для рекомендации"""
    if not user_genre_prefs:
        return "Рекомендуем на основе популярности"

    movie_genres = movie_details.get('genres', [])
    matching_genres = []

    for genre in movie_genres:
        for user_genre in user_genre_prefs.keys():
            if user_genre.lower() in genre.lower() or genre.lower() in user_genre.lower():
                matching_genres.append(genre)
                break

    if matching_genres:
        return f"Вам нравятся фильмы жанра {matching_genres[0]}"

    return "Рекомендуем на основе ваших предпочтений"

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


@app.route('/api/cache/invalidate/<path:user_url>', methods=['POST'])
def invalidate_user_cache(user_url):
    """Инвалидация кэша для пользователя"""
    user_url = unquote(user_url)

    def normalize_url(url):
        url = str(url).strip()
        url = url.replace('https://www.imdb.com', '')
        url = url.replace('http://www.imdb.com', '')
        url = url.rstrip('/')
        url = url.split('?')[0]
        return url

    normalized_user_id = normalize_url(user_url)
    cache_manager.invalidate_user_cache(normalized_user_id)

    logger.info(f"Кэш инвалидирован для {normalized_user_id}")
    return jsonify({'success': True, 'user_id': normalized_user_id})


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


@app.route('/api/users/list', methods=['GET'])
def get_users_list():
    """Получить список всех пользователей для входа (PostgreSQL)"""
    try:
        if data_provider is None or data_provider.user_main_df is None:
            # Загружаем пользователей напрямую из БД
            async def get_users():
                if not data_pipeline.connection:
                    data_pipeline._create_connection()

                with data_pipeline.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT user_url, username FROM db.users 
                        ORDER BY username
                    """)
                    return cursor.fetchall()

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            users = loop.run_until_complete(get_users())
            loop.close()

            return jsonify({'users': users})

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


@app.route('/favorites')
def favorites_page():
    """Страница избранных фильмов"""
    if 'user_url' not in session or not session['user_url']:
        return redirect(url_for('login_page'))
    return render_template('favorites.html',
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
            url = re.sub(r'/+', '/', url)
            if url and not url.startswith('/'):
                url = f'/{url}'
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_url = normalize_url(user_url)
        watched_movies = data_provider.get_user_watched_movies(normalized_user_url)

        if not watched_movies:
            return jsonify({'movies': [], 'total': 0})

        # Обработка NaN значений
        def clean_value(value, default=''):
            if value is None:
                return default
            if isinstance(value, float):
                import math
                if math.isnan(value):
                    return default
            if pd.isna(value):
                return default
            return value

        enriched = []
        for movie in watched_movies:
            details = dict(movie)

            details['user_rating'] = clean_value(movie.get('rating'), None)
            details['user_review'] = clean_value(movie.get('review_text'), '')
            details['review_date'] = clean_value(movie.get('review_date') or movie.get('date'), '')
            details['poster'] = get_poster_filename(
                clean_value(details.get('title', '')),
                clean_value(details.get('year', ''))
            )

            # Конвертируем жанры в русские названия
            genres_ru = []
            genre_val = details.get('genre', '')
            if genre_val and pd.notna(genre_val) and genre_val != 'nan':
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
            details['genres'] = genres_ru if genres_ru else details.get('genres', [])

            # Режиссеры на русском
            directors_ru = []
            directors_val = details.get('directors_ru', '')
            if directors_val is None or (
                    isinstance(directors_val, str) and (not directors_val or directors_val == 'nan')):
                directors_val = details.get('directors', '')
            if isinstance(directors_val, str) and directors_val and directors_val != 'nan':
                directors_ru = [d.strip() for d in str(directors_val).split(',') if d.strip()]
            elif isinstance(directors_val, (list, tuple, np.ndarray)):
                directors_ru = [str(d).strip() for d in directors_val if d and str(d) != 'nan']
            details['directors_ru'] = directors_ru
            details['directors'] = directors_ru

            # Актеры на русском
            actors_ru = []
            actors_val = details.get('actors_ru', '')
            if actors_val is None or (isinstance(actors_val, str) and (not actors_val or actors_val == 'nan')):
                actors_val = details.get('actors', '')
            if isinstance(actors_val, str) and actors_val and actors_val != 'nan':
                actors_ru = [a.strip() for a in str(actors_val).split(',') if a.strip()]
            elif isinstance(actors_val, (list, tuple, np.ndarray)):
                actors_ru = [str(a).strip() for a in actors_val if a and str(a) != 'nan']
            details['actors_ru'] = actors_ru
            details['actors'] = actors_ru

            # Очищаем числовые значения
            if 'imdb_rating' in details:
                imdb_val = details['imdb_rating']
                if imdb_val is None or (isinstance(imdb_val, float) and math.isnan(imdb_val)):
                    details['imdb_rating'] = None

            # Очищаем год
            if 'year' in details:
                year_val = details['year']
                if year_val is None or (isinstance(year_val, float) and math.isnan(year_val)):
                    details['year'] = ''
                else:
                    details['year'] = str(year_val)

            enriched.append(details)

        enriched.sort(key=lambda x: x.get('review_date', ''), reverse=True)

        # Используем jsonify с обработкой NaN
        response_data = {'movies': enriched, 'total': len(enriched)}
        return app.response_class(
            response=json.dumps(response_data, default=handle_nan),
            status=200,
            mimetype='application/json'
        )

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
        user_url = unquote(user_url)
        logger.info(f"Получение статистики для пользователя: {user_url}")

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = re.sub(r'/+', '/', url)
            if url and not url.startswith('/'):
                url = f'/{url}'
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_url = normalize_url(user_url)
        watched_movies = data_provider.get_user_watched_movies(normalized_user_url)

        if not watched_movies:
            result = {
                'total_watched': 0,
                'avg_rating': 0,
                'genre_distribution': [],
                'year_distribution': [],
                'rating_distribution': []
            }
            return app.response_class(
                response=json.dumps(result, default=handle_nan),
                status=200,
                mimetype='application/json'
            )

        total_watched = len(watched_movies)

        def parse_rating(value):
            if value is None:
                return None
            if isinstance(value, float) and math.isnan(value):
                return None
            if pd.isna(value):
                return None
            try:
                if isinstance(value, str):
                    normalized = value.strip().replace(',', '.')
                    if '/' in normalized:
                        normalized = normalized.split('/')[0].strip()
                    value = normalized
                return float(value)
            except Exception:
                return None

        # Фильтруем оценки, убирая None и NaN
        ratings = []
        for m in watched_movies:
            rating = parse_rating(m.get('rating', m.get('user_rating')))
            if rating is not None:
                ratings.append(rating)

        avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else 0

        # Распределение по жанрам
        genre_counts = {}
        for movie in watched_movies:
            genre_val = movie.get('genre', '')
            if genre_val and pd.notna(genre_val) and genre_val != 'nan':
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
                    genre_counts[genre_ru] = genre_counts.get(genre_ru, 0) + 1

        genre_distribution = [{'genre': k, 'count': v} for k, v in
                              sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:10]]

        # Распределение по годам
        year_counts = {}
        for movie in watched_movies:
            year_val = movie.get('year')
            if year_val and pd.notna(year_val) and year_val != 'nan':
                try:
                    year_int = int(float(year_val))
                    if 1900 <= year_int <= 2030:
                        year_counts[year_int] = year_counts.get(year_int, 0) + 1
                except Exception:
                    pass

        year_distribution = [{'year': str(k), 'count': v} for k, v in sorted(year_counts.items())]

        # Распределение оценок
        rating_distribution = []
        for i in range(1, 11):
            count = 0
            for m in watched_movies:
                rating = parse_rating(m.get('rating', m.get('user_rating')))
                if rating is not None and int(rating) == i:
                    count += 1
            rating_distribution.append({'rating': i, 'count': count})

        result = {
            'total_watched': total_watched,
            'avg_rating': avg_rating,
            'genre_distribution': genre_distribution,
            'year_distribution': year_distribution,
            'rating_distribution': rating_distribution
        }

        return app.response_class(
            response=json.dumps(result, default=handle_nan),
            status=200,
            mimetype='application/json'
        )

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


@app.route('/api/movies/<path:movie_id>', methods=['GET'])
@app.route('/api/movie/<path:movie_id>', methods=['GET'])
def get_movie_details_api(movie_id):
    """API для получения деталей фильма"""
    if recommender is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        # Очищаем movie_id от лишних символов
        movie_id = unquote(movie_id)  # Декодируем URL
        movie_id = movie_id.strip()

        # Извлекаем ID из URL, если передан полный URL
        import re
        match = re.search(r'(tt\d+)', movie_id)
        if match:
            movie_id = match.group(1)

        logger.info(f"Получение деталей фильма: {movie_id}")

        # Проверяем, что movie_id валидный
        if not movie_id or movie_id == 'nan' or movie_id == 'undefined':
            logger.error(f"Невалидный movie_id: {movie_id}")
            return jsonify({'error': 'Неверный идентификатор фильма'}), 400

        movie = recommender.get_movie_details(movie_id)
        if movie is None:
            logger.warning(f"Фильм {movie_id} не найден")
            return jsonify({'error': f'Фильм {movie_id} не найден'}), 404

        movie['poster'] = get_poster_filename(movie.get('title', ''), movie.get('year', ''))

        # Добавляем рейтинг IMDb, если его нет
        if 'imdb_rating' not in movie or movie['imdb_rating'] is None:
            movie['imdb_rating'] = movie.get('imdb', None)

        return jsonify(movie)

    except Exception as e:
        logger.error(f"Ошибка при получении фильма {movie_id}: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/movies/<movie_id>/similar', methods=['GET'])
@app.route('/api/movie/<movie_id>/similar', methods=['GET'])
def get_similar_movies_api(movie_id):
    """API для получения похожих фильмов."""
    if recommender is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        similar_source = models_provider if models_provider is not None else recommender
        if similar_source is None:
            return jsonify({'movies': []})

        raw_similar = similar_source.get_similar_movies(movie_id, n=20)
        if not raw_similar and models_provider is not None and models_provider.movies_df is not None:
            # Fallback: если model-based similar недоступен, берем похожие по жанру/году из каталога.
            current_movie = models_provider.movies_df[models_provider.movies_df['movie_id'] == movie_id]
            if len(current_movie) > 0:
                current = current_movie.iloc[0]
                genre_value = str(current.get('genre', '') or '')
                year_value = current.get('year')

                candidates = models_provider.movies_df[models_provider.movies_df['movie_id'] != movie_id].copy()
                if genre_value:
                    candidates = candidates[
                        candidates['genre'].astype(str).str.contains(genre_value.split(',')[0].strip(), case=False, na=False)
                    ]
                if year_value is not None and pd.notna(year_value):
                    try:
                        year_num = int(float(year_value))
                        candidates = candidates[
                            pd.to_numeric(candidates['year'], errors='coerce').between(year_num - 3, year_num + 3, inclusive='both')
                        ]
                    except Exception:
                        pass

                raw_similar = []
                for _, row in candidates.head(20).iterrows():
                    raw_similar.append({
                        'movie_id': str(row.get('movie_id', '')),
                        'title': str(row.get('title', '')),
                        'year': row.get('year'),
                        'similarity': 0.75
                    })

        if not raw_similar:
            return jsonify({'movies': []})

        movies = []
        for item in raw_similar:
            similar_id = str(item.get('movie_id', '')).strip()
            if not similar_id:
                continue

            details = recommender.get_movie_details(similar_id) or {}
            title = details.get('title') or item.get('title') or ''
            year = details.get('year') if details.get('year') is not None else item.get('year')

            movies.append({
                'movie_id': similar_id,
                'title': title,
                'title_ru': details.get('title_ru', title),
                'year': str(year) if year not in (None, '', 'nan') else '',
                'poster': get_poster_filename(title, year),
                'similarity': float(item.get('similarity', 0))
            })

        return jsonify({'movies': movies})

    except Exception as e:
        logger.error(f"Ошибка получения похожих фильмов для {movie_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/search')
def search_movies_api():
    """API для поиска фильмов"""
    query = request.args.get('q', '')
    limit = int(request.args.get('limit', 20))

    # Декодируем query, если пришло в URL-encoded формате
    if query:
        query = unquote(query)  # Декодируем русские символы
        query = query.strip()

    logger.info(f"Поиск по запросу: '{query}'")

    if models_provider is None or models_provider.movies_df is None:
        return jsonify({'error': 'Система не инициализирована'}), 500

    try:
        movies_df = models_provider.movies_df.copy()

        # Создаем маску для поиска по русским и английским названиям
        mask = pd.Series([False] * len(movies_df))

        if 'title_ru' in movies_df.columns:
            mask = mask | movies_df['title_ru'].str.contains(query, case=False, na=False, regex=False)

        if 'title' in movies_df.columns:
            mask = mask | movies_df['title'].str.contains(query, case=False, na=False, regex=False)

        # Также ищем по оригинальному названию (если есть)
        if 'original_title' in movies_df.columns:
            mask = mask | movies_df['original_title'].str.contains(query, case=False, na=False, regex=False)

        result_df = movies_df[mask].head(limit)

        # Логируем количество найденных результатов
        logger.info(f"Найдено {len(result_df)} фильмов по запросу '{query}'")

        movies = []
        for _, row in result_df.iterrows():
            movie_id = row.get('movie_id')
            if pd.isna(movie_id) or movie_id is None:
                continue

            # Убеждаемся, что movie_id - строка
            movie_id = str(movie_id).strip()
            if not movie_id or movie_id == 'nan':
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

            # Получаем рейтинг
            imdb_rating = None
            if 'imdb' in row and pd.notna(row['imdb']):
                try:
                    imdb_rating = float(row['imdb'])
                except:
                    pass

            movies.append({
                'movie_id': movie_id,  # Убеждаемся, что это строка с ID
                'title': str(row.get('title', '')),
                'title_ru': str(row.get('title_ru', row.get('title', ''))),
                'year': str(row.get('year', '')) if pd.notna(row.get('year')) else None,
                'poster': get_poster_filename(row.get('title', ''), row.get('year', '')),
                'genres': genres_ru,
                'imdb_rating': imdb_rating,
            })

        return jsonify({'movies': movies})

    except Exception as e:
        logger.error(f"Ошибка при поиске: {e}")
        import traceback
        traceback.print_exc()
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


# ==================== ПОЛЬЗОВАТЕЛИ (POSTGRESQL) ====================


@app.route('/api/user/create', methods=['POST'])
def create_user():
    """Создание нового пользователя в PostgreSQL"""
    try:
        data = request.json
        username = data.get('username', '').strip()

        if not username:
            return jsonify({'error': 'Имя пользователя обязательно'}), 400

        # Создаем пользователя через data_pipeline
        async def create():
            return await data_pipeline.create_user(username)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        new_user = loop.run_until_complete(create())
        loop.close()

        if new_user:
            # Обновляем data_provider
            if data_provider and hasattr(data_provider, 'user_main_df'):
                new_df = pd.DataFrame([new_user])
                data_provider.user_main_df = pd.concat([data_provider.user_main_df, new_df], ignore_index=True)

            return jsonify({
                'success': True,
                'user_url': new_user['user_url'],
                'username': new_user['username'],
                'message': 'Пользователь успешно создан'
            })
        else:
            return jsonify({'error': 'Не удалось создать пользователя'}), 500

    except Exception as e:
        logger.error(f"Ошибка создания пользователя: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/movie/<movie_id>/rate', methods=['POST'])
def rate_movie(movie_id):
    """Оценка фильма пользователем с немедленным обновлением рекомендаций"""
    try:
        data = request.json
        user_id = data.get('user_id') or session.get('user_url')
        rating = data.get('rating')
        review_text = data.get('review_text', '').strip()

        if not user_id:
            return jsonify({'error': 'Пользователь не авторизован'}), 401

        if rating is None:
            return jsonify({'error': 'Оценка обязательна'}), 400

        try:
            rating = float(rating)
            if rating < 1 or rating > 10:
                return jsonify({'error': 'Оценка должна быть от 1 до 10'}), 400
        except ValueError:
            return jsonify({'error': 'Неверный формат оценки'}), 400

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = re.sub(r'/+', '/', url)
            if url and not url.startswith('/'):
                url = f'/{url}'
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_id = normalize_url(user_id)

        # Асинхронное сохранение в БД
        async def save_rating():
            return await data_pipeline.save_user_rating(
                normalized_user_id, movie_id, rating, review_text
            )

        # Используем новый event loop для каждой операции
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success = loop.run_until_complete(save_rating())
        finally:
            loop.close()

        if success:
            # 1. Обновляем сессионные предпочтения (синхронно)
            if incremental_updater:
                incremental_updater.process_user_action(
                    normalized_user_id,
                    'rating',
                    movie_id,
                    {'rating': rating}
                )

            # 2. Обновляем контекст пользователя (синхронно)
            if context_handler:
                context_handler.update_user_preference(
                    normalized_user_id, 'rating', movie_id, rating
                )

            # 3. Инвалидируем кэш
            cache_manager.invalidate_user_cache(normalized_user_id)

            # 4. Обновляем онлайн-данные в отдельном потоке (не блокируем)
            def update_online_data():
                """Запускаем обновление в отдельном event loop"""
                try:
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    new_loop.run_until_complete(
                        online_updater.add_user_rating_online(
                            normalized_user_id, movie_id, rating, review_text
                        )
                    )
                    new_loop.close()
                except Exception as e:
                    logger.warning(f"Фоновое обновление не удалось: {e}")

            # Запускаем в отдельном потоке, чтобы не блокировать ответ
            import threading
            threading.Thread(target=update_online_data, daemon=True).start()

            # 5. Логируем обратную связь (синхронно в отдельном loop)
            if feedback_logger:
                try:
                    fb_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(fb_loop)
                    fb_loop.run_until_complete(
                        feedback_logger.log_rating(normalized_user_id, movie_id, rating)
                    )
                    fb_loop.close()
                except Exception as feedback_error:
                    logger.warning(f"Не удалось записать feedback log: {feedback_error}")

            logger.info(
                f"Пользователь {normalized_user_id} оценил фильм {movie_id} на {rating} - рекомендации обновлены")

            return jsonify({
                'success': True,
                'message': 'Оценка сохранена, рекомендации обновлены'
            })
        else:
            return jsonify({'error': 'Не удалось сохранить оценку'}), 500

    except Exception as e:
        logger.error(f"Ошибка сохранения оценки: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/movie/<movie_id>/watched', methods=['POST'])
def add_to_watched(movie_id):
    """Добавление фильма в просмотренное (PostgreSQL)"""
    try:
        data = request.json
        user_id = data.get('user_id') or session.get('user_url')

        if not user_id:
            return jsonify({'error': 'Пользователь не авторизован'}), 401

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_id = normalize_url(user_id)

        async def add_watched():
            return await data_pipeline.add_to_watched(normalized_user_id, movie_id)

        # Создаем новый event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success = loop.run_until_complete(add_watched())
        finally:
            loop.close()

        if success:
            # Обновляем сессионные предпочтения
            if incremental_updater:
                incremental_updater.process_user_action(
                    normalized_user_id, 'watched', movie_id
                )

            # Инвалидируем кэш
            cache_manager.invalidate_user_cache(normalized_user_id)

            logger.info(f"Фильм {movie_id} добавлен в просмотренные пользователем {normalized_user_id}")
            return jsonify({'success': True, 'message': 'Фильм добавлен в просмотренные'})
        else:
            return jsonify({'error': 'Не удалось добавить фильм'}), 500

    except Exception as e:
        logger.error(f"Ошибка добавления в просмотренное: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/movie/<movie_id>/favorite', methods=['POST', 'DELETE'])
def toggle_favorite(movie_id):
    """Добавление/удаление фильма в избранное (PostgreSQL)"""
    try:
        data = request.json
        user_id = data.get('user_id') or session.get('user_url')

        if not user_id:
            return jsonify({'error': 'Пользователь не авторизован'}), 401

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_id = normalize_url(user_id)

        async def toggle():
            if request.method == 'POST':
                return await data_pipeline.add_to_favorites(normalized_user_id, movie_id)
            else:
                return await data_pipeline.remove_from_favorites(normalized_user_id, movie_id)

        # Создаем новый event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success = loop.run_until_complete(toggle())
        finally:
            loop.close()

        if success:
            action_type = 'favorite' if request.method == 'POST' else 'unfavorite'

            # Обновляем сессионные предпочтения
            if incremental_updater:
                incremental_updater.process_user_action(
                    normalized_user_id, action_type, movie_id
                )

            # Инвалидируем кэш
            cache_manager.invalidate_user_cache(normalized_user_id)

            action = "добавлен в" if request.method == 'POST' else "удален из"
            logger.info(f"Фильм {movie_id} {action} избранного пользователем {normalized_user_id}")

            return jsonify({
                'success': True,
                'message': f'Фильм {action} избранного'
            })
        else:
            return jsonify({'error': 'Не удалось выполнить действие'}), 500

    except Exception as e:
        logger.error(f"Ошибка работы с избранным: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/user/<path:user_url>/favorites', methods=['GET'])
def get_user_favorites(user_url):
    """Получение списка избранных фильмов пользователя (PostgreSQL)"""
    try:
        user_url = unquote(user_url)

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_id = normalize_url(user_url)

        async def get_favorites():
            return await data_pipeline.get_user_favorites(normalized_user_id)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        favorite_movie_ids = loop.run_until_complete(get_favorites())
        loop.close()

        # Обогащаем данными о фильмах
        enriched = []
        for movie_id in favorite_movie_ids:
            if recommender:
                details = recommender.get_movie_details(movie_id)
                if details:
                    details['poster'] = get_poster_filename(details.get('title', ''), details.get('year', ''))
                    enriched.append(details)

        return jsonify({'movies': enriched, 'total': len(enriched)})

    except Exception as e:
        logger.error(f"Ошибка получения избранного: {e}")
        return jsonify({'error': str(e)}), 500


def _update_user_rating_stats(user_id):
    """Обновление статистики пользователя после оценки"""
    try:
        reviews_file = 'data/reviews.csv'
        if not os.path.exists(reviews_file):
            return

        import csv
        user_ratings = []

        with open(reviews_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('user_url') == user_id:
                    rating = row.get('rating')
                    if rating:
                        try:
                            user_ratings.append(float(rating))
                        except:
                            pass

        # Обновляем users.csv
        users_file = 'data/users.csv'
        if os.path.exists(users_file):
            rows = []
            with open(users_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    if row.get('user_url') == user_id:
                        row['ratings_count'] = len(user_ratings)
                    rows.append(row)

            with open(users_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

    except Exception as e:
        logger.error(f"Ошибка обновления статистики: {e}")


@app.route('/api/movie/<movie_id>/user-rating', methods=['GET'])
def get_user_movie_rating(movie_id):
    """Получение оценки пользователя для фильма (PostgreSQL)"""
    try:
        user_id = session.get('user_url')
        if not user_id:
            return jsonify({'rating': None})

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_id = normalize_url(user_id)

        async def get_rating():
            return await data_pipeline.get_user_rating(normalized_user_id, movie_id)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        rating_data = loop.run_until_complete(get_rating())
        loop.close()

        if rating_data:
            return jsonify({
                'rating': rating_data.get('rating'),
                'review_text': rating_data.get('review_text', '')
            })

        return jsonify({'rating': None, 'review_text': None})

    except Exception as e:
        logger.error(f"Ошибка получения оценки: {e}")
        return jsonify({'rating': None})


@app.route('/api/movie/<movie_id>/check-watched', methods=['GET'])
def check_movie_watched(movie_id):
    """Проверка, добавлен ли фильм в просмотренные (PostgreSQL)"""
    try:
        user_id = session.get('user_url')
        if not user_id:
            return jsonify({'watched': False})

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_id = normalize_url(user_id)

        async def check_watched():
            return await data_pipeline.check_movie_watched(normalized_user_id, movie_id)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        is_watched = loop.run_until_complete(check_watched())
        loop.close()

        return jsonify({'watched': is_watched})

    except Exception as e:
        logger.error(f"Ошибка проверки просмотра: {e}")
        return jsonify({'watched': False})


@app.route('/api/movie/<movie_id>/check-favorite', methods=['GET'])
def check_movie_favorite(movie_id):
    """Проверка, добавлен ли фильм в избранное (PostgreSQL)"""
    try:
        user_id = session.get('user_url')
        if not user_id:
            return jsonify({'favorite': False})

        def normalize_url(url):
            url = str(url).strip()
            url = url.replace('https://www.imdb.com', '')
            url = url.replace('http://www.imdb.com', '')
            url = url.rstrip('/')
            url = url.split('?')[0]
            return url

        normalized_user_id = normalize_url(user_id)

        async def check_favorite():
            return await data_pipeline.check_movie_favorite(normalized_user_id, movie_id)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        is_favorite = loop.run_until_complete(check_favorite())
        loop.close()

        return jsonify({'favorite': is_favorite})

    except Exception as e:
        logger.error(f"Ошибка проверки избранного: {e}")
        return jsonify({'favorite': False})


@app.route('/api/metrics/evaluate', methods=['POST'])
def evaluate_recommendations():
    """Оценка качества рекомендаций по Precision@K, Recall@K, NDCG@K"""
    data = request.json
    user_id = data.get('user_id')
    k = data.get('k', 10)

    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400

    # Получаем рекомендации
    if loop is None or loop.is_closed():
        current_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(current_loop)
        recommendations = current_loop.run_until_complete(
            get_recommendations_for_user(user_id, {})
        )
        current_loop.close()
    else:
        recommendations = loop.run_until_complete(
            get_recommendations_for_user(user_id, {})
        )

    # Извлекаем ID рекомендованных фильмов
    recommended_ids = [rec.get('movie_id') for rec in recommendations[:k] if rec.get('movie_id')]

    # Получаем реальные оценки пользователя
    actual_ratings = data_provider.get_user_watched_movies(user_id)

    # Релевантные фильмы (оценка >= 7 из 10)
    relevant_ids = [m['movie_id'] for m in actual_ratings if m.get('rating', 0) >= 7]

    # Создаем словарь с реальными оценками для NDCG
    relevance_scores = {}
    for movie in actual_ratings:
        movie_id = movie.get('movie_id')
        rating = movie.get('rating', 0)
        if rating >= 7:
            # Нормализуем оценку для NDCG: 7->0.7, 8->0.8, 9->0.9, 10->1.0
            relevance_scores[movie_id] = (rating - 6) / 4  # 7->0.25, 8->0.5, 9->0.75, 10->1.0
        else:
            relevance_scores[movie_id] = 0

    # Вычисляем метрики
    metrics = {
        'user_id': user_id,
        'k': k,
        'total_recommendations': len(recommendations),
        'total_relevant': len(relevant_ids),
        'precision@k': RecommendationMetrics.precision_at_k(recommended_ids, relevant_ids, k),
        'recall@k': RecommendationMetrics.recall_at_k(recommended_ids, relevant_ids, k),
        'ndcg@k': RecommendationMetrics.ndcg_at_k(recommended_ids, relevant_ids, relevance_scores, k),
        'map@k': RecommendationMetrics.map_at_k(recommended_ids, relevant_ids, k),
        'hit_rate@k': RecommendationMetrics.hit_rate_at_k(recommended_ids, relevant_ids, k)
    }

    # Добавляем дополнительную статистику
    if len(recommended_ids) > 0:
        hits = [m for m in recommended_ids[:k] if m in set(relevant_ids)]
        metrics['hits'] = hits
        metrics['hits_count'] = len(hits)

    return jsonify(metrics)


@app.route('/api/metrics/evaluate/batch', methods=['POST'])
def evaluate_batch_metrics():
    """Пакетная оценка качества для множества пользователей"""
    data = request.json
    user_ids = data.get('user_ids', [])
    k = data.get('k', 10)
    sample_size = data.get('sample_size', 100)

    if not user_ids:
        # Если список не указан, берем случайных пользователей
        if data_provider and data_provider.user_main_df is not None:
            user_ids = data_provider.user_main_df['user_url'].head(sample_size).tolist()
        else:
            return jsonify({'error': 'No users available'}), 400

    all_metrics = {
        'precision': [],
        'recall': [],
        'ndcg': [],
        'map': [],
        'hit_rate': []
    }

    for user_id in user_ids[:sample_size]:
        try:
            # Получаем рекомендации
            if loop is None or loop.is_closed():
                current_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(current_loop)
                recommendations = current_loop.run_until_complete(
                    get_recommendations_for_user(user_id, {})
                )
                current_loop.close()
            else:
                recommendations = loop.run_until_complete(
                    get_recommendations_for_user(user_id, {})
                )

            recommended_ids = [rec.get('movie_id') for rec in recommendations[:k] if rec.get('movie_id')]

            # Получаем реальные оценки
            actual_ratings = data_provider.get_user_watched_movies(user_id)
            relevant_ids = [m['movie_id'] for m in actual_ratings if m.get('rating', 0) >= 7]

            # Создаем relevance scores
            relevance_scores = {}
            for movie in actual_ratings:
                movie_id = movie.get('movie_id')
                rating = movie.get('rating', 0)
                if rating >= 7:
                    relevance_scores[movie_id] = (rating - 6) / 4
                else:
                    relevance_scores[movie_id] = 0

            # Вычисляем метрики
            if relevant_ids:  # Только если у пользователя есть оценки
                all_metrics['precision'].append(
                    RecommendationMetrics.precision_at_k(recommended_ids, relevant_ids, k)
                )
                all_metrics['recall'].append(
                    RecommendationMetrics.recall_at_k(recommended_ids, relevant_ids, k)
                )
                all_metrics['ndcg'].append(
                    RecommendationMetrics.ndcg_at_k(recommended_ids, relevant_ids, relevance_scores, k)
                )
                all_metrics['map'].append(
                    RecommendationMetrics.map_at_k(recommended_ids, relevant_ids, k)
                )
                all_metrics['hit_rate'].append(
                    RecommendationMetrics.hit_rate_at_k(recommended_ids, relevant_ids, k)
                )
        except Exception as e:
            logger.error(f"Error evaluating user {user_id}: {e}")
            continue

    # Агрегируем результаты
    results = {
        'total_users_evaluated': len(all_metrics['precision']),
        'k': k,
        'average_metrics': {
            'precision@k': np.mean(all_metrics['precision']) if all_metrics['precision'] else 0,
            'recall@k': np.mean(all_metrics['recall']) if all_metrics['recall'] else 0,
            'ndcg@k': np.mean(all_metrics['ndcg']) if all_metrics['ndcg'] else 0,
            'map@k': np.mean(all_metrics['map']) if all_metrics['map'] else 0,
            'hit_rate@k': np.mean(all_metrics['hit_rate']) if all_metrics['hit_rate'] else 0
        },
        'std_metrics': {
            'precision@k': np.std(all_metrics['precision']) if all_metrics['precision'] else 0,
            'recall@k': np.std(all_metrics['recall']) if all_metrics['recall'] else 0,
            'ndcg@k': np.std(all_metrics['ndcg']) if all_metrics['ndcg'] else 0,
            'map@k': np.std(all_metrics['map']) if all_metrics['map'] else 0,
            'hit_rate@k': np.std(all_metrics['hit_rate']) if all_metrics['hit_rate'] else 0
        },
        'percentiles': {
            'precision@k': {
                'p25': np.percentile(all_metrics['precision'], 25) if all_metrics['precision'] else 0,
                'p50': np.percentile(all_metrics['precision'], 50) if all_metrics['precision'] else 0,
                'p75': np.percentile(all_metrics['precision'], 75) if all_metrics['precision'] else 0
            },
            'recall@k': {
                'p25': np.percentile(all_metrics['recall'], 25) if all_metrics['recall'] else 0,
                'p50': np.percentile(all_metrics['recall'], 50) if all_metrics['recall'] else 0,
                'p75': np.percentile(all_metrics['recall'], 75) if all_metrics['recall'] else 0
            }
        }
    }

    return jsonify(results)


@app.route('/api/metrics/holdout', methods=['POST'])
def evaluate_holdout():
    """
    Оценка качества на holdout выборке (последние 20% оценок пользователя)
    """
    data = request.json
    user_id = data.get('user_id')
    test_ratio = data.get('test_ratio', 0.2)  # 20% на тест
    k = data.get('k', 10)

    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400

    # Получаем все оценки пользователя
    actual_ratings = data_provider.get_user_watched_movies(user_id)

    if len(actual_ratings) < 5:
        return jsonify({'error': 'Not enough ratings for evaluation (need at least 5)'}), 400

    # Сортируем по дате
    sorted_ratings = sorted(actual_ratings, key=lambda x: x.get('date', ''), reverse=True)

    # Разделяем на train (первые 80%) и test (последние 20%)
    split_idx = int(len(sorted_ratings) * (1 - test_ratio))
    train_ratings = sorted_ratings[:split_idx]
    test_ratings = sorted_ratings[split_idx:]

    # Используем train для получения рекомендаций (через историю)
    # Для этого нужно временно модифицировать контекст

    # Получаем рекомендации на основе train
    context = {
        'user_rated_movies': [m['movie_id'] for m in train_ratings],
        'user_ratings': {m['movie_id']: m.get('rating', 0) for m in train_ratings}
    }

    if loop is None or loop.is_closed():
        current_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(current_loop)
        recommendations = current_loop.run_until_complete(
            get_recommendations_for_user(user_id, context)
        )
        current_loop.close()
    else:
        recommendations = loop.run_until_complete(
            get_recommendations_for_user(user_id, context)
        )

    recommended_ids = [rec.get('movie_id') for rec in recommendations[:k] if rec.get('movie_id')]

    # Релевантные из test (оценка >= 7)
    relevant_ids = [m['movie_id'] for m in test_ratings if m.get('rating', 0) >= 7]

    # Relevance scores для NDCG
    relevance_scores = {}
    for movie in test_ratings:
        movie_id = movie.get('movie_id')
        rating = movie.get('rating', 0)
        if rating >= 7:
            relevance_scores[movie_id] = (rating - 6) / 4
        else:
            relevance_scores[movie_id] = 0

    # Вычисляем метрики
    metrics = {
        'user_id': user_id,
        'train_size': len(train_ratings),
        'test_size': len(test_ratings),
        'test_ratio': test_ratio,
        'k': k,
        'recommendations': recommended_ids[:k],
        'precision@k': RecommendationMetrics.precision_at_k(recommended_ids, relevant_ids, k),
        'recall@k': RecommendationMetrics.recall_at_k(recommended_ids, relevant_ids, k),
        'ndcg@k': RecommendationMetrics.ndcg_at_k(recommended_ids, relevant_ids, relevance_scores, k),
        'map@k': RecommendationMetrics.map_at_k(recommended_ids, relevant_ids, k),
        'hit_rate@k': RecommendationMetrics.hit_rate_at_k(recommended_ids, relevant_ids, k)
    }

    return jsonify(metrics)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)