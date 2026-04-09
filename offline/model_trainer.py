import numpy as np
from scipy.sparse import csr_matrix, load_npz, save_npz
from sklearn.decomposition import TruncatedSVD, NMF
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors
from implicit.als import AlternatingLeastSquares
from implicit.nearest_neighbours import bm25_weight
import pickle
import logging
import asyncio
from datetime import datetime
from typing import Dict, Optional, Tuple
import os

logger = logging.getLogger(__name__)


class ModelTrainer:
    """Модуль обучения и валидации моделей"""

    def __init__(self, models_path: str):
        self.models_path = models_path
        os.makedirs(models_path, exist_ok=True)

        # Модели
        self.svd_model = None
        self.nmf_model = None
        self.als_model = None
        self.rating_predictor = None
        self.ranking_model = None
        self.nn_model = None

        # Факторы
        self.user_factors = None
        self.item_factors = None
        self.user_factors_nmf = None
        self.item_factors_nmf = None

    async def build_svd_model(self, user_item_matrix: csr_matrix, n_components: int = None) -> Dict:
        """Построение SVD модели"""
        if user_item_matrix is None:
            return None

        logger.info("Построение SVD модели...")

        n_components = n_components or min(100, min(user_item_matrix.shape) - 1)
        n_components = max(20, n_components)

        self.svd_model = TruncatedSVD(
            n_components=n_components,
            random_state=42,
            n_iter=7
        )

        self.item_factors = self.svd_model.fit_transform(user_item_matrix.T)
        self.user_factors = user_item_matrix @ self.item_factors

        explained_variance = self.svd_model.explained_variance_ratio_.sum()

        logger.info(f"SVD завершена: {n_components} компонент, дисперсия: {explained_variance:.4f}")

        return {
            'model': self.svd_model,
            'user_factors': self.user_factors,
            'item_factors': self.item_factors,
            'explained_variance': explained_variance
        }

    async def build_nmf_model(self, user_item_matrix: csr_matrix, n_components: int = None) -> Optional[Dict]:
        """Построение NMF модели"""
        if user_item_matrix is None:
            return None

        logger.info("Построение NMF модели...")

        n_components = n_components or min(50, min(user_item_matrix.shape) - 1)
        n_components = max(15, n_components)

        self.nmf_model = NMF(
            n_components=n_components,
            random_state=42,
            init='random',
            max_iter=200
        )

        self.item_factors_nmf = self.nmf_model.fit_transform(user_item_matrix.T)
        self.user_factors_nmf = user_item_matrix @ self.item_factors_nmf

        logger.info(f"NMF завершена: {n_components} компонент, ошибка: {self.nmf_model.reconstruction_err_:.4f}")

        return {
            'model': self.nmf_model,
            'user_factors': self.user_factors_nmf,
            'item_factors': self.item_factors_nmf,
            'reconstruction_error': self.nmf_model.reconstruction_err_
        }

    async def build_als_model(self, user_item_matrix: csr_matrix, factors: int = 50) -> Optional[Dict]:
        """Построение ALS модели для implicit feedback"""
        if user_item_matrix is None:
            return None

        logger.info("Построение ALS модели...")

        # BM25 взвешивание
        weighted_matrix = bm25_weight(user_item_matrix.T)

        self.als_model = AlternatingLeastSquares(
            factors=factors,
            regularization=0.1,
            iterations=15,
            random_state=42,
            use_gpu=False
        )

        self.als_model.fit(weighted_matrix)

        logger.info(f"ALS модель обучена с {factors} факторами")

        return {
            'model': self.als_model,
            'factors': factors
        }

    async def build_rating_predictor(self, user_item_matrix: csr_matrix,
                                     user_factors: np.ndarray,
                                     item_factors: np.ndarray,
                                     user_factors_nmf: np.ndarray = None,
                                     item_factors_nmf: np.ndarray = None,
                                     popularity_scores: np.ndarray = None,
                                     recency_scores: np.ndarray = None) -> Optional[Dict]:
        """Построение модели предсказания оценок"""
        if user_item_matrix is None or user_factors is None:
            return None

        logger.info("Построение модели предсказания оценок...")

        X_train, y_train = [], []
        rows, cols = user_item_matrix.nonzero()

        # Сэмплирование для ускорения
        sample_size = min(50000, len(rows))
        indices = np.random.choice(len(rows), sample_size, replace=False) if len(rows) > sample_size else range(
            len(rows))

        # Проверяем наличие NMF факторов
        has_nmf = user_factors_nmf is not None and item_factors_nmf is not None

        for i in indices:
            user_idx = rows[i]
            movie_idx = cols[i]
            rating = user_item_matrix[user_idx, movie_idx]

            if rating > 0:
                # SVD признаки
                svd_features = np.concatenate([
                    user_factors[user_idx][:20],
                    item_factors[movie_idx][:20]
                ])

                # NMF признаки
                if has_nmf:
                    nmf_features = np.concatenate([
                        user_factors_nmf[user_idx][:15],
                        item_factors_nmf[movie_idx][:15]
                    ])
                    # Объединяем SVD и NMF
                    combined_features = np.concatenate([svd_features, nmf_features])
                else:
                    combined_features = svd_features

                # Добавляем дополнительные признаки
                extra_features = []
                if popularity_scores is not None and movie_idx < len(popularity_scores):
                    extra_features.append(popularity_scores[movie_idx])
                if recency_scores is not None and movie_idx < len(recency_scores):
                    extra_features.append(recency_scores[movie_idx])

                # Финальный вектор признаков
                if extra_features:
                    features = np.concatenate([combined_features, extra_features])
                else:
                    features = combined_features

                X_train.append(features)
                y_train.append(rating)

        if len(X_train) < 100:
            logger.warning(f"Недостаточно данных для обучения: {len(X_train)}")
            return None

        X_train = np.array(X_train)
        y_train = np.array(y_train)

        # Обработка пропусков
        nan_mask = np.isnan(X_train).any(axis=1)
        if nan_mask.any():
            logger.info(f"Удаляем {nan_mask.sum()} строк с NaN")
            X_train = X_train[~nan_mask]
            y_train = y_train[~nan_mask]

        # Если после удаления NaN осталось мало данных
        if len(X_train) < 100:
            logger.warning(f"После удаления NaN осталось {len(X_train)} примеров")
            return None

        self.rating_predictor = HistGradientBoostingRegressor(
            max_iter=200,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
            early_stopping=True,
            validation_fraction=0.1,
            n_iter_no_change=10
        )

        self.rating_predictor.fit(X_train, y_train)
        score = self.rating_predictor.score(X_train, y_train)

        logger.info(f"Модель предсказания обучена на {len(X_train)} примерах, R²: {score:.4f}")

        return {
            'model': self.rating_predictor,
            'train_size': len(X_train),
            'r2_score': score
        }

    async def build_ranking_model(self, user_item_matrix: csr_matrix,
                                  popularity_scores: np.ndarray,
                                  recency_scores: np.ndarray) -> Optional[Dict]:
        """Построение модели ранжирования"""
        if user_item_matrix is None:
            return None

        logger.info("Построение модели ранжирования...")

        X_train, y_train = [], []
        rows, cols = user_item_matrix.nonzero()

        sample_size = min(20000, len(rows))
        if len(rows) > sample_size:
            indices = np.random.choice(len(rows), sample_size, replace=False)
        else:
            indices = range(len(rows))

        for i in indices:
            user_idx = rows[i]
            movie_idx = cols[i]
            rating = user_item_matrix[user_idx, movie_idx]

            if rating > 0:
                features = np.array([
                    float(rating),
                    float(user_item_matrix[user_idx].nnz),
                    float(user_item_matrix[:, movie_idx].nnz),
                    float(popularity_scores[movie_idx]) if popularity_scores is not None and movie_idx < len(
                        popularity_scores) else 0.0,
                    float(recency_scores[movie_idx]) if recency_scores is not None and movie_idx < len(
                        recency_scores) else 0.0
                ])

                X_train.append(features)
                y_train.append(rating)

        if len(X_train) < 100:
            logger.warning(f"Недостаточно данных для ранжирования: {len(X_train)}")
            return None

        X_train = np.array(X_train, dtype=np.float32)
        y_train = np.array(y_train, dtype=np.float32)

        # Обработка пропусков
        nan_mask = np.isnan(X_train).any(axis=1)
        if nan_mask.any():
            X_train = X_train[~nan_mask]
            y_train = y_train[~nan_mask]

        if len(X_train) < 100:
            return None

        self.ranking_model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )

        self.ranking_model.fit(X_train, y_train)

        logger.info(f"Модель ранжирования обучена на {len(X_train)} примерах")

        return {
            'model': self.ranking_model,
            'train_size': len(X_train)
        }

    async def build_similarity_index(self, combined_features: csr_matrix) -> Optional[Dict]:
        """Построение индекса для быстрого поиска похожих фильмов"""
        if combined_features is None:
            return None

        logger.info("Построение индекса схожести...")

        self.nn_model = NearestNeighbors(
            n_neighbors=100,
            metric='cosine',
            n_jobs=-1
        )

        self.nn_model.fit(combined_features)

        logger.info(f"Индекс схожести построен для {combined_features.shape[0]} объектов")

        return {
            'model': self.nn_model,
            'n_samples': combined_features.shape[0]
        }

    async def train_all_models(self, data: Dict) -> Dict:
        """Обучение всех моделей"""
        logger.info("Начало обучения всех моделей...")

        results = {}

        user_item_matrix = data.get('user_item_matrix')
        combined_features = data.get('combined_features')
        popularity_scores = data.get('popularity_scores')
        recency_scores = data.get('recency_scores')

        # SVD
        if user_item_matrix is not None:
            results['svd'] = await self.build_svd_model(user_item_matrix)
            results['nmf'] = await self.build_nmf_model(user_item_matrix)
            results['als'] = await self.build_als_model(user_item_matrix)

            # Модель предсказания
            if self.user_factors is not None and self.item_factors is not None:
                results['rating_predictor'] = await self.build_rating_predictor(
                    user_item_matrix,
                    self.user_factors,
                    self.item_factors,
                    self.user_factors_nmf,
                    self.item_factors_nmf,
                    popularity_scores,
                    recency_scores
                )

            # Модель ранжирования
            results['ranking_model'] = await self.build_ranking_model(
                user_item_matrix,
                popularity_scores,
                recency_scores
            )

        # Индекс схожести
        if combined_features is not None:
            results['similarity_index'] = await self.build_similarity_index(combined_features)

        logger.info("Обучение моделей завершено")

        return results

    def save_models(self):
        """Сохранение моделей"""
        logger.info("Сохранение моделей...")

        if self.svd_model is not None:
            with open(f'{self.models_path}svd_model.pkl', 'wb') as f:
                pickle.dump(self.svd_model, f)
            np.save(f'{self.models_path}user_factors.npy', self.user_factors)
            np.save(f'{self.models_path}item_factors.npy', self.item_factors)

        if self.nmf_model is not None:
            with open(f'{self.models_path}nmf_model.pkl', 'wb') as f:
                pickle.dump(self.nmf_model, f)
            np.save(f'{self.models_path}user_factors_nmf.npy', self.user_factors_nmf)
            np.save(f'{self.models_path}item_factors_nmf.npy', self.item_factors_nmf)

        if self.als_model is not None:
            with open(f'{self.models_path}als_model.pkl', 'wb') as f:
                pickle.dump(self.als_model, f)

        if self.rating_predictor is not None:
            with open(f'{self.models_path}rating_predictor.pkl', 'wb') as f:
                pickle.dump(self.rating_predictor, f)

        if self.ranking_model is not None:
            with open(f'{self.models_path}ranking_model.pkl', 'wb') as f:
                pickle.dump(self.ranking_model, f)

        if self.nn_model is not None:
            with open(f'{self.models_path}nn_model.pkl', 'wb') as f:
                pickle.dump(self.nn_model, f)

        logger.info("Модели сохранены")

    def load_models(self) -> bool:
        """Загрузка сохраненных моделей"""
        try:
            import os

            # Проверка существования файлов
            required_files = ['svd_model.pkl', 'user_factors.npy', 'item_factors.npy']
            for file in required_files:
                if not os.path.exists(f'{self.models_path}{file}'):
                    return False

            # Загрузка SVD
            with open(f'{self.models_path}svd_model.pkl', 'rb') as f:
                self.svd_model = pickle.load(f)
            self.user_factors = np.load(f'{self.models_path}user_factors.npy')
            self.item_factors = np.load(f'{self.models_path}item_factors.npy')

            # Загрузка NMF
            if os.path.exists(f'{self.models_path}nmf_model.pkl'):
                with open(f'{self.models_path}nmf_model.pkl', 'rb') as f:
                    self.nmf_model = pickle.load(f)
                self.user_factors_nmf = np.load(f'{self.models_path}user_factors_nmf.npy')
                self.item_factors_nmf = np.load(f'{self.models_path}item_factors_nmf.npy')

            # Загрузка ALS
            if os.path.exists(f'{self.models_path}als_model.pkl'):
                with open(f'{self.models_path}als_model.pkl', 'rb') as f:
                    self.als_model = pickle.load(f)

            # Загрузка моделей предсказания
            if os.path.exists(f'{self.models_path}rating_predictor.pkl'):
                with open(f'{self.models_path}rating_predictor.pkl', 'rb') as f:
                    self.rating_predictor = pickle.load(f)

            if os.path.exists(f'{self.models_path}ranking_model.pkl'):
                with open(f'{self.models_path}ranking_model.pkl', 'rb') as f:
                    self.ranking_model = pickle.load(f)

            # Загрузка индекса схожести
            if os.path.exists(f'{self.models_path}nn_model.pkl'):
                with open(f'{self.models_path}nn_model.pkl', 'rb') as f:
                    self.nn_model = pickle.load(f)

            logger.info("Модели загружены")
            return True

        except Exception as e:
            logger.error(f"Ошибка загрузки моделей: {e}")
            return False
