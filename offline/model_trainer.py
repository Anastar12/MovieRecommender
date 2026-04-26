import hashlib
import numpy as np
from scipy.sparse import csr_matrix, load_npz, save_npz
from sklearn.decomposition import TruncatedSVD, NMF
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import mean_squared_error
from sklearn.neighbors import NearestNeighbors
from implicit.als import AlternatingLeastSquares
from implicit.nearest_neighbours import bm25_weight
from sklearn.model_selection import train_test_split
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

        # Система версионирования
        from offline.model_versioning import ModelVersioning
        self.versioning = ModelVersioning(models_path)

    def _get_model_signature(self) -> Dict[str, str]:
        """Получает сигнатуру моделей для проверки целостности"""
        signature = {}

        model_files = [
            'svd_model.pkl', 'user_factors.npy', 'item_factors.npy',
            'nmf_model.pkl', 'als_model.pkl', 'rating_predictor.pkl',
            'ranking_model.pkl', 'nn_model.pkl'
        ]

        for filename in model_files:
            filepath = os.path.join(self.models_path, filename)
            if os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    signature[filename] = hashlib.md5(f.read()).hexdigest()

        return signature

    async def build_svd_model(self, user_item_matrix: csr_matrix, n_components: int = None) -> Dict:
        """Построение SVD модели с увеличенным числом компонент (300)"""
        if user_item_matrix is None:
            return None

        logger.info("Построение SVD модели...")

        # Увеличиваем число компонент до 300 для лучшего качества
        n_components = n_components or min(300, min(user_item_matrix.shape) - 1)
        n_components = max(50, n_components)

        self.svd_model = TruncatedSVD(
            n_components=n_components,
            random_state=42,
            n_iter=7
        )

        self.item_factors = self.svd_model.fit_transform(user_item_matrix.T)
        self.user_factors = user_item_matrix @ self.item_factors

        explained_variance = self.svd_model.explained_variance_ratio_.sum()
        cumulative_variance = np.cumsum(self.svd_model.explained_variance_ratio_)

        logger.info(f"SVD завершена: {n_components} компонент, дисперсия: {explained_variance:.4f}")
        logger.info(f"  - Для 50 компонент: {cumulative_variance[49]:.4f}")
        logger.info(f"  - Для 100 компонент: {cumulative_variance[99]:.4f}")
        logger.info(f"  - Для {n_components} компонент: {explained_variance:.4f}")

        return {
            'model': self.svd_model,
            'user_factors': self.user_factors,
            'item_factors': self.item_factors,
            'explained_variance': explained_variance,
            'n_components': n_components,
            'cumulative_variance': cumulative_variance.tolist()
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
            'reconstruction_error': self.nmf_model.reconstruction_err_,
            'n_components': n_components
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
        """Построение модели предсказания оценок с расширенными признаками"""
        if user_item_matrix is None or user_factors is None:
            return None

        logger.info("Построение модели предсказания оценок...")

        rows, cols = user_item_matrix.nonzero()

        # Используем все данные для лучшего качества
        max_samples = 1000000
        if len(rows) > max_samples:
            logger.info(f"Данных много ({len(rows)}), берем выборку из {max_samples} для ускорения")
            indices = np.random.choice(len(rows), max_samples, replace=False)
        else:
            indices = range(len(rows))

        logger.info(f"Обработка {len(indices)} оценок пользователей...")

        X_train, y_train = [], []
        has_nmf = user_factors_nmf is not None and item_factors_nmf is not None

        # Определяем размерности факторов
        user_factor_dim = min(50, user_factors.shape[1] if len(user_factors.shape) > 1 else user_factors.shape[0])
        item_factor_dim = min(50, item_factors.shape[1] if len(item_factors.shape) > 1 else item_factors.shape[0])

        processed = 0
        for i in indices:
            user_idx = rows[i]
            movie_idx = cols[i]
            rating = user_item_matrix[user_idx, movie_idx]

            if rating > 0:
                user_vec = user_factors[user_idx][:user_factor_dim] if user_factor_dim > 0 else np.array([0.5] * 50)
                item_vec = item_factors[movie_idx][:item_factor_dim] if item_factor_dim > 0 else np.array([0.5] * 50)

                # Дополняем до нужной размерности
                if len(user_vec) < 50:
                    user_vec = np.pad(user_vec, (0, 50 - len(user_vec)), constant_values=0.5)
                if len(item_vec) < 50:
                    item_vec = np.pad(item_vec, (0, 50 - len(item_vec)), constant_values=0.5)

                features = []

                # 1. SVD признаки (100 признаков: 50 пользовательских + 50 фильмовых)
                features.extend(user_vec[:50])
                features.extend(item_vec[:50])

                # 2. NMF признаки (60 признаков: 30 + 30)
                if has_nmf and user_idx < len(user_factors_nmf) and movie_idx < len(item_factors_nmf):
                    nmf_user_dim = min(30, len(user_factors_nmf[user_idx]))
                    nmf_item_dim = min(30, len(item_factors_nmf[movie_idx]))

                    nmf_user = user_factors_nmf[user_idx][:nmf_user_dim] if nmf_user_dim > 0 else np.array([0.5] * 30)
                    nmf_item = item_factors_nmf[movie_idx][:nmf_item_dim] if nmf_item_dim > 0 else np.array([0.5] * 30)

                    if len(nmf_user) < 30:
                        nmf_user = np.pad(nmf_user, (0, 30 - len(nmf_user)), constant_values=0.5)
                    if len(nmf_item) < 30:
                        nmf_item = np.pad(nmf_item, (0, 30 - len(nmf_item)), constant_values=0.5)

                    features.extend(nmf_user[:30])
                    features.extend(nmf_item[:30])
                else:
                    features.extend([0.5] * 60)

                # 3. Статистические признаки (4 признака)
                user_rating_count = min(1.0, user_item_matrix[user_idx].nnz / 200)
                movie_rating_count = min(1.0, user_item_matrix[:, movie_idx].nnz / 200)

                features.append(user_rating_count)
                features.append(movie_rating_count)

                # 4. Популярность и свежесть (2 признака)
                if popularity_scores is not None and movie_idx < len(popularity_scores):
                    features.append(float(popularity_scores[movie_idx]))
                else:
                    features.append(0.5)

                if recency_scores is not None and movie_idx < len(recency_scores):
                    features.append(float(recency_scores[movie_idx]))
                else:
                    features.append(0.5)

                X_train.append(features)
                y_train.append(rating)

            processed += 1
            if processed % 50000 == 0:
                logger.info(f"Обработано {processed}/{len(indices)} оценок...")

        if len(X_train) < 1000:
            logger.warning(f"Недостаточно данных для обучения: {len(X_train)}")
            return None

        X_train = np.array(X_train, dtype=np.float32)
        y_train = np.array(y_train, dtype=np.float32)

        # Удаляем NaN
        nan_mask = np.isnan(X_train).any(axis=1)
        if nan_mask.any():
            logger.info(f"Удаляем {nan_mask.sum()} строк с NaN")
            X_train = X_train[~nan_mask]
            y_train = y_train[~nan_mask]

        # Разделяем на обучающую и валидационную выборки
        X_train_split, X_val, y_train_split, y_val = train_test_split(
            X_train, y_train, test_size=0.2, random_state=42
        )

        logger.info(f"Обучающая выборка: {len(X_train_split)}, Валидационная: {len(X_val)}")
        logger.info(f"Количество признаков: {X_train.shape[1]}")

        # Используем GradientBoostingRegressor
        self.rating_predictor = GradientBoostingRegressor(
            n_estimators=150,  # Увеличиваем для лучшего качества
            max_depth=5,
            learning_rate=0.05,  # Уменьшаем для лучшей обобщающей способности
            random_state=42,
            subsample=0.8,
            verbose=1
        )

        # Обучаем модель
        self.rating_predictor.fit(X_train_split, y_train_split)

        # Оценка на обучающей выборке
        train_score = self.rating_predictor.score(X_train_split, y_train_split)
        train_pred = self.rating_predictor.predict(X_train_split)
        train_mse = mean_squared_error(y_train_split, train_pred)
        train_rmse = np.sqrt(train_mse)

        # Оценка на валидационной выборке
        val_score = self.rating_predictor.score(X_val, y_val)
        val_pred = self.rating_predictor.predict(X_val)
        val_mse = mean_squared_error(y_val, val_pred)
        val_rmse = np.sqrt(val_mse)

        logger.info(f"Модель предсказания обучена")
        logger.info(f"  - Обучающий R²: {train_score:.4f}")
        logger.info(f"  - Обучающий RMSE: {train_rmse:.4f}")
        logger.info(f"  - Валидационный R²: {val_score:.4f}")
        logger.info(f"  - Валидационный RMSE: {val_rmse:.4f}")
        logger.info(f"  - Размер обучающей выборки: {len(X_train_split)}")
        logger.info(f"  - Количество признаков: {X_train.shape[1]}")

        return {
            'model': self.rating_predictor,
            'train_size': len(X_train_split),
            'val_size': len(X_val),
            'train_r2': train_score,
            'val_r2': val_score,
            'train_rmse': train_rmse,
            'val_rmse': val_rmse,
            'n_features': X_train.shape[1]
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

    def save_models(self, data_snapshot: Dict[str, str] = None):
        """Сохранение моделей с версионированием"""
        logger.info("Сохранение моделей...")

        # Сохраняем модели
        if self.svd_model is not None:
            with open(os.path.join(self.models_path, 'svd_model.pkl'), 'wb') as f:
                pickle.dump(self.svd_model, f)
            np.save(os.path.join(self.models_path, 'user_factors.npy'), self.user_factors)
            np.save(os.path.join(self.models_path, 'item_factors.npy'), self.item_factors)
            logger.info("SVD модель сохранена")

        if self.nmf_model is not None:
            with open(os.path.join(self.models_path, 'nmf_model.pkl'), 'wb') as f:
                pickle.dump(self.nmf_model, f)
            if self.user_factors_nmf is not None:
                np.save(os.path.join(self.models_path, 'user_factors_nmf.npy'), self.user_factors_nmf)
                np.save(os.path.join(self.models_path, 'item_factors_nmf.npy'), self.item_factors_nmf)
            logger.info("NMF модель сохранена")

        if self.als_model is not None:
            with open(os.path.join(self.models_path, 'als_model.pkl'), 'wb') as f:
                pickle.dump(self.als_model, f)
            logger.info("ALS модель сохранена")

        if self.rating_predictor is not None:
            with open(os.path.join(self.models_path, 'rating_predictor.pkl'), 'wb') as f:
                pickle.dump(self.rating_predictor, f)
            logger.info("Модель предсказания сохранена")

        if self.ranking_model is not None:
            with open(os.path.join(self.models_path, 'ranking_model.pkl'), 'wb') as f:
                pickle.dump(self.ranking_model, f)
            logger.info("Модель ранжирования сохранена")

        if self.nn_model is not None:
            with open(os.path.join(self.models_path, 'nn_model.pkl'), 'wb') as f:
                pickle.dump(self.nn_model, f)
            logger.info("Индекс схожести сохранен")

        # Сохраняем информацию о версии
        version_info = {
            'version': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'data_snapshot': data_snapshot or {},
            'model_files': self.versioning.get_model_files()
        }

        self.versioning.save_version_info(version_info)
        logger.info("Модели сохранены")

    def load_models(self, data_snapshot: Dict[str, str] = None) -> bool:
        """Загрузка моделей с проверкой актуальности"""

        # Проверяем, нужно ли переобучать модели
        if data_snapshot:
            if not self.versioning.are_models_valid(data_snapshot):
                logger.info("Данные изменились или модели отсутствуют, требуется переобучение")
                return False
        else:
            # Если нет снапшота для проверки, проверяем только существование файлов
            if self.versioning.check_model_files_exist():
                logger.info("Файлы моделей существуют, пробуем загрузить")
            else:
                logger.info("Файлы моделей отсутствуют, требуется обучение")
                return False

        try:
            # Загрузка SVD
            svd_path = os.path.join(self.models_path, 'svd_model.pkl')
            if os.path.exists(svd_path):
                with open(svd_path, 'rb') as f:
                    self.svd_model = pickle.load(f)
                self.user_factors = np.load(os.path.join(self.models_path, 'user_factors.npy'))
                self.item_factors = np.load(os.path.join(self.models_path, 'item_factors.npy'))
                logger.info("SVD модель загружена")

            # Загрузка NMF
            nmf_path = os.path.join(self.models_path, 'nmf_model.pkl')
            if os.path.exists(nmf_path):
                with open(nmf_path, 'rb') as f:
                    self.nmf_model = pickle.load(f)
                if os.path.exists(os.path.join(self.models_path, 'user_factors_nmf.npy')):
                    self.user_factors_nmf = np.load(os.path.join(self.models_path, 'user_factors_nmf.npy'))
                    self.item_factors_nmf = np.load(os.path.join(self.models_path, 'item_factors_nmf.npy'))
                logger.info("NMF модель загружена")

            # Загрузка ALS
            als_path = os.path.join(self.models_path, 'als_model.pkl')
            if os.path.exists(als_path):
                with open(als_path, 'rb') as f:
                    self.als_model = pickle.load(f)
                logger.info("ALS модель загружена")

            # Загрузка моделей предсказания
            rating_path = os.path.join(self.models_path, 'rating_predictor.pkl')
            if os.path.exists(rating_path):
                with open(rating_path, 'rb') as f:
                    self.rating_predictor = pickle.load(f)
                logger.info("Модель предсказания загружена")

            ranking_path = os.path.join(self.models_path, 'ranking_model.pkl')
            if os.path.exists(ranking_path):
                with open(ranking_path, 'rb') as f:
                    self.ranking_model = pickle.load(f)
                logger.info("Модель ранжирования загружена")

            # Загрузка индекса схожести
            nn_path = os.path.join(self.models_path, 'nn_model.pkl')
            if os.path.exists(nn_path):
                with open(nn_path, 'rb') as f:
                    self.nn_model = pickle.load(f)
                logger.info("Индекс схожести загружен")

            logger.info("Все модели успешно загружены")
            return True

        except Exception as e:
            logger.error(f"Ошибка загрузки моделей: {e}")
            return False
