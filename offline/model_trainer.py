import torch
import torch.nn as nn
import torch.optim as optim
from implicit.als import AlternatingLeastSquares
from implicit.nearest_neighbours import bm25_weight
from sklearn.decomposition import TruncatedSVD, NMF
from torch.utils.data import Dataset, DataLoader
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import QuantileTransformer
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from datetime import datetime
import logging
import pickle
from scipy.sparse import csr_matrix, load_npz, save_npz
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors
import asyncio
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class ModelTrainer:
    """Улучшенный тренер с нейронными сетями"""
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
        self.rating_transformer = None

        # Факторы
        self.user_factors = None
        self.item_factors = None
        self.user_factors_nmf = None
        self.item_factors_nmf = None

        # Система версионирования
        from offline.model_versioning import ModelVersioning
        self.versioning = ModelVersioning(models_path)

    def build_neural_collaborative_filtering(self,
                                             user_item_matrix: csr_matrix,
                                             n_users: int,
                                             n_items: int,
                                             embedding_dim: int = 128) -> nn.Module:
        """Neural Collaborative Filtering (NeuMF)"""

        class NeuMF(nn.Module):
            def __init__(self, n_users, n_items, embedding_dim):
                super().__init__()

                # GMF часть
                self.user_embedding_gmf = nn.Embedding(n_users, embedding_dim)
                self.item_embedding_gmf = nn.Embedding(n_items, embedding_dim)

                # MLP часть
                self.user_embedding_mlp = nn.Embedding(n_users, embedding_dim)
                self.item_embedding_mlp = nn.Embedding(n_items, embedding_dim)

                # MLP слои
                self.mlp_layers = nn.Sequential(
                    nn.Linear(embedding_dim * 2, 256),
                    nn.BatchNorm1d(256),
                    nn.ReLU(),
                    nn.Dropout(0.2),

                    nn.Linear(256, 128),
                    nn.BatchNorm1d(128),
                    nn.ReLU(),
                    nn.Dropout(0.2),

                    nn.Linear(128, 64),
                    nn.BatchNorm1d(64),
                    nn.ReLU(),
                    nn.Dropout(0.1),
                )

                # Финальный слой
                self.final_layer = nn.Linear(embedding_dim + 64, 1)
                self.sigmoid = nn.Sigmoid()

            def forward(self, user_ids, item_ids):
                # GMF
                user_gmf = self.user_embedding_gmf(user_ids)
                item_gmf = self.item_embedding_gmf(item_ids)
                gmf_output = user_gmf * item_gmf

                # MLP
                user_mlp = self.user_embedding_mlp(user_ids)
                item_mlp = self.item_embedding_mlp(item_ids)
                mlp_input = torch.cat([user_mlp, item_mlp], dim=1)
                mlp_output = self.mlp_layers(mlp_input)

                # Конкатенация
                concat = torch.cat([gmf_output, mlp_output], dim=1)
                output = self.final_layer(concat)

                return self.sigmoid(output).squeeze()

        model = NeuMF(n_users, n_items, embedding_dim).to(self.device)
        return model

    def build_two_tower_model(self, n_users: int, n_items: int,
                              user_features_dim: int = 256,
                              item_features_dim: int = 256) -> nn.Module:
        """Two-Tower модель"""

        class TwoTowerModel(nn.Module):
            def __init__(self, n_users, n_items, user_features_dim, item_features_dim):
                super().__init__()

                # User Tower
                self.user_tower = nn.Sequential(
                    nn.Linear(n_users, 512),
                    nn.BatchNorm1d(512),
                    nn.ReLU(),
                    nn.Dropout(0.3),

                    nn.Linear(512, 256),
                    nn.BatchNorm1d(256),
                    nn.ReLU(),
                    nn.Dropout(0.2),

                    nn.Linear(256, user_features_dim),
                    nn.BatchNorm1d(user_features_dim),
                    nn.ReLU()
                )

                # Item Tower
                self.item_tower = nn.Sequential(
                    nn.Linear(n_items, 512),
                    nn.BatchNorm1d(512),
                    nn.ReLU(),
                    nn.Dropout(0.3),

                    nn.Linear(512, 256),
                    nn.BatchNorm1d(256),
                    nn.ReLU(),
                    nn.Dropout(0.2),

                    nn.Linear(256, item_features_dim),
                    nn.BatchNorm1d(item_features_dim),
                    nn.ReLU()
                )

                # Cosine similarity
                self.cos = nn.CosineSimilarity(dim=1)

            def forward(self, user_id, item_id):
                # Преобразуем ID в one-hot или embedding
                user_vec = torch.eye(n_users)[user_id].to(self.device)
                item_vec = torch.eye(n_items)[item_id].to(self.device)

                user_embedding = self.user_tower(user_vec)
                item_embedding = self.item_tower(item_vec)

                similarity = self.cos(user_embedding, item_embedding)
                return (similarity + 1) / 2  # Normalize to [0, 1]

        model = TwoTowerModel(n_users, n_items, user_features_dim, item_features_dim).to(self.device)
        return model

    async def train_rating_predictor_improved(self,
                                              user_item_matrix: csr_matrix,
                                              user_factors: np.ndarray,
                                              item_factors: np.ndarray,
                                              user_factors_nmf: np.ndarray,
                                              item_factors_nmf: np.ndarray,
                                              popularity_scores: np.ndarray,
                                              recency_scores: np.ndarray,
                                              movies_df: pd.DataFrame) -> Dict:
        """Улучшенное обучение с градиентным бустингом и нейросетями"""

        # 1. Создаем расширенные признаки (200+)
        X_train, y_train = self._create_enhanced_features(
            user_item_matrix, user_factors, item_factors,
            user_factors_nmf, item_factors_nmf,
            popularity_scores, recency_scores, movies_df
        )

        # 2. Нормализация целевой переменной
        transformer = QuantileTransformer(output_distribution='normal')
        y_train_normalized = transformer.fit_transform(y_train.reshape(-1, 1)).ravel()

        # 3. LightGBM модель (быстрее и точнее GradientBoosting)
        params = {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt',
            'num_leaves': 255,
            'learning_rate': 0.05,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'n_estimators': 500,
            'early_stopping_rounds': 50,
            'lambda_l1': 0.1,
            'lambda_l2': 0.1,
            'min_child_samples': 20,
            'max_depth': 12
        }

        # Разделение на train/val
        X_train_split, X_val, y_train_split, y_val = train_test_split(
            X_train, y_train_normalized, test_size=0.2, random_state=42
        )

        # Обучение LightGBM
        train_data = lgb.Dataset(X_train_split, label=y_train_split)
        val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)

        model = lgb.train(
            params,
            train_data,
            valid_sets=[val_data],
            callbacks=[lgb.early_stopping(50), lgb.log_evaluation(50)]
        )

        # Оценка
        y_pred = model.predict(X_val)

        # Обратное преобразование
        y_pred_original = transformer.inverse_transform(y_pred.reshape(-1, 1)).ravel()
        y_val_original = transformer.inverse_transform(y_val.reshape(-1, 1)).ravel()

        # Метрики
        r2 = r2_score(y_val_original, y_pred_original)
        rmse = np.sqrt(mean_squared_error(y_val_original, y_pred_original))
        mae = mean_absolute_error(y_val_original, y_pred_original)

        logger.info(f"Улучшенная модель: R²={r2:.4f}, RMSE={rmse:.4f}, MAE={mae:.4f}")

        return {
            'model': model,
            'transformer': transformer,
            'r2_score': r2,
            'rmse': rmse,
            'mae': mae
        }

    def _create_enhanced_features(self, user_item_matrix, user_factors, item_factors,
                                  user_factors_nmf, item_factors_nmf,
                                  popularity_scores, recency_scores, movies_df) -> Tuple[np.ndarray, np.ndarray]:
        """Создание улучшенных признаков (200+ фичей)"""

        rows, cols = user_item_matrix.nonzero()
        features_list = []
        targets = []

        for i in range(len(rows)):
            user_idx = rows[i]
            movie_idx = cols[i]
            rating = user_item_matrix[user_idx, movie_idx]

            # Базовые факторы (100 признаков)
            features = []
            features.extend(user_factors[user_idx][:50])
            features.extend(item_factors[movie_idx][:50])

            # NMF факторы (60 признаков)
            if user_factors_nmf is not None:
                features.extend(user_factors_nmf[user_idx][:30])
                features.extend(item_factors_nmf[movie_idx][:30])
            else:
                features.extend([0.5] * 60)

            # Статистические признаки (15)
            user_rating_count = user_item_matrix[user_idx].nnz
            item_rating_count = user_item_matrix[:, movie_idx].nnz
            features.extend([
                np.log1p(user_rating_count),
                np.log1p(item_rating_count),
                user_rating_count / 1000,
                item_rating_count / 1000,
                popularity_scores[movie_idx],
                recency_scores[movie_idx]
            ])

            # Жанровые признаки (one-hot для топ-20 жанров)
            movie_data = movies_df.iloc[movie_idx]
            genres = str(movie_data.get('genre', '')).split(',')
            genre_vector = self._create_genre_vector(genres)
            features.extend(genre_vector)

            # Годовые признаки (синус/косинус)
            year = movie_data.get('year')
            if year and pd.notna(year):
                year_float = float(year)
                features.append(np.sin(2 * np.pi * year_float / 10))
                features.append(np.cos(2 * np.pi * year_float / 10))
            else:
                features.extend([0, 0])

            # Временные фичи
            current_year = datetime.now().year
            age = current_year - year_float if year and pd.notna(year) else 50
            features.append(np.exp(-age / 10))

            features_list.append(features)
            targets.append(rating)

            if len(features_list) % 10000 == 0:
                logger.info(f"Создано {len(features_list)} примеров")

        return np.array(features_list, dtype=np.float32), np.array(targets, dtype=np.float32)

    def _create_genre_vector(self, genres: List[str], top_genres: List[str] = None) -> List[float]:
        """Создание one-hot вектора для жанров"""
        if top_genres is None:
            top_genres = ['Action', 'Adventure', 'Animation', 'Biography', 'Comedy',
                          'Crime', 'Documentary', 'Drama', 'Family', 'Fantasy',
                          'History', 'Horror', 'Music', 'Musical', 'Mystery',
                          'Romance', 'Sci-Fi', 'Sport', 'Thriller', 'War']

        vector = [1.0 if genre in genres else 0.0 for genre in top_genres]
        return vector

        # Добавьте этот метод, если его нет

    async def build_svd_model(self, user_item_matrix: csr_matrix, n_components: int = 300) -> Dict:
        """Построение SVD модели"""
        if user_item_matrix is None:
            return None

        logger.info("Построение SVD модели...")

        # Увеличиваем число компонент до 300 для лучшего качества
        n_components = min(n_components, min(user_item_matrix.shape) - 1)
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

        return {
            'model': self.svd_model,
            'user_factors': self.user_factors,
            'item_factors': self.item_factors,
            'explained_variance': explained_variance,
            'n_components': n_components,
            'cumulative_variance': cumulative_variance.tolist()
        }

        # Добавьте этот метод, если его нет

    async def build_nmf_model(self, user_item_matrix: csr_matrix, n_components: int = 50) -> Optional[Dict]:
        """Построение NMF модели"""
        if user_item_matrix is None:
            return None

        logger.info("Построение NMF модели...")

        n_components = min(n_components, min(user_item_matrix.shape) - 1)
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

        # Добавьте этот метод, если его нет

    async def build_als_model(self, user_item_matrix: csr_matrix, factors: int = 100) -> Optional[Dict]:
        """Построение ALS модели"""
        if user_item_matrix is None:
            return None

        logger.info("Построение ALS модели...")

        # BM25 взвешивание
        weighted_matrix = bm25_weight(user_item_matrix.T)

        self.als_model = AlternatingLeastSquares(
            factors=factors,
            regularization=0.1,
            iterations=20,  # Увеличиваем итерации
            random_state=42,
            use_gpu=False,
            calculate_training_loss=True
        )

        self.als_model.fit(weighted_matrix)

        logger.info(f"ALS модель обучена с {factors} факторами")

        return {
            'model': self.als_model,
            'factors': factors
        }

        # Добавьте этот метод, если его нет

    async def build_rating_predictor(self,
                                     user_item_matrix: csr_matrix,
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

        rows, cols = user_item_matrix.nonzero()

        # Используем все данные
        max_samples = 1000000
        if len(rows) > max_samples:
            logger.info(f"Данных много ({len(rows)}), берем выборку из {max_samples}")
            indices = np.random.choice(len(rows), max_samples, replace=False)
        else:
            indices = range(len(rows))

        logger.info(f"Обработка {len(indices)} оценок пользователей...")

        X_train, y_train = [], []
        has_nmf = user_factors_nmf is not None and item_factors_nmf is not None

        # Увеличиваем размерность факторов
        user_factor_dim = min(100, user_factors.shape[1] if len(user_factors.shape) > 1 else user_factors.shape[0])
        item_factor_dim = min(100, item_factors.shape[1] if len(item_factors.shape) > 1 else item_factors.shape[0])

        processed = 0
        for i in indices:
            user_idx = rows[i]
            movie_idx = cols[i]
            rating = user_item_matrix[user_idx, movie_idx]

            if rating > 0:
                # Берем больше факторов
                user_vec = user_factors[user_idx][:user_factor_dim] if user_factor_dim > 0 else np.array([0.5] * 100)
                item_vec = item_factors[movie_idx][:item_factor_dim] if item_factor_dim > 0 else np.array([0.5] * 100)

                # Дополняем до нужной размерности
                if len(user_vec) < 100:
                    user_vec = np.pad(user_vec, (0, 100 - len(user_vec)), constant_values=0.5)
                if len(item_vec) < 100:
                    item_vec = np.pad(item_vec, (0, 100 - len(item_vec)), constant_values=0.5)

                features = []

                # 1. SVD признаки (200 признаков: 100 пользовательских + 100 фильмовых)
                features.extend(user_vec[:100])
                features.extend(item_vec[:100])

                # 2. NMF признаки (100 признаков: 50 + 50)
                if has_nmf and user_idx < len(user_factors_nmf) and movie_idx < len(item_factors_nmf):
                    nmf_user_dim = min(50, len(user_factors_nmf[user_idx]))
                    nmf_item_dim = min(50, len(item_factors_nmf[movie_idx]))

                    nmf_user = user_factors_nmf[user_idx][:nmf_user_dim] if nmf_user_dim > 0 else np.array([0.5] * 50)
                    nmf_item = item_factors_nmf[movie_idx][:nmf_item_dim] if nmf_item_dim > 0 else np.array([0.5] * 50)

                    if len(nmf_user) < 50:
                        nmf_user = np.pad(nmf_user, (0, 50 - len(nmf_user)), constant_values=0.5)
                    if len(nmf_item) < 50:
                        nmf_item = np.pad(nmf_item, (0, 50 - len(nmf_item)), constant_values=0.5)

                    features.extend(nmf_user[:50])
                    features.extend(nmf_item[:50])
                else:
                    features.extend([0.5] * 100)

                # 3. Статистические признаки (10 признаков)
                user_rating_count = min(1.0, user_item_matrix[user_idx].nnz / 200)
                movie_rating_count = min(1.0, user_item_matrix[:, movie_idx].nnz / 200)
                user_rating_log = np.log1p(user_item_matrix[user_idx].nnz)
                movie_rating_log = np.log1p(user_item_matrix[:, movie_idx].nnz)

                features.extend([
                    user_rating_count,
                    movie_rating_count,
                    user_rating_log / 10,
                    movie_rating_log / 10,
                    user_rating_count * movie_rating_count,
                ])

                # 4. Популярность и свежесть (2 признака)
                if popularity_scores is not None and movie_idx < len(popularity_scores):
                    features.append(float(popularity_scores[movie_idx]))
                else:
                    features.append(0.5)

                if recency_scores is not None and movie_idx < len(recency_scores):
                    features.append(float(recency_scores[movie_idx]))
                else:
                    features.append(0.5)

                # 5. Взаимодействие признаков (3 признака)
                features.append(
                    user_rating_count * float(popularity_scores[movie_idx]) if popularity_scores is not None else 0.25)
                features.append(
                    movie_rating_count * float(recency_scores[movie_idx]) if recency_scores is not None else 0.25)
                features.append(user_rating_count * movie_rating_count * float(
                    popularity_scores[movie_idx]) if popularity_scores is not None else 0.125)

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

        # Нормализуем целевую переменную
        from sklearn.preprocessing import QuantileTransformer
        transformer = QuantileTransformer(output_distribution='normal', random_state=42)
        y_train_normalized = transformer.fit_transform(y_train.reshape(-1, 1)).ravel()

        # Разделяем на обучающую и валидационную выборки
        from sklearn.model_selection import train_test_split
        X_train_split, X_val, y_train_split, y_val = train_test_split(
            X_train, y_train_normalized, test_size=0.2, random_state=42
        )

        logger.info(f"Обучающая выборка: {len(X_train_split)}, Валидационная: {len(X_val)}")
        logger.info(f"Количество признаков: {X_train.shape[1]}")

        # Используем LightGBM если доступен, иначе GradientBoosting
        try:
            import lightgbm as lgb

            train_data = lgb.Dataset(X_train_split, label=y_train_split)
            val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)

            params = {
                'objective': 'regression',
                'metric': 'rmse',
                'boosting_type': 'gbdt',
                'num_leaves': 255,
                'learning_rate': 0.05,
                'feature_fraction': 0.8,
                'bagging_fraction': 0.8,
                'bagging_freq': 5,
                'verbose': -1,
                'n_estimators': 500,
                'early_stopping_rounds': 50,
                'lambda_l1': 0.1,
                'lambda_l2': 0.1,
                'min_child_samples': 20,
                'max_depth': 12
            }

            self.rating_predictor = lgb.train(
                params,
                train_data,
                valid_sets=[val_data],
                callbacks=[lgb.early_stopping(50), lgb.log_evaluation(50)]
            )

            # Сохраняем трансформер
            self.rating_transformer = transformer

        except ImportError:
            logger.warning("LightGBM не установлен, используем GradientBoosting")
            from sklearn.ensemble import GradientBoostingRegressor

            self.rating_predictor = GradientBoostingRegressor(
                n_estimators=300,
                max_depth=8,
                learning_rate=0.05,
                random_state=42,
                subsample=0.8,
                verbose=1
            )

            self.rating_predictor.fit(X_train_split, y_train_split)
            self.rating_transformer = transformer

        # Оценка на валидационной выборке
        y_pred_normalized = self.rating_predictor.predict(X_val)
        y_pred = transformer.inverse_transform(y_pred_normalized.reshape(-1, 1)).ravel()
        y_val_original = transformer.inverse_transform(y_val.reshape(-1, 1)).ravel()

        r2 = r2_score(y_val_original, y_pred)
        rmse = np.sqrt(mean_squared_error(y_val_original, y_pred))
        mae = mean_absolute_error(y_val_original, y_pred)

        # Также вычислим R² на обучающей выборке
        y_train_pred_normalized = self.rating_predictor.predict(X_train_split)
        y_train_pred = transformer.inverse_transform(y_train_pred_normalized.reshape(-1, 1)).ravel()
        y_train_original = transformer.inverse_transform(y_train_split.reshape(-1, 1)).ravel()

        train_r2 = r2_score(y_train_original, y_train_pred)
        train_rmse = np.sqrt(mean_squared_error(y_train_original, y_train_pred))

        logger.info(f"Модель предсказания обучена")
        logger.info(f"  - Train R²: {train_r2:.4f}")
        logger.info(f"  - Train RMSE: {train_rmse:.4f}")
        logger.info(f"  - Validation R²: {r2:.4f}")
        logger.info(f"  - Validation RMSE: {rmse:.4f}")
        logger.info(f"  - MAE: {mae:.4f}")
        logger.info(f"  - Количество признаков: {X_train.shape[1]}")

        # Возвращаем правильные метрики
        return {
            'model': self.rating_predictor,
            'transformer': transformer,
            'train_size': len(X_train_split),
            'val_size': len(X_val),
            'train_r2': train_r2,
            'val_r2': r2,
            'train_rmse': train_rmse,
            'val_rmse': rmse,
            'mae': mae,
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

        sample_size = min(50000, len(rows))
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
                        recency_scores) else 0.0,
                    np.log1p(user_item_matrix[user_idx].nnz),
                    np.log1p(user_item_matrix[:, movie_idx].nnz)
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

        from sklearn.ensemble import RandomForestRegressor

        self.ranking_model = RandomForestRegressor(
            n_estimators=200,
            max_depth=12,
            random_state=42,
            n_jobs=-1
        )

        self.ranking_model.fit(X_train, y_train)

        logger.info(f"Модель ранжирования обучена на {len(X_train)} примерах")

        return {
            'model': self.ranking_model,
            'train_size': len(X_train)
        }

        # Добавьте этот метод, если его нет

    async def build_similarity_index(self, combined_features: csr_matrix) -> Optional[Dict]:
        """Построение индекса для быстрого поиска похожих фильмов"""
        if combined_features is None:
            return None

        logger.info("Построение индекса схожести...")

        from sklearn.neighbors import NearestNeighbors

        self.nn_model = NearestNeighbors(
            n_neighbors=100,
            metric='cosine',
            n_jobs=-1,
            algorithm='auto'
        )

        self.nn_model.fit(combined_features)

        logger.info(f"Индекс схожести построен для {combined_features.shape[0]} объектов")

        return {
            'model': self.nn_model,
            'n_samples': combined_features.shape[0]
        }

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

            # Сохраняем трансформер если есть
            if hasattr(self, 'rating_transformer') and self.rating_transformer is not None:
                with open(os.path.join(self.models_path, 'rating_transformer.pkl'), 'wb') as f:
                    pickle.dump(self.rating_transformer, f)

            logger.info("Модель предсказания сохранена")

        # Проверяем наличие ranking_model перед сохранением
        if hasattr(self, 'ranking_model') and self.ranking_model is not None:
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
        logger.info("Все модели сохранены")

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

                user_factors_path = os.path.join(self.models_path, 'user_factors.npy')
                item_factors_path = os.path.join(self.models_path, 'item_factors.npy')

                if os.path.exists(user_factors_path):
                    self.user_factors = np.load(user_factors_path)
                if os.path.exists(item_factors_path):
                    self.item_factors = np.load(item_factors_path)

                logger.info("SVD модель загружена")

            # Загрузка NMF
            nmf_path = os.path.join(self.models_path, 'nmf_model.pkl')
            if os.path.exists(nmf_path):
                with open(nmf_path, 'rb') as f:
                    self.nmf_model = pickle.load(f)

                user_factors_nmf_path = os.path.join(self.models_path, 'user_factors_nmf.npy')
                item_factors_nmf_path = os.path.join(self.models_path, 'item_factors_nmf.npy')

                if os.path.exists(user_factors_nmf_path):
                    self.user_factors_nmf = np.load(user_factors_nmf_path)
                if os.path.exists(item_factors_nmf_path):
                    self.item_factors_nmf = np.load(item_factors_nmf_path)

                logger.info("NMF модель загружена")

            # Загрузка ALS
            als_path = os.path.join(self.models_path, 'als_model.pkl')
            if os.path.exists(als_path):
                with open(als_path, 'rb') as f:
                    self.als_model = pickle.load(f)
                logger.info("ALS модель загружена")

            # Загрузка модели предсказания
            rating_path = os.path.join(self.models_path, 'rating_predictor.pkl')
            if os.path.exists(rating_path):
                with open(rating_path, 'rb') as f:
                    self.rating_predictor = pickle.load(f)

                # Загрузка трансформера
                transformer_path = os.path.join(self.models_path, 'rating_transformer.pkl')
                if os.path.exists(transformer_path):
                    with open(transformer_path, 'rb') as f:
                        self.rating_transformer = pickle.load(f)
                    logger.info("Rating transformer загружен")

                logger.info("Модель предсказания загружена")

            # Загрузка модели ранжирования
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
            import traceback
            traceback.print_exc()
            return False

    def save_model(self, model_name: str, model, additional_data: Dict = None):
        """Сохранить конкретную модель"""
        model_path = os.path.join(self.models_path, f'{model_name}.pkl')
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)

        if additional_data:
            for key, value in additional_data.items():
                data_path = os.path.join(self.models_path, f'{model_name}_{key}.npy')
                np.save(data_path, value)

        logger.info(f"Модель {model_name} сохранена")

    def load_model(self, model_name: str, load_additional: List[str] = None):
        """Загрузить конкретную модель"""
        model_path = os.path.join(self.models_path, f'{model_name}.pkl')

        if not os.path.exists(model_path):
            logger.warning(f"Модель {model_name} не найдена")
            return None

        with open(model_path, 'rb') as f:
            model = pickle.load(f)

        additional_data = {}
        if load_additional:
            for key in load_additional:
                data_path = os.path.join(self.models_path, f'{model_name}_{key}.npy')
                if os.path.exists(data_path):
                    additional_data[key] = np.load(data_path)

        logger.info(f"Модель {model_name} загружена")

        if additional_data:
            return model, additional_data
        return model