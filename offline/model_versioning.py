import os
import pickle
import hashlib
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


class ModelVersioning:
    """Система версионирования моделей с проверкой изменений данных"""

    def __init__(self, models_path: str = 'api/models/'):
        self.models_path = models_path
        self.version_file = os.path.join(models_path, 'model_version.json')

        # Получаем корневую директорию проекта
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Файлы, которые отслеживаются для переобучения (только файлы с кодом модели)
        self.TRACKED_CODE_FILES = [
            'offline/data_pipeline.py',
            'offline/model_trainer.py',
            'offline/model_versioning.py',
            'online/candidate_generator.py',
            'online/ranker.py',
            'online/context_handler.py',
            'online/postprocessor.py'
        ]

        os.makedirs(models_path, exist_ok=True)

    def get_code_files_hash(self) -> str:
        """Вычисляет хеш всех отслеживаемых файлов с кодом"""
        hasher = hashlib.md5()

        for filepath in self.TRACKED_CODE_FILES:
            # Строим полный путь относительно корня проекта
            full_path = os.path.join(self.project_root, filepath)

            if os.path.exists(full_path):
                try:
                    with open(full_path, 'rb') as f:
                        hasher.update(f.read())
                    logger.debug(f"Хеширован файл: {filepath}")
                except Exception as e:
                    logger.error(f"Ошибка чтения {full_path}: {e}")
            else:
                logger.warning(f"Файл не найден: {full_path}")

        return hasher.hexdigest()

    def compute_stable_hash(self, df, columns_to_hash=None) -> str:
        """Вычисляет стабильный хеш DataFrame без учета порядка строк"""
        if df is None or len(df) == 0:
            return "empty"

        try:
            if columns_to_hash:
                df_hash = df[columns_to_hash].copy()
            else:
                df_hash = df.copy()

            # Сортируем для стабильности
            if 'movie_id' in df_hash.columns:
                df_hash = df_hash.sort_values('movie_id')
            elif 'user_url' in df_hash.columns:
                df_hash = df_hash.sort_values('user_url')
            elif 'review_url' in df_hash.columns:
                df_hash = df_hash.sort_values('review_url')

            # Преобразуем в строку и берем хеш
            hash_str = df_hash.to_string(index=False)
            return hashlib.md5(hash_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Ошибка вычисления хеша: {e}")
            return str(len(df))

    def get_data_snapshot(self, data_pipeline) -> Dict[str, str]:
        """Создает снапшот текущего состояния данных"""
        snapshot = {
            'code_hash': self.get_code_files_hash()
        }

        # Movies snapshot
        if hasattr(data_pipeline, 'movies_df') and data_pipeline.movies_df is not None:
            snapshot['movies'] = self.compute_stable_hash(
                data_pipeline.movies_df,
                ['movie_id', 'title', 'year', 'imdb', 'genre']
            )

        # Reviews snapshot
        if hasattr(data_pipeline, 'reviews_df') and data_pipeline.reviews_df is not None:
            snapshot['reviews'] = self.compute_stable_hash(
                data_pipeline.reviews_df,
                ['user_url', 'movie_id', 'rating']
            )

        # Users snapshot
        if hasattr(data_pipeline, 'user_main_df') and data_pipeline.user_main_df is not None:
            snapshot['users'] = self.compute_stable_hash(
                data_pipeline.user_main_df,
                ['user_url', 'ratings_count']
            )

        return snapshot

    def save_version_info(self, version_info: Dict[str, Any]):
        """Сохраняет информацию о версии моделей"""
        version_info['timestamp'] = datetime.now().isoformat()
        version_info['code_hash'] = self.get_code_files_hash()

        with open(self.version_file, 'w', encoding='utf-8') as f:
            json.dump(version_info, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Версия моделей сохранена: {version_info.get('version', 'unknown')}")

    def load_version_info(self) -> Optional[Dict[str, Any]]:
        """Загружает информацию о версии моделей"""
        if not os.path.exists(self.version_file):
            return None

        try:
            with open(self.version_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки версии: {e}")
            return None

    def are_models_valid(self, current_snapshot: Dict[str, str]) -> bool:
        """Проверяет, актуальны ли существующие модели"""
        version_info = self.load_version_info()

        if not version_info:
            logger.info("Версия моделей не найдена, требуется обучение")
            return False

        saved_snapshot = version_info.get('data_snapshot', {})

        # Получаем текущий хеш кода
        current_code_hash = self.get_code_files_hash()
        saved_code_hash = saved_snapshot.get('code_hash', '')

        # Если код изменился - обязательно переобучаем
        if current_code_hash != saved_code_hash:
            logger.info("Код моделей изменился, требуется переобучение")
            return False

        # Сравниваем снапшоты данных
        if saved_snapshot != current_snapshot:
            logger.info("Данные изменились, требуется переобучение")

            # Показываем какие именно данные изменились
            for key in current_snapshot:
                if key != 'code_hash':
                    if key in saved_snapshot and saved_snapshot[key] != current_snapshot[key]:
                        logger.info(f"  {key}: изменился")

            return False

        # Проверяем, что все файлы моделей существуют
        model_files = version_info.get('model_files', [])
        missing_files = []

        for model_file in model_files:
            full_path = os.path.join(self.models_path, model_file)
            if not os.path.exists(full_path):
                missing_files.append(model_file)

        if missing_files:
            logger.info(f"Отсутствуют файлы моделей: {missing_files[:5]}...")
            return False

        logger.info("Модели актуальны, переобучение не требуется")
        return True

    def get_model_files(self) -> List[str]:
        """Возвращает список файлов моделей"""
        return [
            'svd_model.pkl', 'user_factors.npy', 'item_factors.npy',
            'nmf_model.pkl', 'als_model.pkl', 'rating_predictor.pkl',
            'ranking_model.pkl', 'nn_model.pkl',
            'tfidf_vectorizer.pkl', 'tfidf_matrix.npz',
            'genre_vectors.npz', 'actor_vectors.npz', 'director_vectors.npz',
            'combined_features.npz', 'user_item_matrix.npz',
            'popularity_scores.npy', 'recency_scores.npy',
            'movies_df.pkl', 'reviews_df.pkl', 'user_main_df.pkl',
            'movie_ids.pkl', 'user_indices.pkl', 'movie_indices.pkl',
            'user_list.pkl', 'movie_list.pkl', 'metadata.pkl'
        ]

    def check_model_files_exist(self) -> bool:
        """Проверяет существование всех файлов моделей"""
        model_files = self.get_model_files()

        for model_file in model_files:
            if not os.path.exists(os.path.join(self.models_path, model_file)):
                logger.debug(f"Файл модели отсутствует: {model_file}")
                return False

        return True
