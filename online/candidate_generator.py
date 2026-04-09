import logging
import numpy as np
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict
import asyncio

logger = logging.getLogger(__name__)


class CandidateGenerator:
    """Модуль генерации кандидатов"""

    def __init__(self, models_provider, data_provider, config: Dict = None):
        self.models = models_provider
        self.data = data_provider
        self.config = config or {}

        # Веса для разных методов
        self.weights = self.config.get('weights', {
            'collaborative': 0.35,
            'content': 0.25,
            'svd': 0.20,
            'als': 0.20
        })

        # Лимиты
        self.candidate_limit = self.config.get('candidate_limit', 200)
        self.per_method_limit = self.config.get('per_method_limit', 100)

    async def generate_candidates(self, context: Dict) -> List[Dict]:
        """Генерация кандидатов из разных источников"""
        user_id = context['user_id']
        candidates = defaultdict(float)

        # Для новых пользователей - популярные фильмы
        if context.get('is_new_user', False):
            return await self._get_popular_candidates(context)

        # Параллельный сбор кандидатов из разных методов
        tasks = []

        if self.weights.get('collaborative', 0) > 0:
            tasks.append(self._get_collaborative_candidates(user_id))

        if self.weights.get('svd', 0) > 0:
            tasks.append(self._get_svd_candidates(user_id))

        if self.weights.get('als', 0) > 0:
            tasks.append(self._get_als_candidates(user_id))

        if self.weights.get('content', 0) > 0 and context.get('current_movie_id'):
            tasks.append(self._get_content_candidates(context['current_movie_id']))

        # Сбор результатов
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Объединение с весами
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка генерации кандидатов: {result}")
                continue

            if result:
                for movie_id, score in result:
                    candidates[movie_id] += score

        # Дедупликация и сортировка
        return self._deduplicate_and_sort(candidates, context)

    async def _get_collaborative_candidates(self, user_id: str) -> List[Tuple[str, float]]:
        """User-based collaborative filtering"""
        try:
            if hasattr(self.models, 'get_user_based_recommendations'):
                recs = self.models.get_user_based_recommendations(user_id, self.per_method_limit)
                return [(r['movie_id'], r.get('score', 0) * self.weights['collaborative'])
                        for r in recs]
        except Exception as e:
            logger.error(f"Ошибка collaborative: {e}")
        return []

    async def _get_svd_candidates(self, user_id: str) -> List[Tuple[str, float]]:
        """SVD-based recommendations"""
        try:
            if hasattr(self.models, 'get_svd_recommendations'):
                recs = self.models.get_svd_recommendations(user_id, self.per_method_limit)
                return [(r['movie_id'], r.get('score', 0) * self.weights['svd'])
                        for r in recs]
        except Exception as e:
            logger.error(f"Ошибка SVD: {e}")
        return []

    async def _get_als_candidates(self, user_id: str) -> List[Tuple[str, float]]:
        """ALS-based recommendations"""
        try:
            if hasattr(self.models, 'get_als_recommendations'):
                recs = self.models.get_als_recommendations(user_id, self.per_method_limit)
                return [(r['movie_id'], r.get('score', 0) * self.weights['als'])
                        for r in recs]
        except Exception as e:
            logger.error(f"Ошибка ALS: {e}")
        return []

    async def _get_content_candidates(self, movie_id: str) -> List[Tuple[str, float]]:
        """Content-based recommendations"""
        try:
            if hasattr(self.models, 'get_similar_movies'):
                recs = self.models.get_similar_movies(movie_id, self.per_method_limit)
                return [(r['movie_id'], r.get('similarity', 0) * self.weights['content'])
                        for r in recs]
        except Exception as e:
            logger.error(f"Ошибка content: {e}")
        return []

    async def _get_popular_candidates(self, context: Dict) -> List[Dict]:
        """Популярные фильмы для холодного старта"""
        try:
            strategy = context.get('cold_start_strategy', 'popular')

            if strategy == 'popular':
                candidates = self.data.get_popular_movies(self.candidate_limit)
            elif strategy == 'recent':
                candidates = self.data.get_recent_movies(self.candidate_limit)
            elif strategy == 'random':
                candidates = self.data.get_random_movies(self.candidate_limit)
            else:
                candidates = self.data.get_popular_movies(self.candidate_limit)

            # Нормализация scores
            for c in candidates:
                c['score'] = 1.0

            return candidates
        except Exception as e:
            logger.error(f"Ошибка получения популярных: {e}")
            return []

    def _deduplicate_and_sort(self, candidates: Dict, context: Dict) -> List[Dict]:
        """Дедупликация и сортировка кандидатов"""

        # Фильтрация уже просмотренных
        user_rated = context.get('user_rated_movies', set())

        filtered = []
        for movie_id, score in candidates.items():
            if movie_id not in user_rated:
                filtered.append({
                    'movie_id': movie_id,
                    'score': score
                })

        # Сортировка по score
        filtered.sort(key=lambda x: x['score'], reverse=True)

        # Ограничение количества
        return filtered[:self.candidate_limit]

    def get_genre_based_candidates(self, user_id: str, genre_prefs: Dict, limit: int = 50) -> List[Dict]:
        """Генерация кандидатов на основе жанровых предпочтений"""
        if not genre_prefs:
            return []

        try:
            # Получение фильмов по жанрам
            genre_movies = defaultdict(list)
            for genre, weight in genre_prefs.items():
                movies = self.data.get_movies_by_genre(genre, limit=limit)
                for movie in movies:
                    genre_movies[movie['movie_id']].append(weight)

            # Агрегация
            candidates = []
            for movie_id, weights in genre_movies.items():
                score = sum(weights) / len(weights)
                candidates.append({
                    'movie_id': movie_id,
                    'score': score,
                    'source': 'genre'
                })

            candidates.sort(key=lambda x: x['score'], reverse=True)
            return candidates[:limit]

        except Exception as e:
            logger.error(f"Ошибка жанровых кандидатов: {e}")
            return []


