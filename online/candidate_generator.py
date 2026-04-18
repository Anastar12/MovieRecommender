import logging
import numpy as np
import pandas as pd
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict
import asyncio
import os

logger = logging.getLogger(__name__)


class CandidateGenerator:
    """Модуль генерации кандидатов"""

    def __init__(self, models_provider, data_provider, config: Dict = None):
        self.models = models_provider
        self.data = data_provider
        self.config = config or {}

        # Веса для разных методов
        self.weights = self.config.get('weights', {
            'collaborative': 0.30,  # User-based collaborative filtering
            'content': 0.25,  # Content-based (похожие фильмы)
            'svd': 0.25,  # SVD матричная факторизация
            'als': 0.20  # ALS implicit feedback
        })

        # Лимиты
        self.candidate_limit = self.config.get('candidate_limit', 200)
        self.per_method_limit = self.config.get('per_method_limit', 100)

    async def generate_candidates(self, context: Dict) -> List[Dict]:
        """Генерация кандидатов из разных источников"""
        user_id = context['user_id']
        candidates = defaultdict(float)

        # Получаем историю оценок пользователя
        user_rated_movies = context.get('user_rated_movies', set())
        user_genres = context.get('user_genre_preferences', {})

        # Для новых пользователей или с малым количеством оценок - используем жанровую персонализацию
        if len(user_rated_movies) < 10:
            logger.info(
                f"Пользователь {user_id} имеет {len(user_rated_movies)} оценок, используем жанровую персонализацию")

            if user_genres:
                # Персонализация на основе жанров
                genre_candidates = await self._get_genre_based_candidates(user_genres, context)
                for movie_id, score in genre_candidates:
                    if movie_id not in user_rated_movies:
                        candidates[movie_id] += score * 0.7  # Высокий вес для жанров

                # Добавляем немного популярных для разнообразия
                popular = await self._get_popular_candidates(context)
                for movie in popular[:20]:
                    if movie['movie_id'] not in user_rated_movies:
                        candidates[movie['movie_id']] += movie.get('score', 0.5) * 0.3
            else:
                # Нет жанровых предпочтений - используем популярные
                return await self._get_popular_candidates(context)

            if candidates:
                return self._deduplicate_and_sort(dict(candidates), context)

        # Для активных пользователей - полная персонализация
        logger.info(
            f"Генерация персонализированных кандидатов для {user_id}, оценено фильмов: {len(user_rated_movies)}")

        # Параллельный сбор кандидатов
        tasks = []

        if self.weights.get('collaborative', 0) > 0:
            tasks.append(self._get_collaborative_candidates(user_id, user_rated_movies))

        if self.weights.get('svd', 0) > 0:
            tasks.append(self._get_svd_candidates(user_id))

        if self.weights.get('als', 0) > 0:
            tasks.append(self._get_als_candidates(user_id))

        if self.weights.get('content', 0) > 0:
            recent_movies = list(user_rated_movies)[:10] if user_rated_movies else []
            tasks.append(self._get_content_candidates_from_history(recent_movies))

        # Жанровые кандидаты для усиления персонализации
        if user_genres:
            tasks.append(self._get_genre_based_candidates(user_genres, context))

        # Сбор результатов
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Объединение с весами
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка генерации кандидатов: {result}")
                continue

            if result:
                for movie_id, score in result:
                    if movie_id not in user_rated_movies:
                        candidates[movie_id] += score

        if not candidates:
            logger.warning(f"Нет кандидатов для {user_id}, используем популярные")
            return await self._get_popular_candidates(context)

        return self._deduplicate_and_sort(candidates, context)

    async def _get_genre_based_candidates(self, user_genres: Dict[str, float], context: Dict, limit: int = 100) -> List[Tuple[str, float]]:
        """Получение кандидатов на основе жанровых предпочтений"""
        try:
            if self.models and hasattr(self.models, 'movies_df') and self.models.movies_df is not None:
                movies_df = self.models.movies_df.copy()

                # Сортируем жанры по предпочтению
                sorted_genres = sorted(user_genres.items(), key=lambda x: x[1], reverse=True)
                top_genres = [g[0] for g in sorted_genres[:3]]  # Топ-3 жанра

                # Ищем фильмы по этим жанрам
                genre_movies = []
                for genre in top_genres:
                    # Поиск фильмов с этим жанром
                    mask = movies_df['genre'].str.contains(genre, case=False, na=False)
                    genre_filtered = movies_df[mask].copy()

                    # Добавляем score на основе предпочтения
                    weight = user_genres.get(genre, 0.5)
                    for _, movie in genre_filtered.iterrows():
                        movie_id = movie.get('movie_id')
                        if movie_id:
                            # Базовый score от жанра
                            score = weight

                            # Добавляем рейтинг IMDb
                            imdb = movie.get('imdb', 0)
                            if pd.notna(imdb):
                                score += float(imdb) / 10 * 0.3

                            genre_movies.append((str(movie_id), score))

                # Сортируем по score
                genre_movies.sort(key=lambda x: x[1], reverse=True)

                # Убираем дубликаты
                seen = set()
                unique_movies = []
                for movie_id, score in genre_movies:
                    if movie_id not in seen:
                        seen.add(movie_id)
                        unique_movies.append((movie_id, score))

                # Нормализуем scores
                if unique_movies:
                    max_score = max([s for _, s in unique_movies[:limit]])
                    return [(m_id, (s / max_score) * self.weights.get('content', 0.25))
                            for m_id, s in unique_movies[:limit]]

                return []
        except Exception as e:
            logger.error(f"Ошибка жанровых кандидатов: {e}")
            return []

    async def _get_collaborative_candidates(self, user_id: str, user_rated_movies: set) -> List[Tuple[str, float]]:
        """User-based collaborative filtering - поиск похожих пользователей"""
        try:
            # Получаем всех пользователей с их оценками
            if self.data and hasattr(self.data, 'user_main_df') and self.data.user_main_df is not None:
                user_df = self.data.user_main_df

                # Находим пользователя
                user_data = user_df[user_df['user_url'] == user_id]
                if len(user_data) == 0:
                    return []

                # Получаем оценки других пользователей из reviews_df
                if hasattr(self.data, 'get_user_watched_movies'):
                    # Получаем оценки текущего пользователя
                    user_ratings = {}
                    for movie in self.data.get_user_watched_movies(user_id):
                        if movie.get('rating') and movie.get('movie_id'):
                            user_ratings[movie['movie_id']] = float(movie['rating'])

                    if not user_ratings:
                        return []

                    # Находим похожих пользователей
                    similar_users = self._find_similar_users(user_ratings)

                    # Собираем рекомендации от похожих пользователей
                    recommendations = self._aggregate_from_similar_users(similar_users, user_ratings)

                    # Нормализуем и применяем вес
                    max_score = max([s for _, s in recommendations]) if recommendations else 1
                    return [(m_id, (score / max_score) * self.weights['collaborative'])
                            for m_id, score in recommendations[:self.per_method_limit]]

            return []

        except Exception as e:
            logger.error(f"Ошибка collaborative filtering: {e}")
            return []

    def _find_similar_users(self, user_ratings: Dict[str, float], top_n: int = 20) -> List[Tuple[str, float]]:
        """Находит пользователей с похожими оценками"""
        try:
            # Загружаем все ревью
            reviews_path = 'models/reviews_df.pkl'

            if os.path.exists(reviews_path):
                reviews_df = pd.read_pickle(reviews_path)

                # Группируем по пользователям
                user_vectors = {}
                for user_url, group in reviews_df.groupby('user_url_normalized'):
                    if pd.isna(user_url) or not user_url:
                        continue

                    ratings_dict = {}
                    for _, row in group.iterrows():
                        movie_id = row.get('movie_id')
                        rating = row.get('rating')
                        if movie_id and pd.notna(rating) and rating > 0:
                            ratings_dict[str(movie_id)] = float(rating)

                    if ratings_dict:
                        user_vectors[user_url] = ratings_dict

                # Вычисляем схожесть с текущим пользователем
                similarities = []
                user_movies = set(user_ratings.keys())

                for other_user, other_ratings in user_vectors.items():
                    if other_user == user_id:
                        continue

                    # Находим общие фильмы
                    common_movies = user_movies.intersection(set(other_ratings.keys()))
                    if len(common_movies) < 3:
                        continue

                    # Вычисляем корреляцию Пирсона
                    user_scores = [user_ratings[m] for m in common_movies]
                    other_scores = [other_ratings[m] for m in common_movies]

                    if user_scores and other_scores:
                        similarity = self._pearson_correlation(user_scores, other_scores)
                        if similarity > 0.3:
                            similarities.append((other_user, similarity, other_ratings))

                # Сортируем по схожести
                similarities.sort(key=lambda x: x[1], reverse=True)
                return [(u, s) for u, s, _ in similarities[:top_n]]

            return []
        except Exception as e:
            logger.error(f"Ошибка поиска похожих пользователей: {e}")
            return []

    def _pearson_correlation(self, x: List[float], y: List[float]) -> float:
        """Вычисление корреляции Пирсона"""
        if len(x) != len(y) or len(x) == 0:
            return 0

        x_mean = np.mean(x)
        y_mean = np.mean(y)

        numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(len(x)))
        denominator = np.sqrt(sum((x[i] - x_mean) ** 2 for i in range(len(x)))) * \
                      np.sqrt(sum((y[i] - y_mean) ** 2 for i in range(len(x))))

        if denominator == 0:
            return 0

        return numerator / denominator

    def _aggregate_from_similar_users(self, similar_users: List[Tuple[str, float]],
                                      user_ratings: Dict[str, float]) -> Dict[str, float]:
        """Агрегирует рекомендации от похожих пользователей"""
        recommendations = defaultdict(float)
        total_similarity = defaultdict(float)

        for other_user, similarity, other_ratings in similar_users:
            for movie_id, rating in other_ratings.items():
                if movie_id not in user_ratings:
                    recommendations[movie_id] += similarity * rating
                    total_similarity[movie_id] += similarity

        # Нормализуем
        for movie_id in recommendations:
            if total_similarity[movie_id] > 0:
                recommendations[movie_id] /= total_similarity[movie_id]

        return dict(recommendations)

    async def _get_svd_candidates(self, user_id: str) -> List[Tuple[str, float]]:
        """SVD-based recommendations"""
        try:
            if hasattr(self.models, 'get_svd_recommendations'):
                recs = self.models.get_svd_recommendations(user_id, self.per_method_limit)
                if recs:
                    max_score = max([r.get('score', 0) for r in recs]) if recs else 1
                    return [(r['movie_id'], (r.get('score', 0) / max_score) * self.weights['svd'])
                            for r in recs if r.get('score', 0) > 0]
        except Exception as e:
            logger.error(f"Ошибка SVD: {e}")
        return []

    async def _get_als_candidates(self, user_id: str) -> List[Tuple[str, float]]:
        """ALS-based recommendations"""
        try:
            if hasattr(self.models, 'get_als_recommendations'):
                recs = self.models.get_als_recommendations(user_id, self.per_method_limit)
                if recs:
                    max_score = max([r.get('score', 0) for r in recs]) if recs else 1
                    return [(r['movie_id'], (r.get('score', 0) / max_score) * self.weights['als'])
                            for r in recs if r.get('score', 0) > 0]
        except Exception as e:
            logger.error(f"Ошибка ALS: {e}")
        return []

    async def _get_content_candidates_from_history(self, movie_ids: List[str]) -> List[Tuple[str, float]]:
        """Content-based recommendations based on user's watch history"""
        if not movie_ids:
            return []

        all_recommendations = defaultdict(float)

        for movie_id in movie_ids:
            if hasattr(self.models, 'get_similar_movies'):
                recs = self.models.get_similar_movies(movie_id, self.per_method_limit // len(movie_ids))
                for r in recs:
                    similarity = r.get('similarity', 0)
                    if similarity > 0.3:  # Порог схожести
                        all_recommendations[r['movie_id']] += similarity

        if all_recommendations:
            max_score = max(all_recommendations.values())
            return [(m_id, (score / max_score) * self.weights['content'])
                    for m_id, score in all_recommendations.items()]

        return []

    async def _get_popular_candidates(self, context: Dict) -> List[Dict]:
        """Популярные фильмы для холодного старта с персонализацией по жанрам"""
        try:
            strategy = context.get('cold_start_strategy', 'popular')
            user_genres = context.get('user_genre_preferences', {})

            if strategy == 'popular':
                candidates = self.data.get_popular_movies(self.candidate_limit) if self.data else []
            elif strategy == 'genre_based' and user_genres:
                # Персонализация для нового пользователя на основе выбранных жанров
                candidates = self._get_genre_based_popular(user_genres)
            else:
                candidates = self.data.get_popular_movies(self.candidate_limit) if self.data else []

            # Добавляем score для популярных фильмов
            for i, c in enumerate(candidates):
                # Чем выше позиция, тем выше score
                c['score'] = 1.0 - (i / len(candidates)) * 0.5 if candidates else 1.0
                c['final_score'] = c['score']

            return candidates
        except Exception as e:
            logger.error(f"Ошибка получения популярных: {e}")
            return []

    def _get_genre_based_popular(self, user_genres: Dict[str, float], limit: int = 100) -> List[Dict]:
        """Получает популярные фильмы с учетом предпочтений по жанрам"""
        try:
            if self.models and hasattr(self.models, 'movies_df') and self.models.movies_df is not None:
                movies_df = self.models.movies_df.copy()

                # Вычисляем жанровый скоринг
                genre_scores = defaultdict(float)
                for genre, weight in user_genres.items():
                    genre_scores[genre] = weight

                # Оцениваем каждый фильм
                scored_movies = []
                for _, movie in movies_df.iterrows():
                    movie_genres = str(movie.get('genre', '')).split(',')
                    movie_score = 0

                    for genre in movie_genres:
                        genre = genre.strip()
                        # Ищем соответствие жанра (русский/английский)
                        for user_genre, weight in user_genres.items():
                            if user_genre.lower() in genre.lower() or genre.lower() in user_genre.lower():
                                movie_score += weight

                    if movie_score > 0:
                        # Добавляем популярность
                        imdb_rating = movie.get('imdb', 0)
                        if pd.notna(imdb_rating):
                            movie_score += float(imdb_rating) / 10 * 0.5

                        scored_movies.append({
                            'movie_id': movie.get('movie_id'),
                            'title': movie.get('title'),
                            'score': movie_score
                        })

                scored_movies.sort(key=lambda x: x['score'], reverse=True)
                return scored_movies[:limit]

            return []
        except Exception as e:
            logger.error(f"Ошибка жанровой фильтрации: {e}")
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