import logging
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime
import sys

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")

logger = logging.getLogger(__name__)

class Ranker:
    """Модуль ранжирования кандидатов"""

    def __init__(self, models_provider, data_provider, config: Dict = None):
        self.models = models_provider
        self.data = data_provider
        self.config = config or {}

        # Параметры ранжирования - усилен вес персонализации
        self.diversity_weight = self.config.get('diversity_weight', 0.10)
        self.recency_weight = self.config.get('recency_weight', 0.05)
        self.popularity_weight = self.config.get('popularity_weight', 0.05)
        self.personalization_weight = self.config.get('personalization_weight', 0.80)  # Увеличен

        self.final_top_n = self.config.get('final_top_n', 50)

    async def rank_candidates(self, candidates: List[Dict], context: Dict) -> List[Dict]:
        """Ранжирование кандидатов с учетом профиля пользователя"""
        if not candidates:
            return []

        logger.info(f"Ранжирование {len(candidates)} кандидатов для пользователя {context.get('user_id')}")

        # Получаем профиль пользователя для персонализации
        user_genres = context.get('user_genre_preferences', {})
        user_ratings = context.get('user_rated_movies', set())

        # Отключаем лишний вывод
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")

            # Параллельное вычисление оценок
            for candidate in candidates:
                scores = await self._compute_scores(candidate, context, user_genres, user_ratings)
                candidate['final_score'] = self._aggregate_scores(scores, candidate, context)

        # Сортировка по финальному score
        candidates.sort(key=lambda x: x['final_score'], reverse=True)

        # Добавление разнообразия
        diversified = self._apply_diversity(candidates, context)

        return diversified[:self.final_top_n]

    async def _compute_scores(self, candidate: Dict, context: Dict,
                              user_genres: Dict, user_ratings: set) -> Dict:
        """Вычисление различных компонент оценки с учетом пользователя"""
        scores = {}

        # Персонализированная полезность - главный фактор
        scores['utility'] = await self._compute_personalized_utility(candidate, context, user_genres, user_ratings)

        # Разнообразие
        scores['diversity'] = await self._compute_diversity(candidate, context)

        # Свежесть
        scores['recency'] = self._compute_recency(candidate)

        # Популярность (пониженный вес)
        scores['popularity'] = self._compute_popularity(candidate)

        # Контекстная релевантность
        scores['contextual'] = self._compute_contextual_relevance(candidate, context)

        return scores

    async def _compute_personalized_utility(self, candidate: Dict, context: Dict,
                                            user_genres: Dict, user_ratings: set) -> float:
        """Вычисление персонализированной полезности на основе истории пользователя"""
        utility = candidate.get('score', 0.5)

        # 1. Учет жанровых предпочтений
        movie_genres = candidate.get('genres', [])
        if movie_genres and user_genres:
            genre_match_score = 0
            total_weight = 0

            for genre in movie_genres:
                # Ищем соответствие с предпочтениями пользователя
                for user_genre, weight in user_genres.items():
                    if user_genre.lower() in genre.lower() or genre.lower() in user_genre.lower():
                        genre_match_score += weight
                        total_weight += 1

            if total_weight > 0:
                genre_score = genre_match_score / total_weight
                utility = utility * 0.5 + genre_score * 0.5

        # 2. Учет похожих фильмов (если есть модель)
        try:
            if hasattr(self.models, 'predict_rating'):
                pred_rating = self.models.predict_rating(context['user_id'], candidate['movie_id'])
                if pred_rating:
                    utility = utility * 0.6 + pred_rating * 0.4
        except Exception as e:
            pass

        return max(0, min(1, utility))

    async def _compute_diversity(self, candidate: Dict, context: Dict) -> float:
        """Вычисление разнообразия относительно уже выбранных"""
        selected = context.get('selected_movies', [])
        if not selected:
            return 1.0

        try:
            similarities = []
            for selected_id in selected:
                sim = self._get_movie_similarity(candidate['movie_id'], selected_id)
                similarities.append(sim)

            avg_similarity = np.mean(similarities) if similarities else 0
            return 1 - avg_similarity
        except Exception as e:
            logger.error(f"Ошибка diversity: {e}")
            return 0.5

    def _compute_recency(self, candidate: Dict) -> float:
        """Вычисление свежести фильма"""
        try:
            year = candidate.get('year')
            if not year:
                return 0.5

            current_year = datetime.now().year
            try:
                year_int = int(str(year)[:4])
                age = current_year - year_int
                recency = np.exp(-age / 20)
                return max(0, min(1, recency))
            except:
                return 0.5
        except Exception as e:
            return 0.5

    def _compute_popularity(self, candidate: Dict) -> float:
        """Вычисление популярности фильма"""
        return candidate.get('popularity', 0.5)

    def _compute_contextual_relevance(self, candidate: Dict, context: Dict) -> float:
        """Вычисление контекстной релевантности"""
        relevance = 0.5

        time_context = context.get('time_context', {})
        if time_context.get('is_weekend', False):
            genre_relevance = self._check_genre_match(candidate, ['Comedy', 'Action', 'Adventure'])
            relevance = max(relevance, genre_relevance * 0.3)

        season = time_context.get('season', '')
        if season == 'winter':
            genre_relevance = self._check_genre_match(candidate, ['Drama', 'Romance', 'Family'])
            relevance = max(relevance, genre_relevance * 0.2)
        elif season == 'summer':
            genre_relevance = self._check_genre_match(candidate, ['Action', 'Adventure', 'Comedy'])
            relevance = max(relevance, genre_relevance * 0.2)

        return relevance

    def _check_genre_match(self, candidate: Dict, target_genres: List[str]) -> float:
        """Проверка соответствия жанров"""
        movie_genres = candidate.get('genres', [])
        if not movie_genres:
            return 0.0

        matches = sum(1 for g in movie_genres if g in target_genres)
        return matches / len(target_genres) if target_genres else 0

    def _aggregate_scores(self, scores: Dict, candidate: Dict, context: Dict) -> float:
        """Агрегация компонент оценки"""
        final_score = (
                scores.get('utility', 0) * self.personalization_weight +
                scores.get('diversity', 0) * self.diversity_weight +
                scores.get('recency', 0) * self.recency_weight +
                scores.get('popularity', 0) * self.popularity_weight +
                scores.get('contextual', 0) * 0.05
        )

        total_weight = (self.personalization_weight + self.diversity_weight +
                        self.recency_weight + self.popularity_weight + 0.05)

        return final_score / total_weight

    def _apply_diversity(self, candidates: List[Dict], context: Dict) -> List[Dict]:
        """Применение разнообразия с чередованием жанров"""
        if len(candidates) < 3:
            return candidates

        diversified = []
        genre_last_seen = {}

        for candidate in candidates:
            genres = candidate.get('genres', [])
            main_genre = genres[0] if genres else 'unknown'

            if main_genre in genre_last_seen:
                last_pos = genre_last_seen[main_genre]
                distance = len(diversified) - last_pos
                if distance < 2 and len(diversified) > 0:
                    continue

            genre_last_seen[main_genre] = len(diversified)
            diversified.append(candidate)

            if len(diversified) >= self.final_top_n:
                break

        if len(diversified) < self.final_top_n and len(candidates) > len(diversified):
            for candidate in candidates:
                if candidate not in diversified:
                    diversified.append(candidate)
                    if len(diversified) >= self.final_top_n:
                        break

        return diversified

    def _get_movie_similarity(self, movie_id1: str, movie_id2: str) -> float:
        """Получение схожести двух фильмов"""
        try:
            if hasattr(self.models, 'get_similar_movies'):
                similar = self.models.get_similar_movies(movie_id1, 100)
                for sim in similar:
                    if sim.get('movie_id') == movie_id2:
                        return sim.get('similarity', 0)
            return 0
        except:
            return 0
