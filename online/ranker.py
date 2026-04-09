import logging
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class Ranker:
    """Модуль ранжирования кандидатов"""

    def __init__(self, models_provider, data_provider, config: Dict = None):
        self.models = models_provider
        self.data = data_provider
        self.config = config or {}

        # Параметры ранжирования
        self.diversity_weight = self.config.get('diversity_weight', 0.15)
        self.recency_weight = self.config.get('recency_weight', 0.10)
        self.popularity_weight = self.config.get('popularity_weight', 0.10)
        self.personalization_weight = self.config.get('personalization_weight', 0.65)

        self.final_top_n = self.config.get('final_top_n', 50)

    async def rank_candidates(self, candidates: List[Dict], context: Dict) -> List[Dict]:
        """Ранжирование кандидатов"""
        if not candidates:
            return []

        logger.info(f"Ранжирование {len(candidates)} кандидатов")

        # Параллельное вычисление оценок
        for candidate in candidates:
            scores = await self._compute_scores(candidate, context)
            candidate['final_score'] = self._aggregate_scores(scores, candidate, context)

        # Сортировка по финальному score
        candidates.sort(key=lambda x: x['final_score'], reverse=True)

        # Добавление разнообразия
        diversified = self._apply_diversity(candidates, context)

        return diversified[:self.final_top_n]

    async def _compute_scores(self, candidate: Dict, context: Dict) -> Dict:
        """Вычисление различных компонент оценки"""
        scores = {}

        # Предсказанная полезность (есть модель)
        # Исправлено: проверяем через model_trainer
        if hasattr(self.models, 'trainer') and self.models.trainer.rating_predictor is not None:
            scores['utility'] = await self._predict_rating(candidate, context)
        else:
            scores['utility'] = candidate.get('score', 0.5)

        # Разнообразие
        scores['diversity'] = await self._compute_diversity(candidate, context)

        # Свежесть
        scores['recency'] = self._compute_recency(candidate)

        # Популярность
        scores['popularity'] = self._compute_popularity(candidate)

        # Контекстная релевантность
        scores['contextual'] = self._compute_contextual_relevance(candidate, context)

        return scores

    async def _predict_rating(self, candidate: Dict, context: Dict) -> float:
        """Предсказание оценки пользователя"""
        try:
            user_id = context['user_id']
            movie_id = candidate['movie_id']

            # Использование модели предсказания через model_trainer
            if hasattr(self.models, 'trainer') and self.models.trainer.rating_predictor is not None:
                # Здесь нужно получить индексы пользователя и фильма
                if hasattr(self.models.data, 'user_indices') and user_id in self.models.data.user_indices:
                    user_idx = self.models.data.user_indices[user_id]
                    if movie_id in self.models.data.movie_indices:
                        movie_idx = self.models.data.movie_indices[movie_id]

                        # Формируем признаки для предсказания
                        try:
                            user_svd = self.models.trainer.user_factors[user_idx][:20] if len(
                                self.models.trainer.user_factors[user_idx]) >= 20 else self.models.trainer.user_factors[
                                user_idx]
                            item_svd = self.models.trainer.item_factors[movie_idx][:20] if len(
                                self.models.trainer.item_factors[movie_idx]) >= 20 else \
                            self.models.trainer.item_factors[movie_idx]
                            svd_features = np.concatenate([user_svd, item_svd])

                            extra_features = []
                            if hasattr(self.models.data, 'popularity_scores') and movie_idx < len(
                                    self.models.data.popularity_scores):
                                extra_features.append(float(self.models.data.popularity_scores[movie_idx]))
                            if hasattr(self.models.data, 'recency_scores') and movie_idx < len(
                                    self.models.data.recency_scores):
                                extra_features.append(float(self.models.data.recency_scores[movie_idx]))

                            if extra_features:
                                features = np.concatenate([svd_features, np.array(extra_features)])
                            else:
                                features = svd_features

                            features = features.reshape(1, -1)
                            rating = self.models.trainer.rating_predictor.predict(features)[0]
                            return max(0, min(1, rating))
                        except Exception as e:
                            logger.error(f"Ошибка предсказания: {e}")
                            return candidate.get('score', 0.5)

            return candidate.get('score', 0.5)
        except Exception as e:
            logger.error(f"Ошибка предсказания: {e}")
            return 0.5

    async def _compute_diversity(self, candidate: Dict, context: Dict) -> float:
        """Вычисление разнообразия относительно уже выбранных"""
        selected = context.get('selected_movies', [])
        if not selected:
            return 1.0

        try:
            # Вычисление средней схожести с выбранными
            similarities = []
            for selected_id in selected:
                sim = self._get_movie_similarity(candidate['movie_id'], selected_id)
                similarities.append(sim)

            avg_similarity = np.mean(similarities) if similarities else 0
            # Чем меньше схожесть, тем выше diversity score
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
                # Экспоненциальное затухание
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

        # Временной контекст
        time_context = context.get('time_context', {})
        if time_context.get('is_weekend', False):
            # В выходные больше развлекательных фильмов
            genre_relevance = self._check_genre_match(candidate, ['Comedy', 'Action', 'Adventure'])
            relevance = max(relevance, genre_relevance * 0.3)

        # Сезонный контекст
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

        # Нормализация
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
            # Получение основного жанра
            genres = candidate.get('genres', [])
            main_genre = genres[0] if genres else 'unknown'

            # Проверка, не было ли слишком много фильмов этого жанра подряд
            if main_genre in genre_last_seen:
                last_pos = genre_last_seen[main_genre]
                distance = len(diversified) - last_pos
                if distance < 2 and len(diversified) > 0:
                    # Пропускаем этот фильм, берем следующий
                    continue

            genre_last_seen[main_genre] = len(diversified)
            diversified.append(candidate)

            if len(diversified) >= self.final_top_n:
                break

        # Если после чередования осталось мало фильмов, добавляем остальные
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
            if hasattr(self.models, 'get_movie_similarity'):
                return self.models.get_movie_similarity(movie_id1, movie_id2)
            return 0
        except:
            return 0
