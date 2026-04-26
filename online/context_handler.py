import logging
from collections import defaultdict
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)


class ContextHandler:
    """Модуль получения контекста запроса"""

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.session_cache = {}

    def get_user_context(self, user_id: str, request_params: Dict = None) -> Dict:
        """Получение полного контекста пользователя"""
        context = {
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'request_params': request_params or {},
            'user_profile': None,
            'user_history': None,
            'is_new_user': False,
            'cold_start_strategy': None
        }

        # Проверка нового пользователя
        user_profile = self._get_user_profile(user_id)

        if user_profile is None or user_profile.get('total_ratings', 0) == 0:
            context['is_new_user'] = True
            context['cold_start_strategy'] = self._determine_cold_start_strategy(request_params)
        else:
            context['user_profile'] = user_profile
            context['user_history'] = self._get_user_history(user_id)

        # Дополнительный контекст
        context['time_context'] = self._get_time_context()
        context['device_context'] = self._get_device_context(request_params)

        return context

    def _get_user_profile(self, user_id: str) -> Optional[Dict]:
        """Получение профиля пользователя"""
        # Проверка кэша сессии
        if user_id in self.session_cache:
            return self.session_cache[user_id].get('profile')

        try:
            # Получение из data_provider
            if hasattr(self.data_provider, 'get_user_stats'):
                profile = self.data_provider.get_user_stats(user_id)
                if profile:
                    # Кэширование
                    if user_id not in self.session_cache:
                        self.session_cache[user_id] = {}
                    self.session_cache[user_id]['profile'] = profile
                return profile
        except Exception as e:
            logger.error(f"Ошибка получения профиля {user_id}: {e}")

        return None

    def _get_user_history(self, user_id: str) -> List[Dict]:
        """Получение истории просмотров/оценок пользователя - СИНХРОННАЯ версия"""
        # Проверка кэша
        if user_id in self.session_cache:
            cached = self.session_cache[user_id].get('history')
            if cached:
                return cached

        try:
            if hasattr(self.data_provider, 'get_user_watched_movies'):
                history = self.data_provider.get_user_watched_movies(user_id)
                if history:
                    if user_id not in self.session_cache:
                        self.session_cache[user_id] = {}
                    self.session_cache[user_id]['history'] = history
                    logger.info(f"Загружена история для {user_id}: {len(history)} фильмов")
                return history or []
        except Exception as e:
            logger.error(f"Ошибка получения истории {user_id}: {e}")
            import traceback
            traceback.print_exc()

        return []

    def _determine_cold_start_strategy(self, request_params: Dict = None) -> str:
        """Определение стратегии холодного старта"""
        if request_params and 'strategy' in request_params:
            return request_params['strategy']

        # По умолчанию - популярные фильмы
        return 'popular'

    def _get_time_context(self) -> Dict:
        """Получение временного контекста"""
        now = datetime.now()
        return {
            'hour': now.hour,
            'day_of_week': now.weekday(),
            'is_weekend': now.weekday() >= 5,
            'month': now.month,
            'season': self._get_season(now.month)
        }

    def _get_season(self, month: int) -> str:
        """Определение сезона"""
        if month in [12, 1, 2]:
            return 'winter'
        elif month in [3, 4, 5]:
            return 'spring'
        elif month in [6, 7, 8]:
            return 'summer'
        else:
            return 'autumn'

    def _get_device_context(self, request_params: Dict = None) -> Dict:
        """Получение контекста устройства"""
        if not request_params:
            return {}

        return {
            'device_type': request_params.get('device_type', 'desktop'),
            'screen_size': request_params.get('screen_size'),
            'platform': request_params.get('platform', 'web')
        }

    def get_user_genre_preferences(self, user_id: str) -> Dict:
        """Получение жанровых предпочтений пользователя на основе его оценок"""
        try:
            history = self._get_user_history(user_id)
            if not history:
                logger.info(f"Нет истории для пользователя {user_id}")
                return {}

            genre_ratings = defaultdict(list)
            genre_counts = defaultdict(int)

            for movie in history:
                rating = movie.get('rating')
                if rating is None or rating == 0:
                    continue

                movie_id = movie.get('movie_id')
                if movie_id and self.data_provider and hasattr(self.data_provider, 'movies_df'):
                    # Получаем жанры фильма
                    movie_data = self.data_provider.movies_df[self.data_provider.movies_df['movie_id'] == movie_id]
                    if len(movie_data) > 0:
                        genres = str(movie_data.iloc[0].get('genre', ''))
                        for genre in genres.split(','):
                            genre = genre.strip()
                            if genre:
                                genre_ratings[genre].append(float(rating))
                                genre_counts[genre] += 1

            # Вычисляем среднюю оценку по жанрам
            genre_prefs = {}
            for genre, ratings in genre_ratings.items():
                if len(ratings) >= 1:  # Минимум 1 оценка
                    avg_rating = sum(ratings) / len(ratings)
                    # Нормализуем от 0 до 1 (оценки от 1 до 10)
                    normalized = (avg_rating - 1) / 9
                    # Учитываем количество просмотров
                    count_weight = min(1.0, genre_counts[genre] / 5)  # До 5 просмотров дают максимум веса
                    final_score = normalized * (0.7 + 0.3 * count_weight)
                    genre_prefs[genre] = max(0, min(1, final_score))

            # Сортируем и берем топ-10 жанров
            sorted_genres = sorted(genre_prefs.items(), key=lambda x: x[1], reverse=True)[:10]

            logger.info(f"Жанровые предпочтения для {user_id}: {dict(sorted_genres)}")
            return dict(sorted_genres)

        except Exception as e:
            logger.error(f"Ошибка получения жанровых предпочтений: {e}")
            return {}

    def get_user_year_preferences(self, user_id: str) -> Dict:
        """Получение предпочтений по годам"""
        profile = self._get_user_profile(user_id)
        if profile and 'top_years' in profile:
            return {str(y['year']): y.get('normalized_weight', 1) for y in profile['top_years']}
        return {}

    def get_user_rated_movies(self, user_id: str) -> set:
        """Получение множества ID фильмов, оцененных пользователем"""
        history = self._get_user_history(user_id)
        rated_movies = set()
        for item in history:
            movie_id = item.get('movie_id')
            # Проверяем, что есть оценка (не просто просмотр)
            rating = item.get('rating') or item.get('user_rating')
            if movie_id and rating is not None and rating > 0:
                rated_movies.add(movie_id)

        # Также добавляем фильмы из user_watched без оценок как просмотренные
        for item in history:
            movie_id = item.get('movie_id')
            if movie_id and movie_id not in rated_movies:
                # Если фильм есть в истории (даже без оценки) - считаем просмотренным
                rated_movies.add(movie_id)

        logger.info(f"Пользователь {user_id} имеет {len(rated_movies)} уникальных просмотренных фильмов")
        return rated_movies

    def clear_session_cache(self, user_id: str = None):
        """Очистка кэша сессии"""
        if user_id:
            self.session_cache.pop(user_id, None)
        else:
            self.session_cache.clear()

    def update_user_preference(self, user_id: str, action_type: str,
                               movie_id: str, rating: float = None):
        """
        Немедленное обновление пользовательских предпочтений
        """
        # Очищаем кэш для этого пользователя
        self.clear_session_cache(user_id)

        # Обновляем жанровые предпочтения
        if action_type == 'rating' and rating:
            # Немедленное обновление в кэше
            if user_id in self.session_cache:
                self.session_cache[user_id]['genre_preferences'] = \
                    self._recalculate_genre_preferences(user_id, movie_id, rating)

        logger.info(f"Контекст пользователя {user_id} обновлен после действия {action_type}")

    def _recalculate_genre_preferences(self, user_id: str,
                                       new_movie_id: str,
                                       new_rating: float) -> Dict:
        """
        Пересчет жанровых предпочтений с учетом новой оценки
        """
        # Получаем историю с новой оценкой
        history = self._get_user_history(user_id)

        # Ищем обновленную историю (с учетом новой оценки)
        # Для этого временно добавляем новую оценку

        genre_ratings = defaultdict(list)

        # Добавляем существующие оценки
        for movie in history:
            if movie.get('rating') and movie.get('movie_id'):
                movie_id = movie['movie_id']
                rating = movie['rating']

                # Добавляем новую оценку (если это она)
                if movie_id == new_movie_id:
                    rating = new_rating

                movie_genres = self._get_movie_genres(movie_id)
                for genre in movie_genres:
                    genre_ratings[genre].append(rating)

        # Если фильма еще нет в истории, добавляем его
        movie_genres = self._get_movie_genres(new_movie_id)
        for genre in movie_genres:
            genre_ratings[genre].append(new_rating)

        # Вычисляем предпочтения
        genre_prefs = {}
        for genre, ratings in genre_ratings.items():
            avg_rating = sum(ratings) / len(ratings)
            normalized = (avg_rating - min(ratings)) / (max(ratings) - min(ratings) + 0.001)
            genre_prefs[genre] = max(0, min(1, normalized))

        return genre_prefs

    def _get_movie_genres(self, movie_id: str) -> List[str]:
        """Получение жанров фильма"""
        if hasattr(self.data_provider, 'movies_df'):
            movie = self.data_provider.movies_df[
                self.data_provider.movies_df['movie_id'] == movie_id
                ]
            if len(movie) > 0:
                genre_str = movie.iloc[0].get('genre', '')
                if genre_str:
                    return [g.strip() for g in str(genre_str).split(',')]
        return []