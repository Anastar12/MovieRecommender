import logging
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
        """Получение истории просмотров/оценок пользователя"""
        if user_id in self.session_cache:
            return self.session_cache[user_id].get('history', [])

        try:
            if hasattr(self.data_provider, 'get_user_watched_movies'):
                history = self.data_provider.get_user_watched_movies(user_id)
                if history:
                    if user_id not in self.session_cache:
                        self.session_cache[user_id] = {}
                    self.session_cache[user_id]['history'] = history
                return history or []
        except Exception as e:
            logger.error(f"Ошибка получения истории {user_id}: {e}")

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
        """Получение жанровых предпочтений пользователя"""
        profile = self._get_user_profile(user_id)
        if profile and 'top_genres' in profile:
            return {g['genre']: g.get('normalized_weight', 1) for g in profile['top_genres']}
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
        return {item['movie_id'] for item in history if item.get('movie_id')}

    def clear_session_cache(self, user_id: str = None):
        """Очистка кэша сессии"""
        if user_id:
            self.session_cache.pop(user_id, None)
        else:
            self.session_cache.clear()
