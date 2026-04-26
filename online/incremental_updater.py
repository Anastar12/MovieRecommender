import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Set, Optional
from datetime import datetime
from collections import defaultdict
import asyncio

logger = logging.getLogger(__name__)


class IncrementalUpdater:
    """
    Компонент для инкрементального обновления рекомендаций
    Реагирует на действия пользователя в реальном времени
    """

    def __init__(self, models_provider, data_provider, cache_manager):
        self.models = models_provider
        self.data = data_provider
        self.cache_manager = cache_manager

        # Кэш временных предпочтений пользователя (сессионные данные)
        self.session_preferences: Dict[str, Dict] = {}

        # Веса для разных типов действий
        self.action_weights = {
            'rating': 1.0,  # Оценка фильма
            'favorite': 0.8,  # Добавление в избранное
            'watched': 0.3,  # Просмотр
            'unfavorite': -0.5,  # Удаление из избранного
            'unwatched': -0.2  # Удаление из просмотренных
        }

        # Временной коэффициент (свежие действия важнее)
        self.temporal_decay = 0.95

    def process_user_action(self, user_id: str, action_type: str,
                            movie_id: str, metadata: Dict = None) -> Dict:
        """
        Обработка действия пользователя и обновление его профиля
        """
        logger.info(f"Обработка действия {action_type} для {user_id} -> {movie_id}")

        # Обновляем сессионные предпочтения
        self._update_session_preferences(user_id, action_type, movie_id, metadata)

        # Обновляем локальный профиль пользователя
        self._update_user_interests(user_id, movie_id, action_type, metadata)

        # Инвалидируем кэш рекомендаций
        self.cache_manager.invalidate_user_cache(user_id)

        # Возвращаем обновленное состояние
        return {
            'user_id': user_id,
            'action': action_type,
            'preferences_updated': True,
            'current_genre_prefs': self.get_user_genre_preferences(user_id),
            'recommendations_invalidated': True
        }

    def _update_session_preferences(self, user_id: str, action_type: str,
                                    movie_id: str, metadata: Dict = None):
        """
        Обновление сессионных предпочтений пользователя
        """
        if user_id not in self.session_preferences:
            self.session_preferences[user_id] = {
                'recent_ratings': [],  # Последние оценки
                'recent_favorites': set(),  # Недавние избранные
                'recent_watched': set(),  # Недавние просмотры
                'rating_history': [],  # История оценок для временного анализа
                'last_update': datetime.now()
            }

        session = self.session_preferences[user_id]
        weight = self.action_weights.get(action_type, 0)

        if action_type == 'rating':
            rating = metadata.get('rating', 0) if metadata else 0
            # Нормализуем оценку (1-10 -> 0-1)
            normalized_rating = rating / 10.0

            session['recent_ratings'].insert(0, {
                'movie_id': movie_id,
                'rating': normalized_rating,
                'weight': weight,
                'timestamp': datetime.now()
            })
            # Оставляем только последние 20 действий
            session['recent_ratings'] = session['recent_ratings'][:20]

            # Добавляем в историю для временного анализа
            session['rating_history'].append({
                'movie_id': movie_id,
                'rating': normalized_rating,
                'timestamp': datetime.now()
            })

        elif action_type == 'favorite':
            session['recent_favorites'].add(movie_id)

        elif action_type == 'watched':
            session['recent_watched'].add(movie_id)

        elif action_type == 'unfavorite':
            session['recent_favorites'].discard(movie_id)

        elif action_type == 'unwatched':
            session['recent_watched'].discard(movie_id)

        session['last_update'] = datetime.now()

    def _update_user_interests(self, user_id: str, movie_id: str,
                               action_type: str, metadata: Dict = None):
        """
        Инкрементальное обновление интересов пользователя
        """
        try:
            # Получаем жанры фильма
            movie_genres = self._get_movie_genres(movie_id)

            if not movie_genres:
                return

            # Обновляем жанровые предпочтения
            current_prefs = self.get_user_genre_preferences(user_id)

            weight = self.action_weights.get(action_type, 0)
            if action_type == 'rating':
                rating = metadata.get('rating', 5) if metadata else 5
                rating_factor = (rating - 5) / 5  # -0.4 до +0.4 (при весе 1)
                update_value = weight * rating_factor
            elif action_type == 'favorite':
                update_value = weight * 0.3
            elif action_type == 'watched':
                update_value = weight * 0.1
            else:
                update_value = weight * 0.2

            # Обновляем предпочтения
            for genre in movie_genres:
                current_prefs[genre] = current_prefs.get(genre, 0.5) + update_value
                # Ограничиваем диапазон [0, 1]
                current_prefs[genre] = max(0, min(1, current_prefs[genre]))

            # Сохраняем обновленные предпочтения
            self._save_user_genre_preferences(user_id, current_prefs)

            # Обновляем профиль в data_provider
            self._update_data_provider_profile(user_id, movie_id, action_type, metadata)

        except Exception as e:
            logger.error(f"Ошибка обновления интересов: {e}")

    def _get_movie_genres(self, movie_id: str) -> List[str]:
        """Получение жанров фильма (с русскими названиями)"""
        try:
            if self.models and hasattr(self.models, 'movies_df'):
                movie = self.models.movies_df[self.models.movies_df['movie_id'] == movie_id]
                if len(movie) > 0:
                    genre_str = movie.iloc[0].get('genre', '')
                    if genre_str:
                        return [g.strip() for g in str(genre_str).split(',') if g.strip()]
            return []
        except Exception as e:
            logger.error(f"Ошибка получения жанров: {e}")
            return []

    def get_user_genre_preferences(self, user_id: str) -> Dict[str, float]:
        """
        Получение актуальных жанровых предпочтений пользователя
        с учетом сессионных данных
        """
        # Базовые предпочтения из офлайн-моделей
        base_prefs = {}
        if self.data and hasattr(self.data, 'get_user_stats'):
            # Пытаемся получить из data_provider
            stats = self.data.get_user_stats(user_id)
            if stats and 'genre_preferences' in stats:
                base_prefs = stats['genre_preferences'].copy()

        # Применяем временные изменения из сессии
        if user_id in self.session_preferences:
            session = self.session_preferences[user_id]

            # Применяем затухание ко времени
            time_diff = (datetime.now() - session['last_update']).total_seconds() / 3600  # часов
            decay = self.temporal_decay ** time_diff

            # Получаем обновления из недавних оценок
            for rating_event in session['recent_ratings']:
                movie_genres = self._get_movie_genres(rating_event['movie_id'])
                for genre in movie_genres:
                    # Взвешиваем оценку с учетом времени
                    age = (datetime.now() - rating_event['timestamp']).total_seconds() / 3600
                    temporal_weight = (self.temporal_decay ** age) * rating_event['weight']
                    update = rating_event['rating'] * temporal_weight * 0.1
                    base_prefs[genre] = base_prefs.get(genre, 0.5) + update

            # Учитываем избранные фильмы
            for fav_id in session['recent_favorites']:
                movie_genres = self._get_movie_genres(fav_id)
                for genre in movie_genres:
                    base_prefs[genre] = base_prefs.get(genre, 0.5) + 0.15

            # Учитываем просмотренные
            for watched_id in session['recent_watched']:
                movie_genres = self._get_movie_genres(watched_id)
                for genre in movie_genres:
                    base_prefs[genre] = base_prefs.get(genre, 0.5) + 0.05

            # Ограничиваем значения и нормализуем
            for genre in base_prefs:
                base_prefs[genre] = max(0, min(1, base_prefs[genre] * decay))

        return base_prefs

    def _save_user_genre_preferences(self, user_id: str, prefs: Dict[str, float]):
        """Сохранение обновленных предпочтений"""
        # Сохраняем в session_preferences как временные данные
        if user_id not in self.session_preferences:
            self.session_preferences[user_id] = {
                'recent_ratings': [],
                'recent_favorites': set(),
                'recent_watched': set(),
                'rating_history': [],
                'last_update': datetime.now()
            }

        # Обновляем кэшированные предпочтения
        self.session_preferences[user_id]['cached_genre_prefs'] = prefs.copy()

    def _update_data_provider_profile(self, user_id: str, movie_id: str,
                                      action_type: str, metadata: Dict = None):
        """
        Обновление профиля в data_provider для немедленного использования
        """
        try:
            if hasattr(self.data, 'update_user_profile'):
                self.data.update_user_profile(user_id, movie_id, action_type, metadata)
        except Exception as e:
            logger.warning(f"Не удалось обновить профиль в data_provider: {e}")

    def get_enhanced_user_context(self, base_context: Dict) -> Dict:
        """
        Обогащение контекста пользователя сессионными данными
        """
        user_id = base_context.get('user_id')
        if not user_id:
            return base_context

        enhanced = base_context.copy()

        # Добавляем обновленные жанровые предпочтения
        enhanced['user_genre_preferences'] = self.get_user_genre_preferences(user_id)

        # Добавляем информацию о недавних действиях
        if user_id in self.session_preferences:
            session = self.session_preferences[user_id]
            enhanced['recent_actions'] = {
                'has_recent_ratings': len(session['recent_ratings']) > 0,
                'has_recent_favorites': len(session['recent_favorites']) > 0,
                'recent_ratings_count': len(session['recent_ratings']),
                'recent_favorites_count': len(session['recent_favorites'])
            }
            enhanced['force_refresh'] = True  # Принудительное обновление

        return enhanced

    def get_realtime_recommendations(self, user_id: str,
                                     base_recommendations: List[Dict],
                                     limit: int = 20) -> List[Dict]:
        """
        Корректировка базовых рекомендаций на основе свежих действий пользователя
        """
        if user_id not in self.session_preferences:
            return base_recommendations

        session = self.session_preferences[user_id]

        # Если нет свежих действий, возвращаем базовые
        if not session['recent_ratings'] and not session['recent_favorites']:
            return base_recommendations

        # Получаем обновленные жанровые предпочтения
        current_prefs = self.get_user_genre_preferences(user_id)

        # Пересчитываем скоры для базовых рекомендаций
        for rec in base_recommendations[:limit]:
            movie_genres = self._get_movie_genres(rec['movie_id'])

            # Вычисляем жанровый бонус
            genre_boost = 0
            for genre in movie_genres:
                genre_boost += current_prefs.get(genre, 0.5)

            if movie_genres:
                genre_boost /= len(movie_genres)

            # Применяем буст к финальному скору
            original_score = rec.get('final_score', rec.get('score', 0.5))
            boosted_score = original_score * (0.5 + genre_boost * 0.5)
            rec['final_score'] = min(1.0, boosted_score)
            rec['personalization_boost'] = genre_boost

            # Добавляем свежие действия в объяснение
            rec['explanation'] = self._generate_realtime_explanation(rec, current_prefs)

        # Пересортировываем
        base_recommendations.sort(key=lambda x: x.get('final_score', 0), reverse=True)

        return base_recommendations[:limit]

    def _generate_realtime_explanation(self, recommendation: Dict,
                                       user_prefs: Dict) -> str:
        """
        Генерация объяснения с учетом свежих действий
        """
        movie_genres = recommendation.get('genres', [])
        if not movie_genres:
            return recommendation.get('explanation', 'Рекомендовано для вас')

        # Находим жанр с наибольшим предпочтением
        best_genre = None
        best_score = 0

        for genre in movie_genres:
            score = user_prefs.get(genre, 0.5)
            if score > best_score:
                best_score = score
                best_genre = genre

        if best_score > 0.7:
            return f"Вам особенно нравятся фильмы жанра {best_genre}"
        elif best_score > 0.55:
            return f"Вам может понравится, вы оценили похожие фильмы жанра {best_genre}"
        else:
            return recommendation.get('explanation', 'Рекомендовано для вас')


# Расширение DataPipeline для поддержки инкрементальных обновлений
class OnlineDataUpdater:
    """
    Обновление пользовательских данных в реальном времени без перезагрузки всей БД
    """

    def __init__(self, data_pipeline, models_provider):
        self.pipeline = data_pipeline
        self.models = models_provider

        # Локальный кэш пользовательских данных
        self.user_data_cache: Dict[str, Dict] = {}

    async def add_user_rating_online(self, user_id: str, movie_id: str,
                                     rating: float, review_text: str = None):
        """
        Добавление оценки в реальном времени
        """
        try:
            # 1. Сохраняем в БД
            success = await self.pipeline.save_user_rating(user_id, movie_id, rating, review_text)

            if success:
                # 2. Обновляем локальный кэш
                if user_id not in self.user_data_cache:
                    self.user_data_cache[user_id] = {
                        'ratings': {},
                        'last_update': datetime.now()
                    }

                self.user_data_cache[user_id]['ratings'][movie_id] = {
                    'rating': rating,
                    'timestamp': datetime.now()
                }

                # 3. Обновляем статистику пользователя в памяти
                await self._update_in_memory_stats(user_id, rating)

                # 4. Обновляем user_item_matrix в памяти
                self._update_in_memory_matrix(user_id, movie_id, rating / 10.0)

                return True
        except Exception as e:
            logger.error(f"Ошибка добавления оценки online: {e}")
            return False

    async def _update_in_memory_stats(self, user_id: str, new_rating: float):
        """
        Обновление статистики пользователя в памяти
        """
        if hasattr(self.models, 'user_main_df') and self.models.user_main_df is not None:
            mask = self.models.user_main_df['user_url'] == user_id
            if mask.any():
                idx = self.models.user_main_df[mask].index[0]
                current_count = self.models.user_main_df.loc[idx, 'ratings_count']
                self.models.user_main_df.loc[idx, 'ratings_count'] = current_count + 1

    def _update_in_memory_matrix(self, user_id: str, movie_id: str, rating: float):
        """
        Обновление user-item матрицы в памяти (требует реализации)
        """
        # Здесь можно реализовать обновление sparse матрицы
        # Для простоты - отмечаем необходимость пересчета
        pass