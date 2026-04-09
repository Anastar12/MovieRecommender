import pickle
import redis
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class CacheManager:
    """Промежуточный кэширующий слой для хранения предвычисленных рекомендаций"""

    def __init__(self, redis_config: dict, cache_ttl: int = 3600, top_n_cached: int = 100):
        self.redis_config = redis_config
        self.cache_ttl = cache_ttl
        self.top_n_cached = top_n_cached

        self._connect()

    def _connect(self):
        """Подключение к Redis"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_config.get('host', 'localhost'),
                port=self.redis_config.get('port', 6379),
                db=self.redis_config.get('db', 0),
                password=self.redis_config.get('password'),
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info("Подключение к Redis установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {e}")
            self.redis_client = None

    def _get_cache_key(self, user_id: str, context: Dict = None) -> str:
        """Генерация ключа кэша"""
        base_key = f"rec:{user_id}"
        if context:
            context_str = json.dumps(context, sort_keys=True)
            hash_suffix = hashlib.md5(context_str.encode()).hexdigest()[:8]
            base_key = f"{base_key}:{hash_suffix}"
        return base_key

    def _get_top_n_key(self, user_id: str) -> str:
        """Ключ для топ-N рекомендаций"""
        return f"top_n:{user_id}"

    def _serialize_recommendations(self, recommendations: List[Dict]) -> str:
        """Сериализация рекомендаций для Redis"""
        try:
            return json.dumps(recommendations, default=str)
        except Exception as e:
            logger.error(f"Ошибка сериализации: {e}")
            return None

    def _deserialize_recommendations(self, data: str) -> List[Dict]:
        """Десериализация рекомендаций"""
        try:
            return json.loads(data)
        except Exception as e:
            logger.error(f"Ошибка десериализации: {e}")
            return []

    def cache_top_n_recommendations(self, user_id: str, recommendations: List[Dict]):
        """Кэширование топ-N рекомендаций для пользователя"""
        if not self.redis_client or not recommendations:
            return

        key = self._get_top_n_key(user_id)
        serialized = self._serialize_recommendations(recommendations[:self.top_n_cached])

        if serialized:
            self.redis_client.setex(key, self.cache_ttl, serialized)
            logger.debug(f"Закэшированы рекомендации для {user_id}")

    def get_cached_top_n(self, user_id: str) -> Optional[List[Dict]]:
        """Получение кэшированных топ-N рекомендаций"""
        if not self.redis_client:
            return None

        key = self._get_top_n_key(user_id)
        data = self.redis_client.get(key)

        if data:
            logger.debug(f"Кэш hit для {user_id}")
            return self._deserialize_recommendations(data)

        logger.debug(f"Кэш miss для {user_id}")
        return None

    def cache_contextual_recommendations(self, user_id: str, context: Dict, recommendations: List[Dict]):
        """Кэширование контекстных рекомендаций"""
        if not self.redis_client or not recommendations:
            return

        key = self._get_cache_key(user_id, context)
        serialized = self._serialize_recommendations(recommendations[:self.top_n_cached])

        if serialized:
            self.redis_client.setex(key, self.cache_ttl // 2, serialized)  # Меньший TTL для контекстных
            logger.debug(f"Закэшированы контекстные рекомендации для {user_id}")

    def get_cached_contextual(self, user_id: str, context: Dict) -> Optional[List[Dict]]:
        """Получение кэшированных контекстных рекомендаций"""
        if not self.redis_client:
            return None

        key = self._get_cache_key(user_id, context)
        data = self.redis_client.get(key)

        if data:
            return self._deserialize_recommendations(data)
        return None

    def invalidate_user_cache(self, user_id: str):
        """Инвалидация кэша для пользователя"""
        if not self.redis_client:
            return

        pattern = f"rec:{user_id}:*"
        keys = self.redis_client.keys(pattern)
        if keys:
            self.redis_client.delete(*keys)

        top_n_key = self._get_top_n_key(user_id)
        self.redis_client.delete(top_n_key)

        logger.debug(f"Инвалидирован кэш для {user_id}")

    def invalidate_all(self):
        """Полная инвалидация кэша"""
        if not self.redis_client:
            return

        pattern = "rec:*"
        keys = self.redis_client.keys(pattern)
        if keys:
            self.redis_client.delete(*keys)

        logger.info("Полная инвалидация кэша")

    def warm_up_cache(self, user_ids: List[str], recommendation_func):
        """Прогрев кэша для списка пользователей"""
        logger.info(f"Прогрев кэша для {len(user_ids)} пользователей...")

        for user_id in user_ids:
            try:
                recommendations = recommendation_func(user_id)
                if recommendations:
                    self.cache_top_n_recommendations(user_id, recommendations)
            except Exception as e:
                logger.error(f"Ошибка прогрева кэша для {user_id}: {e}")

        logger.info("Прогрев кэша завершен")

    def get_cache_stats(self) -> Dict:
        """Получение статистики кэша"""
        if not self.redis_client:
            return {'status': 'disconnected'}

        try:
            pattern = "rec:*"
            keys = self.redis_client.keys(pattern)

            return {
                'status': 'connected',
                'total_keys': len(keys),
                'top_n_keys': len([k for k in keys if k.startswith('top_n:')]),
                'contextual_keys': len([k for k in keys if k.startswith('rec:') and not k.startswith('top_n:')])
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
