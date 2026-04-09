import logging
import json
from datetime import datetime
from typing import Dict, List, Optional
import asyncio
from collections import deque
import aiofiles

logger = logging.getLogger(__name__)


class FeedbackLogger:
    """Модуль логирования обратной связи"""

    def __init__(self, db_connection=None, log_path: str = 'logs/'):
        self.db_connection = db_connection
        self.log_path = log_path

        # Буфер для batch-записи
        self.buffer = deque(maxlen=1000)
        self.buffer_lock = asyncio.Lock()

        # Периодическая запись
        self.flush_task = None

    async def start(self):
        """Запуск фоновой задачи записи"""
        self.flush_task = asyncio.create_task(self._periodic_flush())

    async def stop(self):
        """Остановка фоновой задачи"""
        if self.flush_task:
            self.flush_task.cancel()
            await self.flush()

    async def log_interaction(self, user_id: str, movie_id: str, interaction_type: str,
                              context: Dict = None, metadata: Dict = None):
        """Логирование взаимодействия пользователя"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'movie_id': movie_id,
            'interaction_type': interaction_type,  # view, rating, click, share
            'context': context or {},
            'metadata': metadata or {}
        }

        # Добавление в буфер
        async with self.buffer_lock:
            self.buffer.append(event)

        # Немедленная запись для важных событий
        if interaction_type in ['rating', 'purchase']:
            await self._write_event(event)

        logger.debug(f"Logged {interaction_type} for user {user_id}, movie {movie_id}")

    async def log_rating(self, user_id: str, movie_id: str, rating: float,
                         context: Dict = None):
        """Логирование оценки"""
        await self.log_interaction(user_id, movie_id, 'rating', context, {'rating': rating})

        # Обновление пользовательского профиля (асинхронно)
        asyncio.create_task(self._update_user_profile(user_id, movie_id, rating))

    async def log_view(self, user_id: str, movie_id: str, duration_seconds: int = None,
                       context: Dict = None):
        """Логирование просмотра"""
        metadata = {}
        if duration_seconds:
            metadata['duration_seconds'] = duration_seconds

        await self.log_interaction(user_id, movie_id, 'view', context, metadata)

    async def log_click(self, user_id: str, movie_id: str, position: int = None,
                        context: Dict = None):
        """Логирование клика по рекомендации"""
        metadata = {}
        if position is not None:
            metadata['position'] = position

        await self.log_interaction(user_id, movie_id, 'click', context, metadata)

    async def log_recommendations_served(self, user_id: str, recommendations: List[Dict],
                                         request_id: str = None):
        """Логирование показанных рекомендаций"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'request_id': request_id,
            'event_type': 'recommendations_served',
            'recommendations': [
                {
                    'movie_id': r.get('movie_id'),
                    'position': idx,
                    'score': r.get('final_score', 0)
                }
                for idx, r in enumerate(recommendations)
            ]
        }

        await self._write_event(event)
        logger.info(f"Served {len(recommendations)} recommendations to {user_id}")

    async def _write_event(self, event: Dict):
        """Запись события"""
        # Запись в файл
        await self._write_to_file(event)

        # Запись в БД (если есть)
        if self.db_connection:
            await self._write_to_db(event)

    async def _write_to_file(self, event: Dict):
        """Запись в JSON-файл"""
        try:
            date_str = datetime.now().strftime('%Y-%m-%d')
            filename = f"{self.log_path}feedback_{date_str}.jsonl"

            async with aiofiles.open(filename, 'a', encoding='utf-8') as f:
                await f.write(json.dumps(event, ensure_ascii=False) + '\n')
        except Exception as e:
            logger.error(f"Ошибка записи в файл: {e}")

    async def _write_to_db(self, event: Dict):
        """Запись в БД"""
        try:
            # Асинхронная запись в PostgreSQL
            query = """
                INSERT INTO user_feedback 
                (timestamp, user_id, movie_id, event_type, event_data)
                VALUES (%s, %s, %s, %s, %s)
            """

            async with self.db_connection.cursor() as cursor:
                await cursor.execute(query, (
                    event['timestamp'],
                    event['user_id'],
                    event.get('movie_id'),
                    event['interaction_type'] if 'interaction_type' in event else event['event_type'],
                    json.dumps(event)
                ))
                await self.db_connection.commit()
        except Exception as e:
            logger.error(f"Ошибка записи в БД: {e}")

    async def _periodic_flush(self):
        """Периодическая запись буфера"""
        while True:
            try:
                await asyncio.sleep(10)  # Каждые 10 секунд
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в periodic_flush: {e}")

    async def flush(self):
        """Принудительная запись буфера"""
        async with self.buffer_lock:
            events = list(self.buffer)
            self.buffer.clear()

        if events:
            for event in events:
                await self._write_event(event)
            logger.info(f"Flushed {len(events)} events")

    async def _update_user_profile(self, user_id: str, movie_id: str, rating: float):
        """Обновление пользовательского профиля на основе новой оценки"""
        try:
            # Это может быть вызов офлайн-обновления профиля
            # или добавление в очередь для переобучения
            logger.debug(f"Schedule profile update for {user_id}")

            # TODO: Добавить в очередь для офлайн-обновления
            pass
        except Exception as e:
            logger.error(f"Ошибка обновления профиля: {e}")

    def get_stats(self) -> Dict:
        """Получение статистики логирования"""
        return {
            'buffer_size': len(self.buffer),
            'log_path': self.log_path,
            'is_running': self.flush_task is not None and not self.flush_task.done()
        }
