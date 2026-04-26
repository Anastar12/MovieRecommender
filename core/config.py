import os


class DatabaseConfig:
    def __init__(self):
        self.host = "localhost"
        self.port = 5432
        self.database = "postgres"
        self.user = "postgres"
        self.password = "1234"


class RedisConfig:
    def __init__(self):
        self.host = "localhost"
        self.port = 6379
        self.db = 0
        self.password = None


class OfflineConfig:
    def __init__(self, models_path: str):
        self.models_path = models_path
        self.cache_path = "cache/"
        self.retrain_interval_hours = 24
        self.top_n_cached = 100
        self.cache_ttl_seconds = 3600


class OnlineConfig:
    def __init__(self):
        self.candidate_limit = 200
        self.final_top_n = 50
        self.cold_start_fallback = "popular"
        self.weights = {
            'collaborative': 0.35,
            'content': 0.25,
            'svd': 0.20,
            'als': 0.20
        }


class AppConfig:
    def __init__(self):
        # Получаем корневую директорию проекта
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Путь к моделям
        models_path = os.path.join(self.base_dir, 'api', 'models')

        # Создаем директорию для моделей
        os.makedirs(models_path, exist_ok=True)

        # Инициализация подконфигов
        self.db = DatabaseConfig()
        self.redis = RedisConfig()
        self.offline = OfflineConfig(models_path)
        self.online = OnlineConfig()


# Создаем глобальный экземпляр
config = AppConfig()