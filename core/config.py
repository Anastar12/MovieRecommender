import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "1234"


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None


@dataclass
class OfflineConfig:
    models_path: str = "models/"
    cache_path: str = "cache/"
    retrain_interval_hours: int = 24
    top_n_cached: int = 100
    cache_ttl_seconds: int = 3600


@dataclass
class OnlineConfig:
    candidate_limit: int = 200
    final_top_n: int = 50
    cold_start_fallback: str = "popular"  # popular, recent, random
    weights: dict = None

    def __post_init__(self):
        if self.weights is None:
            self.weights = {
                'collaborative': 0.35,
                'content': 0.25,
                'svd': 0.20,
                'als': 0.20
            }


@dataclass
class AppConfig:
    db: DatabaseConfig = None
    redis: RedisConfig = None
    offline: OfflineConfig = None
    online: OnlineConfig = None

    def __post_init__(self):
        if self.db is None:
            self.db = DatabaseConfig()
        if self.redis is None:
            self.redis = RedisConfig()
        if self.offline is None:
            self.offline = OfflineConfig()
        if self.online is None:
            self.online = OnlineConfig()
