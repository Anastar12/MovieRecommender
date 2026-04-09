import logging
from typing import List, Dict, Set, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


class Postprocessor:
    """Модуль постобработки и фильтрации"""

    def __init__(self, data_provider, config: Dict = None):
        self.data = data_provider
        self.config = config or {}

        # Ограничения
        self.max_per_genre = self.config.get('max_per_genre', 5)
        self.min_rating_threshold = self.config.get('min_rating_threshold', 0.0)

    def process(self, recommendations: List[Dict], context: Dict) -> List[Dict]:
        """Постобработка рекомендаций"""
        if not recommendations:
            return []

        logger.info(f"Постобработка {len(recommendations)} рекомендаций")

        # 1. Исключение уже просмотренного
        filtered = self._filter_watched(recommendations, context)

        # 2. Учет ограничений
        filtered = self._apply_constraints(filtered, context)

        # 3. Чередование жанров
        filtered = self._alternate_genres(filtered)

        # 4. Добавление объяснений
        filtered = self._add_explanations(filtered, context)

        # 5. Обогащение метаданными
        filtered = self._enrich_metadata(filtered)

        logger.info(f"После постобработки: {len(filtered)} рекомендаций")

        return filtered

    def _filter_watched(self, recommendations: List[Dict], context: Dict) -> List[Dict]:
        """Фильтрация уже просмотренных фильмов"""
        watched_movies = context.get('user_rated_movies', set())

        if not watched_movies:
            return recommendations

        filtered = []
        for rec in recommendations:
            if rec['movie_id'] not in watched_movies:
                filtered.append(rec)

        return filtered

    def _apply_constraints(self, recommendations: List[Dict], context: Dict) -> List[Dict]:
        """Применение ограничений"""
        constraints = context.get('constraints', {})

        if not constraints:
            return recommendations

        filtered = []

        for rec in recommendations:
            # Проверка минимального рейтинга
            if rec.get('predicted_rating', 0) < self.min_rating_threshold:
                continue

            # Проверка возрастных ограничений
            if 'age_limit' in constraints:
                user_age = constraints.get('user_age', 18)
                movie_age_limit = self._parse_age_limit(rec.get('age_limit', 'G'))
                if movie_age_limit > user_age:
                    continue

            # Проверка исключенных жанров
            excluded_genres = constraints.get('excluded_genres', set())
            if excluded_genres:
                movie_genres = set(rec.get('genres', []))
                if movie_genres & excluded_genres:
                    continue

            filtered.append(rec)

        return filtered

    def _parse_age_limit(self, age_limit: str) -> int:
        """Парсинг возрастного ограничения"""
        if not age_limit:
            return 0

        age_str = str(age_limit)
        if 'PG-13' in age_str or '13' in age_str:
            return 13
        elif 'R' in age_str or '17' in age_str:
            return 17
        elif 'NC-17' in age_str:
            return 18
        elif 'PG' in age_str:
            return 10
        elif 'G' in age_str:
            return 0

        return 0

    def _alternate_genres(self, recommendations: List[Dict]) -> List[Dict]:
        """Чередование жанров для разнообразия"""
        if len(recommendations) < 3:
            return recommendations

        # Группировка по жанрам
        by_genre = defaultdict(list)
        for rec in recommendations:
            genres = rec.get('genres', [])
            main_genre = genres[0] if genres else 'unknown'
            by_genre[main_genre].append(rec)

        # Чередование
        alternated = []
        max_per_genre = self.max_per_genre

        while len(alternated) < len(recommendations):
            added = False
            for genre in list(by_genre.keys()):
                if by_genre[genre] and len([r for r in alternated if genre in r.get('genres', [])]) < max_per_genre:
                    alternated.append(by_genre[genre].pop(0))
                    added = True
                    if len(alternated) >= len(recommendations):
                        break

            if not added:
                # Добавляем оставшиеся
                for genre in by_genre:
                    alternated.extend(by_genre[genre])
                break

        return alternated

    def _add_explanations(self, recommendations: List[Dict], context: Dict) -> List[Dict]:
        """Добавление объяснений к рекомендациям"""
        for rec in recommendations:
            explanation = self._generate_explanation(rec, context)
            rec['explanation'] = explanation

        return recommendations

    def _generate_explanation(self, recommendation: Dict, context: Dict) -> str:
        """Генерация объяснения для рекомендации"""
        explanations = []

        # На основе источника
        source = recommendation.get('source', '')
        if source == 'collaborative':
            explanations.append("похожим пользователям понравился")
        elif source == 'content':
            explanations.append("похож на фильмы, которые вам нравятся")
        elif source == 'svd':
            explanations.append("рекомендован на основе ваших предпочтений")
        elif source == 'popular':
            explanations.append("популярен среди пользователей")

        # На основе жанров
        genres = recommendation.get('genres', [])
        user_genres = context.get('user_genre_preferences', {})

        matching_genres = [g for g in genres if g in user_genres]
        if matching_genres:
            explanations.append(f"вам нравятся фильмы жанра {matching_genres[0]}")

        # Комбинирование
        if explanations:
            return f"Рекомендуем, так как {explanations[0]}"

        return "Рекомендован на основе ваших предпочтений"

    def _enrich_metadata(self, recommendations: List[Dict]) -> List[Dict]:
        """Обогащение метаданными"""
        enriched = []

        for rec in recommendations:
            # Добавление постера
            if 'poster' not in rec:
                title = rec.get('title', '')
                year = rec.get('year', '')
                rec['poster'] = self._get_poster_filename(title, year)

            # Нормализация рейтинга
            if 'predicted_rating' in rec:
                rec['rating_display'] = round(rec['predicted_rating'] * 10, 1)

            enriched.append(rec)

        return enriched

    def _get_poster_filename(self, title: str, year: str = None) -> str:
        """Генерация имени файла постера"""
        if not title:
            return 'placeholder.jpg'

        clean_title = str(title).lower()
        clean_title = ''.join(c if c.isalnum() or c == ' ' else '' for c in clean_title)
        clean_title = clean_title.replace(' ', '_').strip('_')

        while '__' in clean_title:
            clean_title = clean_title.replace('__', '_')

        if year and str(year) not in ['nan', 'None', '']:
            year = str(year).replace('-', '–')
            return f"{clean_title}_{year}.jpg"

        return f"{clean_title}.jpg"
