import numpy as np
from typing import List, Dict
from sklearn.metrics import ndcg_score, average_precision_score


class RecommendationMetrics:
    """Метрики качества рекомендаций"""

    @staticmethod
    def precision_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Precision@K"""
        recommended_k = recommended[:k]
        relevant_set = set(relevant)
        hits = sum(1 for item in recommended_k if item in relevant_set)
        return hits / k if k > 0 else 0

    @staticmethod
    def recall_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Recall@K"""
        recommended_k = set(recommended[:k])
        relevant_set = set(relevant)
        hits = len(recommended_k & relevant_set)
        return hits / len(relevant_set) if relevant_set else 0

    @staticmethod
    def ndcg_at_k(recommended: List[str], relevant: List[str], relevance_scores: Dict[str, float], k: int) -> float:
        """NDCG@K"""
        recommended_k = recommended[:k]

        # DCG
        dcg = 0.0
        for i, item in enumerate(recommended_k):
            rel = relevance_scores.get(item, 0)
            dcg += rel / np.log2(i + 2)

        # IDCG
        ideal_relevant = sorted(relevant,
                                key=lambda x: relevance_scores.get(x, 0),
                                reverse=True)[:k]
        idcg = 0.0
        for i, item in enumerate(ideal_relevant):
            rel = relevance_scores.get(item, 0)
            idcg += rel / np.log2(i + 2)

        return dcg / idcg if idcg > 0 else 0

    @staticmethod
    def map_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Mean Average Precision@K"""
        if not relevant:
            return 0.0

        relevant_set = set(relevant)
        precisions = []
        hits = 0

        for i, item in enumerate(recommended[:k], 1):
            if item in relevant_set:
                hits += 1
                precisions.append(hits / i)

        return np.mean(precisions) if precisions else 0

    @staticmethod
    def hit_rate_at_k(recommended: List[str], relevant: List[str], k: int) -> float:
        """Hit Rate@K (есть ли хотя бы один релевантный в топ-K)"""
        recommended_k = set(recommended[:k])
        relevant_set = set(relevant)
        return 1.0 if recommended_k & relevant_set else 0.0
