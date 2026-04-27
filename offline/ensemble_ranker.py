from sklearn.ensemble import VotingRegressor, StackingRegressor
from sklearn.linear_model import Ridge
import joblib


class EnsembleRanker:
    """Ансамбль моделей для улучшения качества"""

    def __init__(self, models_path: str):
        self.models_path = models_path
        self.ensemble_model = None
        self.lgb_model = None
        self.xgb_model = None
        self.catboost_model = None
        self.nn_model = None

    async def train_ensemble(self, X_train: np.ndarray, y_train: np.ndarray):
        """Обучение ансамбля из 4 моделей"""

        # 1. LightGBM
        import lightgbm as lgb
        self.lgb_model = lgb.LGBMRegressor(
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=255,
            random_state=42,
            verbose=-1
        )

        # 2. XGBoost
        import xgboost as xgb
        self.xgb_model = xgb.XGBRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=8,
            random_state=42,
            verbose=0
        )

        # 3. CatBoost
        from catboost import CatBoostRegressor
        self.catboost_model = CatBoostRegressor(
            iterations=300,
            learning_rate=0.05,
            depth=8,
            random_seed=42,
            verbose=False
        )

        # 4. Voting ensemble
        self.ensemble_model = VotingRegressor([
            ('lgb', self.lgb_model),
            ('xgb', self.xgb_model),
            ('catboost', self.catboost_model)
        ], weights=[1, 1, 1])

        # Обучение
        self.ensemble_model.fit(X_train, y_train)

        # 5. Stacking для лучшего качества
        stacking_model = StackingRegressor(
            estimators=[
                ('lgb', self.lgb_model),
                ('xgb', self.xgb_model),
                ('catboost', self.catboost_model)
            ],
            final_estimator=Ridge(alpha=1.0)
        )

        stacking_model.fit(X_train, y_train)

        return stacking_model

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Предсказание ансамблем"""
        if self.ensemble_model:
            return self.ensemble_model.predict(X)
        return np.zeros(len(X))
