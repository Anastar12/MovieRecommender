import asyncio
import logging
import warnings
import os
import sys

from core.config import config, AppConfig
from offline.data_pipeline import DataPipeline
from offline.model_trainer import ModelTrainer

# Добавляем путь к корневой директории
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Игнорируем предупреждения pandas и sklearn
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def pre_train():
    """Предварительное обучение всех моделей"""
    logger.info("=" * 60)
    logger.info("НАЧАЛО ПРЕДВАРИТЕЛЬНОГО ОБУЧЕНИЯ МОДЕЛЕЙ")
    logger.info("=" * 60)

    # Используем правильный путь к моделям (через offline)
    logger.info(f"Путь к моделям: {config.offline.models_path}")
    logger.info(f"База данных: {config.db.host}:{config.db.port}/{config.db.database}")

    data_pipeline = DataPipeline(
        db_config={
            'host': config.db.host,
            'port': config.db.port,
            'database': config.db.database,
            'user': config.db.user,
            'password': config.db.password
        },
        models_path=config.offline.models_path
    )

    model_trainer = ModelTrainer(models_path=config.offline.models_path)

    try:
        # Шаг 1: Загрузка данных
        logger.info("\n[1/9] Загрузка данных из PostgreSQL...")
        if not await data_pipeline.load_data():
            logger.error("Не удалось загрузить данные")
            return False

        logger.info(f"✓ Загружено {len(data_pipeline.movies_df)} фильмов")
        logger.info(f"✓ Загружено {len(data_pipeline.reviews_df)} ревью")
        logger.info(f"✓ Загружено {len(data_pipeline.user_main_df)} пользователей")

        # Шаг 2: Предобработка данных
        logger.info("\n[2/9] Предобработка данных...")
        data_pipeline.preprocess_data()
        logger.info("✓ Предобработка завершена")

        # Шаг 3: Создание векторов признаков
        logger.info("\n[3/9] Создание векторов признаков...")
        data_pipeline.create_feature_vectors()
        logger.info("✓ Векторы признаков созданы")

        # Шаг 4: Создание user-item матрицы
        logger.info("\n[4/9] Создание user-item матрицы...")
        user_item_matrix = data_pipeline.create_user_item_matrix()

        if user_item_matrix is None:
            logger.warning("⚠ User-item матрица не создана (недостаточно данных)")
        else:
            logger.info(f"✓ User-item матрица: {user_item_matrix.shape}, ненулевых: {user_item_matrix.nnz}")

        # Шаг 5: Вычисление популярности и свежести
        logger.info("\n[5/9] Вычисление популярности и свежести...")
        popularity_scores = data_pipeline.compute_popularity_scores()
        recency_scores = data_pipeline.compute_recency_scores()
        logger.info(f"✓ Popularity scores: {len(popularity_scores)}")
        logger.info(f"✓ Recency scores: {len(recency_scores)}")

        # Шаг 6: Обучение SVD модели
        logger.info("\n[6/9] Обучение SVD модели...")
        svd_result = None
        if user_item_matrix is not None:
            svd_result = await model_trainer.build_svd_model(user_item_matrix)
            if svd_result:
                logger.info(f"✓ SVD модель обучена: {svd_result.get('explained_variance', 0):.4f} дисперсии")
            else:
                logger.warning("⚠ SVD модель не обучена")
        else:
            logger.warning("⚠ Пропуск SVD (нет user-item матрицы)")

        # Шаг 7: Обучение NMF модели
        logger.info("\n[7/9] Обучение NMF модели...")
        nmf_result = None
        if user_item_matrix is not None:
            nmf_result = await model_trainer.build_nmf_model(user_item_matrix)
            if nmf_result:
                logger.info(f"✓ NMF модель обучена: ошибка {nmf_result.get('reconstruction_error', 0):.4f}")
            else:
                logger.info("ℹ NMF модель пропущена")
        else:
            logger.info("ℹ Пропуск NMF (нет user-item матрицы)")

        # Шаг 8: Обучение ALS модели
        logger.info("\n[8/9] Обучение ALS модели...")
        als_result = None
        if user_item_matrix is not None:
            als_result = await model_trainer.build_als_model(user_item_matrix)
            if als_result:
                logger.info(f"✓ ALS модель обучена с {als_result.get('factors', 50)} факторами")
            else:
                logger.warning("⚠ ALS модель не обучена")
        else:
            logger.warning("⚠ Пропуск ALS (нет user-item матрицы)")

        # Шаг 9: Обучение модели предсказания оценок
        logger.info("\n[9/9] Обучение модели предсказания оценок...")
        rating_predictor_result = None
        if user_item_matrix is not None and model_trainer.user_factors is not None:
            rating_predictor_result = await model_trainer.build_rating_predictor(
                user_item_matrix,
                model_trainer.user_factors,
                model_trainer.item_factors,
                model_trainer.user_factors_nmf,
                model_trainer.item_factors_nmf,
                popularity_scores,
                recency_scores
            )
            if rating_predictor_result:
                train_r2 = rating_predictor_result.get('train_r2', 0)
                val_r2 = rating_predictor_result.get('val_r2', 0)
                train_rmse = rating_predictor_result.get('train_rmse', 0)
                val_rmse = rating_predictor_result.get('val_rmse', 0)
                mae = rating_predictor_result.get('mae', 0)
                train_size = rating_predictor_result.get('train_size', 0)
                val_size = rating_predictor_result.get('val_size', 0)

                logger.info(f"✓ Модель предсказания оценок обучена!")
                logger.info(f"  - Обучающая выборка: {train_size} примеров")
                logger.info(f"  - Валидационная выборка: {val_size} примеров")
                logger.info(f"  - Обучающий R²: {train_r2:.4f}")
                logger.info(f"  - Валидационный R²: {val_r2:.4f}")
                logger.info(f"  - Обучающий RMSE: {train_rmse:.4f}")
                logger.info(f"  - Валидационный RMSE: {val_rmse:.4f}")
                logger.info(f"  - MAE: {mae:.4f}")

                # Дополнительная интерпретация метрики (по валидационному R²)
                if val_r2 > 0.7:
                    logger.info(f"  - Интерпретация: 🌟 Отличное качество предсказаний!")
                elif val_r2 > 0.5:
                    logger.info(f"  - Интерпретация: ✅ Хорошее качество предсказаний")
                elif val_r2 > 0.3:
                    logger.info(f"  - Интерпретация: ⚠ Удовлетворительное качество предсказаний")
                else:
                    logger.info(f"  - Интерпретация: ❌ Низкое качество предсказаний, требуется дообучение")
            else:
                logger.warning("⚠ Модель предсказания оценок не обучена (недостаточно данных)")
        else:
            logger.warning("⚠ Пропуск модели предсказания (нет user-item матрицы или факторов)")

        # Сохранение всех моделей
        logger.info("\n[Сохранение] Сохранение моделей...")

        # Создаем combined_features если нужно
        combined_features = None
        try:
            from sklearn.preprocessing import normalize
            from scipy.sparse import hstack

            if (hasattr(data_pipeline, 'tfidf_matrix') and data_pipeline.tfidf_matrix is not None and
                    hasattr(data_pipeline, 'genre_vectors') and data_pipeline.genre_vectors is not None and
                    hasattr(data_pipeline, 'actor_vectors') and data_pipeline.actor_vectors is not None and
                    hasattr(data_pipeline, 'director_vectors') and data_pipeline.director_vectors is not None):
                tfidf_norm = normalize(data_pipeline.tfidf_matrix, norm='l2')
                genre_norm = normalize(data_pipeline.genre_vectors, norm='l2')
                actor_norm = normalize(data_pipeline.actor_vectors, norm='l2')
                director_norm = normalize(data_pipeline.director_vectors, norm='l2')

                combined_features = hstack([
                    tfidf_norm * 0.4,
                    genre_norm * 0.3,
                    actor_norm * 0.2,
                    director_norm * 0.1
                ])
                logger.info("✓ combined_features создан")

                # Обучаем индекс схожести
                await model_trainer.build_similarity_index(combined_features)
        except Exception as e:
            logger.warning(f"Не удалось создать combined_features: {e}")

        # Собираем все данные для сохранения
        data_for_saving = {
            'movies_df': data_pipeline.movies_df,
            'reviews_df': data_pipeline.reviews_df,
            'user_main_df': data_pipeline.user_main_df,
            'genres_df': data_pipeline.genres_df,
            'subgenres_df': getattr(data_pipeline, 'subgenres_df', None),
            'user_genres_df': getattr(data_pipeline, 'user_genres_df', None),
            'user_years_df': getattr(data_pipeline, 'user_years_df', None),
            'countries_df': getattr(data_pipeline, 'countries_df', None),
            'tfidf_vectorizer': getattr(data_pipeline, 'tfidf_vectorizer', None),
            'tfidf_matrix': getattr(data_pipeline, 'tfidf_matrix', None),
            'genre_vectors': getattr(data_pipeline, 'genre_vectors', None),
            'actor_vectors': getattr(data_pipeline, 'actor_vectors', None),
            'director_vectors': getattr(data_pipeline, 'director_vectors', None),
            'user_item_matrix': user_item_matrix,
            'popularity_scores': popularity_scores,
            'recency_scores': recency_scores,
            'combined_features': combined_features,
            'user_indices': getattr(data_pipeline, 'user_indices', None),
            'movie_indices': getattr(data_pipeline, 'movie_indices', None),
            'user_list': getattr(data_pipeline, 'user_list', None),
            'movie_list': getattr(data_pipeline, 'movie_list', None),
            'genre_list': getattr(data_pipeline, 'genre_list', None),
            'top_actors': getattr(data_pipeline, 'top_actors', None),
            'top_directors': getattr(data_pipeline, 'top_directors', None),
            'data_hash': getattr(data_pipeline, 'data_hash', None)
        }

        # Сохраняем данные через data_pipeline
        data_pipeline.save_data(data_for_saving)

        # Сохраняем модели через model_trainer
        model_trainer.save_models()

        logger.info("✓ Все данные и модели сохранены")

        # Финальная статистика
        logger.info("\n" + "=" * 60)
        logger.info("ИТОГОВЫЕ МЕТРИКИ ОБУЧЕНИЯ")
        logger.info("=" * 60)

        logger.info(f"\n📊 SVD:")
        logger.info(f"   - Компонент: {svd_result.get('n_components', 'N/A') if svd_result else 'N/A'}")
        logger.info(
            f"   - Объясненная дисперсия: {svd_result.get('explained_variance', 0):.4f} ({svd_result.get('explained_variance', 0) * 100:.2f}%)" if svd_result else "   - Не обучена")

        logger.info(f"\n📊 NMF:")
        logger.info(f"   - Компонент: {nmf_result.get('n_components', 'N/A') if nmf_result else 'N/A'}")
        logger.info(
            f"   - Ошибка реконструкции: {nmf_result.get('reconstruction_error', 0):.4f}" if nmf_result else "   - Не обучена")

        logger.info(f"\n📊 ALS:")
        logger.info(
            f"   - Факторы: {als_result.get('factors', 'N/A') if als_result else 'N/A'}" if als_result else "   - Не обучена")

        logger.info(f"\n📊 Модель предсказания оценок (Rating Predictor):")
        if rating_predictor_result:
            train_r2 = rating_predictor_result.get('train_r2', 0)
            val_r2 = rating_predictor_result.get('val_r2', 0)
            train_rmse = rating_predictor_result.get('train_rmse', 0)
            val_rmse = rating_predictor_result.get('val_rmse', 0)

            logger.info(f"   - Обучающий R²: {train_r2:.4f}")
            logger.info(f"   - Валидационный R²: {val_r2:.4f}")
            logger.info(f"   - Обучающий RMSE: {train_rmse:.4f}")
            logger.info(f"   - Валидационный RMSE: {val_rmse:.4f}")
            logger.info(f"   - Размер обучающей выборки: {rating_predictor_result.get('train_size', 0)}")

            # Оценка по валидационному R²
            if val_r2 > 0.7:
                logger.info(f"   - Оценка: 🌟 Отлично!")
            elif val_r2 > 0.5:
                logger.info(f"   - Оценка: ✅ Хорошо")
            elif val_r2 > 0.3:
                logger.info(f"   - Оценка: ⚠ Удовлетворительно")
            else:
                logger.info(f"   - Оценка: ❌ Низкое качество")
        else:
            logger.info(f"   - Не обучена (недостаточно данных)")

        models_path = config.offline.models_path
        model_files = os.listdir(models_path) if os.path.exists(models_path) else []

        logger.info(f"\n💾 Файлы моделей: {len(model_files)}")

        # Показываем размеры основных файлов
        main_files = ['svd_model.pkl', 'movies_df.pkl', 'user_main_df.pkl', 'rating_predictor.pkl']
        for f in main_files:
            if f in model_files:
                size = os.path.getsize(os.path.join(models_path, f)) / 1024 / 1024
                logger.info(f"  ✓ {f} ({size:.2f} MB)")
            else:
                logger.info(f"  ✗ {f} (отсутствует)")

        logger.info("\n" + "=" * 60)
        logger.info("ПРЕДВАРИТЕЛЬНОЕ ОБУЧЕНИЕ УСПЕШНО ЗАВЕРШЕНО!")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error(f"\n❌ Ошибка при обучении моделей: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Главная функция"""
    import argparse

    parser = argparse.ArgumentParser(description='Предварительное обучение моделей рекомендательной системы')
    parser.add_argument('--check', action='store_true', help='Только проверить наличие моделей')
    parser.add_argument('--force', action='store_true', help='Принудительное переобучение (удаляет старые модели)')

    args = parser.parse_args()

    if args.check:
        config = AppConfig()
        models_path = config.offline.models_path
        if os.path.exists(models_path):
            files = os.listdir(models_path)
            logger.info(f"Модели в {models_path}: {len(files)} файлов")
            if 'rating_predictor.pkl' in files:
                logger.info("✓ rating_predictor.pkl существует")
                size = os.path.getsize(os.path.join(models_path, 'rating_predictor.pkl')) / 1024 / 1024
                logger.info(f"  Размер: {size:.2f} MB")
            else:
                logger.info("✗ rating_predictor.pkl НЕ СУЩЕСТВУЕТ")
        else:
            logger.info(f"Директория {models_path} не существует")
        return

    if args.force:
        config = AppConfig()
        models_path = config.offline.models_path
        import shutil
        if os.path.exists(models_path):
            logger.info(f"Удаление старых моделей из {models_path}...")
            shutil.rmtree(models_path)
            logger.info("Старые модели удалены")

    success = await pre_train()

    if success:
        logger.info("\n✅ Готово! Теперь можно запускать основное приложение.")
    else:
        logger.error("\n❌ Обучение не удалось. Проверьте подключение к БД и наличие данных.")


if __name__ == "__main__":
    asyncio.run(main())