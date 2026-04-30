const movieId = window.location.pathname.split('/').pop();

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

function createPeopleSection(title, people, type) {
    if (!people || people.length === 0) return '';

    const peopleListId = `${type}-list`;
    const isLongList = people.length > 12;

    let html = `
        <div class="info-card-glass">
            <h3>
                ${title}
                <span class="count-badge">${people.length}</span>
            </h3>
            <div class="people-list ${isLongList ? 'collapsed' : ''}" id="${peopleListId}">
    `;

    people.forEach(person => {
        const safePerson = escapeHtml(String(person).trim());
        if (safePerson && safePerson !== 'nan' && safePerson !== '') {
            html += `<span class="person-chip" onclick="window.location.href='/${type}/${encodeURIComponent(safePerson)}'">${safePerson}</span>`;
        }
    });

    html += `</div>`;

    if (isLongList) {
        html += `
            <button class="toggle-btn" onclick="togglePeopleList('${peopleListId}', this)">
                <i class="fas fa-chevron-down"></i> Показать всех (${people.length})
            </button>
        `;
    }

    html += `</div>`;
    return html;
}

window.togglePeopleList = function(listId, button) {
    const list = document.getElementById(listId);
    if (list) {
        if (list.classList.contains('collapsed')) {
            list.classList.remove('collapsed');
            button.innerHTML = '<i class="fas fa-chevron-up"></i> Свернуть';
        } else {
            list.classList.add('collapsed');
            button.innerHTML = '<i class="fas fa-chevron-down"></i> Показать всех';
        }
    }
};

// Функция для получения URL постера с поддержкой разных форматов
function getPosterUrl(movie) {
    // Пробуем разные варианты полей с постером
    if (movie.poster) {
        // Если poster содержит имя файла
        if (!movie.poster.startsWith('http')) {
            console.log('Постер (файл):', movie.poster);
            return `/img/long/${movie.poster}`;
        }
        return movie.poster;
    }
    
    if (movie.poster_url) {
        return movie.poster_url;
    }
    
    if (movie.poster_path) {
        if (!movie.poster_path.startsWith('http')) {
            return `/img/long/${movie.poster_path}`;
        }
        return movie.poster_path;
    }
    
    if (movie.backdrop_path) {
        if (!movie.backdrop_path.startsWith('http')) {
            return `/img/long/${movie.backdrop_path}`;
        }
        return movie.backdrop_path;
    }
    
    console.warn('Постер не найден, используем плейсхолдер');
    return '/img/long/placeholder.jpg';
}

function renderMovieDetails(movie) {
    console.log('Рендеринг фильма:', movie);
    console.log('Постер из API:', movie.poster);
    
    // Получаем URL постера
    const backdropUrl = getPosterUrl(movie);
    console.log('Сформированный URL постера:', backdropUrl);

    const displayTitle = movie.display_title || movie.title_ru || movie.title || 'Без названия';
    const displayPlot = movie.display_plot || movie.plot_ru || movie.plot || 'Описание отсутствует.';
    const displayType = movie.display_type || movie.type_ru || movie.type || 'Фильм';
    const displayAgeLimit = movie.display_age_limit || movie.age_limit_ru || movie.age_limit || '18+';

    const year = movie.year || 'N/A';

    // Преобразование времени (если есть)
    let runtime = '';
    if (movie.runtime) {
        let runtimeStr = String(movie.runtime);
        runtime = runtimeStr
            .replace(/h/g, 'ч')
            .replace(/m/g, 'м')
            .replace(/min/g, 'м');
    }

    // Жанры
    let genresArray = movie.genres || [];
    if (typeof genresArray === 'string') {
        genresArray = genresArray.split(',').map(g => g.trim()).filter(g => g && g !== 'nan');
    }
    if (genresArray.length === 0 && movie.genre_ru && movie.genre_ru !== 'nan') {
        genresArray = [movie.genre_ru];
    } else if (genresArray.length === 0 && movie.genre && movie.genre !== 'nan') {
        genresArray = movie.genre.split(',').map(g => g.trim()).filter(g => g && g !== 'nan');
    }

    // Режиссеры
    let directorsArray = [];
    if (movie.directors_ru) {
        if (typeof movie.directors_ru === 'string') {
            directorsArray = movie.directors_ru.split(',').map(d => d.trim()).filter(d => d && d !== 'nan');
        } else if (Array.isArray(movie.directors_ru)) {
            directorsArray = movie.directors_ru.filter(d => d && d !== 'nan');
        }
    }
    if (directorsArray.length === 0 && movie.directors) {
        if (typeof movie.directors === 'string') {
            directorsArray = movie.directors.split(',').map(d => d.trim()).filter(d => d && d !== 'nan');
        } else if (Array.isArray(movie.directors)) {
            directorsArray = movie.directors.filter(d => d && d !== 'nan');
        }
    }

    // Актеры (показываем только первых 20 для читаемости)
    let actorsArray = [];
    if (movie.actors_ru) {
        if (typeof movie.actors_ru === 'string') {
            actorsArray = movie.actors_ru.split(',').map(a => a.trim()).filter(a => a && a !== 'nan');
        } else if (Array.isArray(movie.actors_ru)) {
            actorsArray = movie.actors_ru.filter(a => a && a !== 'nan');
        }
    }
    if (actorsArray.length === 0 && movie.actors) {
        if (typeof movie.actors === 'string') {
            actorsArray = movie.actors.split(',').map(a => a.trim()).filter(a => a && a !== 'nan');
        } else if (Array.isArray(movie.actors)) {
            actorsArray = movie.actors.filter(a => a && a !== 'nan');
        }
    }

    // Страны
    let countriesArray = [];
    if (movie.countries) {
        if (typeof movie.countries === 'string') {
            countriesArray = movie.countries.split(',').map(c => c.trim()).filter(c => c && c !== 'nan');
        } else if (Array.isArray(movie.countries)) {
            countriesArray = movie.countries.filter(c => c && c !== 'nan');
        }
    }

    let html = `
        <div class="movie-hero-layout">
            <div class="full-width-backdrop" style="background-image: url('${backdropUrl}'); background-size: cover; background-position: center 20%;"></div>
            <div class="movie-info-forward">
                <div class="container">
                    <div class="row">
                        <div class="col-lg-10 content-left-emphasis">
                            <div class="d-flex align-items-center gap-3 flex-wrap">
                                <h1 class="movie-title-super">${escapeHtml(displayTitle)}</h1>
                            </div>

                            <div class="movie-meta-group">
                                ${movie.imdb_rating ? `<span class="meta-chip rating-star"><i class="fab fa-imdb"></i> IMDB: ${movie.imdb_rating}</span>` : ''}
                                ${movie.kinopoisk ? `<span class="meta-chip rating-star"><i class="fas fa-star"></i> Кинопоиск: ${movie.kinopoisk}</span>` : ''}
                                <span class="meta-chip"><i class="far fa-calendar-alt"></i> ${escapeHtml(year)}</span>
                                ${runtime ? `<span class="meta-chip"><i class="far fa-clock"></i> ${runtime}</span>` : ''}
                                <span class="meta-chip"><i class="fas fa-tag"></i> ${escapeHtml(displayType)}</span>
                                <span class="meta-chip age-tag"><i class="fas fa-child"></i> ${escapeHtml(displayAgeLimit)}</span>
                            </div>

                            <div class="genre-cloud">
    `;

    genresArray.forEach(genre => {
        const trimmedGenre = genre.trim();
        if (trimmedGenre && trimmedGenre !== 'nan') {
            html += `<span class="genre-pill" onclick="window.location.href='/genre/${encodeURIComponent(trimmedGenre)}'">${escapeHtml(trimmedGenre)}</span>`;
        }
    });

    html += `
                            </div>
                            
                            <!-- Панель действий пользователя -->
                            <div class="user-actions-panel" style="margin-top: 1.5rem;">
                                <div class="btn-group" role="group">
                                    <button class="btn btn-outline-warning" id="rate-movie-btn" data-bs-toggle="modal" data-bs-target="#rateMovieModal">
                                        <i class="fas fa-star"></i> Оценить
                                    </button>
                                    <button class="btn btn-outline-info" id="add-to-watched-btn">
                                        <i class="fas fa-check-circle"></i> <span id="watched-btn-text">Просмотрено</span>
                                    </button>
                                    <button class="btn btn-outline-danger" id="add-to-favorite-btn">
                                        <i class="fas fa-heart"></i> <span id="favorite-btn-text">В избранное</span>
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Модальное окно для оценки фильма -->
        <div class="modal fade" id="rateMovieModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">Оценить фильм</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">
                        <div class="text-center mb-3">
                            <div id="rate-movie-title" class="h5">${escapeHtml(displayTitle)}</div>
                        </div>

                        <div class="mb-3">
                            <label class="form-label">Ваша оценка (1-10)</label>
                            <div class="rating-stars">
                                <select id="rating-select" class="form-select">
                                    <option value="">Выберите оценку</option>
                                    <option value="1">1 ★ - Ужасно</option>
                                    <option value="2">2 ★ - Очень плохо</option>
                                    <option value="3">3 ★ - Плохо</option>
                                    <option value="4">4 ★ - Ниже среднего</option>
                                    <option value="5">5 ★ - Средне</option>
                                    <option value="6">6 ★ - Выше среднего</option>
                                    <option value="7">7 ★ - Хорошо</option>
                                    <option value="8">8 ★ - Очень хорошо</option>
                                    <option value="9">9 ★ - Отлично</option>
                                    <option value="10">10 ★ - Шедевр</option>
                                </select>
                            </div>
                        </div>

                        <div class="mb-3">
                            <label class="form-label">Ваш отзыв (необязательно)</label>
                            <textarea id="review-text" class="form-control" rows="4" placeholder="Поделитесь впечатлениями о фильме..."></textarea>
                        </div>

                        <div id="rate-error" class="alert alert-danger" style="display: none;"></div>
                        <div id="rate-success" class="alert alert-success" style="display: none;"></div>

                        <div id="current-rating-info" class="text-muted small"></div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Отмена</button>
                        <button type="button" class="btn btn-primary" id="submit-rating-btn">
                            <i class="fas fa-save"></i> Сохранить оценку
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;

    html += `
        <div class="container" style="position: relative; z-index: 20; margin-top: -1rem; padding-bottom: 2rem;">
            <div class="row">
                <div class="col-lg-10">
                    <div class="plot-glass">
                        <i class="fas fa-quote-left me-2" style="color:#8bc34a"></i>
                        ${escapeHtml(displayPlot)}
                    </div>

                    <div class="info-blocks-wrapper">
    `;

    if (directorsArray.length > 0) {
        html += createPeopleSection('Режиссёры', directorsArray, 'director');
    }

    if (actorsArray.length > 0) {
        html += createPeopleSection('Актеры', actorsArray, 'actor');
    }

    if (countriesArray.length > 0) {
        html += `
            <div class="info-card-glass">
                <h3>
                    Страны
                    <span class="count-badge">${countriesArray.length}</span>
                </h3>
                <div class="people-list">
        `;
        countriesArray.forEach(country => {
            if (country && country.trim() && country !== 'nan') {
                html += `<span class="person-chip" onclick="window.location.href='/country/${encodeURIComponent(country)}'">${escapeHtml(country)}</span>`;
            }
        });
        html += `</div></div>`;
    }

    html += `</div></div></div></div></div>`;

    const root = document.getElementById('movie-detail-root');
    if (root) {
        root.innerHTML = html;
        
        // Инициализируем обработчики после рендеринга
        initEventHandlers();
        
        // Проверяем, загрузилось ли изображение постера
        setTimeout(() => {
            const backdropDiv = document.querySelector('.full-width-backdrop');
            if (backdropDiv) {
                const imgUrl = backdropDiv.style.backgroundImage.slice(5, -2);
                console.log('URL фонового изображения:', imgUrl);
                
                // Создаем тест-изображение для проверки доступности
                const testImg = new Image();
                testImg.onload = () => console.log('✅ Постер успешно загружен:', imgUrl);
                testImg.onerror = () => console.error('❌ Ошибка загрузки постера:', imgUrl, '- файл не найден!');
                testImg.src = imgUrl;
            }
        }, 100);
    }
}

// Функция для инициализации обработчиков событий
function initEventHandlers() {
    const addToWatchedBtn = document.getElementById('add-to-watched-btn');
    const addToFavoriteBtn = document.getElementById('add-to-favorite-btn');
    const submitRatingBtn = document.getElementById('submit-rating-btn');
    const logoutBtn = document.getElementById('logout-btn');
    
    if (addToWatchedBtn) {
        addToWatchedBtn.addEventListener('click', handleAddToWatched);
    }
    
    if (addToFavoriteBtn) {
        addToFavoriteBtn.addEventListener('click', handleAddToFavorite);
    }
    
    if (submitRatingBtn) {
        submitRatingBtn.addEventListener('click', handleSubmitRating);
    }
    
    if (logoutBtn) {
        logoutBtn.removeEventListener('click', handleLogout);
        logoutBtn.addEventListener('click', handleLogout);
    }
    
    // Загружаем состояние пользователя
    loadUserMovieState(movieId);
}

// Функция обработки добавления в просмотренные
async function handleAddToWatched() {
    try {
        const response = await fetch(`/api/movie/${movieId}/watched`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        });

        const data = await response.json();

        if (data.success) {
            isWatched = true;
            updateWatchedButton();
            showToast('Успешно', 'Фильм добавлен в просмотренные', 'success');
        } else {
            showToast('Ошибка', data.error || 'Не удалось добавить', 'danger');
        }
    } catch (error) {
        console.error('Ошибка:', error);
        showToast('Ошибка', 'Ошибка соединения', 'danger');
    }
}

// Функция обработки добавления/удаления из избранного
async function handleAddToFavorite() {
    try {
        const method = isFavorite ? 'DELETE' : 'POST';

        const response = await fetch(`/api/movie/${movieId}/favorite`, {
            method: method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        });

        const data = await response.json();

        if (data.success) {
            isFavorite = !isFavorite;
            updateFavoriteButton();

            const message = isFavorite ? 'Фильм добавлен в избранное' : 'Фильм удален из избранного';
            showToast('Успешно', message, 'success');
        } else {
            showToast('Ошибка', data.error || 'Не удалось выполнить действие', 'danger');
        }
    } catch (error) {
        console.error('Ошибка:', error);
        showToast('Ошибка', 'Ошибка соединения', 'danger');
    }
}

// Функция обработки отправки оценки
async function handleSubmitRating() {
    const rating = document.getElementById('rating-select').value;
    const reviewText = document.getElementById('review-text').value;
    const errorDiv = document.getElementById('rate-error');
    const successDiv = document.getElementById('rate-success');

    errorDiv.style.display = 'none';
    successDiv.style.display = 'none';

    if (!rating) {
        errorDiv.textContent = 'Пожалуйста, выберите оценку';
        errorDiv.style.display = 'block';
        return;
    }

    const btn = this;
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Сохранение...';

    try {
        const response = await fetch(`/api/movie/${movieId}/rate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                rating: parseInt(rating),
                review_text: reviewText
            })
        });

        const data = await response.json();

        if (data.success) {
            successDiv.textContent = 'Оценка сохранена! Спасибо за отзыв.';
            successDiv.style.display = 'block';
            currentUserRating = parseInt(rating);
            document.getElementById('current-rating-info').innerHTML = `
                <i class="fas fa-info-circle"></i> Ваша оценка: ${currentUserRating}/10
                ${reviewText ? `<br><small>Отзыв: "${reviewText.substring(0, 100)}"</small>` : ''}
            `;

            setTimeout(() => {
                const modal = bootstrap.Modal.getInstance(document.getElementById('rateMovieModal'));
                if (modal) modal.hide();
                document.getElementById('review-text').value = '';
            }, 1500);
        } else {
            errorDiv.textContent = data.error || 'Ошибка сохранения оценки';
            errorDiv.style.display = 'block';
        }
    } catch (error) {
        errorDiv.textContent = 'Ошибка соединения с сервером';
        errorDiv.style.display = 'block';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-save"></i> Сохранить оценку';
    }
}

function handleLogout(e) {
    e.preventDefault();
    fetch('/api/logout', { method: 'POST' })
        .then(() => window.location.href = '/login')
        .catch(() => window.location.href = '/login');
}

// Переменные для состояния
let currentUserRating = null;
let isWatched = false;
let isFavorite = false;

// Загрузка состояния пользователя для фильма
async function loadUserMovieState(movieId) {
    try {
        // Проверяем оценку
        const ratingResp = await fetch(`/api/movie/${movieId}/user-rating`);
        const ratingData = await ratingResp.json();
        if (ratingData.rating) {
            currentUserRating = ratingData.rating;
            const ratingInfo = document.getElementById('current-rating-info');
            if (ratingInfo) {
                ratingInfo.innerHTML = `
                    <i class="fas fa-info-circle"></i> Ваша текущая оценка: ${currentUserRating}/10
                    ${ratingData.review_text ? `<br><small>Отзыв: "${ratingData.review_text.substring(0, 100)}"</small>` : ''}
                `;
            }
            const ratingSelect = document.getElementById('rating-select');
            if (ratingSelect) {
                ratingSelect.value = currentUserRating;
            }
        }

        // Проверяем просмотрено
        const watchedResp = await fetch(`/api/movie/${movieId}/check-watched`);
        const watchedData = await watchedResp.json();
        isWatched = watchedData.watched;
        updateWatchedButton();

        // Проверяем избранное
        const favResp = await fetch(`/api/movie/${movieId}/check-favorite`);
        const favData = await favResp.json();
        isFavorite = favData.favorite;
        updateFavoriteButton();

    } catch (error) {
        console.error('Ошибка загрузки состояния:', error);
    }
}

function updateWatchedButton() {
    const btnText = document.getElementById('watched-btn-text');
    const btn = document.getElementById('add-to-watched-btn');
    if (!btn || !btnText) return;
    
    if (isWatched) {
        btnText.textContent = 'Просмотрено ✓';
        btn.classList.remove('btn-outline-info');
        btn.classList.add('btn-info');
    } else {
        btnText.textContent = 'Просмотрено';
        btn.classList.remove('btn-info');
        btn.classList.add('btn-outline-info');
    }
}

function updateFavoriteButton() {
    const btnText = document.getElementById('favorite-btn-text');
    const btn = document.getElementById('add-to-favorite-btn');
    if (!btn || !btnText) return;
    
    if (isFavorite) {
        btnText.textContent = 'В избранном ★';
        btn.classList.remove('btn-outline-danger');
        btn.classList.add('btn-danger');
    } else {
        btnText.textContent = 'В избранное';
        btn.classList.remove('btn-danger');
        btn.classList.add('btn-outline-danger');
    }
}

// Функция для показа уведомлений (toast)
function showToast(title, message, type = 'info') {
    let toastContainer = document.querySelector('.toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        toastContainer.style.zIndex = '1100';
        document.body.appendChild(toastContainer);
    }

    const toastId = 'toast-' + Date.now();
    const bgClass = type === 'success' ? 'bg-success' : (type === 'danger' ? 'bg-danger' : 'bg-info');

    const toastHtml = `
        <div id="${toastId}" class="toast" role="alert" aria-live="assertive" aria-atomic="true" data-bs-autohide="true" data-bs-delay="3000">
            <div class="toast-header ${bgClass} text-white">
                <i class="fas ${type === 'success' ? 'fa-check-circle' : (type === 'danger' ? 'fa-exclamation-circle' : 'fa-info-circle')} me-2"></i>
                <strong class="me-auto">${title}</strong>
                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="toast"></button>
            </div>
            <div class="toast-body">
                ${message}
            </div>
        </div>
    `;

    toastContainer.insertAdjacentHTML('beforeend', toastHtml);
    const toastElement = document.getElementById(toastId);
    const toast = new bootstrap.Toast(toastElement);
    toast.show();

    toastElement.addEventListener('hidden.bs.toast', () => {
        toastElement.remove();
    });
}

// Исправление проблемы с фоном модального окна
document.addEventListener('hidden.bs.modal', function (event) {
    const backdrops = document.querySelectorAll('.modal-backdrop');
    backdrops.forEach(backdrop => {
        if (backdrop && backdrop.parentNode) {
            backdrop.parentNode.removeChild(backdrop);
        }
    });

    if (document.body.classList.contains('modal-open')) {
        document.body.classList.remove('modal-open');
    }

    document.body.style.overflow = '';
    document.body.style.paddingRight = '';
});

document.addEventListener('show.bs.modal', function () {
    const existingBackdrops = document.querySelectorAll('.modal-backdrop');
    if (existingBackdrops.length > 1) {
        for (let i = 0; i < existingBackdrops.length - 1; i++) {
            existingBackdrops[i].remove();
        }
    }
});

// Загрузка данных фильма
function loadMovieDetails() {
    const root = document.getElementById('movie-detail-root');
    if (!root) return;

    root.innerHTML = `
        <div class="loading-spinner text-center p-5">
            <i class="fas fa-spinner fa-pulse fa-3x"></i>
            <p class="mt-3">Загружаем киноданные...</p>
        </div>
    `;

    fetch(`/api/movies/${movieId}`)
        .then(response => {
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return response.json();
        })
        .then(movie => {
            console.log('Получен фильм:', movie);
            renderMovieDetails(movie);
            loadSimilarMovies(movieId);
        })
        .catch(error => {
            console.error('Ошибка загрузки фильма:', error);
            root.innerHTML = `
                <div class="container text-center py-5">
                    <div class="alert alert-danger bg-transparent border-danger text-light">
                        <i class="fas fa-exclamation-triangle fs-2"></i><br>
                        Не удалось загрузить информацию о фильме.
                        <br><small>${error.message}</small>
                    </div>
                    <button class="btn btn-outline-light mt-3" onclick="window.location.href='/'">На главную</button>
                </div>
            `;
        });
}

function loadSimilarMovies(id) {
    fetch(`/api/movies/${id}/similar`)
        .then(resp => {
            if (!resp.ok) {
                throw new Error(`HTTP ${resp.status}`);
            }
            return resp.json();
        })
        .then(data => {
            if (data && data.movies && data.movies.length) {
                renderSimilarMovies(data.movies);
                const similarBlock = document.getElementById('similar-movies-block');
                if (similarBlock) {
                    similarBlock.style.display = 'block';
                }
            }
        })
        .catch(err => {
            console.warn("Similar movies error:", err);
            const similarBlock = document.getElementById('similar-movies-block');
            if (similarBlock) {
                similarBlock.style.display = 'none';
            }
        });
}

function renderSimilarMovies(movies) {
    const container = document.getElementById('similar-movies-grid-container');
    if (!container) return;

    let gridHtml = `<div class="similar-grid">`;
    movies.forEach(m => {
        let posterSim = '/img/horizontal/placeholder.jpg';
        if (m.poster) {
            posterSim = `/img/horizontal/${m.poster}`;
        } else if (m.poster_url) {
            posterSim = m.poster_url;
        }
        
        const similarityPercent = m.similarity ? (m.similarity * 100).toFixed(1) : '75';
        const simTitle = m.title_ru || m.title || 'Без названия';

        gridHtml += `
            <div class="sim-card" onclick="window.location.href='/movie/${m.movie_id}'">
                <img src="${posterSim}" class="sim-img" alt="${escapeHtml(simTitle)}" onerror="this.src='/img/horizontal/placeholder.jpg'">
                <div class="sim-info">
                    <div class="sim-title">${escapeHtml(simTitle)}</div>
                    <div class="sim-year">${m.year || '—'}</div>
                </div>
            </div>
        `;
    });
    gridHtml += `</div>`;
    container.innerHTML = gridHtml;
}

// Запускаем загрузку
loadMovieDetails();