// Глобальные переменные
let movieModal = null;
let currentMovieId = null;

// Ждем полной загрузки DOM
document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM загружен');
    console.log('Текущий пользователь:', window.currentUsername);

    // Инициализация модального окна
    const modalElement = document.getElementById('movieModal');
    if (modalElement) {
        movieModal = new bootstrap.Modal(modalElement);
    }

    // Настройка слайдеров
    setupSliders();

    // Обработка навигации
    document.body.addEventListener('click', function(e) {
        const viewLink = e.target.closest('[data-view]');
        if (viewLink) {
            e.preventDefault();
            const view = viewLink.getAttribute('data-view');
            console.log('Переключение на вкладку:', view);
            switchView(view);
        }
    });

    // Поиск при нажатии Enter
    const searchInput = document.getElementById('search-input');
    if (searchInput) {
        searchInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                searchMovies();
            }
        });
    }

    // Обработка выхода
    const logoutBtn = document.getElementById('logout-btn');
    if (logoutBtn) {
        logoutBtn.addEventListener('click', function(e) {
            e.preventDefault();
            logout();
        });
    }

    // Загружаем статистику текущего пользователя
    loadUserStats();

    // Показываем главную вкладку по умолчанию
    switchView('home');
});

// Выход из системы
function logout() {
    fetch('/api/logout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            window.location.href = '/login';
        }
    })
    .catch(error => {
        console.error('Ошибка при выходе:', error);
        window.location.href = '/login';
    });
}

// Переключение между представлениями
function switchView(viewName) {
    console.log('switchView вызван с:', viewName);

    const views = document.querySelectorAll('.view');
    views.forEach(view => {
        view.classList.remove('active');
    });

    const activeView = document.getElementById(`${viewName}-view`);
    if (activeView) {
        activeView.classList.add('active');
        console.log(`Показана вкладка: ${viewName}-view`);
    } else {
        console.error(`Вкладка ${viewName}-view не найдена`);
    }

    const navLinks = document.querySelectorAll('[data-view]');
    navLinks.forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('data-view') === viewName) {
            link.classList.add('active');
        }
    });
}

// Загрузка статистики текущего пользователя
function loadUserStats() {
    if (!window.currentUserUrl) {
        console.error('Пользователь не авторизован');
        return;
    }

    console.log('Загрузка статистики для пользователя:', window.currentUserUrl);

    fetch(`/api/user/${encodeURIComponent(window.currentUserUrl)}`)
        .then(response => response.json())
        .then(stats => {
            displayUserStats(stats);
        })
        .catch(error => console.error('Ошибка загрузки статистики:', error));
}

// Отображение статистики пользователя
function displayUserStats(stats) {
    const container = document.getElementById('user-stats');

    if (!stats || stats.error) {
        if (container) container.innerHTML = '<div class="alert alert-warning">Статистика не найдена</div>';
        return;
    }

    let html = '<div class="user-stats">';
    html += `<h6><i class="fas fa-user"></i> ${escapeHtml(stats.username)}</h6>`;
    html += `<div class="stat-item">
                <span class="stat-label">Всего оценок:</span>
                <span class="stat-value">${stats.total_ratings || 0}</span>
             </div>`;
    html += `<div class="stat-item">
                <span class="stat-label">Дата регистрации:</span>
                <span class="stat-value">${stats.joined || 'N/A'}</span>
             </div>`;

    if (stats.top_genres && stats.top_genres.length > 0) {
        html += '<div class="stat-item"><strong>Любимые жанры:</strong><br>';
        stats.top_genres.slice(0, 3).forEach(genre => {
            html += `<span class="genre-tag clickable" onclick="showMoviesByGenre('${escapeHtml(genre.genre)}')"
                         style="cursor: pointer;">${escapeHtml(genre.genre)} (${genre.ratings} оценок)</span>`;
        });
        html += '</div>';
    }

    if (stats.top_years && stats.top_years.length > 0) {
        html += '<div class="stat-item"><strong>Любимые годы:</strong><br>';
        stats.top_years.slice(0, 3).forEach(year => {
            html += `<span class="genre-tag clickable" onclick="showMoviesByYear(${year.year})"
                         style="cursor: pointer;">${year.year} (${year.ratings} оценок)</span>`;
        });
        html += '</div>';
    }

    if (stats.rating_distribution) {
        html += '<div class="stat-item"><strong>Распределение оценок:</strong><br>';
        stats.rating_distribution.forEach((count, i) => {
            if (count > 0) {
                html += `<span class="genre-tag">${i+1}★: ${count}</span>`;
            }
        });
        html += '</div>';
    }

    html += '</div>';
    if (container) container.innerHTML = html;
}

// Получение рекомендаций
function getRecommendations() {
    if (!window.currentUserUrl) {
        alert('Пользователь не авторизован');
        return;
    }

    const contentWeight = parseFloat(document.getElementById('content-weight')?.value || 0.3);
    const cfWeight = parseFloat(document.getElementById('cf-weight')?.value || 0.4);
    const profileWeight = parseFloat(document.getElementById('profile-weight')?.value || 0.3);
    const topN = parseInt(document.getElementById('top-n')?.value || 20);

    const data = {
        top_n: topN,
        content_weight: contentWeight,
        cf_weight: cfWeight,
        profile_weight: profileWeight
    };

    showLoading('recommendations-results');

    fetch('/api/recommendations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    })
    .then(response => response.json())
    .then(data => {
        displayRecommendations(data.recommendations);
    })
    .catch(error => {
        console.error('Ошибка получения рекомендаций:', error);
        const container = document.getElementById('recommendations-results');
        if (container) {
            container.innerHTML = '<div class="alert alert-danger">Ошибка при получении рекомендаций</div>';
        }
    });
}

// Поиск фильмов
function searchMovies() {
    const query = document.getElementById('search-input')?.value.trim();
    if (!query) return;

    showLoading('search-results');

    fetch(`/api/search?q=${encodeURIComponent(query)}&limit=20`)
        .then(response => response.json())
        .then(data => {
            displaySearchResults(data.movies);
        })
        .catch(error => {
            console.error('Ошибка поиска:', error);
            const container = document.getElementById('search-results');
            if (container) {
                container.innerHTML = '<div class="alert alert-danger">Ошибка при поиске фильмов</div>';
            }
        });
}

// Отображение рекомендаций
function displayRecommendations(recommendations) {
    const container = document.getElementById('recommendations-results');

    if (!container) return;

    if (!recommendations || recommendations.length === 0) {
        container.innerHTML = '<div class="alert alert-info">Нет рекомендаций для отображения</div>';
        return;
    }

    let html = '<div class="row">';
    recommendations.forEach((movie, index) => {
        const posterUrl = movie.poster ? `/img/horizontal/${movie.poster}` : '/img/horizontal/placeholder.jpg';

        html += `
            <div class="col-md-4 col-lg-3 movie-card" onclick="showMovieDetails('${movie.movie_id}')">
                <div class="card h-100">
                    <div class="position-relative">
                        <img src="${posterUrl}" class="card-img-top" alt="${escapeHtml(movie.title)}"
                             onerror="this.src='/img/horizontal/placeholder.jpg'">
                        <div class="movie-score">${movie.score?.toFixed(3) || '0.000'}</div>
                    </div>
                    <div class="card-body">
                        <div class="movie-title">${escapeHtml(movie.title_ru || movie.title)}</div>
                        <div class="movie-year">${movie.year || 'N/A'}</div>
                        <div class="movie-genre">${movie.genre ? escapeHtml(movie.genre.substring(0, 60)) : 'Жанр не указан'}</div>
                        <small class="text-muted">Рекомендация #${index + 1}</small>
                    </div>
                </div>
            </div>
        `;
    });
    html += '</div>';

    container.innerHTML = html;
}

// Отображение результатов поиска
function displaySearchResults(movies) {
    const container = document.getElementById('search-results');

    if (!container) return;

    if (!movies || movies.length === 0) {
        container.innerHTML = '<div class="alert alert-info">Фильмы не найдены</div>';
        return;
    }

    let html = '<div class="row">';
    movies.forEach(movie => {
        const posterUrl = movie.poster ? `/img/horizontal/${movie.poster}` : '/img/horizontal/placeholder.jpg';
        const displayTitle = movie.title_ru || movie.title;

        html += `
            <div class="col-md-4 col-lg-3 movie-card" onclick="showMovieDetails('${movie.movie_id}')">
                <div class="card h-100">
                    <div class="image-container">
                        <img src="${posterUrl}" class="card-img-top" alt="${escapeHtml(displayTitle)}"
                             onerror="this.src='/img/horizontal/placeholder.jpg'">
                    </div>
                    <div class="card-body">
                        <div class="movie-title">${escapeHtml(displayTitle)}</div>
                        <div class="movie-year">${movie.year || 'N/A'}</div>
                        <div class="movie-genre">${movie.genre ? escapeHtml(movie.genre.substring(0, 60)) : 'Жанр не указан'}</div>
                        ${movie.imdb_rating ? `<div class="mt-2"><i class="fas fa-star text-warning"></i> ${movie.imdb_rating}</div>` : ''}
                    </div>
                </div>
            </div>
        `;
    });
    html += '</div>';

    container.innerHTML = html;
}

// Показать детали фильма в модальном окне
function showMovieDetails(movieId) {
    currentMovieId = movieId;

    const modalBody = document.getElementById('movie-modal-body');
    if (!modalBody) return;

    // Показываем загрузку в модальном окне
    modalBody.innerHTML = `
        <div class="text-center p-5">
            <i class="fas fa-spinner fa-spin fa-3x"></i>
            <p class="mt-3">Загрузка информации о фильме...</p>
        </div>
    `;

    // Открываем модальное окно
    if (movieModal) movieModal.show();

    // Загружаем детали фильма
    fetch(`/api/movies/${movieId}`)
        .then(response => response.json())
        .then(movie => {
            displayMovieModal(movie);
        })
        .catch(error => {
            console.error('Ошибка загрузки деталей фильма:', error);
            modalBody.innerHTML = '<div class="alert alert-danger">Ошибка при загрузке информации о фильме</div>';
        });
}

// Отображение модального окна с деталями фильма
function displayMovieModal(movie) {
    const modalBody = document.getElementById('movie-modal-body');
    if (!modalBody) return;

    const posterUrl = movie.poster ? `/img/vertical/${movie.poster}` : '/img/vertical/placeholder.jpg';
    const displayTitle = movie.display_title || movie.title_ru || movie.title;
    const displayPlot = movie.display_plot || movie.plot_ru || movie.plot || 'Описание отсутствует.';
    const displayType = movie.display_type || movie.type_ru || movie.type || 'Фильм';
    const displayAgeLimit = movie.display_age_limit || movie.age_limit_ru || movie.age_limit || '18+';

    let genresArray = movie.genres || [];
    if (movie.genre_ru && genresArray.length === 0) {
        genresArray = [movie.genre_ru];
    } else if (movie.genre && genresArray.length === 0) {
        genresArray = movie.genre.split(',').map(g => g.trim());
    }

    let directorsArray = movie.directors || [];
    if (movie.directors_ru && directorsArray.length === 0) {
        directorsArray = [movie.directors_ru];
    }

    let actorsArray = movie.actors || [];
    if (movie.actors_ru && actorsArray.length === 0) {
        actorsArray = [movie.actors_ru];
    }

    let html = `
        <div class="row">
            <div class="col-md-4">
                <img src="${posterUrl}" class="modal-poster" alt="${escapeHtml(displayTitle)}"
                     onerror="this.src='/img/vertical/placeholder.jpg'">
            </div>
            <div class="col-md-8">
                <h4>${escapeHtml(displayTitle)} (${movie.year || 'N/A'})</h4>
                <div class="mb-3">
                    <span class="badge bg-primary">${escapeHtml(displayType)}</span>
                    <span class="badge bg-secondary">${escapeHtml(displayAgeLimit)}</span>
                    ${movie.imdb_rating ? `<span class="badge bg-warning ms-2">IMDb: ${movie.imdb_rating}</span>` : ''}
                    ${movie.kinopoisk ? `<span class="badge bg-info ms-2">Кинопоиск: ${movie.kinopoisk}</span>` : ''}
                </div>
    `;

    // Жанры (кликабельные)
    if (genresArray.length > 0) {
        html += '<div class="mb-3"><strong>Жанры:</strong><br>';
        genresArray.forEach(genre => {
            const genreStr = String(genre).trim();
            if (genreStr && genreStr !== 'nan') {
                html += `<span class="genre-tag clickable" onclick="event.stopPropagation(); showMoviesByGenre('${escapeHtml(genreStr)}')"
                             style="cursor: pointer;">${escapeHtml(genreStr)}</span>`;
            }
        });
        html += '</div>';
    }

    // Режиссеры
    if (directorsArray.length > 0) {
        html += '<div class="mb-3"><strong>Режиссеры:</strong><br>';
        const topDirectors = directorsArray.slice(0, 5);
        topDirectors.forEach(director => {
            const directorStr = String(director).trim();
            if (directorStr && directorStr !== 'nan') {
                html += `<span class="director-tag clickable" onclick="event.stopPropagation(); showMoviesByDirector('${escapeHtml(directorStr)}')"
                             style="cursor: pointer;">${escapeHtml(directorStr)}</span>`;
            }
        });
        if (directorsArray.length > 5) {
            html += `<span class="text-muted ms-2">и еще ${directorsArray.length - 5} режиссеров...</span>`;
        }
        html += '</div>';
    }

    // Актеры
    if (actorsArray.length > 0) {
        html += '<div class="mb-3"><strong>Актеры:</strong><br>';
        const topActors = actorsArray.slice(0, 5);
        topActors.forEach(actor => {
            const actorStr = String(actor).trim();
            if (actorStr && actorStr !== 'nan') {
                html += `<span class="actor-tag clickable" onclick="event.stopPropagation(); showMoviesByActor('${escapeHtml(actorStr)}')"
                             style="cursor: pointer;">${escapeHtml(actorStr)}</span>`;
            }
        });
        if (actorsArray.length > 5) {
            html += `<span class="text-muted ms-2">и еще ${actorsArray.length - 5} актеров...</span>`;
        }
        html += '</div>';
    }

    // Сюжет
    if (displayPlot && displayPlot !== 'Описание отсутствует.') {
        const plotText = escapeHtml(displayPlot);
        const plotLimit = 200;
        if (plotText.length > plotLimit) {
            html += `<div class="mb-3"><strong>Сюжет:</strong><br><div class="card-text">${plotText.substring(0, plotLimit)}...</div></div>`;
        } else {
            html += `<div class="mb-3"><strong>Сюжет:</strong><br><div class="card-text">${plotText}</div></div>`;
        }
    }

    if (movie.countries) {
        html += `<div class="mb-3"><strong>Страны:</strong> ${escapeHtml(movie.countries)}</div>`;
    }

    // Кнопка перехода на полную страницу
    html += `
                <hr>
                <div class="text-center">
                    <button class="btn btn-primary btn-lg" onclick="window.location.href='/movie/${movie.movie_id}'">
                        <i class="fas fa-external-link-alt"></i> Открыть полную страницу
                    </button>
                </div>
            `;

    html += `</div></div>`;

    modalBody.innerHTML = html;
}

// Функции для перехода на страницы категорий
function showMoviesByActor(actorName) {
    if (movieModal) movieModal.hide();
    window.location.href = `/actor/${encodeURIComponent(actorName)}`;
}

function showMoviesByDirector(directorName) {
    if (movieModal) movieModal.hide();
    window.location.href = `/director/${encodeURIComponent(directorName)}`;
}

function showMoviesByGenre(genreName) {
    if (movieModal) movieModal.hide();
    window.location.href = `/genre/${encodeURIComponent(genreName)}`;
}

function showMoviesByYear(year) {
    if (movieModal) movieModal.hide();
    window.location.href = `/year/${year}`;
}

// Настройка слайдеров
function setupSliders() {
    const sliders = ['content-weight', 'cf-weight', 'profile-weight', 'top-n'];
    sliders.forEach(sliderId => {
        const slider = document.getElementById(sliderId);
        if (slider) {
            slider.addEventListener('input', function() {
                const spanId = `${sliderId}-val`;
                const span = document.getElementById(spanId);
                if (span) {
                    span.textContent = this.value;
                }
            });
        }
    });
}

// Показать индикатор загрузки
function showLoading(containerId) {
    const container = document.getElementById(containerId);
    if (container) {
        container.innerHTML = `
            <div class="loading">
                <i class="fas fa-spinner fa-spin"></i>
                <p>Загрузка...</p>
            </div>
        `;
    }
}

// Функция для экранирования HTML
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
