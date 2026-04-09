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

function renderMovieDetails(movie) {
    console.log('Рендеринг фильма:', movie);

    // Используем русские поля для отображения
    const displayTitle = movie.display_title || movie.title_ru || movie.title || 'Без названия';
    const displayPlot = movie.display_plot || movie.plot_ru || movie.plot || 'Описание отсутствует.';
    const displayType = movie.display_type || movie.type_ru || movie.type || 'Фильм';
    const displayAgeLimit = movie.display_age_limit || movie.age_limit_ru || movie.age_limit || '18+';

    // Определяем URL постера - пробуем разные варианты
    let backdropUrl = '/img/long/placeholder.jpg';
    if (movie.poster) {
        backdropUrl = `/img/long/${movie.poster}`;
    } else if (movie.poster_url) {
        backdropUrl = movie.poster_url;
    }

    const year = movie.year || 'N/A';

    // Преобразование времени
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

    // Актеры
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
            <div class="full-width-backdrop" style="background-image: url('${backdropUrl}');"></div>
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
                        </div>
                    </div>
                </div>
            </div>
        </div>
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
    }
}

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
        .then(resp => resp.json())
        .then(data => {
            if (data.movies && data.movies.length) {
                renderSimilarMovies(data.movies);
                const similarBlock = document.getElementById('similar-movies-block');
                if (similarBlock) {
                    similarBlock.style.display = 'block';
                }
            }
        })
        .catch(err => {
            console.warn("similar error", err);
        });
}

function renderSimilarMovies(movies) {
    const container = document.getElementById('similar-movies-grid-container');
    if (!container) return;

    let gridHtml = `<div class="similar-grid">`;
    movies.forEach(m => {
        const posterSim = m.poster ? `/img/horizontal/${m.poster}` : '/img/horizontal/placeholder.jpg';
        const similarityPercent = m.similarity ? (m.similarity * 100).toFixed(1) : '75';
        const simTitle = m.title_ru || m.title || 'Без названия';

        gridHtml += `
            <div class="sim-card" onclick="window.location.href='/movie/${m.movie_id}'">
                <img src="${posterSim}" class="sim-img" alt="${escapeHtml(simTitle)}" onerror="this.src='/img/horizontal/placeholder.jpg'">
                <div class="sim-info">
                    <div class="sim-title">${escapeHtml(simTitle)}</div>
                    <div class="sim-year">${m.year || '—'}</div>
                    <div class="sim-badge"><i class="fas fa-chart-line"></i> схожесть ${similarityPercent}%</div>
                </div>
            </div>
        `;
    });
    gridHtml += `</div>`;
    container.innerHTML = gridHtml;
}

// Обработка выхода
const logoutBtn = document.getElementById('logout-btn');
if (logoutBtn) {
    logoutBtn.addEventListener('click', function(e) {
        e.preventDefault();
        fetch('/api/logout', { method: 'POST' })
            .then(() => window.location.href = '/login')
            .catch(() => window.location.href = '/login');
    });
}

// Запускаем загрузку
loadMovieDetails();