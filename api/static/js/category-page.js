let movieModal = null;

document.addEventListener('DOMContentLoaded', function() {
    const modalElement = document.getElementById('movieModal');
    if (modalElement) {
        movieModal = new bootstrap.Modal(modalElement);
    }

    const categoryType = window.categoryType;
    const categoryName = decodeURIComponent(window.categoryName);

    if (categoryType === 'genre') {
        loadGenreDescription(categoryName);
    }

    loadMovies(categoryType, categoryName);
});

function loadMovies(categoryType, categoryName) {
    fetch(`/api/category/${categoryType}/${encodeURIComponent(categoryName)}`)
        .then(response => response.json())
        .then(data => {
            displayMovies(data.movies, data.total);
        })
        .catch(error => {
            console.error('Ошибка загрузки фильмов:', error);
            document.getElementById('movies-container').innerHTML =
                '<div class="alert alert-danger">Ошибка при загрузке фильмов</div>';
        });
}

function displayMovies(movies, total) {
    const container = document.getElementById('movies-container');
    const totalCount = document.getElementById('total-count');

    if (!movies || movies.length === 0) {
        container.innerHTML = '<div class="alert alert-info">Фильмы не найдены</div>';
        if (totalCount) totalCount.textContent = 'Найдено фильмов: 0';
        return;
    }

    if (totalCount) totalCount.textContent = `Найдено фильмов: ${total || movies.length}`;

    let html = '<div class="row">';
    movies.forEach(movie => {
        if (!movie.movie_id || movie.movie_id === 'nan' || movie.movie_id === 'None') {
            return;
        }

        const posterUrl = movie.poster ? `/img/horizontal/${movie.poster}` : '/img/horizontal/placeholder.jpg';
        const displayTitle = movie.title_ru || movie.title || 'Без названия';

        let genreDisplay = 'Жанр не указан';
        if (movie.genres && movie.genres.length > 0) {
            genreDisplay = movie.genres.slice(0, 2).join(', ');
        } else if (movie.genre && movie.genre !== 'nan') {
            genreDisplay = movie.genre;
        }

        html += `
            <div class="col-md-6 col-lg-4 col-xl-3 movie-card" onclick="showMovieDetails('${movie.movie_id}')">
                <div class="card h-100">
                    <div class="image-container">
                        <img src="${posterUrl}" class="card-img-top" alt="${escapeHtml(displayTitle)}"
                             loading="lazy"
                             onerror="this.src='/img/horizontal/placeholder.jpg'">
                    </div>
                    <div class="card-body">
                        <div class="movie-title">${escapeHtml(displayTitle)}</div>
                        <div class="movie-year">${movie.year || 'N/A'}</div>
                        <div class="movie-genre">${escapeHtml(genreDisplay)}</div>
                        ${movie.imdb_rating ? `<div class="movie-rating"><i class="fas fa-star"></i> IMDb: ${movie.imdb_rating}</div>` : ''}
                    </div>
                </div>
            </div>
        `;
    });
    html += '</div>';
    container.innerHTML = html;
}

function showMovieDetails(movieId) {
    const modalBody = document.getElementById('movie-modal-body');
    if (!modalBody) return;

    modalBody.innerHTML = `
        <div class="text-center p-5">
            <i class="fas fa-spinner fa-spin fa-3x"></i>
            <p class="mt-3">Загрузка информации о фильме...</p>
        </div>
    `;

    if (movieModal) movieModal.show();

    fetch(`/api/movies/${movieId}`)
        .then(response => response.json())
        .then(movie => displayMovieModal(movie))
        .catch(error => {
            console.error('Ошибка загрузки деталей фильма:', error);
            modalBody.innerHTML = `
                <div class="alert alert-danger m-3">
                    <i class="fas fa-exclamation-triangle"></i>
                    Ошибка при загрузке информации о фильме: ${error.message}
                </div>
            `;
        });
}

function displayMovieModal(movie) {
    const posterUrl = movie.poster ? `/img/vertical/${movie.poster}` : '/img/vertical/placeholder.jpg';
    const displayTitle = movie.display_title || movie.title_ru || movie.title || 'Без названия';
    const displayPlot = movie.display_plot || movie.plot_ru || movie.plot || 'Описание отсутствует.';
    const displayType = movie.display_type || movie.type_ru || movie.type || 'Фильм';
    const displayAgeLimit = movie.display_age_limit || movie.age_limit_ru || movie.age_limit || '18+';

    let genresArray = movie.genres || [];
    if (typeof genresArray === 'string') {
        genresArray = genresArray.split(',').map(g => g.trim()).filter(g => g && g !== 'nan');
    }

    let directorsArray = movie.directors_ru || movie.directors || [];
    if (typeof directorsArray === 'string') {
        directorsArray = directorsArray.split(',').map(d => d.trim()).filter(d => d && d !== 'nan');
    }

    let actorsArray = movie.actors_ru || movie.actors || [];
    if (typeof actorsArray === 'string') {
        actorsArray = actorsArray.split(',').map(a => a.trim()).filter(a => a && a !== 'nan');
    }

    let html = `
        <div class="row">
            <div class="col-md-4">
                <img src="${posterUrl}" class="modal-poster img-fluid" alt="${escapeHtml(displayTitle)}"
                     onerror="this.src='/img/vertical/placeholder.jpg'"
                     style="max-height: 400px; width: 100%; object-fit: cover;">
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

    if (genresArray.length > 0) {
        html += '<div class="mb-3"><strong>Жанры:</strong><br>';
        genresArray.forEach(genre => {
            const genreStr = String(genre).trim();
            if (genreStr && genreStr !== 'nan') {
                html += `<span class="genre-tag clickable" onclick="event.stopPropagation(); window.location.href='/genre/${encodeURIComponent(genreStr)}'">${escapeHtml(genreStr)}</span>`;
            }
        });
        html += '</div>';
    }

    if (directorsArray.length > 0) {
        html += '<div class="mb-3"><strong>Режиссеры:</strong><br>';
        directorsArray.slice(0, 5).forEach(director => {
            const directorStr = String(director).trim();
            if (directorStr && directorStr !== 'nan') {
                html += `<span class="director-tag clickable" onclick="event.stopPropagation(); window.location.href='/director/${encodeURIComponent(directorStr)}'">${escapeHtml(directorStr)}</span>`;
            }
        });
        if (directorsArray.length > 5) {
            html += `<span class="text-muted ms-2">и еще ${directorsArray.length - 5} режиссеров...</span>`;
        }
        html += '</div>';
    }

    if (actorsArray.length > 0) {
        html += '<div class="mb-3"><strong>Актеры:</strong><br>';
        actorsArray.slice(0, 10).forEach(actor => {
            const actorStr = String(actor).trim();
            if (actorStr && actorStr !== 'nan') {
                html += `<span class="actor-tag clickable" onclick="event.stopPropagation(); window.location.href='/actor/${encodeURIComponent(actorStr)}'">${escapeHtml(actorStr)}</span>`;
            }
        });
        if (actorsArray.length > 10) {
            html += `<span class="text-muted ms-2">и еще ${actorsArray.length - 10} актеров...</span>`;
        }
        html += '</div>';
    }

    if (displayPlot && displayPlot !== 'Описание отсутствует.') {
        const plotText = escapeHtml(displayPlot);
        const plotLimit = 300;
        if (plotText.length > plotLimit) {
            html += `<div class="mb-3"><strong>Сюжет:</strong><br><small>${plotText.substring(0, plotLimit)}...</small></div>`;
        } else {
            html += `<div class="mb-3"><strong>Сюжет:</strong><br><small>${plotText}</small></div>`;
        }
    }

    html += `
                <hr>
                <div class="text-center">
                    <button class="btn btn-primary btn-lg" onclick="window.location.href='/movie/${movie.movie_id}'">
                        <i class="fas fa-external-link-alt"></i> Открыть полную страницу
                    </button>
                </div>
            </div>
        </div>
    `;

    const modalBody = document.getElementById('movie-modal-body');
    if (modalBody) modalBody.innerHTML = html;
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

const logoutBtn = document.getElementById('logout-btn');
if (logoutBtn) {
    logoutBtn.addEventListener('click', function(e) {
        e.preventDefault();
        fetch('/api/logout', { method: 'POST' })
            .then(() => window.location.href = '/login');
    });
}

function loadGenreDescription(genreName) {
    const container = document.getElementById('genre-description-container');
    if (!container) return;

    fetch(`/api/genre/${encodeURIComponent(genreName)}/description`)
        .then(response => response.json())
        .then(data => {
            if (data && !data.error) {
                container.style.display = 'block';
                container.innerHTML = `
                    <div class="genre-description-card">
                        <div class="genre-badge">
                            <i class="fas fa-tag"></i> ${data.type_ru || 'Жанр'}
                        </div>
                        <h2>
                            ${escapeHtml(data.name_ru)}
                            ${data.name_en ? `<small class="text-muted ms-2">(${escapeHtml(data.name_en)})</small>` : ''}
                        </h2>
                        <div class="description-text">
                            <i class="fas fa-quote-left"></i>
                            ${escapeHtml(data.description_ru)}
                        </div>
                        <hr>
                        <div class="text-muted small">
                            <i class="fas fa-info-circle"></i> В этой подборке собраны фильмы, относящиеся к данному жанру.
                        </div>
                    </div>
                `;

                const pageTitle = document.getElementById('page-title');
                if (data.name_ru && data.name_ru !== genreName && pageTitle) {
                    pageTitle.innerHTML = `Фильмы жанра: ${escapeHtml(data.name_ru)}`;
                }
            }
        })
        .catch(error => {
            console.error('Ошибка загрузки описания жанра:', error);
        });
}