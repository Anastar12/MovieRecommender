let movieModal = null;

document.addEventListener('DOMContentLoaded', function() {
    const modalElement = document.getElementById('movieModal');
    if (modalElement) {
        movieModal = new bootstrap.Modal(modalElement);
    }
    getRecommendations();
});

function getRecommendations() {
    if (!window.currentUserUrl) {
        console.error('Пользователь не авторизован');
        document.getElementById('recommendations-results').innerHTML =
            '<div class="alert alert-danger">Пользователь не авторизован. Пожалуйста, войдите в систему.</div>';
        return;
    }

    const requestBody = {
        top_n: 20,
        content_weight: 0.3,
        cf_weight: 0.4,
        profile_weight: 0.3
    };

    fetch('/api/recommendations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
    })
    .then(response => response.json())
    .then(data => {
        displayRecommendations(data.recommendations);
    })
    .catch(error => {
        console.error('Ошибка:', error);
        document.getElementById('recommendations-results').innerHTML =
            '<div class="alert alert-danger">Ошибка при получении рекомендаций. Пожалуйста, попробуйте позже.</div>';
    });
}

function displayRecommendations(recommendations) {
    const container = document.getElementById('recommendations-results');
    if (!recommendations || recommendations.length === 0) {
        container.innerHTML = '<div class="alert alert-info text-center">К сожалению, пока нет рекомендаций. Оцените больше фильмов, чтобы получить персонализированные рекомендации.</div>';
        return;
    }

    let html = '<div class="row">';
    recommendations.forEach((movie, index) => {
        const posterUrl = movie.poster ? `/img/horizontal/${movie.poster}` : '/img/horizontal/placeholder.jpg';
        const rating = movie.imdb_rating ? `IMDb: ${movie.imdb_rating}` : '';
        const genresDisplay = movie.genres && movie.genres.length > 0
            ? movie.genres.slice(0, 2).join(', ')
            : 'Жанр не указан';
        const displayTitle = movie.title_ru || movie.title;

        html += `
            <div class="col-md-4 col-lg-3 movie-card" onclick="showMovieDetails('${movie.movie_id}')">
                <div class="card h-100">
                    <div class="image-container">
                        <img src="${posterUrl}" class="card-img-top" alt="${escapeHtml(displayTitle)}"
                             loading="lazy"
                             onerror="this.src='/img/horizontal/placeholder.jpg'">
                    </div>
                    <div class="card-body">
                        <div class="movie-title">${escapeHtml(displayTitle)}</div>
                        <div class="movie-year">${movie.year || 'Год не указан'}</div>
                        <div class="movie-genre">${escapeHtml(genresDisplay)}</div>
                        ${rating ? `<div class="movie-rating"><i class="fas fa-star"></i> ${rating}</div>` : ''}
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
            <p class="mt-3">Загрузка...</p>
        </div>
    `;

    if (movieModal) movieModal.show();

    fetch(`/api/movies/${movieId}`)
        .then(response => response.json())
        .then(movie => displayMovieModal(movie))
        .catch(error => {
            console.error('Ошибка:', error);
            modalBody.innerHTML = '<div class="alert alert-danger">Ошибка загрузки информации о фильме</div>';
        });
}

function displayMovieModal(movie) {
    const posterUrl = movie.poster ? `/img/vertical/${movie.poster}` : '/img/vertical/placeholder.jpg';
    const displayTitle = movie.display_title || movie.title_ru || movie.title;
    const displayPlot = movie.display_plot || movie.plot_ru || movie.plot || 'Описание отсутствует.';
    const displayType = movie.display_type || movie.type_ru || movie.type || 'Фильм';
    const displayAgeLimit = movie.display_age_limit || movie.age_limit_ru || movie.age_limit || '18+';

    let genresArray = movie.genres || [];
    if (typeof genresArray === 'string') {
        genresArray = genresArray.split(',').map(g => g.trim()).filter(g => g);
    }

    let directorsArray = movie.directors_ru || movie.directors || [];
    if (typeof directorsArray === 'string') {
        directorsArray = directorsArray.split(',').map(d => d.trim()).filter(d => d);
    }

    let actorsArray = movie.actors_ru || movie.actors || [];
    if (typeof actorsArray === 'string') {
        actorsArray = actorsArray.split(',').map(a => a.trim()).filter(a => a);
    }

    let html = `
        <div class="row">
            <div class="col-md-4">
                <img src="${posterUrl}" class="img-fluid rounded" alt="${escapeHtml(displayTitle)}"
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
            if (genre && genre.trim()) {
                html += `<span class="genre-tag clickable" onclick="event.stopPropagation(); window.location.href='/genre/${encodeURIComponent(genre)}'">${escapeHtml(genre)}</span>`;
            }
        });
        html += '</div>';
    }

    if (directorsArray.length > 0) {
        html += '<div class="mb-3"><strong>Режиссёры:</strong><br>';
        directorsArray.slice(0, 5).forEach(director => {
            if (director && director.trim()) {
                html += `<span class="director-tag clickable" onclick="event.stopPropagation(); window.location.href='/director/${encodeURIComponent(director)}'">${escapeHtml(director)}</span>`;
            }
        });
        if (directorsArray.length > 5) {
            html += `<span class="text-muted"> и еще ${directorsArray.length - 5}</span>`;
        }
        html += '</div>';
    }

    if (actorsArray.length > 0) {
        html += '<div class="mb-3"><strong>Актёры:</strong><br>';
        actorsArray.slice(0, 5).forEach(actor => {
            if (actor && actor.trim()) {
                html += `<span class="actor-tag clickable" onclick="event.stopPropagation(); window.location.href='/actor/${encodeURIComponent(actor)}'">${escapeHtml(actor)}</span>`;
            }
        });
        if (actorsArray.length > 5) {
            html += `<span class="text-muted"> и еще ${actorsArray.length - 5}</span>`;
        }
        html += '</div>';
    }

    if (displayPlot && displayPlot !== 'nan') {
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
                    <button class="btn btn-primary" onclick="window.location.href='/movie/${movie.movie_id}'">
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