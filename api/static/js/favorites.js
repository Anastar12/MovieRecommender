let movieModal = null;

document.addEventListener('DOMContentLoaded', function() {
    const modalElement = document.getElementById('movieModal');
    if (modalElement) {
        movieModal = new bootstrap.Modal(modalElement);
    }
    loadFavorites();
});

async function loadFavorites() {
    try {
        const response = await fetch(`/api/user/${encodeURIComponent(window.currentUserUrl)}/favorites`);
        const data = await response.json();
        displayMovies(data.movies);
    } catch (error) {
        console.error('Ошибка загрузки избранного:', error);
        document.getElementById('movies-container').innerHTML =
            '<div class="alert alert-danger">Ошибка при загрузке избранных фильмов</div>';
    }
}

function displayMovies(movies) {
    const container = document.getElementById('movies-container');

    if (!movies || movies.length === 0) {
        container.innerHTML = `
            <div class="alert alert-info text-center p-5">
                <i class="fas fa-heart fa-3x mb-3"></i>
                <p>У вас пока нет избранных фильмов</p>
                <a href="/catalog" class="btn btn-primary">Перейти в каталог</a>
            </div>
        `;
        return;
    }

    let html = '<div class="row">';
    movies.forEach(movie => {
        const posterUrl = movie.poster ? `/img/horizontal/${movie.poster}` : '/img/horizontal/placeholder.jpg';
        const displayTitle = movie.title_ru || movie.title;
        const genresDisplay = movie.genres && movie.genres.length > 0
            ? movie.genres.slice(0, 2).join(', ')
            : 'Жанр не указан';

        html += `
            <div class="col-md-4 col-lg-3 movie-card" onclick="showMovieDetails('${movie.movie_id}')">
                <div class="card h-100">
                    <div class="image-container">
                        <img src="${posterUrl}" class="card-img-top" alt="${escapeHtml(displayTitle)}"
                             loading="lazy"
                             onerror="this.src='/img/horizontal/placeholder.jpg'">
                        <div class="rating-badge" style="background: #dc3545;">
                            <i class="fas fa-heart"></i> Избранное
                        </div>
                    </div>
                    <div class="card-body">
                        <div class="movie-title">${escapeHtml(displayTitle)}</div>
                        <div class="movie-year">${movie.year || 'Год не указан'}</div>
                        <div class="movie-genre">${escapeHtml(genresDisplay)}</div>
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
            modalBody.innerHTML = '<div class="alert alert-danger">Ошибка загрузки</div>';
        });
}

function displayMovieModal(movie) {
    const posterUrl = movie.poster ? `/img/vertical/${movie.poster}` : '/img/vertical/placeholder.jpg';
    const displayTitle = movie.title_ru || movie.title;
    const displayPlot = movie.plot_ru || movie.plot || 'Описание отсутствует.';

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
                    ${movie.imdb_rating ? `<span class="badge bg-warning">IMDb: ${movie.imdb_rating}</span>` : ''}
                </div>
                <div class="mb-3"><strong>Сюжет:</strong><br><small>${escapeHtml(displayPlot)}</small></div>
                <hr>
                <div class="text-center">
                    <button class="btn btn-primary" onclick="window.location.href='/movie/${movie.movie_id}'">
                        <i class="fas fa-external-link-alt"></i> Открыть страницу фильма
                    </button>
                </div>
            </div>
        </div>
    `;
    document.getElementById('movie-modal-body').innerHTML = html;
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