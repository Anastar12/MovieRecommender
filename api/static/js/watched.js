let movieModal = null;
let allMovies = [];
let genreChart = null;
let yearChart = null;
let ratingChart = null;

document.addEventListener('DOMContentLoaded', function() {
    const modalElement = document.getElementById('movieModal');
    if (modalElement) {
        movieModal = new bootstrap.Modal(modalElement);
    }

    const userUrl = window.currentUserUrl;

    loadWatchedMovies(userUrl);
    loadWatchedStats(userUrl);

    const sortSelect = document.getElementById('sort-by');
    if (sortSelect) {
        sortSelect.addEventListener('change', () => filterAndSortMovies());
    }

    const searchInput = document.getElementById('search-title');
    if (searchInput) {
        searchInput.addEventListener('input', () => filterAndSortMovies());
    }

    const ratingFilter = document.getElementById('rating-filter');
    if (ratingFilter) {
        ratingFilter.addEventListener('change', () => filterAndSortMovies());
    }

    const logoutBtn = document.getElementById('logout-btn');
    if (logoutBtn) {
        logoutBtn.addEventListener('click', function(e) {
            e.preventDefault();
            fetch('/api/logout', { method: 'POST' })
                .then(() => window.location.href = '/login');
        });
    }
});

function loadWatchedMovies(userUrl) {
    const url = `/api/user/${encodeURIComponent(userUrl)}/watched`;
    console.log('Загрузка фильмов по URL:', url);

    fetch(url)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Получены данные:', data);
            if (data.movies && data.movies.length > 0) {
                allMovies = data.movies;
                filterAndSortMovies();
            } else {
                document.getElementById('movies-container').innerHTML =
                    '<div class="alert alert-info">Вы еще не оставили отзывов на фильмы</div>';
            }
        })
        .catch(error => {
            console.error('Ошибка загрузки фильмов:', error);
            document.getElementById('movies-container').innerHTML =
                '<div class="alert alert-danger">Ошибка при загрузке фильмов: ' + error.message + '</div>';
        });
}

function loadWatchedStats(userUrl) {
    const url = `/api/user/${encodeURIComponent(userUrl)}/watched/stats`;
    console.log('Загрузка статистики по URL:', url);

    fetch(url)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Получена статистика:', data);
            document.getElementById('total-watched').textContent = data.total_watched;
            document.getElementById('avg-rating').textContent = data.avg_rating;

            createGenreChart(data.genre_distribution);
            createYearChart(data.year_distribution);
            createRatingChart(data.rating_distribution);
        })
        .catch(error => {
            console.error('Ошибка загрузки статистики:', error);
            document.getElementById('total-watched').textContent = '0';
            document.getElementById('avg-rating').textContent = '0';
        });
}

function filterAndSortMovies() {
    let filtered = [...allMovies];

    const searchTerm = document.getElementById('search-title').value.toLowerCase();
    if (searchTerm) {
        filtered = filtered.filter(movie =>
            movie.title.toLowerCase().includes(searchTerm)
        );
    }

    const ratingFilter = document.getElementById('rating-filter').value;
    if (ratingFilter !== 'all') {
        filtered = filtered.filter(movie => movie.rating == ratingFilter);
    }

    const sortBy = document.getElementById('sort-by').value;
    switch(sortBy) {
        case 'date_desc':
            filtered.sort((a, b) => (b.review_date || '').localeCompare(a.review_date || ''));
            break;
        case 'date_asc':
            filtered.sort((a, b) => (a.review_date || '').localeCompare(b.review_date || ''));
            break;
        case 'rating_desc':
            filtered.sort((a, b) => (b.rating || 0) - (a.rating || 0));
            break;
        case 'rating_asc':
            filtered.sort((a, b) => (a.rating || 0) - (b.rating || 0));
            break;
        case 'title_asc':
            filtered.sort((a, b) => a.title.localeCompare(b.title));
            break;
        case 'title_desc':
            filtered.sort((a, b) => b.title.localeCompare(a.title));
            break;
        case 'year_desc':
            filtered.sort((a, b) => (b.year || 0) - (a.year || 0));
            break;
        case 'year_asc':
            filtered.sort((a, b) => (a.year || 0) - (b.year || 0));
            break;
    }

    displayMovies(filtered);
}

function displayMovies(movies) {
    const container = document.getElementById('movies-container');

    if (movies.length === 0) {
        container.innerHTML = '<div class="alert alert-info">Фильмы не найдены</div>';
        return;
    }

    let html = '<div class="row">';
    movies.forEach(movie => {
        const posterUrl = movie.poster ? `/img/horizontal/${movie.poster}` : '/img/horizontal/placeholder.jpg';
        const userRating = movie.user_rating ? `${movie.user_rating}★` : 'Нет оценки';
        const displayTitle = movie.title_ru || movie.title;

        let genres = movie.genres || [];
        if (typeof genres === 'string') {
            genres = genres.split(',').map(g => g.trim()).filter(g => g);
        }
        const genresDisplay = genres.length > 0 ? genres.slice(0, 2).join(', ') : 'Жанр не указан';

        html += `
            <div class="col-md-4 col-lg-3 movie-card" onclick="showMovieDetails('${movie.movie_id}')">
                <div class="card h-100">
                    <div class="image-container">
                        <img src="${posterUrl}" class="card-img-top" alt="${escapeHtml(displayTitle)}"
                             loading="lazy"
                             onerror="this.src='/img/horizontal/placeholder.jpg'">
                        <div class="rating-badge">
                            <i class="fas fa-star"></i> ${userRating}
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

function displayMovieModal(movie) {
    const posterUrl = movie.poster ? `/img/vertical/${movie.poster}` : '/img/vertical/placeholder.jpg';
    const displayTitle = movie.display_title || movie.title_ru || movie.title;
    const displayPlot = movie.display_plot || movie.plot_ru || movie.plot || 'Описание отсутствует.';
    const displayType = movie.display_type || movie.type_ru || movie.type || 'Фильм';
    const displayAgeLimit = movie.display_age_limit || movie.age_limit_ru || movie.age_limit || '18+';

    let genres = movie.genres || [];
    if (typeof genres === 'string') {
        genres = genres.split(',').map(g => g.trim()).filter(g => g);
    }

    let directors = movie.directors_ru || movie.directors || [];
    if (typeof directors === 'string') {
        directors = directors.split(',').map(d => d.trim()).filter(d => d);
    }

    let actors = movie.actors_ru || movie.actors || [];
    if (typeof actors === 'string') {
        actors = actors.split(',').map(a => a.trim()).filter(a => a);
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
                </div>
    `;

    if (movie.user_rating) {
        html += `
            <div class="user-rating">
                <strong><i class="fas fa-star text-warning"></i> Ваша оценка: ${movie.user_rating}★</strong>
                ${movie.user_review ? `<div class="review-text">"${escapeHtml(movie.user_review)}"</div>` : ''}
                ${movie.review_date ? `<div class="text-muted mt-2"><small>Дата: ${escapeHtml(movie.review_date)}</small></div>` : ''}
            </div>
        `;
    }

    if (genres && genres.length > 0) {
        html += '<div class="mb-3 mt-3"><strong>Жанры:</strong><br>';
        genres.forEach(genre => {
            if (genre && genre.trim()) {
                html += `<span class="badge" onclick="event.stopPropagation(); window.location.href='/genre/${encodeURIComponent(genre)}'">${escapeHtml(genre)}</span>`;
            }
        });
        html += '</div>';
    }

    if (directors && directors.length > 0) {
        html += '<div class="mb-3"><strong>Режиссёры:</strong><br>';
        directors.slice(0, 5).forEach(director => {
            if (director && director.trim()) {
                html += `<span class="badge" onclick="event.stopPropagation(); window.location.href='/director/${encodeURIComponent(director)}'">${escapeHtml(director)}</span>`;
            }
        });
        if (directors.length > 5) {
            html += `<span class="text-muted"> и еще ${directors.length - 5}</span>`;
        }
        html += '</div>';
    }

    if (actors && actors.length > 0) {
        html += '<div class="mb-3"><strong>Актёры:</strong><br>';
        actors.slice(0, 10).forEach(actor => {
            if (actor && actor.trim()) {
                html += `<span class="badge" onclick="event.stopPropagation(); window.location.href='/actor/${encodeURIComponent(actor)}'">${escapeHtml(actor)}</span>`;
            }
        });
        if (actors.length > 10) {
            html += `<span class="text-muted"> и еще ${actors.length - 10}</span>`;
        }
        html += '</div>';
    }

    if (displayPlot && displayPlot !== 'Описание отсутствует.') {
        html += `<div class="mb-3"><strong>Сюжет:</strong><br><div style="line-height: 1.6;">${escapeHtml(displayPlot)}</div></div>`;
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
    document.getElementById('movie-modal-body').innerHTML = html;
}

function showMovieDetails(movieId) {
    document.getElementById('movie-modal-body').innerHTML = `
        <div class="text-center p-5">
            <i class="fas fa-spinner fa-spin fa-3x"></i>
            <p class="mt-3">Загрузка информации о фильме...</p>
        </div>
    `;

    if (movieModal) movieModal.show();

    fetch(`/api/movies/${movieId}`)
        .then(response => response.json())
        .then(movie => {
            console.log('Детали фильма:', movie);
            displayMovieModal(movie);
        })
        .catch(error => {
            console.error('Ошибка загрузки деталей фильма:', error);
            document.getElementById('movie-modal-body').innerHTML = `
                <div class="alert alert-danger">
                    Ошибка при загрузке информации о фильме: ${error.message}
                </div>
            `;
        });
}

function createGenreChart(data) {
    const ctx = document.getElementById('genre-chart').getContext('2d');
    if (genreChart) genreChart.destroy();

    if (!data || data.length === 0) {
        ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
        ctx.fillStyle = '#fff';
        ctx.font = '14px Arial';
        ctx.fillText('Нет данных для отображения', 50, 50);
        return;
    }

    genreChart = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: data.map(d => d.genre),
            datasets: [{
                data: data.map(d => d.count),
                backgroundColor: [
                    '#8bc34a', '#ff9800', '#f44336', '#2196f3', '#9c27b0',
                    '#00bcd4', '#e91e63', '#ffc107', '#4caf50', '#ff5722',
                    '#795548', '#607d8b', '#3f51b5', '#009688', '#cddc39'
                ]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: '#fff', font: { size: 11 } }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.raw || 0;
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((value / total) * 100).toFixed(1);
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    }
                }
            }
        }
    });
}

function createYearChart(data) {
    const ctx = document.getElementById('year-chart').getContext('2d');
    if (yearChart) yearChart.destroy();

    if (!data || data.length === 0) {
        ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
        ctx.fillStyle = '#fff';
        ctx.font = '14px Arial';
        ctx.fillText('Нет данных для отображения', 50, 50);
        return;
    }

    yearChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.year),
            datasets: [{
                label: 'Количество фильмов',
                data: data.map(d => d.count),
                backgroundColor: '#8bc34a',
                borderColor: '#689f38',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    labels: { color: '#fff' }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `Фильмов: ${context.raw}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#fff',
                        maxRotation: 45,
                        minRotation: 45
                    },
                    title: {
                        display: true,
                        text: 'Год',
                        color: '#fff'
                    }
                },
                y: {
                    ticks: { color: '#fff' },
                    title: {
                        display: true,
                        text: 'Количество фильмов',
                        color: '#fff'
                    },
                    beginAtZero: true
                }
            }
        }
    });
}

function createRatingChart(data) {
    const ctx = document.getElementById('rating-chart').getContext('2d');
    if (ratingChart) ratingChart.destroy();

    if (!data || data.length === 0) {
        ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
        ctx.fillStyle = '#fff';
        ctx.font = '14px Arial';
        ctx.fillText('Нет данных для отображения', 50, 50);
        return;
    }

    ratingChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => `${d.rating}★`),
            datasets: [{
                label: 'Количество оценок',
                data: data.map(d => d.count),
                borderColor: '#8bc34a',
                backgroundColor: 'rgba(139, 195, 74, 0.1)',
                tension: 0.4,
                fill: true,
                pointBackgroundColor: '#8bc34a',
                pointBorderColor: '#fff',
                pointRadius: 4,
                pointHoverRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { labels: { color: '#fff' } },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `Оценок: ${context.raw}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: { color: '#fff' },
                    title: {
                        display: true,
                        text: 'Оценка',
                        color: '#fff'
                    }
                },
                y: {
                    ticks: { color: '#fff' },
                    title: {
                        display: true,
                        text: 'Количество',
                        color: '#fff'
                    },
                    beginAtZero: true
                }
            }
        }
    });
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
