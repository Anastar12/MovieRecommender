// catalog.js - исправленная версия с бесконечной прокруткой
// Каталог фильмов
let allMoviesForFilters = [];   // Отдельный массив для фильтров
let loadedMovies = [];          // Загруженные фильмы
let currentOffset = 0;
let totalMoviesCount = 0;
let isLoadingMore = false;
let isLoadingInitial = false;
let filtersModal = null;
let currentSort = 'rating_desc';
let observer = null; // Храним ссылку на observer

// Состояние фильтров
let filters = {
    genres: [],
    years: [],
    countries: [],
    actors: [],
    directors: []
};

// Доступные опции для фильтров
let availableFilters = {
    genres_tree: [],
    genres_flat: [],
    years: [],
    countries: [],
    actors: [],
    directors: []
};

// Количество фильмов за одну загрузку
const LOAD_BATCH_SIZE = 20;

// Флаг, показывающий, что больше нет данных для загрузки
let hasMoreData = true;

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    console.log('Страница загружена, инициализация...');
    const modalElement = document.getElementById('filtersModal');
    if (modalElement) {
        filtersModal = new bootstrap.Modal(modalElement);
    }
    initCatalog();
    setupSearchListeners();
});

function setupInfiniteScroll() {
    // Отключаем предыдущий observer, если есть
    if (observer) {
        observer.disconnect();
        observer = null;
    }

    // Находим сторожевой элемент
    const sentinel = document.getElementById('scroll-sentinel');
    if (!sentinel) {
        console.log('Сторожевой элемент не найден, пропускаем настройку');
        return;
    }

    console.log('Настройка бесконечной прокрутки...');

    // Создаем новый observer
    observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting && !isLoadingMore && hasMoreData && currentOffset < totalMoviesCount) {
                console.log('Достигнут конец списка, загружаем еще фильмы...');
                loadMovies(false);
            }
        });
    }, { threshold: 0.1, rootMargin: '200px' });

    observer.observe(sentinel);
}

async function initCatalog() {
    // Сбрасываем все данные
    currentOffset = 0;
    loadedMovies = [];
    totalMoviesCount = 0;
    hasMoreData = true;

    // Отключаем старый observer
    if (observer) {
        observer.disconnect();
        observer = null;
    }

    // Очищаем контейнер полностью
    const container = document.getElementById('movies-container');
    if (container) {
        container.innerHTML = '<div class="loading text-center p-5"><i class="fas fa-spinner fa-spin fa-3x"></i><p class="mt-3">Загрузка фильмов...</p></div>';
    }

    // Загружаем первую порцию
    await loadMovies(true);
}

async function loadMovies(isInitial = false) {
    if (isInitial) {
        if (isLoadingInitial) return;
        isLoadingInitial = true;
    } else {
        if (isLoadingMore) return;
        isLoadingMore = true;
        showLoadMoreIndicator(true);
    }

    try {
        const params = new URLSearchParams({
            offset: currentOffset,
            limit: LOAD_BATCH_SIZE,
            sort_by: currentSort,
            filters: JSON.stringify(filters)
        });

        console.log(`Загрузка фильмов: offset=${currentOffset}, limit=${LOAD_BATCH_SIZE}`);
        const response = await fetch(`/api/catalog?${params}`);
        const data = await response.json();

        if (data.error) throw new Error(data.error);

        // При первом запросе сохраняем общее количество и фильтры
        if (isInitial) {
            totalMoviesCount = data.total;
            console.log(`Всего фильмов: ${totalMoviesCount}`);

            // Сохраняем все фильмы для фильтров (один раз)
            if (data.all_movies_for_filters) {
                allMoviesForFilters = data.all_movies_for_filters;
            }

            if (data.filters) {
                availableFilters = data.filters;
                renderFilterOptions();
            }

            // Обновляем счетчик найденных фильмов
            const resultsCount = document.getElementById('results-count');
            if (resultsCount) resultsCount.textContent = totalMoviesCount;
        }

        // Добавляем новые фильмы (только если они есть и не дублируются)
        if (data.movies && data.movies.length > 0) {
            console.log(`Получено ${data.movies.length} фильмов`);

            // Проверяем на дубликаты перед добавлением
            const existingIds = new Set(loadedMovies.map(m => m.movie_id));
            const newMovies = data.movies.filter(m => !existingIds.has(m.movie_id));

            if (newMovies.length > 0) {
                loadedMovies = [...loadedMovies, ...newMovies];
                displayMovies(loadedMovies);
            }

            // Обновляем offset
            currentOffset += data.movies.length;
            console.log(`Загружено ${loadedMovies.length} из ${totalMoviesCount} фильмов`);

            // Проверяем, есть ли еще данные
            if (currentOffset >= totalMoviesCount) {
                hasMoreData = false;
                showEndOfListIndicator();
            } else {
                // Убеждаемся, что сторожевой элемент есть и observer настроен
                setTimeout(() => {
                    setupInfiniteScroll();
                }, 100);
            }
        } else {
            hasMoreData = false;
            showEndOfListIndicator();
            if (isInitial) {
                displayMovies([]);
            }
        }

    } catch (error) {
        console.error('Ошибка загрузки:', error);
        if (isInitial) {
            const container = document.getElementById('movies-container');
            if (container) {
                container.innerHTML = `
                    <div class="alert alert-danger text-center p-5">
                        <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                        <p>Ошибка при загрузке: ${error.message}</p>
                        <button class="btn btn-outline-danger" onclick="initCatalog()">Повторить</button>
                    </div>
                `;
            }
        } else {
            if (typeof showToast === 'function') {
                showToast('Ошибка загрузки', error.message, 'error');
            }
        }
        // В случае ошибки все равно пытаемся показать сторожевой элемент
        hasMoreData = false;
    } finally {
        if (isInitial) {
            isLoadingInitial = false;
        } else {
            isLoadingMore = false;
            showLoadMoreIndicator(false);
        }
    }
}

function showLoadMoreIndicator(show) {
    const indicator = document.getElementById('loading-indicator');
    if (indicator) {
        indicator.style.display = show ? 'block' : 'none';
    }
}

function showEndOfListIndicator() {
    const endIndicator = document.getElementById('end-of-list');
    if (endIndicator) {
        endIndicator.style.display = 'block';
    }
    // Отключаем observer, если достигнут конец
    if (observer) {
        observer.disconnect();
        observer = null;
    }
    // Скрываем сторожевой элемент
    const sentinel = document.getElementById('scroll-sentinel');
    if (sentinel) {
        sentinel.style.display = 'none';
    }
}

// Функция для обратной совместимости
function loadMoreMovies() {
    if (!isLoadingMore && hasMoreData && currentOffset < totalMoviesCount) {
        loadMovies(false);
    }
}

function displayMovies(movies) {
    const container = document.getElementById('movies-container');
    if (!container) return;

    if (!movies || movies.length === 0) {
        container.innerHTML = '<div class="no-results"><i class="fas fa-film fa-3x mb-3"></i><p>Фильмы не найдены</p></div>';
        return;
    }

    // Строим HTML для всех загруженных фильмов
    let html = '<div class="row">';
    movies.forEach(movie => {
        const posterUrl = movie.poster ? `/img/horizontal/${movie.poster}` : '/img/horizontal/placeholder.jpg';
        const rating = movie.imdb_rating ? `<i class="fas fa-star"></i> ${movie.imdb_rating.toFixed(1)}` : '';
        const displayTitle = movie.title_ru || movie.title;
        const displayGenres = movie.genres?.slice(0, 2).join(', ') || 'Жанр не указан';

        html += `
            <div class="col-md-4 col-lg-3 movie-card" onclick="showMovieDetails('${movie.movie_id}')">
                <div class="card h-100">
                    <div class="image-container">
                        <img src="${posterUrl}" class="card-img-top" alt="${escapeHtml(displayTitle)}"
                             loading="lazy" onerror="this.src='/img/horizontal/placeholder.jpg'">
                    </div>
                    <div class="card-body">
                        <div class="movie-title">${escapeHtml(displayTitle)}</div>
                        <div class="movie-year">${movie.year || 'Год не указан'}</div>
                        <div class="movie-genre">${escapeHtml(displayGenres)}</div>
                        ${rating ? `<div class="movie-rating">${rating}</div>` : ''}
                    </div>
                </div>
            </div>
        `;
    });
    html += '</div>';

    // Добавляем индикаторы загрузки и конца списка
    html += `
        <div id="loading-indicator" class="text-center p-3" style="display: none;">
            <i class="fas fa-spinner fa-spin"></i> Загрузка...
        </div>
        <div id="end-of-list" class="text-center p-3 text-muted" style="display: none;">
            <i class="fas fa-check-circle"></i> Вы просмотрели все фильмы
        </div>
        <div id="scroll-sentinel" style="height: 10px;"></div>
    `;

    container.innerHTML = html;

    // Настраиваем бесконечную прокрутку после добавления элементов
    setTimeout(() => {
        setupInfiniteScroll();
    }, 100);
}

// Остальные функции (applyFilters, changeSort, resetFilters, removeFilter,
// renderGenreTree, handleMainGenreChange, handleSubgenreChange, toggleSubgenres,
// filterGenreList, renderFilterOptions, renderCheckboxList, toggleFilterFromModal,
// findGenreInTree, updateAllGenreCheckboxesState, updateFiltersCount,
// applyFiltersAndClose, updateSelectedFiltersDisplay, getFilterTypeName,
// openFiltersModal, getItemCount, setupSearchListeners, filterList, escapeHtml)
// остаются без изменений, они такие же как в предыдущей версии

function applyFilters() {
    // Полностью перезагружаем каталог с новыми фильтрами
    initCatalog();
    updateSelectedFiltersDisplay();
    if (filtersModal) filtersModal.hide();
}

function changeSort() {
    const sortSelect = document.getElementById('sort-by');
    if (sortSelect) {
        currentSort = sortSelect.value;
    }
    // Полностью перезагружаем с новой сортировкой
    initCatalog();
}

function resetFilters() {
    filters = {
        genres: [],
        years: [],
        countries: [],
        actors: [],
        directors: []
    };

    // Сбрасываем все чекбоксы в модальном окне
    const modalCheckboxes = document.querySelectorAll('#filtersModal input[type="checkbox"]');
    modalCheckboxes.forEach(cb => {
        cb.checked = false;
        cb.indeterminate = false;
    });

    // Перезагружаем каталог
    initCatalog();
    updateSelectedFiltersDisplay();
    updateFiltersCount();

    if (filtersModal) filtersModal.hide();
}

function removeFilter(type, value) {
    if (!filters[type]) {
        console.error('Неизвестный тип фильтра:', type);
        return;
    }

    const index = filters[type].indexOf(value);
    if (index !== -1) {
        filters[type].splice(index, 1);
    }

    if (type === 'genres' && availableFilters.genres_tree) {
        const checkbox = document.querySelector(`#genre-list input[value="${value.replace(/"/g, '\\"')}"]`);
        if (checkbox) {
            checkbox.checked = false;
            checkbox.indeterminate = false;
        }

        const genreInfo = findGenreInTree(availableFilters.genres_tree, value);
        if (genreInfo && !genreInfo.isMain && genreInfo.parentName) {
            const parentCheckboxId = `genre_${genreInfo.parentName.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
            const parentCheckbox = document.getElementById(parentCheckboxId);
            if (parentCheckbox) {
                const parentInfo = findGenreInTree(availableFilters.genres_tree, genreInfo.parentName);
                if (parentInfo && parentInfo.isMain && parentInfo.subgenres) {
                    let anySubSelected = false;
                    parentInfo.subgenres.forEach(sub => {
                        if (filters.genres.includes(sub.name)) {
                            anySubSelected = true;
                        }
                    });

                    if (!anySubSelected) {
                        parentCheckbox.checked = false;
                        parentCheckbox.indeterminate = false;
                    } else {
                        parentCheckbox.indeterminate = true;
                        parentCheckbox.checked = false;
                    }
                }
            }
        }

        updateAllGenreCheckboxesState();
    }

    // Перезагружаем каталог
    initCatalog();
}

function renderGenreTree(genresTree, selectedGenres) {
    const container = document.getElementById('genre-list');
    if (!container) return;

    if (!genresTree || genresTree.length === 0) {
        container.innerHTML = '<div class="text-muted">Нет данных</div>';
        return;
    }

    const sortedGenresTree = [...genresTree].sort((a, b) => {
        return a.name.localeCompare(b.name, 'ru');
    });

    let html = '';
    sortedGenresTree.forEach(genre => {
        const genreName = genre.name;
        const safeGenreName = escapeHtml(String(genreName));
        const genreId = `genre_${safeGenreName.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
        const subId = `${genreId}_sub`;
        const hasSubgenres = genre.subgenres && genre.subgenres.length > 0;

        let sortedSubgenres = [];
        if (hasSubgenres) {
            sortedSubgenres = [...genre.subgenres].sort((a, b) => {
                return a.name.localeCompare(b.name, 'ru');
            });
        }

        let selectedSubCount = 0;
        if (hasSubgenres) {
            sortedSubgenres.forEach(subgenre => {
                if (selectedGenres.includes(subgenre.name)) {
                    selectedSubCount++;
                }
            });
        }

        const isChecked = selectedGenres.includes(genreName);
        const isIndeterminate = hasSubgenres && selectedSubCount > 0 && selectedSubCount < sortedSubgenres.length;

        html += `
            <div class="genre-group" data-genre="${safeGenreName}">
                <div class="genre-header">
                    <div class="genre-main">
                        <input type="checkbox" id="${genreId}"
                               value="${safeGenreName}" ${isChecked ? 'checked' : ''}
                               data-is-main="true"
                               data-genre-name="${safeGenreName}"
                               onchange="handleMainGenreChange('${safeGenreName.replace(/'/g, "\\'")}', this)">
                        <label for="${genreId}">${safeGenreName}</label>
                    </div>
                    <span class="count">(${getItemCount('genre', genreName)})</span>
                    ${hasSubgenres ?
                        `<i class="fas fa-chevron-down genre-toggle" data-target="${subId}" onclick="toggleSubgenres('${subId}', this)"></i>` : ''}
                </div>
        `;

        if (hasSubgenres) {
            html += `<div id="${subId}" class="subgenre-list" style="display: none;">`;
            sortedSubgenres.forEach(subgenre => {
                const subName = subgenre.name;
                const isSubChecked = selectedGenres.includes(subName);
                const safeSubName = escapeHtml(String(subName));
                const subCheckboxId = `subgenre_${safeSubName.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;

                html += `
                    <div class="filter-option">
                        <input type="checkbox" id="${subCheckboxId}"
                               value="${safeSubName}" ${isSubChecked ? 'checked' : ''}
                               data-is-sub="true"
                               data-parent-genre="${safeGenreName}"
                               onchange="handleSubgenreChange('${safeSubName.replace(/'/g, "\\'")}', '${safeGenreName.replace(/'/g, "\\'")}', this)">
                        <label for="${subCheckboxId}">${safeSubName}</label>
                        <span class="count">(${getItemCount('genre', subName)})</span>
                    </div>
                `;
            });
            html += `</div>`;
        }

        html += `</div>`;

        if (isIndeterminate) {
            setTimeout(() => {
                const parentCheckbox = document.getElementById(genreId);
                if (parentCheckbox) {
                    parentCheckbox.indeterminate = true;
                    parentCheckbox.checked = false;
                }
            }, 0);
        }
    });

    container.innerHTML = html;
}

function handleMainGenreChange(genreName, checkboxElement) {
    const isChecked = checkboxElement.checked;
    const genreInfo = findGenreInTree(availableFilters.genres_tree, genreName);

    if (genreInfo && genreInfo.isMain && genreInfo.subgenres) {
        if (isChecked) {
            if (!filters.genres.includes(genreName)) {
                filters.genres.push(genreName);
            }
            genreInfo.subgenres.forEach(subgenre => {
                const subgenreName = subgenre.name;
                if (!filters.genres.includes(subgenreName)) {
                    filters.genres.push(subgenreName);
                }
                const subCheckboxId = `subgenre_${subgenreName.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
                const subCheckbox = document.getElementById(subCheckboxId);
                if (subCheckbox) subCheckbox.checked = true;
            });
        } else {
            const mainIndex = filters.genres.indexOf(genreName);
            if (mainIndex !== -1) {
                filters.genres.splice(mainIndex, 1);
            }
            genreInfo.subgenres.forEach(subgenre => {
                const subgenreName = subgenre.name;
                const subIndex = filters.genres.indexOf(subgenreName);
                if (subIndex !== -1) {
                    filters.genres.splice(subIndex, 1);
                }
                const subCheckboxId = `subgenre_${subgenreName.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
                const subCheckbox = document.getElementById(subCheckboxId);
                if (subCheckbox) subCheckbox.checked = false;
            });
        }

        checkboxElement.indeterminate = false;
    } else {
        const index = filters.genres.indexOf(genreName);
        if (isChecked && index === -1) {
            filters.genres.push(genreName);
        } else if (!isChecked && index !== -1) {
            filters.genres.splice(index, 1);
        }
    }

    updateFiltersCount();
}

function handleSubgenreChange(subgenreName, parentGenreName, checkboxElement) {
    const isChecked = checkboxElement.checked;

    const index = filters.genres.indexOf(subgenreName);
    if (isChecked && index === -1) {
        filters.genres.push(subgenreName);
    } else if (!isChecked && index !== -1) {
        filters.genres.splice(index, 1);
    }

    const parentCheckboxId = `genre_${parentGenreName.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
    const parentCheckbox = document.getElementById(parentCheckboxId);

    if (parentCheckbox) {
        const parentInfo = findGenreInTree(availableFilters.genres_tree, parentGenreName);

        if (parentInfo && parentInfo.isMain && parentInfo.subgenres) {
            let selectedSubCount = 0;
            parentInfo.subgenres.forEach(subgenre => {
                if (filters.genres.includes(subgenre.name)) {
                    selectedSubCount++;
                }
            });

            if (selectedSubCount === parentInfo.subgenres.length && selectedSubCount > 0) {
                if (!filters.genres.includes(parentGenreName)) {
                    filters.genres.push(parentGenreName);
                }
                parentCheckbox.checked = true;
                parentCheckbox.indeterminate = false;
            } else if (selectedSubCount === 0) {
                const parentIndex = filters.genres.indexOf(parentGenreName);
                if (parentIndex !== -1) {
                    filters.genres.splice(parentIndex, 1);
                }
                parentCheckbox.checked = false;
                parentCheckbox.indeterminate = false;
            } else {
                const parentIndex = filters.genres.indexOf(parentGenreName);
                if (parentIndex !== -1) {
                    filters.genres.splice(parentIndex, 1);
                }
                parentCheckbox.checked = false;
                parentCheckbox.indeterminate = true;
            }
        }
    }

    updateFiltersCount();
}

function toggleSubgenres(subgenreListId, toggleIcon) {
    const subList = document.getElementById(subgenreListId);
    if (subList) {
        if (subList.style.display === 'none' || subList.style.display === '') {
            subList.style.display = 'block';
            if (toggleIcon) {
                toggleIcon.classList.add('rotated');
            }
        } else {
            subList.style.display = 'none';
            if (toggleIcon) {
                toggleIcon.classList.remove('rotated');
            }
        }
    }
}

function filterGenreList(searchTerm) {
    const container = document.getElementById('genre-list');
    if (!container) return;

    const genreGroups = container.querySelectorAll('.genre-group');
    const term = searchTerm.toLowerCase();

    genreGroups.forEach(group => {
        const mainLabel = group.querySelector('.genre-main label');
        const subItems = group.querySelectorAll('.subgenre-list .filter-option');
        let hasMatch = false;

        if (mainLabel && mainLabel.textContent.toLowerCase().includes(term)) {
            hasMatch = true;
        } else {
            subItems.forEach(item => {
                const label = item.querySelector('label');
                if (label && label.textContent.toLowerCase().includes(term)) {
                    hasMatch = true;
                    const subList = group.querySelector('.subgenre-list');
                    if (subList) {
                        subList.style.display = 'block';
                        const toggleIcon = group.querySelector('.genre-toggle');
                        if (toggleIcon) {
                            toggleIcon.classList.add('rotated');
                        }
                    }
                }
            });
        }

        group.style.display = hasMatch ? 'block' : 'none';
    });
}

function renderFilterOptions() {
    if (availableFilters.genres_tree && availableFilters.genres_tree.length > 0) {
        renderGenreTree(availableFilters.genres_tree, filters.genres);
        setTimeout(() => {
            updateAllGenreCheckboxesState();
        }, 0);
    } else if (availableFilters.genres_flat && availableFilters.genres_flat.length > 0) {
        const sortedGenres = [...availableFilters.genres_flat].sort((a, b) => a.localeCompare(b, 'ru'));
        renderCheckboxList('genre-list', sortedGenres, filters.genres, 'genre');
    } else {
        document.getElementById('genre-list').innerHTML = '<div class="text-muted">Нет данных</div>';
    }

    if (availableFilters.years && availableFilters.years.length > 0) {
        const sortedYears = [...availableFilters.years].sort((a, b) => parseInt(b) - parseInt(a));
        renderCheckboxList('year-list', sortedYears, filters.years, 'year');
    } else {
        document.getElementById('year-list').innerHTML = '<div class="text-muted">Нет данных</div>';
    }

    if (availableFilters.countries && availableFilters.countries.length > 0) {
        const sortedCountries = [...availableFilters.countries].sort((a, b) => a.localeCompare(b, 'ru'));
        renderCheckboxList('country-list', sortedCountries, filters.countries, 'country');
    } else {
        document.getElementById('country-list').innerHTML = '<div class="text-muted">Нет данных</div>';
    }

    if (availableFilters.actors && availableFilters.actors.length > 0) {
        const sortedActors = [...availableFilters.actors].sort((a, b) => a.localeCompare(b, 'ru'));
        renderCheckboxList('actor-list', sortedActors, filters.actors, 'actor');
    } else {
        document.getElementById('actor-list').innerHTML = '<div class="text-muted">Нет данных</div>';
    }

    if (availableFilters.directors && availableFilters.directors.length > 0) {
        const sortedDirectors = [...availableFilters.directors].sort((a, b) => a.localeCompare(b, 'ru'));
        renderCheckboxList('director-list', sortedDirectors, filters.directors, 'director');
    } else {
        document.getElementById('director-list').innerHTML = '<div class="text-muted">Нет данных</div>';
    }
}

function renderCheckboxList(containerId, items, selectedItems, type) {
    const container = document.getElementById(containerId);
    if (!container) return;

    if (!items || items.length === 0) {
        container.innerHTML = '<div class="text-muted">Нет данных</div>';
        return;
    }

    const sortedItems = [...items].sort((a, b) => {
        if (type === 'year') {
            return parseInt(b) - parseInt(a);
        } else {
            return String(a).localeCompare(String(b), 'ru');
        }
    });

    let html = '';
    sortedItems.forEach(item => {
        if (!item) return;
        const isChecked = selectedItems.includes(item);
        const safeItem = escapeHtml(String(item));

        let filterType = type;
        if (type === 'genre') filterType = 'genres';
        if (type === 'year') filterType = 'years';
        if (type === 'country') filterType = 'countries';
        if (type === 'actor') filterType = 'actors';
        if (type === 'director') filterType = 'directors';

        const itemId = `${filterType}_${safeItem.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
        html += `
            <div class="filter-option">
                <input type="checkbox" id="${itemId}"
                       value="${safeItem}" ${isChecked ? 'checked' : ''}
                       onchange="toggleFilterFromModal('${filterType}', '${safeItem.replace(/'/g, "\\'")}')">
                <label for="${itemId}">${safeItem}</label>
                <span class="count">(${getItemCount(type, item)})</span>
            </div>
        `;
    });
    container.innerHTML = html;
}

function toggleFilterFromModal(type, value) {
    if (!filters[type]) {
        console.error('Неизвестный тип фильтра:', type);
        return;
    }

    const index = filters[type].indexOf(value);
    const isAdding = index === -1;

    if (isAdding) {
        filters[type].push(value);
    } else {
        filters[type].splice(index, 1);
    }

    if (type === 'genres' && availableFilters.genres_tree) {
        const genreInfo = findGenreInTree(availableFilters.genres_tree, value);

        if (genreInfo) {
            if (genreInfo.isMain) {
                if (isAdding) {
                    genreInfo.subgenres.forEach(subgenre => {
                        if (!filters.genres.includes(subgenre.name)) {
                            filters.genres.push(subgenre.name);
                        }
                    });
                } else {
                    genreInfo.subgenres.forEach(subgenre => {
                        const subIndex = filters.genres.indexOf(subgenre.name);
                        if (subIndex !== -1) {
                            filters.genres.splice(subIndex, 1);
                        }
                    });
                }
                genreInfo.subgenres.forEach(subgenre => {
                    const subCheckboxId = `subgenre_${subgenre.name.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
                    const subCheckbox = document.getElementById(subCheckboxId);
                    if (subCheckbox) {
                        subCheckbox.checked = isAdding;
                    }
                });
            } else if (genreInfo.parent) {
                setTimeout(() => {
                    const parentCheckboxId = `genre_${genreInfo.parentName.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
                    const parentCheckbox = document.getElementById(parentCheckboxId);
                    if (parentCheckbox) {
                        let selectedSubCount = 0;
                        genreInfo.parent.subgenres.forEach(sub => {
                            if (filters.genres.includes(sub.name)) {
                                selectedSubCount++;
                            }
                        });

                        if (selectedSubCount === genreInfo.parent.subgenres.length && selectedSubCount > 0) {
                            if (!filters.genres.includes(genreInfo.parentName)) {
                                filters.genres.push(genreInfo.parentName);
                            }
                            parentCheckbox.checked = true;
                            parentCheckbox.indeterminate = false;
                        } else if (selectedSubCount === 0) {
                            const parentIndex = filters.genres.indexOf(genreInfo.parentName);
                            if (parentIndex !== -1) {
                                filters.genres.splice(parentIndex, 1);
                            }
                            parentCheckbox.checked = false;
                            parentCheckbox.indeterminate = false;
                        } else {
                            const parentIndex = filters.genres.indexOf(genreInfo.parentName);
                            if (parentIndex !== -1) {
                                filters.genres.splice(parentIndex, 1);
                            }
                            parentCheckbox.checked = false;
                            parentCheckbox.indeterminate = true;
                        }
                    }
                }, 0);
            }
        }
    }

    updateFiltersCount();

    if (type === 'genres' && availableFilters.genres_tree) {
        updateAllGenreCheckboxesState();
    }
}

function findGenreInTree(genresTree, genreName, parent = null, parentName = null) {
    for (const genre of genresTree) {
        if (genre.name === genreName) {
            return {
                isMain: true,
                name: genre.name,
                subgenres: genre.subgenres || [],
                parent: null,
                parentName: null
            };
        }

        if (genre.subgenres && genre.subgenres.length > 0) {
            for (const subgenre of genre.subgenres) {
                if (subgenre.name === genreName) {
                    return {
                        isMain: false,
                        name: subgenre.name,
                        subgenres: [],
                        parent: genre,
                        parentName: genre.name
                    };
                }
            }
        }
    }
    return null;
}

function updateAllGenreCheckboxesState() {
    const allCheckboxes = document.querySelectorAll('#genre-list input[type="checkbox"]');
    allCheckboxes.forEach(cb => {
        const value = cb.value;
        cb.checked = filters.genres.includes(value);
        cb.indeterminate = false;
    });

    if (availableFilters.genres_tree) {
        availableFilters.genres_tree.forEach(genre => {
            if (genre.subgenres && genre.subgenres.length > 0) {
                let selectedCount = 0;
                genre.subgenres.forEach(subgenre => {
                    if (filters.genres.includes(subgenre.name)) {
                        selectedCount++;
                    }
                });

                const parentCheckboxId = `genre_${genre.name.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
                const parentCheckbox = document.getElementById(parentCheckboxId);

                if (parentCheckbox) {
                    if (selectedCount > 0 && selectedCount < genre.subgenres.length) {
                        parentCheckbox.indeterminate = true;
                        parentCheckbox.checked = false;
                    } else {
                        parentCheckbox.indeterminate = false;
                        parentCheckbox.checked = selectedCount === genre.subgenres.length && selectedCount > 0;
                    }
                }
            }
        });
    }
}

function updateFiltersCount() {
    let totalCount = 0;
    for (const [type, values] of Object.entries(filters)) {
        if (values && values.length > 0) {
            totalCount += values.length;
        }
    }

    const filtersCountSpan = document.getElementById('filters-count');
    if (filtersCountSpan) {
        if (totalCount > 0) {
            filtersCountSpan.style.display = 'inline-block';
            filtersCountSpan.textContent = totalCount;
        } else {
            filtersCountSpan.style.display = 'none';
        }
    }
}

function applyFiltersAndClose() {
    applyFilters();
    if (filtersModal) filtersModal.hide();
}

function updateSelectedFiltersDisplay() {
    const container = document.getElementById('selected-filters');
    const listContainer = document.getElementById('selected-filters-list');

    if (!container || !listContainer) return;

    let hasFilters = false;
    let html = '';

    for (const [type, values] of Object.entries(filters)) {
        if (values && values.length > 0) {
            hasFilters = true;
            values.forEach(value => {
                html += `
                    <span class="selected-filter-tag">
                        ${getFilterTypeName(type)}: ${escapeHtml(value)}
                        <i class="fas fa-times" onclick="removeFilter('${type}', '${escapeHtml(value)}')"></i>
                    </span>
                `;
            });
        }
    }

    if (hasFilters) {
        container.style.display = 'block';
        listContainer.innerHTML = html;
    } else {
        container.style.display = 'none';
    }

    updateFiltersCount();
}

function getFilterTypeName(type) {
    const names = {
        genres: 'Жанр',
        years: 'Год',
        countries: 'Страна',
        actors: 'Актер',
        directors: 'Режиссер'
    };
    return names[type] || type;
}

function openFiltersModal() {
    renderFilterOptions();
    if (filtersModal) filtersModal.show();
}

function getItemCount(type, item) {
    if (!allMoviesForFilters || allMoviesForFilters.length === 0) return 0;

    if (type === 'genre') {
        return allMoviesForFilters.filter(movie => {
            return movie.genres && movie.genres.includes(item);
        }).length;
    }

    if (type === 'year') {
        return allMoviesForFilters.filter(movie => {
            if (!movie.year) return false;
            const yearNum = parseInt(item);
            const movieYearNum = parseInt(movie.year);
            return !isNaN(movieYearNum) && movieYearNum === yearNum;
        }).length;
    }

    if (type === 'country') {
        return allMoviesForFilters.filter(movie => {
            return movie.countries && movie.countries.includes(item);
        }).length;
    }

    if (type === 'actor') {
        return allMoviesForFilters.filter(movie => {
            return movie.actors && movie.actors.includes(item);
        }).length;
    }

    if (type === 'director') {
        return allMoviesForFilters.filter(movie => {
            return movie.directors && movie.directors.includes(item);
        }).length;
    }

    return 0;
}

function setupSearchListeners() {
    const genreSearch = document.getElementById('genre-search');
    if (genreSearch) {
        genreSearch.addEventListener('input', function() {
            filterGenreList(this.value);
        });
    }

    const searchInputs = ['country-search', 'actor-search', 'director-search'];
    searchInputs.forEach(id => {
        const input = document.getElementById(id);
        if (input) {
            input.addEventListener('input', function() {
                const listId = id.replace('-search', '-list');
                filterList(listId, this.value);
            });
        }
    });
}

function filterList(listId, searchTerm) {
    const container = document.getElementById(listId);
    if (!container) return;

    const items = container.querySelectorAll('.filter-option');
    const term = searchTerm.toLowerCase();

    items.forEach(item => {
        const label = item.querySelector('label');
        if (label && label.textContent.toLowerCase().includes(term)) {
            item.style.display = 'flex';
        } else {
            item.style.display = 'none';
        }
    });
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

window.showMovieDetails = window.showMovieDetails || function(movieId) {
    console.log('Show movie details:', movieId);
    if (typeof window.loadMovieDetails === 'function') {
        window.loadMovieDetails(movieId);
    }
};