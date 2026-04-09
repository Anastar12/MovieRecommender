// Каталог фильмов
let allMovies = [];
let filteredMovies = [];
let currentPage = 1;
let itemsPerPage = 20;
let filtersModal = null;

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
    genres: [],
    years: [],
    countries: [],
    actors: [],
    directors: []
};

let genreMapping = {};

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    console.log('Страница загружена, инициализация...');
    const modalElement = document.getElementById('filtersModal');
    if (modalElement) {
        filtersModal = new bootstrap.Modal(modalElement);
    }
    loadCatalog();
    setupSearchListeners();
});

function loadCatalog() {
    console.log('Загрузка каталога...');

    fetch('/api/catalog')
        .then(response => {
            console.log('Ответ получен, статус:', response.status);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Данные получены:', data);

            if (data.error) {
                throw new Error(data.error);
            }

            if (data.movies && data.movies.length > 0) {
                allMovies = data.movies;
                availableFilters = data.filters || {
                    genres_tree: [],
                    genres_flat: [],
                    years: [],
                    countries: [],
                    actors: [],
                    directors: []
                };
                renderFilterOptions();
                filteredMovies = [...allMovies];
                displayMovies();
            } else {
                document.getElementById('movies-container').innerHTML = `
                    <div class="alert alert-warning text-center p-5">
                        <i class="fas fa-exclamation-triangle fa-3x mb-3"></i>
                        <p>Фильмы не найдены в базе данных</p>
                        <small>Проверьте наличие данных в recommender.movies_df</small>
                    </div>
                `;
            }
        })
        .catch(error => {
            console.error('Ошибка загрузки каталога:', error);
            document.getElementById('movies-container').innerHTML = `
                <div class="alert alert-danger text-center p-5">
                    <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                    <p>Ошибка при загрузке каталога</p>
                    <small>${error.message}</small>
                    <div class="mt-3">
                        <button class="btn btn-outline-danger" onclick="loadCatalog()">
                            <i class="fas fa-sync"></i> Попробовать снова
                        </button>
                    </div>
                </div>
            `;
        });
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
                    const subCheckboxId = `subgenre_${subgenreName.replace(/[^a-zA-Z0-9а-яА-ЯёЁ]/g, '_')}`;
                    const subCheckbox = document.getElementById(subCheckboxId);
                    if (subCheckbox) subCheckbox.checked = true;
                }
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

    console.log('Фильтры после изменения:', filters);
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

    console.log('Фильтры после изменения:', filters);
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
    console.log('Рендеринг фильтров...');

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

function getItemCount(type, item) {
    if (!allMovies || allMovies.length === 0) return 0;

    if (type === 'genre') {
        let isParentGenre = false;
        let subgenresList = [];

        if (availableFilters.genres_tree) {
            const genreInfo = findGenreInTree(availableFilters.genres_tree, item);
            if (genreInfo && genreInfo.isMain && genreInfo.subgenres && genreInfo.subgenres.length > 0) {
                isParentGenre = true;
                subgenresList = genreInfo.subgenres.map(sub => sub.name);
            }
        }

        if (isParentGenre) {
            const movieIdsWithSubgenres = new Set();
            allMovies.forEach(movie => {
                if (movie.genres && movie.genres.length > 0) {
                    const hasSubgenre = movie.genres.some(genre => subgenresList.includes(genre));
                    if (hasSubgenre) {
                        movieIdsWithSubgenres.add(movie.movie_id);
                    }
                }
            });
            return movieIdsWithSubgenres.size;
        } else {
            return allMovies.filter(movie => {
                return movie.genres && movie.genres.includes(item);
            }).length;
        }
    }

    if (type === 'year') {
        return allMovies.filter(movie => {
            if (!movie.year_range && !movie.year) return false;
            const yearNum = parseInt(item);
            if (isNaN(yearNum)) return false;
            if (movie.year_range) {
                return yearNum >= movie.year_range.start && yearNum <= movie.year_range.end;
            } else if (movie.year) {
                const movieYearNum = parseInt(movie.year);
                return !isNaN(movieYearNum) && movieYearNum === yearNum;
            }
            return false;
        }).length;
    }

    if (type === 'country') {
        return allMovies.filter(movie => {
            return movie.countries && movie.countries.includes(item);
        }).length;
    }

    if (type === 'actor') {
        return allMovies.filter(movie => {
            return movie.actors && movie.actors.includes(item);
        }).length;
    }

    if (type === 'director') {
        return allMovies.filter(movie => {
            return movie.directors && movie.directors.includes(item);
        }).length;
    }

    return 0;
}

function toggleFilterFromModal(type, value) {
    console.log('toggleFilterFromModal вызван:', type, value);

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

    console.log('Фильтры после изменения:', filters);
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

function updateParentGenreState(parentGenre, parentName) {
    if (!parentGenre || !parentGenre.subgenres) return;

    let allSubgenresSelected = true;
    let anySubgenreSelected = false;

    for (const subgenre of parentGenre.subgenres) {
        const isSelected = filters.genres.includes(subgenre.name);
        if (!isSelected) {
            allSubgenresSelected = false;
        }
        if (isSelected) {
            anySubgenreSelected = true;
        }
    }

    const parentCheckboxId = `genre_${parentName.replace(/[^a-zA-Z0-9]/g, '_')}`;
    const parentCheckbox = document.getElementById(parentCheckboxId);

    if (parentCheckbox) {
        if (allSubgenresSelected && anySubgenreSelected) {
            if (!filters.genres.includes(parentName)) {
                filters.genres.push(parentName);
                parentCheckbox.checked = true;
            }
        } else if (!anySubgenreSelected) {
            const parentIndex = filters.genres.indexOf(parentName);
            if (parentIndex !== -1) {
                filters.genres.splice(parentIndex, 1);
                parentCheckbox.checked = false;
            }
        } else {
            const parentIndex = filters.genres.indexOf(parentName);
            if (parentIndex !== -1) {
                filters.genres.splice(parentIndex, 1);
                parentCheckbox.checked = false;
            }
            parentCheckbox.indeterminate = true;
        }
    }
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

function updateGenreCheckboxesState() {
    const checkboxes = document.querySelectorAll('#genre-list input[type="checkbox"]');
    checkboxes.forEach(cb => {
        const value = cb.value;
        cb.checked = filters.genres.includes(value);
    });
}

function updateFiltersCount() {
    let totalCount = 0;
    for (const [type, values] of Object.entries(filters)) {
        if (values && values.length > 0) {
            totalCount += values.length;
        }
    }

    const filtersCountSpan = document.getElementById('filters-count');
    if (totalCount > 0) {
        filtersCountSpan.style.display = 'inline-block';
        filtersCountSpan.textContent = totalCount;
    } else {
        filtersCountSpan.style.display = 'none';
    }
}

function applyFiltersAndClose() {
    applyFilters();
    if (filtersModal) filtersModal.hide();
}

function applyFilters() {
    console.log('Применяем фильтры:', filters);
    console.log('Всего фильмов до фильтрации:', allMovies.length);

    if (!allMovies || allMovies.length === 0) {
        filteredMovies = [];
        displayMovies();
        return;
    }

    filteredMovies = allMovies.filter(movie => {
        if (filters.genres.length > 0) {
            if (!movie.genres_en || movie.genres_en.length === 0) return false;

            let expandedGenres = [...filters.genres];

            if (availableFilters.genres_tree) {
                filters.genres.forEach(genre => {
                    const genreInfo = findGenreInTree(availableFilters.genres_tree, genre);
                    if (genreInfo && genreInfo.isMain && genreInfo.subgenres && genreInfo.subgenres.length > 0) {
                        genreInfo.subgenres.forEach(subgenre => {
                            if (!expandedGenres.includes(subgenre.name)) {
                                expandedGenres.push(subgenre.name);
                            }
                        });
                    }
                });
            }

            const hasGenre = expandedGenres.some(genreRu => {
                const genreEn = genreMapping[genreRu] || genreRu;
                return movie.genres_en.includes(genreEn);
            });

            if (!hasGenre) return false;
        }

        if (filters.years.length > 0) {
            if (!movie.year_range && !movie.year) return false;

            let movieYearRange = movie.year_range;

            if (movieYearRange) {
                const hasMatchingYear = filters.years.some(filterYear => {
                    const yearNum = parseInt(filterYear);
                    if (isNaN(yearNum)) return false;
                    return yearNum >= movieYearRange.start && yearNum <= movieYearRange.end;
                });
                if (!hasMatchingYear) return false;
            } else if (movie.year) {
                const movieYear = parseInt(movie.year);
                if (isNaN(movieYear)) return false;
                const hasMatchingYear = filters.years.some(filterYear => {
                    const yearNum = parseInt(filterYear);
                    return !isNaN(yearNum) && movieYear === yearNum;
                });
                if (!hasMatchingYear) return false;
            } else {
                return false;
            }
        }

        if (filters.countries.length > 0) {
            if (!movie.countries || movie.countries.length === 0) return false;
            const hasCountry = filters.countries.some(c => movie.countries.includes(c));
            if (!hasCountry) return false;
        }

        if (filters.actors.length > 0) {
            if (!movie.actors || movie.actors.length === 0) return false;
            const hasActor = filters.actors.some(a => movie.actors.includes(a));
            if (!hasActor) return false;
        }

        if (filters.directors.length > 0) {
            if (!movie.directors || movie.directors.length === 0) return false;
            const hasDirector = filters.directors.some(d => movie.directors.includes(d));
            if (!hasDirector) return false;
        }

        return true;
    });

    console.log('Найдено фильмов после фильтрации:', filteredMovies.length);

    currentPage = 1;
    displayMovies();
    updateSelectedFiltersDisplay();
}

function resetFilters() {
    filters = {
        genres: [],
        years: [],
        countries: [],
        actors: [],
        directors: []
    };

    const modalCheckboxes = document.querySelectorAll('#filtersModal input[type="checkbox"]');
    modalCheckboxes.forEach(cb => {
        cb.checked = false;
        cb.indeterminate = false;
    });

    filteredMovies = [...allMovies];
    currentPage = 1;
    displayMovies();
    updateSelectedFiltersDisplay();
    updateFiltersCount();
}

function updateSelectedFiltersDisplay() {
    const container = document.getElementById('selected-filters');
    const listContainer = document.getElementById('selected-filters-list');

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

function removeFilter(type, value) {
    console.log('removeFilter вызван:', type, value);

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

    applyFilters();
}

function openFiltersModal() {
    renderFilterOptions();
    if (filtersModal) filtersModal.show();
}

function displayMovies() {
    const container = document.getElementById('movies-container');
    const resultsCount = document.getElementById('results-count');

    if (!filteredMovies || filteredMovies.length === 0) {
        container.innerHTML = '<div class="no-results"><i class="fas fa-film fa-3x mb-3"></i><p>Фильмы не найдены</p></div>';
        document.getElementById('pagination').innerHTML = '';
        if (resultsCount) resultsCount.textContent = '0';
        return;
    }

    const sortBy = document.getElementById('sort-by')?.value || 'rating_desc';
    let sorted = [...filteredMovies];

    switch(sortBy) {
        case 'rating_desc':
            sorted.sort((a, b) => (b.imdb_rating || 0) - (a.imdb_rating || 0));
            break;
        case 'rating_asc':
            sorted.sort((a, b) => (a.imdb_rating || 0) - (b.imdb_rating || 0));
            break;
        case 'year_desc':
            sorted.sort((a, b) => (b.year || 0) - (a.year || 0));
            break;
        case 'year_asc':
            sorted.sort((a, b) => (a.year || 0) - (b.year || 0));
            break;
        case 'title_asc':
            sorted.sort((a, b) => (a.title_ru || a.title).localeCompare(b.title_ru || b.title));
            break;
        case 'title_desc':
            sorted.sort((a, b) => (b.title_ru || b.title).localeCompare(a.title_ru || a.title));
            break;
    }

    if (resultsCount) resultsCount.textContent = sorted.length;

    const totalPages = Math.ceil(sorted.length / itemsPerPage);
    const start = (currentPage - 1) * itemsPerPage;
    const end = start + itemsPerPage;
    const pageMovies = sorted.slice(start, end);

    let html = '<div class="row">';
    pageMovies.forEach(movie => {
        const posterUrl = movie.poster ? `/img/horizontal/${movie.poster}` : '/img/horizontal/placeholder.jpg';
        const rating = movie.imdb_rating ? `IMDb: ${movie.imdb_rating}` : '';
        const displayTitle = movie.title_ru || movie.title;

        let displayGenres = 'Жанр не указан';
        if (movie.genres && movie.genres.length > 0) {
            displayGenres = movie.genres.slice(0, 2).join(', ');
        } else if (movie.genre_ru) {
            displayGenres = movie.genre_ru;
        } else if (movie.genre) {
            displayGenres = movie.genre;
        }

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
                        <div class="movie-genre">${escapeHtml(displayGenres)}</div>
                        ${rating ? `<div class="movie-rating"><i class="fas fa-star"></i> ${rating}</div>` : ''}
                    </div>
                </div>
            </div>
        `;
    });
    html += '</div>';
    container.innerHTML = html;

    renderPagination(totalPages);
}

function renderPagination(totalPages) {
    const paginationContainer = document.getElementById('pagination');
    if (totalPages <= 1) {
        paginationContainer.innerHTML = '';
        return;
    }

    let html = '';

    html += `
        <li class="page-item ${currentPage === 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage(${currentPage - 1}); return false;">«</a>
        </li>
    `;

    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);

    for (let i = startPage; i <= endPage; i++) {
        html += `
            <li class="page-item ${i === currentPage ? 'active' : ''}">
                <a class="page-link" href="#" onclick="changePage(${i}); return false;">${i}</a>
            </li>
        `;
    }

    html += `
        <li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage(${currentPage + 1}); return false;">»</a>
        </li>
    `;

    paginationContainer.innerHTML = html;
}

function changePage(page) {
    currentPage = page;
    displayMovies();
    window.scrollTo({ top: 0, behavior: 'smooth' });
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
