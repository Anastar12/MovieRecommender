let usersList = [];
let currentMode = 'select';

async function loadUsers() {
    try {
        const response = await fetch('/api/users/list');
        const data = await response.json();

        const select = document.getElementById('user-select');

        if (data.users && data.users.length > 0) {
            usersList = data.users;
            select.innerHTML = '<option value="">Выберите пользователя...</option>';

            const uniqueUsers = new Map();
            data.users.forEach(user => {
                if (!uniqueUsers.has(user.user_url)) {
                    uniqueUsers.set(user.user_url, user.username);
                }
            });

            uniqueUsers.forEach((username, userUrl) => {
                const option = document.createElement('option');
                option.value = userUrl;
                option.textContent = username;
                select.appendChild(option);
            });

            console.log(`Загружено ${uniqueUsers.size} пользователей`);
        } else {
            select.innerHTML = '<option value="">Нет доступных пользователей</option>';
        }
    } catch (error) {
        console.error('Ошибка загрузки пользователей:', error);
        const select = document.getElementById('user-select');
        select.innerHTML = '<option value="">Ошибка загрузки пользователей</option>';
        showError('Не удалось загрузить список пользователей');
    }
}

function findUserByUsername(username) {
    if (!usersList.length) return null;

    const normalizedInput = username.toLowerCase().trim();
    const foundUser = usersList.find(user =>
        user.username && user.username.toLowerCase() === normalizedInput
    );

    return foundUser || null;
}

function showError(message) {
    const errorDiv = document.getElementById('error-message');
    const errorText = document.getElementById('error-text');
    if (errorDiv && errorText) {
        errorText.textContent = message;
        errorDiv.style.display = 'block';
        setTimeout(() => {
            errorDiv.style.display = 'none';
        }, 5000);
    }
}

function setLoginMode(mode) {
    currentMode = mode;

    const selectModeDiv = document.getElementById('select-mode');
    const inputModeDiv = document.getElementById('input-mode');
    const selectBtn = document.getElementById('select-mode-btn');
    const inputBtn = document.getElementById('input-mode-btn');
    const userSelect = document.getElementById('user-select');
    const usernameInput = document.getElementById('username-input');

    if (mode === 'select') {
        selectModeDiv.style.display = 'block';
        inputModeDiv.style.display = 'none';
        selectBtn.classList.add('active');
        inputBtn.classList.remove('active');
        // Включаем required только для select
        if (userSelect) userSelect.required = true;
        if (usernameInput) usernameInput.required = false;
    } else {
        selectModeDiv.style.display = 'none';
        inputModeDiv.style.display = 'block';
        selectBtn.classList.remove('active');
        inputBtn.classList.add('active');
        // Отключаем required для select, включаем для input
        if (userSelect) userSelect.required = false;
        if (usernameInput) usernameInput.required = true;
    }
}

async function performLogin(userUrl) {
    const loginBtn = document.getElementById('login-btn');
    const loadingSpinner = document.getElementById('loading-spinner');

    if (loginBtn) loginBtn.disabled = true;
    if (loadingSpinner) loadingSpinner.style.display = 'block';

    try {
        console.log('Выполняется вход с user_url:', userUrl);

        const response = await fetch('/api/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_url: userUrl })
        });

        const data = await response.json();

        if (data.success) {
            window.location.href = '/main';
        } else {
            showError(data.error || 'Ошибка при входе');
            if (loginBtn) loginBtn.disabled = false;
            if (loadingSpinner) loadingSpinner.style.display = 'none';
        }
    } catch (error) {
        console.error('Ошибка при входе:', error);
        showError('Ошибка соединения с сервером');
        if (loginBtn) loginBtn.disabled = false;
        if (loadingSpinner) loadingSpinner.style.display = 'none';
    }
}

async function createNewUserAndLogin(username) {
    const loginBtn = document.getElementById('login-btn');
    const loadingSpinner = document.getElementById('loading-spinner');

    if (loginBtn) loginBtn.disabled = true;
    if (loadingSpinner) loadingSpinner.style.display = 'block';

    try {
        const response = await fetch('/api/user/create', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: username })
        });

        const data = await response.json();

        if (data.success) {
            await loadUsers();
            await performLogin(data.user_url);
        } else {
            showError(data.error || 'Ошибка создания пользователя');
            if (loginBtn) loginBtn.disabled = false;
            if (loadingSpinner) loadingSpinner.style.display = 'none';
        }
    } catch (error) {
        console.error('Ошибка создания пользователя:', error);
        showError('Ошибка соединения с сервером');
        if (loginBtn) loginBtn.disabled = false;
        if (loadingSpinner) loadingSpinner.style.display = 'none';
    }
}

const loginForm = document.getElementById('login-form');
if (loginForm) {
    loginForm.addEventListener('submit', async (e) => {
        e.preventDefault();

        if (currentMode === 'select') {
            const userSelect = document.getElementById('user-select');
            const userUrl = userSelect.value;

            if (!userUrl) {
                showError('Пожалуйста, выберите пользователя');
                return;
            }

            await performLogin(userUrl);
        } else {
            const usernameInput = document.getElementById('username-input');
            const username = usernameInput.value.trim();

            if (!username) {
                showError('Пожалуйста, введите имя пользователя');
                return;
            }

            if (username.length < 2) {
                showError('Имя пользователя должно содержать минимум 2 символа');
                return;
            }

            if (username.length > 50) {
                showError('Имя пользователя не должно превышать 50 символов');
                return;
            }

            const existingUser = findUserByUsername(username);

            if (existingUser && existingUser.user_url) {
                await performLogin(existingUser.user_url);
            } else {
                const createNew = confirm(`Пользователь "${username}" не найден. Хотите создать нового пользователя с этим именем?`);

                if (createNew) {
                    await createNewUserAndLogin(username);
                }
            }
        }
    });
}

document.addEventListener('DOMContentLoaded', function() {
    const modalElement = document.getElementById('createUserModal');
    const openBtn = document.getElementById('openModalBtn');
    const closeBtn = document.getElementById('closeModalBtn');
    const cancelBtn = document.getElementById('cancelModalBtn');
    const createUserBtn = document.getElementById('create-user-btn');
    const usernameInput = document.getElementById('new-username');
    const errorDiv = document.getElementById('create-user-error');
    const successDiv = document.getElementById('create-user-success');

    const selectModeBtn = document.getElementById('select-mode-btn');
    const inputModeBtn = document.getElementById('input-mode-btn');

    if (selectModeBtn && inputModeBtn) {
        selectModeBtn.addEventListener('click', () => setLoginMode('select'));
        inputModeBtn.addEventListener('click', () => setLoginMode('input'));
    }

    let modal = null;
    if (modalElement) {
        modal = new bootstrap.Modal(modalElement, {
            backdrop: 'static',
            keyboard: true
        });
    }

    if (openBtn) {
        openBtn.addEventListener('click', function() {
            if (usernameInput) usernameInput.value = '';
            if (errorDiv) errorDiv.style.display = 'none';
            if (successDiv) successDiv.style.display = 'none';
            if (modal) modal.show();
            setTimeout(() => {
                if (usernameInput) usernameInput.focus();
            }, 100);
        });
    }

    function closeModal() {
        if (modal) modal.hide();
        setTimeout(() => {
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
            document.body.style.paddingRight = '';
        }, 150);
    }

    if (closeBtn) closeBtn.addEventListener('click', closeModal);
    if (cancelBtn) cancelBtn.addEventListener('click', closeModal);

    if (createUserBtn && usernameInput) {
        createUserBtn.addEventListener('click', async function() {
            const username = usernameInput.value.trim();

            if (errorDiv) errorDiv.style.display = 'none';
            if (successDiv) successDiv.style.display = 'none';

            if (!username) {
                if (errorDiv) {
                    errorDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> <span>Введите имя пользователя</span>';
                    errorDiv.style.display = 'block';
                }
                usernameInput.focus();
                return;
            }

            if (username.length < 2) {
                if (errorDiv) {
                    errorDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> <span>Имя пользователя должно содержать минимум 2 символа</span>';
                    errorDiv.style.display = 'block';
                }
                usernameInput.focus();
                return;
            }

            if (username.length > 50) {
                if (errorDiv) {
                    errorDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> <span>Имя пользователя не должно превышать 50 символов</span>';
                    errorDiv.style.display = 'block';
                }
                usernameInput.focus();
                return;
            }

            this.disabled = true;
            const originalText = this.innerHTML;
            this.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Создание...';

            try {
                const response = await fetch('/api/user/create', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username: username })
                });

                const data = await response.json();

                if (data.success) {
                    if (successDiv) {
                        successDiv.innerHTML = '<i class="fas fa-check-circle"></i> <span>Пользователь создан! Выполняется вход...</span>';
                        successDiv.style.display = 'block';
                    }

                    await loadUsers();

                    if (currentMode === 'input') {
                        const inputField = document.getElementById('username-input');
                        if (inputField) inputField.value = username;
                    }

                    await performLogin(data.user_url);

                    setTimeout(() => {
                        if (modal) modal.hide();
                    }, 1000);
                } else {
                    if (errorDiv) {
                        errorDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> <span>' + (data.error || 'Ошибка создания пользователя') + '</span>';
                        errorDiv.style.display = 'block';
                    }
                    this.disabled = false;
                    this.innerHTML = originalText;
                }
            } catch (error) {
                console.error('Ошибка:', error);
                if (errorDiv) {
                    errorDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> <span>Ошибка соединения с сервером</span>';
                    errorDiv.style.display = 'block';
                }
                this.disabled = false;
                this.innerHTML = originalText;
            }
        });

        usernameInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                createUserBtn.click();
            }
        });
    }

    if (modalElement) {
        modalElement.addEventListener('hidden.bs.modal', function() {
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
            document.body.style.paddingRight = '';
        });
    }
});

loadUsers();
setLoginMode('select');