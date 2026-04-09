// Загрузка списка пользователей
async function loadUsers() {
    try {
        const response = await fetch('/api/users/list');
        const data = await response.json();

        const select = document.getElementById('user-select');

        if (data.users && data.users.length > 0) {
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

// Показать ошибку
function showError(message) {
    const errorDiv = document.getElementById('error-message');
    const errorText = document.getElementById('error-text');
    errorText.textContent = message;
    errorDiv.style.display = 'block';

    setTimeout(() => {
        errorDiv.style.display = 'none';
    }, 5000);
}

// Обработка входа
document.getElementById('login-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const userSelect = document.getElementById('user-select');
    const userUrl = userSelect.value;

    if (!userUrl) {
        showError('Пожалуйста, выберите пользователя');
        return;
    }

    const loginBtn = document.getElementById('login-btn');
    const loadingSpinner = document.getElementById('loading-spinner');

    loginBtn.disabled = true;
    loadingSpinner.style.display = 'block';

    try {
        const response = await fetch('/api/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ user_url: userUrl })
        });

        const data = await response.json();

        if (data.success) {
            window.location.href = '/main';
        } else {
            showError(data.error || 'Ошибка при входе');
            loginBtn.disabled = false;
            loadingSpinner.style.display = 'none';
        }
    } catch (error) {
        console.error('Ошибка при входе:', error);
        showError('Ошибка соединения с сервером');
        loginBtn.disabled = false;
        loadingSpinner.style.display = 'none';
    }
});

// Загружаем пользователей при загрузке страницы
loadUsers();
