const express = require('express');
const { createServer } = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const path = require('path');

const app = express();
const server = createServer(app);
const port = process.env.PORT || 10000;

// Разрешаем CORS (на случай если клиент будет с другого домена)
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
});

app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public'))); // Сюда положим index.html

// --- WebSocket сервер для Пати ---
const wss = new WebSocketServer({ server, path: '/ws' });
const rooms = new Map(); // roomId -> Set of clients

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const roomId = url.searchParams.get('room');
    const userName = url.searchParams.get('name') || 'Аноним';

    if (!roomId) {
        ws.close(1008, 'Room ID required');
        return;
    }

    if (!rooms.has(roomId)) {
        rooms.set(roomId, new Set());
    }
    const clients = rooms.get(roomId);
    clients.add(ws);

    console.log(`[Party] ${userName} подключился к комнате ${roomId}. Всего: ${clients.size}`);

    // Уведомляем всех о новом участнике
    broadcast(roomId, { type: 'join', user: userName, count: clients.size });

    // Обработка сообщений
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            // Пересылаем сообщение всем в комнате, кроме отправителя
            broadcast(roomId, msg, ws);
        } catch (e) {
            console.error('Ошибка парсинга сообщения:', e);
        }
    });

    ws.on('close', () => {
        clients.delete(ws);
        console.log(`[Party] ${userName} отключился от ${roomId}. Осталось: ${clients.size}`);
        broadcast(roomId, { type: 'leave', user: userName, count: clients.size });
        if (clients.size === 0) {
            rooms.delete(roomId);
        }
    });

    // Heartbeat для Render (чтобы соединение не считалось мёртвым)
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
});

// Пингуем клиентов каждые 30 секунд
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => clearInterval(interval));

function broadcast(roomId, message, excludeWs = null) {
    const clients = rooms.get(roomId);
    if (!clients) return;
    const data = JSON.stringify(message);
    clients.forEach((client) => {
        if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
            client.send(data);
        }
    });
}

// --- API для публикации работ ---
let projects = []; // В памяти (для продакшена нужна БД, но для старта сойдёт)

app.post('/api/publish', (req, res) => {
    const { title, author, code, language } = req.body;
    if (!title || !code) {
        return res.status(400).json({ error: 'Нужны title и code' });
    }
    const project = {
        id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
        title,
        author: author || 'Аноним',
        code,
        language: language || 'text',
        publishedAt: new Date().toISOString()
    };
    projects.unshift(project); // Новые сверху
    // Ограничим до 50 последних
    if (projects.length > 50) projects = projects.slice(0, 50);
    console.log(`[Publish] Опубликовано: ${title} от ${author}`);
    res.json({ success: true, project });
});

app.get('/api/projects', (req, res) => {
    res.json(projects);
});

// Health check для Render
app.get('/health', (req, res) => {
    res.json({ status: 'ok', rooms: rooms.size, projects: projects.length });
});

server.listen(port, () => {
    console.log(`Server listening on port ${port}`);
});
