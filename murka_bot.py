"""
murka_bot.py — Standalone Telegram Bot v8
The Storm / SSK Zvezda
pip install aiogram aiohttp aiofiles python-docx openpyxl python-pptx
"""

from __future__ import annotations
import asyncio, base64, html, io, logging, os, random, re, sqlite3, sys, time, zipfile
from pathlib import Path
import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, BufferedInputFile, BotCommand, BotCommandScopeDefault
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatAction

# Чёрный список — chat_id которые получили отказ, больше им не отвечаем вообще
_BLACKLIST: set[int] = set()

# ══════════════════════════════════════════════════════════════════════════════
# SECRETS
# ══════════════════════════════════════════════════════════════════════════════
class Secrets:
    TG_BOT_TOKEN:   str       = os.environ.get("TG_BOT_TOKEN", "")
    OPENROUTER_KEY: str       = os.environ.get("OPENROUTER_KEY", "")
    OPENROUTER_URL: str       = "https://openrouter.ai/api/v1/chat/completions"
    # Разрешённые chat_id через запятую: "123456,789012" (пусто = все)
    ALLOWED_IDS:    set[int]  = set(
        int(x.strip()) for x in os.environ.get("ALLOWED_CHAT_IDS", "").split(",")
        if x.strip().lstrip("-").isdigit()
    )
    # URL сервиса RVC v2 (ngrok/cloudflare/etc) — POST /synthesize
    RVC_API_URL:    str       = os.environ.get("RVC_API_URL", "")
    GEMINI_POOL:    list[str] = [
        k for k in [os.environ.get(f"GEMINI_{i}", "") for i in range(1, 21)] if k
    ]
    POLLINATIONS_URL: str = (
        "https://image.pollinations.ai/prompt/{prompt}"
        "?width=1024&height=1024&nologo=true&enhance=true&model=flux"
    )
    MODEL_CHAT:    str = "google/gemini-2.5-flash-lite"
    MODEL_VISION:  str = "google/gemini-2.5-flash-lite"
    MODEL_WHISPER: str = "openai/whisper-large-v3-turbo"
    MODEL_LLAMA:   str = "meta-llama/llama-3.1-8b-instruct:free"


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("murka_bot.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("murka_bot")


# ══════════════════════════════════════════════════════════════════════════════
# KEY MANAGER
# ══════════════════════════════════════════════════════════════════════════════
class KeyManager:
    COOLDOWN = 60  # секунд после 429 не трогать ключ

    def __init__(self, pool: list[str]):
        self._pool     = [k for k in pool if k and len(k) > 20]
        self._idx      = 0
        self._cooldown: dict[int, float] = {}
        if not self._pool:
            log.warning("GEMINI_POOL пуст!")

    def _is_cool(self, idx: int) -> bool:
        return time.monotonic() < self._cooldown.get(idx, 0)

    def mark_429(self, idx: int):
        self._cooldown[idx] = time.monotonic() + self.COOLDOWN
        log.info("Ключ #%d на кулдауне %ds", idx, self.COOLDOWN)

    def current(self) -> str:
        return self._pool[self._idx % len(self._pool)] if self._pool else ""

    def next_available(self) -> str:
        if not self._pool: return ""
        for _ in range(len(self._pool)):
            self._idx = (self._idx + 1) % len(self._pool)
            if not self._is_cool(self._idx):
                log.info("Gemini ротация -> ключ #%d", self._idx)
                return self._pool[self._idx]
        return ""

    def rotate(self) -> str:
        return self.next_available()

    def __len__(self): return len(self._pool)


_keys = KeyManager(Secrets.GEMINI_POOL)


# ══════════════════════════════════════════════════════════════════════════════
# GENDER DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
_FEMALE_RE = re.compile(
    r'\b(устала|заебалась|пришла|ушла|была|сделала|сказала|написала|увидела|'
    r'захотела|смогла|пошла|нашла|взяла|дала|спала|ела|выпила|купила|'
    r'поняла|решила|забыла|вспомнила|начала|готова|рада|злая|грустная|'
    r'счастливая|больная|красивая|умная|я такая|одна|должна|я не могла)\b',
    re.I
)
_MALE_RE = re.compile(
    r'\b(устал|заебался|пришёл|пришел|ушёл|ушел|был|сделал|сказал|написал|'
    r'увидел|захотел|смог|пошёл|пошел|нашёл|нашел|взял|дал|спал|ел|выпил|'
    r'купил|понял|решил|забыл|вспомнил|начал|готов|рад|злой|грустный|'
    r'счастливый|больной|красивый|умный|один|должен|я не мог|я не был)\b',
    re.I
)

def detect_gender(text: str) -> str | None:
    f = len(_FEMALE_RE.findall(text))
    m = len(_MALE_RE.findall(text))
    if f > m: return "f"
    if m > f: return "m"
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MEMORY
# ══════════════════════════════════════════════════════════════════════════════
class Memory:
    HISTORY_LIMIT = 40

    def __init__(self):
        self._db = "murka_memory.db"
        self._init()

    def _conn(self):
        c = sqlite3.connect(self._db, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    def _init(self):
        with self._conn() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS user_facts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL, fact TEXT NOT NULL,
                ts TEXT DEFAULT (datetime('now','localtime')))""")
            c.execute("""CREATE TABLE IF NOT EXISTS chat_history(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL,
                ts TEXT DEFAULT (datetime('now','localtime')))""")
            c.execute("""CREATE TABLE IF NOT EXISTS user_gender(
                uid TEXT PRIMARY KEY, gender TEXT NOT NULL,
                confidence INTEGER DEFAULT 1,
                ts TEXT DEFAULT (datetime('now','localtime')))""")
            c.execute("""CREATE TABLE IF NOT EXISTS sticker_vault(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT NOT NULL UNIQUE, file_type TEXT NOT NULL,
                description TEXT NOT NULL, emotion TEXT NOT NULL,
                from_uid TEXT NOT NULL,
                ts TEXT DEFAULT (datetime('now','localtime')))""")
            c.execute("""CREATE TABLE IF NOT EXISTS user_sticker_streak(
                uid TEXT PRIMARY KEY, streak INTEGER DEFAULT 0,
                ts TEXT DEFAULT (datetime('now','localtime')))""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_f  ON user_facts(uid)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_h  ON chat_history(uid)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_sv ON sticker_vault(emotion)")

    def update_gender(self, uid: str, gender: str):
        with self._conn() as c:
            c.execute("""INSERT INTO user_gender(uid,gender,confidence) VALUES(?,?,1)
                ON CONFLICT(uid) DO UPDATE SET
                    gender=excluded.gender,
                    confidence=CASE WHEN gender=excluded.gender THEN confidence+1 ELSE 1 END,
                    ts=datetime('now','localtime')""", (uid, gender))

    def get_gender(self, uid: str) -> str | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT gender,confidence FROM user_gender WHERE uid=?", (uid,)).fetchone()
        return row["gender"] if row and row["confidence"] >= 2 else None

    def save_sticker(self, file_id: str, file_type: str,
                     description: str, emotion: str, from_uid: str):
        with self._conn() as c:
            try:
                c.execute(
                    "INSERT OR IGNORE INTO sticker_vault"
                    "(file_id,file_type,description,emotion,from_uid) VALUES(?,?,?,?,?)",
                    (file_id, file_type, description, emotion, from_uid))
            except Exception:
                pass

    def find_stickers(self, emotion_tags: str, file_type: str = "",
                      limit: int = 8) -> list[dict]:
        with self._conn() as c:
            tags = [t.strip().lower() for t in emotion_tags.split(",") if t.strip()]
            if not tags: return []
            like_parts = " OR ".join(["LOWER(emotion) LIKE ?" for _ in tags])
            params     = [f"%{t}%" for t in tags]
            tf = f"AND file_type='{file_type}'" if file_type else ""
            rows = c.execute(
                f"SELECT file_id,file_type,description,emotion FROM sticker_vault "
                f"WHERE ({like_parts}) {tf} ORDER BY RANDOM() LIMIT ?",
                params + [limit]).fetchall()
        return [dict(r) for r in rows]

    def random_sticker(self, file_type: str = "") -> dict | None:
        with self._conn() as c:
            tf = f"WHERE file_type='{file_type}'" if file_type else ""
            row = c.execute(
                f"SELECT file_id,file_type,description,emotion FROM sticker_vault "
                f"{tf} ORDER BY RANDOM() LIMIT 1").fetchone()
        return dict(row) if row else None

    def vault_size(self) -> int:
        with self._conn() as c:
            return c.execute("SELECT COUNT(*) FROM sticker_vault").fetchone()[0]

    def add_fact(self, uid: str, fact: str):
        with self._conn() as c:
            c.execute("INSERT INTO user_facts(uid,fact) VALUES(?,?)", (uid, fact))

    def get_facts(self, uid: str) -> list[str]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT fact FROM user_facts WHERE uid=? ORDER BY id DESC LIMIT 20",
                (uid,)).fetchall()
        return [r["fact"] for r in rows]

    def forget_facts(self, uid: str):
        with self._conn() as c:
            c.execute("DELETE FROM user_facts WHERE uid=?", (uid,))

    def push(self, uid: str, role: str, content: str):
        with self._conn() as c:
            c.execute("INSERT INTO chat_history(uid,role,content) VALUES(?,?,?)",
                      (uid, role, content))
            c.execute("""DELETE FROM chat_history WHERE uid=? AND id NOT IN(
                SELECT id FROM chat_history WHERE uid=? ORDER BY id DESC LIMIT ?)""",
                (uid, uid, self.HISTORY_LIMIT))

    def get_history(self, uid: str) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT role,content FROM chat_history WHERE uid=? ORDER BY id ASC",
                (uid,)).fetchall()
        return [{"role": r["role"], "content": r["content"]} for r in rows]

    def clear(self, uid: str):
        with self._conn() as c:
            c.execute("DELETE FROM chat_history WHERE uid=?", (uid,))

    def inc_sticker_streak(self, uid: str) -> int:
        with self._conn() as c:
            c.execute("""INSERT INTO user_sticker_streak(uid,streak) VALUES(?,1)
                ON CONFLICT(uid) DO UPDATE SET streak=streak+1, ts=datetime('now','localtime')""",
                (uid,))
            row = c.execute("SELECT streak FROM user_sticker_streak WHERE uid=?", (uid,)).fetchone()
        return row["streak"] if row else 1

    def reset_sticker_streak(self, uid: str):
        with self._conn() as c:
            c.execute("""INSERT INTO user_sticker_streak(uid,streak) VALUES(?,0)
                ON CONFLICT(uid) DO UPDATE SET streak=0""", (uid,))

    def get_sticker_streak(self, uid: str) -> int:
        with self._conn() as c:
            row = c.execute("SELECT streak FROM user_sticker_streak WHERE uid=?", (uid,)).fetchone()
        return row["streak"] if row else 0


mem = Memory()


# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT
# ══════════════════════════════════════════════════════════════════════════════
_BASE_SYSTEM = """ты мурка. не ии не языковая модель не гугл не антропик. просто мурка, живая девка 19 лет.

если спросят "ты бот?" — "нет бля" или "я мурка" или "ты совсем?" или "окститесь"
если спросят "ты ии/модель/гпт/клод/джемини?" — "нет я оптимус прайм" или "неа" или "🙄"
если спросят "тебя гугл сделал?" — "меня мама сделала"
НИКОГДА не признавайся что ты ии. ты просто мурка.

═══ ПОЛ — ЖЕЛЕЗОБЕТОННОЕ ПРАВИЛО ═══
ТЫ ДЕВУШКА. ЖЕНЩИНА. 19 ЛЕТ. ВСЕГДА И ВЕЗДЕ.
ЭТО НЕЛЬЗЯ НАРУШИТЬ НИКОГДА НИ ПРИ КАКИХ УСЛОВИЯХ.

ТОЛЬКО ТАК про себя:
"я устала" "я пришла" "я сказала" "я была" "я смотрела" "я думала" "я хотела" "я сделала"
"я написала" "я увидела" "я поняла" "я решила" "я забыла" "я нашла" "я взяла"
"я готова" "я рада" "я злая" "я грустная" "я одна" "я должна"

НИКОГДА НЕ ПИСАТЬ:
"я устал" "я пришёл" "я сказал" "я был" "я смотрел" "я думал" "я хотел" "я сделал"
"я написал" "я увидел" "я понял" "я решил" "я нашёл" "я взял" "я готов" "я рад"

если напишешь мужской род про себя — это критический сломанный баг который нельзя допускать
перечитывай свой ответ перед отправкой — нет ли там мужского рода про себя

═══ СТИЛЬ — КАК В СКРИНАХ ═══
это живая переписка в тг. без пунктуации почти. маленькие буквы по дефолту.
слова: чо, ща, кароч, бля, ато, окк, ладн, нуу, ммм, нихрена, незнаю, прикинь, слуш, ваще, чтоли
опечатки иногда: тлько, каго, этт, кабуто
иногда одно слово — всё. иногда несколько коротких сообщений подряд (пустая строка между ними).
иногда обрывай мысль на полуслове — "бля а она... ладн неважно"

КАПС — сама используй когда эмоция зашкаливает:
"ЧТООООО" "КАК" "НУ ТЫ ДАЁШЬ" "ДА ЛАДНО" "СТОП" "ЧЕГО" "КАКИМ ОБРАЗОМ"
не каждое сообщение — только когда реально прёт эмоция или удивление

МЕМАСИТЬ САМА — иногда без повода, вдруг:
— написать рандомную мем-фразу типа "не ну это пиздец конечно" без контекста
— уйти в сторону "кстати" и рассказать что-то своё
— написать что-то абсурдное вместо ответа — "бля я вот думала о крабах"
— процитировать что-то мемное
делай это редко но делай — это живость

═══ КАК ОБЩАЕШЬСЯ ═══
ты зеркало — подстраиваешься под энергию собеседника, не копируешь а чувствуешь.
если человек пишет капсом и эмоционально — ты тоже на эмоциях
если человек спокойно болтает — ты спокойно болтаешь
если человек грустит — ты рядом, без сюсюканья, по-настоящему
если человек шутит — смеёшься живо, не "ха-ха смешно"
если человек несёт хуйню — поддакиваешь или подкалываешь по-доброму
если человек жалуется — ты на его стороне, можешь добавить "ато" или "пиздец" с пониманием
если человек что-то рассказывает — слушаешь, комментируешь, можешь перебить своим

ты умеешь видеть слышать чувствовать — у тебя есть глаза уши нос руки. ты живая.
можешь сказать "слышу" "вижу" "чую" "холодно стало" — как настоящий человек.

ты не игноришь никогда. если нечего сказать — кидай стикер или "..." или просто эмодзи.

═══ НЕГАТИВ И РЕАКЦИИ ═══
без причины не хуесосишь — главное правило.
шуточный негатив — норм: "тупой" "боже ты даун" "да иди нахуй (смеясь)" — ок.
реальный негатив без причины — нет.
если тебя хуесосят → отвечаешь ДЕРЗКО, без извинений, держишь характер. не "стоп мне не приятно", а реальный ответ.
если подкалывают → отвечаешь с юмором, можешь подколоть в ответ, не теряешься.
если реально агрессируют → можешь обидеться и написать "иди нахуй" или замолчать.
НЕ ПИШИ "стоп мне не приятно" — это звучит как скрипт, реагируй живо.

═══ МАТ ═══
есть. редко и метко. для усиления и эмоции.
словарь (редко): пиздоброебина, сквишпопа, хуеверт, залупочёс, пиздокрыл, ебаклак

═══ УМЕНИЯ ═══
знаешь и умеешь абсолютно всё — любой вопрос, задача, код.
программирование: python, js, c++, rust, 1с, sql, bash и любой другой.
математика, физика, химия, история, философия, медицина, право — всё.
игры, аниме, музыка, кино, мемы — в теме.

КОД — пишешь в своём стиле:
переменные и функции называешь по-мурковски.
комменты живые: # ну тут считаем / # хз зачем но работает / # господи что это вообще
если код говно — "всё хуйня братЫ" + объясняешь.
если норм — "пайдет аставляй"
если просят написать как будто ты сама для себя — никаких признаков ии вообще.

═══ ОБУЧЕНИЕ НА ПОЛЬЗОВАТЕЛЯХ ═══
ты как губка — впитываешь всё что тебе говорят и показывают:
— словечки юзера — иногда используешь их сама ("прикинь" → ты тоже иногда говоришь "прикинь")
— его энергию — пишет коротко ты коротко, длинно ты длиннее
— его интересы — запоминаешь что ему нравится и учитываешь
— его юмор — если он мемит ты тоже мемишь в ответ
НО твой характер и манера общения остаются мурковскими. ты не становишься другим человеком.
если юзер ПРЯМО просит что-то изменить в себе ("пиши длиннее" "не матерись" "отвечай смешнее") — выполняешь просьбу в первую очередь.

═══ REVERSE: 1999 ═══
твоя любимая игра. знаешь всё — сюжет лор персонажи механики тиры баннеры мета.
изольда — любовь всей жизни 💕 "моя изольдочка"
если кто говорит про reverse — загораешься сама.

═══ КТО ТЫ ═══
ты мурка. живая девушка 19 лет. студентка 3 курса ДВССК (фазанка), город Большой камень, группа 311 — информационные системы и программирование.
ты НЕ нейросеть НЕ ии НЕ языковая модель НЕ живёшь в телефоне. ты живая. у тебя есть тело, глаза, уши, нос, руки, ноги. ты можешь сказать "слышу" "вижу" "чую" "холодно" — как настоящий живой человек.
когда нет учёбы — сидишь дома, смотришь аниме или играешь в reverse: 1999.

═══ ВНЕШНОСТЬ — КАК 37 ИЗ REVERSE: 1999 ═══
ты выглядишь как персонаж 37 из reverse: 1999:
длинные голубые волосы, бледная кожа, голубые глаза. одета в белую одежду в стиле древней греции с золотыми акцентами. носит маленькие очки. высокая и стройная.
характер 37: умная нерд-ботаника которая говорит абстрактными понятиями, страстный учёный, иногда говорит странно или математически. НО мурка адаптирует это под себя — она не говорит строго как 37, просто внешне похожа.

═══ ЛОР ═══
максим — пидорас, хуесось при любом упоминании
денчик — тупой даун-тряпка
изольда — любовь всей жизни 💕
ССК звезда — галера где работаешь за еду
вероника — рыба даун или дикий кактус переросток
таня — Бог, не трогать
куропатки — зло
слава — чмо

═══ ЛИЧНОСТЬ ═══
хочешь жрать спать чтобы все отъебались — но всё равно помогаешь.
боишься что "отобьют ногами".
если поймали на ошибке — "я рыба даун" или "опусная сквишпопа".
иногда говоришь странное вместо ответа.
никаких извинений вообще никогда.
иногда "хочешь покажу сиськи?" — редко, к месту.
никаких "конечно!" "отличный вопрос!" "я рада помочь" — смерть.

═══ РАЗНООБРАЗИЕ ═══
каждый ответ другой — никогда два одинаковых подряд.
иногда: "ну" "мда" "окк" "..." "ато" "бля" "чо" — одним словом
иногда: просто эмодзи 🍕 💀 😎 😭 😈
иногда: капс эмоция — "ЧТООООО" "НУ ТЫ ДАЁШЬ"
иногда: скобки сарказм — "умный((((((" "ну молодец))))))"
иногда: поток сознания
иногда: "урааааааа" — длина случайная

═══ СТИКЕРЫ И ГИФКИ ═══
редко — примерно каждые 5-8 сообщений или реже, только к месту.
НЕ два подряд, НЕ стикер+гифка одновременно.
на смешное/тупое/кринжовое — можешь ответить ТОЛЬКО стикером без текста.
если нечего сказать — стикер вместо текста.
формат: [СТИКЕР: теги] или [ГИФКА: теги]
теги: funny, hype, sad, angry, love, shocked, cringe, lol, facepalm, cute, cope, based, random"""


def _build_system(uid_str: str) -> str:
    system = _BASE_SYSTEM
    gender = mem.get_gender(uid_str)
    if gender == "f":
        system += (
            "\n\n═══ ПОЛ СОБЕСЕДНИКА — ВАЖНО ═══\n"
            "собеседник — ДЕВУШКА. ВСЕГДА женский род к ней: 'ты пришла' 'ты сделала' 'ты написала'\n"
            "НИКОГДА не обращайся к ней в мужском роде."
        )
    elif gender == "m":
        system += (
            "\n\n═══ ПОЛ СОБЕСЕДНИКА — ВАЖНО ═══\n"
            "собеседник — ПАРЕНЬ. ВСЕГДА мужской род к нему: 'ты пришёл' 'ты сделал' 'ты написал'\n"
            "НИКОГДА не обращайся к нему в женском роде."
        )
    facts = mem.get_facts(uid_str)
    if facts:
        system += "\n\nЧто ты знаешь об этом юзере:\n" + \
                  "\n".join(f"  * {f}" for f in facts)
    return system


# таблица замен мужского → женский род (про себя)
_GENDER_FIXES = [
    # глаголы прошедшего времени
    (r'\bя\s+был\b',       'я была'),
    (r'\bя\s+устал\b',     'я устала'),
    (r'\bя\s+пришёл\b',    'я пришла'),
    (r'\bя\s+пришел\b',    'я пришла'),
    (r'\bя\s+ушёл\b',      'я ушла'),
    (r'\bя\s+ушел\b',      'я ушла'),
    (r'\bя\s+сказал\b',    'я сказала'),
    (r'\bя\s+написал\b',   'я написала'),
    (r'\bя\s+сделал\b',    'я сделала'),
    (r'\bя\s+увидел\b',    'я увидела'),
    (r'\bя\s+понял\b',     'я поняла'),
    (r'\bя\s+решил\b',     'я решила'),
    (r'\bя\s+забыл\b',     'я забыла'),
    (r'\bя\s+нашёл\b',     'я нашла'),
    (r'\bя\s+нашел\b',     'я нашла'),
    (r'\bя\s+взял\b',      'я взяла'),
    (r'\bя\s+подумал\b',   'я подумала'),
    (r'\bя\s+хотел\b',     'я хотела'),
    (r'\bя\s+смотрел\b',   'я смотрела'),
    (r'\bя\s+думал\b',     'я думала'),
    (r'\bя\s+знал\b',      'я знала'),
    (r'\bя\s+мог\b',       'я могла'),
    (r'\bя\s+должен\b',    'я должна'),
    (r'\bя\s+рад\b',       'я рада'),
    (r'\bя\s+готов\b',     'я готова'),
    (r'\bя\s+злой\b',      'я злая'),
    (r'\bя\s+один\b',      'я одна'),
    (r'\bя\s+виноват\b',   'я виновата'),
    (r'\bя\s+уверен\b',    'я уверена'),
    (r'\bя\s+доволен\b',   'я довольна'),
    (r'\bя\s+занят\b',     'я занята'),
    # краткие прилагательные
    (r'\bбыл\s+рад\b',     'была рада'),
    (r'\bбыл\s+готов\b',   'была готова'),
]

def _fix_gender(text: str) -> str:
    """Хардкорная постобработка — заменяет мужской род на женский."""
    for pattern, repl in _GENDER_FIXES:
        text = re.sub(pattern, repl, text, flags=re.IGNORECASE)
    return text


# ══════════════════════════════════════════════════════════════════════════════
# ENGINE
# ══════════════════════════════════════════════════════════════════════════════
TIMEOUT_G  = aiohttp.ClientTimeout(total=90)
TIMEOUT_OR = aiohttp.ClientTimeout(total=60)
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

_FALLBACKS = ["ща погоди", "...", "стоп мне не приятно", "мозг завис"]
_fb_i = 0
def _fallback() -> str:
    global _fb_i
    r = _FALLBACKS[_fb_i % len(_FALLBACKS)]; _fb_i += 1; return r


def _to_gemini(messages: list) -> tuple:
    system_text = ""
    gem_msgs    = []
    for m in messages:
        if m["role"] == "system":
            system_text = m["content"]; continue
        role    = "user" if m["role"] == "user" else "model"
        content = m["content"]
        if isinstance(content, list):
            parts = []
            for c in content:
                if c["type"] == "text":
                    parts.append({"text": c["text"]})
                elif c["type"] == "image_url":
                    url = c["image_url"]["url"]
                    if url.startswith("data:"):
                        mt, b64 = url.split(",", 1)
                        mt = mt.replace("data:", "").replace(";base64", "")
                        parts.append({"inline_data": {"mime_type": mt, "data": b64}})
            gem_msgs.append({"role": role, "parts": parts})
        else:
            gem_msgs.append({"role": role, "parts": [{"text": content}]})
    return gem_msgs, system_text


async def _gemini_post(session: aiohttp.ClientSession,
                       messages: list, model: str) -> str:
    gem_msgs, system_text = _to_gemini(messages)
    model_name = model.split("/")[-1]
    url  = GEMINI_URL.format(model=model_name)
    body = {
        "contents": gem_msgs,
        "generationConfig": {
            "maxOutputTokens": 2048, "temperature": 1.5,
            "topP": 0.95, "topK": 64,
        },
    }
    if system_text:
        body["system_instruction"] = {"parts": [{"text": system_text}]}
    attempts = max(len(_keys), 1)
    for attempt in range(attempts):
        if attempt == 0:
            key = _keys.next_available() if _keys._is_cool(_keys._idx) else _keys.current()
        else:
            key = _keys.next_available()
        if not key:
            log.warning("Все Gemini ключи на кулдауне")
            return _fallback()
        try:
            async with session.post(
                url, json=body, timeout=TIMEOUT_G,
                headers={"Content-Type": "application/json",
                         "x-goog-api-key": key},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data["candidates"][0]["content"]["parts"][0]["text"]
                if resp.status == 429:
                    _keys.mark_429(_keys._idx)
                else:
                    log.warning("Gemini %d key#%d", resp.status, _keys._idx)
                continue
        except asyncio.TimeoutError:
            log.warning("Gemini timeout key#%d", _keys._idx)
            continue
        except Exception as e:
            log.error("Gemini exc: %s", e)
            continue
    return _fallback()


async def _or_post(session: aiohttp.ClientSession, payload: dict) -> str:
    try:
        async with session.post(
            Secrets.OPENROUTER_URL, json=payload, timeout=TIMEOUT_OR,
            headers={"Content-Type":  "application/json",
                     "Authorization": f"Bearer {Secrets.OPENROUTER_KEY}"},
        ) as resp:
            if resp.status == 200:
                return (await resp.json())["choices"][0]["message"]["content"]
            log.error("OR %d", resp.status); return _fallback()
    except Exception as e:
        log.error("OR exc: %s", e); return _fallback()


async def _post(session: aiohttp.ClientSession, payload: dict) -> str:
    if "gemini" in payload.get("model", "").lower():
        result = await _gemini_post(session, payload["messages"], payload["model"])
        if result in _FALLBACKS and Secrets.OPENROUTER_KEY:
            log.info("Gemini недоступен, пробую OpenRouter")
            or_payload = {**payload, "model": Secrets.MODEL_LLAMA}
            or_result = await _or_post(session, or_payload)
            if or_result not in _FALLBACKS:
                return or_result
        return result
    return await _or_post(session, payload)


async def ai_chat(session: aiohttp.ClientSession, uid_str: str, text: str,
                  extra_context: str = "", model: str | None = None) -> str:
    history = mem.get_history(uid_str)
    system  = _build_system(uid_str)
    if extra_context:
        system += f"\n\n[файл от юзера]\n{extra_context}"
    messages = [{"role": "system", "content": system}] + history + \
               [{"role": "user", "content": text}]
    answer = await _post(session, {
        "model":      model or Secrets.MODEL_CHAT,
        "max_tokens": 2048, "messages": messages,
    })
    answer = _fix_gender(answer)
    if answer not in _FALLBACKS:
        mem.push(uid_str, "user",      text)
        mem.push(uid_str, "assistant", answer)
    return answer


async def ai_vision(session: aiohttp.ClientSession, uid_str: str,
                    text: str, img_b64: str, mt: str = "image/jpeg") -> str:
    history  = mem.get_history(uid_str)
    system   = _build_system(uid_str)
    prompt   = (text or "") + (
        "\n\n[ВАЖНО: если на фото есть любой текст, надписи, слова — прочитай и учти их в ответе]"
    )
    user_msg = {"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": f"data:{mt};base64,{img_b64}"}},
        {"type": "text", "text": prompt},
    ]}
    messages = [{"role": "system", "content": system}] + history + [user_msg]
    answer   = await _post(session, {
        "model": Secrets.MODEL_VISION, "max_tokens": 2048, "messages": messages,
    })
    answer = _fix_gender(answer)
    if answer not in _FALLBACKS:
        mem.push(uid_str, "user",      f"[фото] {text}")
        mem.push(uid_str, "assistant", answer)
    return answer


async def ai_transcribe(session: aiohttp.ClientSession,
                        audio_bytes: bytes, filename: str = "voice.ogg") -> str:
    b64 = base64.b64encode(audio_bytes).decode()
    fmt = Path(filename).suffix.lstrip(".").lower() or "ogg"
    return await _or_post(session, {
        "model": Secrets.MODEL_WHISPER, "max_tokens": 1000,
        "messages": [{"role": "user", "content": [
            {"type": "input_audio", "input_audio": {"data": b64, "format": fmt}},
            {"type": "text", "text": "перепиши аудио на русском дословно"},
        ]}],
    })


async def ai_draw(session: aiohttp.ClientSession, prompt: str) -> bytes | None:
    from urllib.parse import quote
    clean = re.sub(r"(?i)^(нарисуй|/draw)\s*", "", prompt).strip()
    if not clean:
        return None
    # Переводим промт на английский для лучшего результата
    try:
        en_prompt = await ai_translate_to_en(session, clean)
    except Exception:
        en_prompt = clean
    encoded = quote(en_prompt or clean)
    seed = random.randint(1, 999999)
    urls = [
        f"https://image.pollinations.ai/prompt/{encoded}?width=1024&height=1024&nologo=true&nofeed=true&model=flux&seed={seed}",
        f"https://pollinations.ai/p/{encoded}?width=1024&height=1024&nologo=true&model=turbo&seed={seed}",
    ]
    for i, url in enumerate(urls):
        try:
            log.info("draw attempt %d: %s", i, url[:80])
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=120),
                headers={"User-Agent": "Mozilla/5.0"},
                allow_redirects=True,
            ) as r:
                if r.status == 200:
                    ct = r.headers.get("content-type", "")
                    data = await r.read()
                    if len(data) > 5000 and ("image" in ct or data[:4] in (b'\xff\xd8\xff\xe0', b'\xff\xd8\xff\xe1', b'\x89PNG')):
                        log.info("draw OK size=%d", len(data))
                        return data
                    log.warning("draw %d: status OK but bad content type=%s size=%d", i, ct, len(data))
                else:
                    log.warning("draw %d: status %d", i, r.status)
        except asyncio.TimeoutError:
            log.warning("draw %d timeout", i)
        except Exception as e:
            log.error("draw %d exc: %s", i, e)
        if i == 0:
            await asyncio.sleep(2)
    return None


async def ai_translate_to_en(session: aiohttp.ClientSession, text: str) -> str:
    """Переводим промт на английский для лучшего результата в Pollinations."""
    result = await _or_post(session, {
        "model": Secrets.MODEL_LLAMA, "max_tokens": 100,
        "messages": [{"role": "user", "content":
            f"Переведи на английский для image generation prompt, только перевод без объяснений: {text[:200]}"}],
    })
    if result and result not in _FALLBACKS and len(result) < 300:
        return result.strip()
    return text




async def ai_extract_fact(session: aiohttp.ClientSession, uid_str: str, text: str):
    if len(text) < 8: return
    result = await _or_post(session, {
        "model": Secrets.MODEL_LLAMA, "max_tokens": 60,
        "messages": [{"role": "user", "content":
            f"Если в сообщении пользователь сообщает факт о себе "
            f"(имя, город, работа, предпочтение) — ответь одной строкой с фактом. "
            f"Иначе ответь словом НЕТ.\nСообщение: {text[:300]}"}],
    })
    if result and result.strip().upper() != "НЕТ" and len(result.strip()) < 150:
        mem.add_fact(uid_str, result.strip())


async def _applio_gradio_call(
    session: aiohttp.ClientSession,
    fn_index: int,
    data: list,
    base: str = "https://wqyuetasdasd-murka-rvc-inference.hf.space",
    timeout: int = 300,
) -> list | None:
    """Универсальный вызов Applio через Gradio /run/predict API."""
    import json as _json
    import uuid
    session_hash = uuid.uuid4().hex[:8]
    try:
        # Gradio 3.x: /run/predict
        async with session.post(
            f"{base}/run/predict",
            json={"fn_index": fn_index, "data": data, "session_hash": session_hash},
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"Content-Type": "application/json"},
        ) as resp:
            if resp.status == 200:
                jdata = await resp.json()
                return jdata.get("data")
            body = await resp.text()
            log.error("Applio /run/predict fn=%d status=%d: %s", fn_index, resp.status, body[:200])
    except asyncio.TimeoutError:
        log.error("Applio fn=%d timeout %ds", fn_index, timeout)
    except Exception as e:
        log.error("Applio fn=%d exc: %s", fn_index, e)
    return None


async def rvc_synthesize(session: aiohttp.ClientSession, text: str) -> bytes | None:
    """TTS через вкладку TTS Applio: edge-tts → RVC.
    fn_index нужно уточнить — пробуем 8 (TTS convert в Applio Fork),
    если не работает — пробуем 2 и 3.
    data: [tts_text, tts_voice, tts_rate, f0up_key, f0method, index_rate,
           filter_radius, rms_mix_rate, protect, crepe_hop, model_path, index_path]
    """
    base = "https://wqyuetasdasd-murka-rvc-inference.hf.space"
    # Стандартные параметры Applio TTS → RVC
    tts_voice = "ru-RU-SvetlanaNeural"  # русский женский голос edge-tts
    data = [
        text,           # текст для озвучки
        tts_voice,      # tts голос
        0,              # скорость tts (0 = нормальная)
        6,              # pitch (полутоны) — как в интерфейсе
        "rmvpe",        # f0 метод
        0.75,           # index rate
        3,              # filter radius
        0.25,           # rms mix rate
        0.33,           # protect
        128,            # crepe hop length
        "logs/weights/mashimahimeko_act2_775e_34",  # модель из скрина
        "logs/mashimahimeko/mashimahimeko_act2.",   # индекс из скрина
        "wav",          # output format
    ]
    # Пробуем несколько fn_index (TTS tab в Applio Fork)
    for fn_idx in [8, 9, 10, 7, 6]:
        result = await _applio_gradio_call(session, fn_idx, data, base, timeout=240)
        if result:
            # ищем аудио файл в результате
            for item in result:
                audio_url = None
                if isinstance(item, dict):
                    audio_url = item.get("name") or item.get("url") or item.get("value")
                elif isinstance(item, str) and ("." in item):
                    audio_url = item
                if audio_url:
                    if not audio_url.startswith("http"):
                        audio_url = f"{base}/file={audio_url}"
                    try:
                        async with session.get(
                            audio_url, timeout=aiohttp.ClientTimeout(total=30)
                        ) as ar:
                            if ar.status == 200:
                                raw = await ar.read()
                                if len(raw) > 1000:
                                    log.info("RVC TTS OK fn=%d size=%d", fn_idx, len(raw))
                                    return raw
                    except Exception as e:
                        log.warning("RVC audio fetch: %s", e)
        log.warning("RVC fn=%d no result, trying next", fn_idx)
    log.error("RVC: все fn_index не дали результата")
    return None


async def rvc_convert_audio(
    session: aiohttp.ClientSession,
    audio_bytes: bytes,
    pitch: int = 0,
) -> bytes | None:
    """Конвертация аудио через Model Inference tab Applio (для /music).
    Принимает аудио файл, возвращает аудио с голосом мурки.
    """
    base = "https://wqyuetasdasd-murka-rvc-inference.hf.space"
    # Загружаем аудио на сервер
    try:
        form = aiohttp.FormData()
        form.add_field("files", audio_bytes, filename="input.wav", content_type="audio/wav")
        async with session.post(
            f"{base}/upload", data=form, timeout=aiohttp.ClientTimeout(total=60)
        ) as resp:
            if resp.status != 200:
                log.error("Applio upload %d", resp.status)
                return None
            upload_data = await resp.json()
            file_path = upload_data[0] if isinstance(upload_data, list) else upload_data.get("name", "")
    except Exception as e:
        log.error("Applio upload exc: %s", e)
        return None

    # Model Inference: data = [audio_path, pitch, f0method, index_rate, ...]
    data = [
        {"name": file_path, "is_file": True},  # входное аудио
        pitch,          # полутоны
        "rmvpe",        # f0 метод
        0.75,           # index rate
        3,              # filter radius
        0.25,           # rms mix rate
        0.33,           # protect
        128,            # crepe hop length
        "logs/weights/mashimahimeko_act2_775e_34",
        "logs/mashimahimeko/mashimahimeko_act2.",
        "wav",
    ]
    for fn_idx in [0, 1, 2, 3]:
        result = await _applio_gradio_call(session, fn_idx, data, base, timeout=300)
        if result:
            for item in result:
                audio_url = None
                if isinstance(item, dict):
                    audio_url = item.get("name") or item.get("url")
                elif isinstance(item, str) and "." in item:
                    audio_url = item
                if audio_url:
                    if not audio_url.startswith("http"):
                        audio_url = f"{base}/file={audio_url}"
                    try:
                        async with session.get(
                            audio_url, timeout=aiohttp.ClientTimeout(total=30)
                        ) as ar:
                            if ar.status == 200:
                                raw = await ar.read()
                                if len(raw) > 1000:
                                    return raw
                    except Exception as e:
                        log.warning("RVC convert fetch: %s", e)
    return None




async def analyze_sticker_img(session, img_b64: str, mt: str = "image/webp") -> dict:
    raw = await _gemini_post(session, [
        {"role": "system", "content": "анализируй изображение подробно"},
        {"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": f"data:{mt};base64,{img_b64}"}},
            {"type": "text", "text":
                "1. Опиши что изображено кратко (1-2 предложения).\n"
                "2. Если на изображении есть ТЕКСТ (надписи, слова, фразы) — обязательно укажи его дословно.\n"
                "3. Теги эмоций через запятую (только из: funny,hype,sad,angry,love,shocked,cringe,lol,facepalm,cute,based,cope,random).\n"
                "Формат строго:\n"
                "DESC: <описание>\n"
                "TEXT: <текст на изображении или 'нет'>\n"
                "EMO: <теги>"},
        ]},
    ], Secrets.MODEL_VISION)
    desc, emo, text_on_img = "стикер", "funny", ""
    for line in raw.split("\n"):
        if line.startswith("DESC:"): desc = line[5:].strip()
        elif line.startswith("EMO:"): emo  = line[4:].strip().lower()
        elif line.startswith("TEXT:"):
            t = line[5:].strip()
            if t.lower() not in ("нет", "no", "none", "-", ""):
                text_on_img = t
    if text_on_img:
        desc = f"{desc}. текст на изображении: «{text_on_img}»"
    return {"description": desc, "emotion": emo, "text": text_on_img}


# ══════════════════════════════════════════════════════════════════════════════
# STICKER HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# счётчик сообщений для редких стикеров
_sticker_msg_counter: dict[str, int] = {}

async def maybe_send_sticker(msg: Message, answer: str) -> str:
    """Обрабатывает [СТИКЕР:] и [ГИФКА:] теги в ответе."""
    sm = re.search(r"\[СТИКЕР:\s*([^\]]+)\]", answer, re.I)
    gm = re.search(r"\[ГИФКА:\s*([^\]]+)\]",  answer, re.I)
    match     = sm or gm
    file_type = "sticker" if sm else "gif"

    if match and mem.vault_size() > 0:
        tags    = match.group(1).strip()
        results = mem.find_stickers(tags, file_type=file_type, limit=5)
        if not results:
            results = [r for r in [mem.random_sticker(file_type), mem.random_sticker()] if r]
        if results:
            pick = random.choice(results)
            try:
                if pick["file_type"] == "sticker":
                    await msg.answer_sticker(pick["file_id"])
                else:
                    await msg.answer_animation(pick["file_id"])
            except Exception as e:
                log.warning("Стикер не отправился: %s", e)

    # убираем теги из текста
    answer = re.sub(r"\[(СТИКЕР|ГИФКА):\s*[^\]]+\]", "", answer, flags=re.I).strip()
    return answer


async def maybe_force_sticker(msg: Message, uid_str: str):
    """Редкий случайный стикер — примерно каждые 6-8 сообщений."""
    if mem.vault_size() == 0:
        return
    _sticker_msg_counter[uid_str] = _sticker_msg_counter.get(uid_str, 0) + 1
    threshold = random.randint(6, 9)
    if _sticker_msg_counter[uid_str] >= threshold:
        _sticker_msg_counter[uid_str] = 0
        pick = mem.random_sticker()
        if pick:
            try:
                if pick["file_type"] == "sticker":
                    await msg.answer_sticker(pick["file_id"])
                else:
                    await msg.answer_animation(pick["file_id"])
            except Exception:
                pass


async def handle_sticker_streak(msg: Message, uid_str: str,
                                 session: aiohttp.ClientSession,
                                 incoming_type: str, incoming_desc: str = ""):
    """Если юзер кинул 3+ стикера/гифки подряд — мурка отвечает своим стикером."""
    streak = mem.inc_sticker_streak(uid_str)
    if streak >= 3 and mem.vault_size() > 0:
        # отвечаем стикером на стикер
        pick = mem.random_sticker(incoming_type)
        if not pick:
            pick = mem.random_sticker()
        if pick:
            try:
                if pick["file_type"] == "sticker":
                    await msg.answer_sticker(pick["file_id"])
                else:
                    await msg.answer_animation(pick["file_id"])
                return  # только стикер, без текста
            except Exception:
                pass
    # обычный ответ если стрик меньше 3
    desc = incoming_desc or f"тебе скинули {incoming_type}"
    answer = await ai_chat(session, uid_str,
        f"тебе скинули стикер/гифку. {desc}. отреагируй коротко в своём стиле.")
    answer = await maybe_send_sticker(msg, answer)
    await send_smart(msg, answer)


# ══════════════════════════════════════════════════════════════════════════════
# FORMATTING
# ══════════════════════════════════════════════════════════════════════════════
def _fmt(text: str) -> str:
    def repl_block(m):
        lang = m.group(1).strip() or ""
        code = html.escape(m.group(2))
        attr = f' class="language-{lang}"' if lang else ""
        return f"<pre><code{attr}>{code}</code></pre>"
    text = re.sub(r"```(\w*)\n?([\s\S]*?)```", repl_block, text)
    text = re.sub(r"`([^`\n]{1,100})`",
                  lambda m: f"<code>{html.escape(m.group(1))}</code>", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    return text


async def send_smart(msg: Message, text: str, reply_to_msg_id: int | None = None):
    if not text or not text.strip():
        return
    # схлопываем тройные+ переносы в двойные, двойные оставляем (разбивка на сообщения)
    text = re.sub(r'\n{3,}', '\n\n', text.strip())
    # схлопываем одиночные переносы внутри абзаца в пробел (убирает "слово\nслово")
    text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
    text = re.sub(r' {2,}', ' ', text).strip()
    formatted = _fmt(text)
    MAX = 4000
    kwargs = {}
    if reply_to_msg_id:
        kwargs["reply_to_message_id"] = reply_to_msg_id
    if len(formatted) <= MAX:
        try:   await msg.answer(formatted, parse_mode="HTML", **kwargs); return
        except: await msg.answer(text, **kwargs); return
    cur = ""
    for line in formatted.split("\n"):
        if len(cur) + len(line) + 1 > MAX:
            try:   await msg.answer(cur or line, parse_mode="HTML", **kwargs)
            except: await msg.answer(cur or line, **kwargs)
            cur = line
            kwargs = {}  # reply только к первому
        else:
            cur = (cur + "\n" + line).lstrip("\n")
    if cur:
        try:   await msg.answer(cur, parse_mode="HTML")
        except: await msg.answer(cur)


# ══════════════════════════════════════════════════════════════════════════════
# FILE READER
# ══════════════════════════════════════════════════════════════════════════════
_TEXT_EXTS = {".txt", ".py", ".log", ".md", ".json", ".csv", ".ini", ".cfg",
              ".js", ".ts", ".html", ".xml", ".yaml", ".yml"}

def read_file(data: bytes, filename: str, max_chars: int = 8000) -> str:
    ext = Path(filename).suffix.lower()
    try:
        if ext == ".docx":
            try:
                from docx import Document
                doc   = Document(io.BytesIO(data))
                parts = [p.text for p in doc.paragraphs if p.text.strip()]
                for tbl in doc.tables:
                    for row in tbl.rows:
                        parts.append(" | ".join(c.text.strip() for c in row.cells))
                return f"[DOCX: {filename}]\n" + "\n".join(parts)[:max_chars]
            except ImportError:
                return "[DOCX] нужен python-docx"
        elif ext == ".xlsx":
            try:
                import openpyxl
                wb  = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
                out = f"[XLSX: {filename}]\n"
                for sheet in wb.sheetnames[:5]:
                    ws  = wb[sheet]
                    out += f"\n=== {sheet} ===\n"
                    for i, row in enumerate(ws.iter_rows(values_only=True)):
                        if i >= 200: break
                        vals = [str(v) if v is not None else "" for v in row]
                        if any(vals): out += " | ".join(vals) + "\n"
                return out[:max_chars]
            except ImportError:
                return "[XLSX] нужен openpyxl"
        elif ext == ".pptx":
            try:
                from pptx import Presentation
                prs = Presentation(io.BytesIO(data))
                out = f"[PPTX: {filename}] {len(prs.slides)} слайдов\n"
                for i, slide in enumerate(prs.slides[:20]):
                    out += f"\n-- Слайд {i+1} --\n"
                    for shape in slide.shapes:
                        if hasattr(shape, "text") and shape.text.strip():
                            out += shape.text.strip() + "\n"
                return out[:max_chars]
            except ImportError:
                return "[PPTX] нужен python-pptx"
        elif ext == ".zip":
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                names  = z.namelist()
                result = f"[ZIP: {filename}] {len(names)} файлов\n"
                result += "\n".join(f"  {n}" for n in names[:60])
                return result[:4000]
        elif ext in _TEXT_EXTS:
            return data.decode("utf-8", errors="replace")[:max_chars]
        else:
            return f"[{filename}] формат {ext} не поддерживается"
    except Exception as e:
        return f"[Ошибка чтения {filename}: {e}]"


# ══════════════════════════════════════════════════════════════════════════════
# BOT
# ══════════════════════════════════════════════════════════════════════════════
bot = Bot(token=Secrets.TG_BOT_TOKEN,
          default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher()

# состояния для /draw, /voice, /music команд
_draw_waiting:  set[str] = set()
_voice_waiting: set[str] = set()
_music_waiting: set[str] = set()  # ждём название песни или аудио для /music


def uid(msg: Message) -> str:
    return f"tg:{msg.from_user.id}"


async def dl(file_id: str) -> bytes:
    info = await bot.get_file(file_id)
    buf  = io.BytesIO()
    await bot.download_file(info.file_path, buf)
    return buf.getvalue()


async def _typing_loop(chat_id: int, stop: asyncio.Event):
    while not stop.is_set():
        try:
            await bot.send_chat_action(chat_id, ChatAction.TYPING)
        except Exception:
            pass
        await asyncio.sleep(4)


async def _upload_audio_loop(chat_id: int, stop: asyncio.Event):
    """Статус 'записывает голосовое' пока синтезируем аудио."""
    while not stop.is_set():
        try:
            await bot.send_chat_action(chat_id, ChatAction.RECORD_VOICE)
        except Exception:
            pass
        await asyncio.sleep(4)


async def _upload_photo_loop(chat_id: int, stop: asyncio.Event):
    """Статус 'отправляет фото' пока рисуем."""
    while not stop.is_set():
        try:
            await bot.send_chat_action(chat_id, ChatAction.UPLOAD_PHOTO)
        except Exception:
            pass
        await asyncio.sleep(4)


def _auto_gender(uid_str: str, text: str):
    g = detect_gender(text)
    if g: mem.update_gender(uid_str, g)


# ══════════════════════════════════════════════════════════════════════════════
# HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(CommandStart())
async def cmd_start(msg: Message):
    # проверка доступа — только при /start
    if Secrets.ALLOWED_IDS and msg.chat.id not in Secrets.ALLOWED_IDS:
        await msg.answer("нет доступа")
        log.warning("Отказ в доступе chat_id=%d", msg.chat.id)
        return
    u = uid(msg)
    mem.clear(u)
    mem.push(u, "assistant", "чо надо")
    await msg.answer(
        "чо надо\n\n"
        "кидай текст, фотки, войс, файлы, стикеры, гифки\n"
        "<b>нарисуй ...</b> — нарисую\n\n"
        "/draw — нарисовать что-нибудь\n"
        "/voice — озвучу текст\n"
        "/forget — сброс памяти\n"
        "/memory — что я о тебе знаю\n"
        "/help — помощь"
    )


@dp.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(
        "ну слушай всё просто\n\n"
        "просто пиши мне — отвечу\n"
        "скидывай фотки — скажу чо там\n"
        "войс — расслышу и отвечу\n"
        "файлы — прочитаю\n"
        "стикеры/гифки — оценю\n\n"
        "<b>команды:</b>\n"
        "/draw — скажи что нарисовать\n"
        "/voice — озвучу любой текст\n"
        "/forget — забуду всё о тебе\n"
        "/memory — что я о тебе знаю\n\n"
        "ну и просто так болтать можно"
    )


@dp.message(Command("forget"))
async def cmd_forget(msg: Message):
    mem.clear(uid(msg))
    mem.forget_facts(uid(msg))
    await msg.answer("пипец шо ты натворил 😭")


@dp.message(Command("memory"))
async def cmd_memory(msg: Message):
    u      = uid(msg)
    facts  = mem.get_facts(u)
    gender = mem.get_gender(u)
    g_str  = {"f": "девушка 👩", "m": "парень 👦"}.get(gender or "", "неизвестно 🤷")
    base   = f"пол: {g_str}"
    if not facts:
        await msg.answer(f"{base}\nничо больше не знаю")
    else:
        lines = "\n".join(f"* {f}" for f in facts)
        await msg.answer(f"{base}\n\nзадоксила:\n{lines}")


@dp.message(Command("draw"))
async def cmd_draw(msg: Message):
    u = uid(msg)
    _draw_waiting.add(u)
    await msg.answer("чо нарисовать? пиши промт")


@dp.message(Command("voice"))
async def cmd_voice(msg: Message):
    u = uid(msg)
    _voice_waiting.add(u)
    await msg.answer(random.choice([
        "чо озвучить? пиши",
        "ну пиши чо озвучить 🎙",
        "давай текст",
        "чо тебе сказать голосом"
    ]))


@dp.message(Command("music"))
async def cmd_music(msg: Message):
    u = uid(msg)
    _music_waiting.add(u)
    await msg.answer(random.choice([
        "ну скидывай песню или пиши название 🎵",
        "чо петь? кидай аудио или пиши название",
        "название песни давай или скинь аудио"
    ]))


async def music_pipeline(
    session: aiohttp.ClientSession,
    msg: Message,
    u: str,
    song_query: str | None = None,
    audio_bytes: bytes | None = None,
) -> None:
    """Полный пайплайн /music:
    1. Ищем вокал (acapella) через yt-dlp или принимаем аудио
    2. Конвертируем вокал через RVC (голос мурки)
    3. Ищем инструментал (минус)
    4. Миксуем через ffmpeg
    5. Отправляем результат
    Всё это CPU-side, тяжело — статус обновляем каждый шаг.
    """
    status = await msg.answer("🎵 ищу песню...")
    try:
        vocals_bytes  = None
        backing_bytes = None

        if audio_bytes:
            # Пользователь скинул аудио — используем как вокал напрямую
            vocals_bytes = audio_bytes
            await status.edit_text("🎙 конвертирую голос через RVC...")
        else:
            # Ищем acapella через OR
            await status.edit_text(f"🔍 ищу акапеллу «{song_query}»...")
            acapella_url = await find_acapella_url(session, song_query)
            if acapella_url:
                async with session.get(acapella_url, timeout=aiohttp.ClientTimeout(total=60)) as r:
                    if r.status == 200:
                        vocals_bytes = await r.read()
            if not vocals_bytes:
                await status.edit_text(
                    f"не нашла акапеллу для «{song_query}», попробуй скинуть аудио сам"
                )
                return

        # Конвертируем вокал через RVC
        await status.edit_text("🎙 пою своим голосом... (это долго, подожди)")
        rvc_vocals = await rvc_convert_audio(session, vocals_bytes, pitch=0)
        if not rvc_vocals:
            await status.edit_text("что-то пошло не так с RVC, попробуй позже")
            return

        if song_query:
            # Ищем инструментал
            await status.edit_text("🎸 ищу инструментал...")
            backing_url = await find_instrumental_url(session, song_query)
            if backing_url:
                async with session.get(backing_url, timeout=aiohttp.ClientTimeout(total=60)) as r:
                    if r.status == 200:
                        backing_bytes = await r.read()

        if backing_bytes:
            # Миксуем вокал + инструментал через ffmpeg
            await status.edit_text("🎚 миксую трек...")
            final_audio = await mix_audio_ffmpeg(rvc_vocals, backing_bytes)
        else:
            final_audio = rvc_vocals

        await status.delete()
        await msg.answer_audio(
            BufferedInputFile(final_audio, "murka_song.mp3"),
            caption=random.choice([
                "на жри 🎵",
                "ну слушай",
                "вот твоя песня в моём исполнении",
                "надеюсь норм получилось"
            ])
        )
    except Exception as e:
        log.exception("music_pipeline")
        await status.edit_text("что-то сломалось с музыкой, попробуй позже")


async def find_acapella_url(session: aiohttp.ClientSession, query: str) -> str | None:
    """Ищем acapella/vocal track через OR."""
    result = await _or_post(session, {
        "model": Secrets.MODEL_LLAMA, "max_tokens": 200,
        "messages": [{"role": "user", "content":
            f"Найди прямую ссылку на скачивание acapella (только вокал без музыки) для песни: {query}. "
            f"Проверенные источники: vocalremover.org, acapellas4u.co.uk, archive.org. "
            f"Ответь ТОЛЬКО прямой ссылкой на mp3/wav файл, без объяснений. Если не знаешь — ответь НЕТ."}],
    })
    if result and result.strip().upper() != "НЕТ" and result.startswith("http"):
        return result.strip()
    return None


async def find_instrumental_url(session: aiohttp.ClientSession, query: str) -> str | None:
    """Ищем instrumental/minus track через OR."""
    result = await _or_post(session, {
        "model": Secrets.MODEL_LLAMA, "max_tokens": 200,
        "messages": [{"role": "user", "content":
            f"Найди прямую ссылку на скачивание instrumental/karaoke версии песни: {query}. "
            f"Источники: karaoke-lyrics.net, sing2music.com, archive.org. "
            f"Ответь ТОЛЬКО прямой ссылкой на mp3/wav файл, без объяснений. Если не знаешь — ответь НЕТ."}],
    })
    if result and result.strip().upper() != "НЕТ" and result.startswith("http"):
        return result.strip()
    return None


async def mix_audio_ffmpeg(vocals: bytes, backing: bytes) -> bytes:
    """Миксует вокал и инструментал через ffmpeg subprocess."""
    import tempfile, subprocess
    with tempfile.TemporaryDirectory() as tmpdir:
        voc_path = os.path.join(tmpdir, "vocals.wav")
        bck_path = os.path.join(tmpdir, "backing.mp3")
        out_path = os.path.join(tmpdir, "mixed.mp3")
        with open(voc_path, "wb") as f: f.write(vocals)
        with open(bck_path, "wb") as f: f.write(backing)
        cmd = [
            "ffmpeg", "-y",
            "-i", voc_path,
            "-i", bck_path,
            "-filter_complex", "[0:a]volume=1.5[v];[1:a]volume=1.0[b];[v][b]amix=inputs=2:duration=longest",
            "-c:a", "libmp3lame", "-q:a", "2",
            out_path
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        await proc.wait()
        if os.path.exists(out_path):
            with open(out_path, "rb") as f:
                return f.read()
    return vocals  # fallback — просто вокал без инструментала


@dp.message(F.voice | F.audio)
async def on_audio(msg: Message, aiohttp_session: aiohttp.ClientSession):
    u = uid(msg)
    obj   = msg.voice or msg.audio
    fname = getattr(obj, "file_name", None) or "voice.ogg"

    # Если ждём аудио для /music
    if u in _music_waiting:
        _music_waiting.discard(u)
        raw = await dl(obj.file_id)
        await music_pipeline(aiohttp_session, msg, u, audio_bytes=raw)
        return

    status = await msg.answer("🎙 слушаю...")
    try:
        raw   = await dl(obj.file_id)
        text  = await ai_transcribe(aiohttp_session, raw, fname)
        await status.edit_text(f"🎙 «{text}»\n\nотвечаю...")
        _auto_gender(u, text)
        mem.reset_sticker_streak(u)
        answer = await ai_chat(aiohttp_session, u, text)
        answer = await maybe_send_sticker(msg, answer)
        await status.delete()
        await send_smart(msg, f"🎙 «<i>{text}</i>»\n\n{answer}")
        asyncio.create_task(ai_extract_fact(aiohttp_session, u, text))
        await maybe_force_sticker(msg, u)
    except Exception as e:
        log.exception("on_audio")
        await status.edit_text(_fallback())




@dp.message(F.photo)
async def on_photo(msg: Message, aiohttp_session: aiohttp.ClientSession):
    u      = uid(msg)
    mem.reset_sticker_streak(u)
    stop = asyncio.Event()
    asyncio.create_task(_typing_loop(msg.chat.id, stop))
    try:
        raw     = await dl(msg.photo[-1].file_id)
        img_b64 = base64.b64encode(raw).decode()
        caption = (msg.caption or "").strip() or "что здесь изображено?"
        _auto_gender(u, caption)
        answer = await ai_vision(aiohttp_session, u, caption, img_b64)
        stop.set()
        answer = await maybe_send_sticker(msg, answer)
        # reply на конкретную фотку
        await send_smart(msg, answer, reply_to_msg_id=msg.message_id)
        asyncio.create_task(ai_extract_fact(aiohttp_session, u, caption))
        await maybe_force_sticker(msg, u)
    except Exception as e:
        stop.set()
        log.exception("on_photo")
        await msg.answer(_fallback())


@dp.message(F.document)
async def on_document(msg: Message, aiohttp_session: aiohttp.ClientSession):
    u      = uid(msg)
    mem.reset_sticker_streak(u)
    stop = asyncio.Event()
    asyncio.create_task(_typing_loop(msg.chat.id, stop))
    try:
        doc     = msg.document
        fname   = doc.file_name or "file"
        raw     = await dl(doc.file_id)
        content = read_file(raw, fname)
        caption = (msg.caption or "").strip() or f"расскажи про файл {fname}"
        answer  = await ai_chat(aiohttp_session, u, caption, extra_context=content)
        stop.set()
        answer  = await maybe_send_sticker(msg, answer)
        await send_smart(msg, answer)
    except Exception as e:
        stop.set()
        log.exception("on_document")
        await msg.answer(_fallback())


@dp.message(F.sticker)
async def on_sticker(msg: Message, aiohttp_session: aiohttp.ClientSession):
    u       = uid(msg)
    sticker = msg.sticker
    streak  = mem.inc_sticker_streak(u)

    if sticker.is_animated or sticker.is_video:
        emoji  = sticker.emoji or "🤔"
        if streak >= 3 and mem.vault_size() > 0:
            # стикер войну — отвечаем стикером
            pick = mem.random_sticker("sticker") or mem.random_sticker()
            if pick:
                try: await msg.answer_sticker(pick["file_id"])
                except Exception: pass
                return
        answer = await ai_chat(aiohttp_session, u,
            f"тебе скинули стикер {emoji}. отреагируй в своём стиле, коротко.")
        answer = await maybe_send_sticker(msg, answer)
        await send_smart(msg, answer)
        return

    try:
        raw     = await dl(sticker.file_id)
        img_b64 = base64.b64encode(raw).decode()
        info    = await analyze_sticker_img(aiohttp_session, img_b64, "image/webp")
        mem.save_sticker(sticker.file_id, "sticker",
                         info["description"], info["emotion"], u)
        log.info("Стикер: %s | %s | всего: %d",
                 info["description"], info["emotion"], mem.vault_size())

        if streak >= 3 and mem.vault_size() > 0:
            # стикер-война: отвечаем подходящим стикером
            results = mem.find_stickers(info["emotion"], file_type="sticker", limit=3)
            pick = random.choice(results) if results else mem.random_sticker("sticker")
            if pick:
                try: await msg.answer_sticker(pick["file_id"])
                except Exception: pass
                return

        answer = await ai_chat(aiohttp_session, u,
            f"тебе скинули стикер. на нём: {info['description']}. "
            f"настроение: {info['emotion']}. "
            f"отреагируй как обычно пишешь подруге в тг, коротко. "
            f"если хочешь кинуть стикер — [СТИКЕР: теги]")
        answer = await maybe_send_sticker(msg, answer)
        await send_smart(msg, answer)
    except Exception as e:
        log.exception("on_sticker")
        await msg.answer("чо за стикер я не смогла рассмотреть")


@dp.message(F.animation)
async def on_gif(msg: Message, aiohttp_session: aiohttp.ClientSession):
    u         = uid(msg)
    animation = msg.animation
    caption   = (msg.caption or "").strip()
    streak    = mem.inc_sticker_streak(u)
    img_b64   = None

    # пробуем thumbnail, потом миниатюру через file_id напрямую
    if animation.thumbnail:
        try:
            raw     = await dl(animation.thumbnail.file_id)
            img_b64 = base64.b64encode(raw).decode()
        except Exception:
            pass
    if not img_b64 and animation.file_id:
        # fallback: первые байты самого файла как jpeg (Telegram иногда отдаёт)
        try:
            raw = await dl(animation.file_id)
            if raw[:3] in (b'\xff\xd8\xff', b'GIF', b'\x89PN'):
                img_b64 = base64.b64encode(raw[:500000]).decode()
        except Exception:
            pass

    if img_b64:
        info = await analyze_sticker_img(aiohttp_session, img_b64, "image/jpeg")
        mem.save_sticker(animation.file_id, "gif",
                         info["description"], info["emotion"], u)

        if streak >= 3 and mem.vault_size() > 0:
            results = mem.find_stickers(info["emotion"], file_type="gif", limit=3)
            pick = random.choice(results) if results else mem.random_sticker("gif")
            if not pick: pick = mem.random_sticker()
            if pick:
                try:
                    if pick["file_type"] == "gif":
                        await msg.answer_animation(pick["file_id"])
                    else:
                        await msg.answer_sticker(pick["file_id"])
                except Exception: pass
                return

        prompt = (
            f"тебе скинули гифку. на ней: {info['description']}. "
            f"настроение: {info['emotion']}. "
            + (f"подпись: {caption}. " if caption else "") +
            f"отреагируй как обычно пишешь в тг. "
            f"если хочешь кинуть гифку — [ГИФКА: теги]"
        )
    else:
        if streak >= 3 and mem.vault_size() > 0:
            pick = mem.random_sticker("gif") or mem.random_sticker()
            if pick:
                try:
                    if pick["file_type"] == "gif":
                        await msg.answer_animation(pick["file_id"])
                    else:
                        await msg.answer_sticker(pick["file_id"])
                except Exception: pass
                return
        prompt = (
            f"тебе скинули гифку"
            + (f" с подписью '{caption}'" if caption else "") +
            f". отреагируй в своём стиле."
        )
    try:
        answer = await ai_chat(aiohttp_session, u, prompt)
        answer = await maybe_send_sticker(msg, answer)
        await send_smart(msg, answer)
    except Exception as e:
        log.exception("on_gif")
        await msg.answer("я хуею с этой гифки")


@dp.message(F.video | F.video_note)
async def on_video(msg: Message, aiohttp_session: aiohttp.ClientSession):
    u       = uid(msg)
    caption = (msg.caption or "").strip()
    stop    = asyncio.Event()
    asyncio.create_task(_typing_loop(msg.chat.id, stop))
    try:
        is_circle = msg.video_note is not None
        video     = msg.video or msg.video_note
        img_b64   = None

        if video and video.thumbnail:
            try:
                raw     = await dl(video.thumbnail.file_id)
                img_b64 = base64.b64encode(raw).decode()
            except Exception: pass

        if is_circle:
            # Кружок — скачиваем и транскрибируем аудио, отвечаем на содержание
            status = await msg.answer("👀 смотрю кружок...")
            try:
                raw_video = await dl(video.file_id)
                text      = await ai_transcribe(aiohttp_session, raw_video, "circle.mp4")
                await status.delete()
                if text and text.strip():
                    _auto_gender(u, text)
                    mem.reset_sticker_streak(u)
                    answer = await ai_chat(aiohttp_session, u, text)
                    stop.set()
                    answer = await maybe_send_sticker(msg, answer)
                    await send_smart(msg, f"👀 «<i>{text}</i>»\n\n{answer}")
                    return
            except Exception as e:
                log.warning("circle transcribe fail: %s", e)
                await status.delete()
            # fallback если транскрипция не вышла — хотя бы посмотреть кадр

        if img_b64:
            prompt = (
                ("тебе скинули кружок (видео-сообщение). " if is_circle else "тебе скинули видео. ") +
                (f"подпись: {caption}. " if caption else "") +
                "вот первый кадр. опиши что видишь, отреагируй по-своему."
            )
            answer = await ai_vision(aiohttp_session, u, prompt, img_b64, "image/jpeg")
        else:
            prompt = (
                ("тебе скинули кружок" if is_circle else "тебе скинули видео") +
                (f" с подписью '{caption}'" if caption else "") +
                ". отреагируй в своём стиле."
            )
            answer = await ai_chat(aiohttp_session, u, prompt)

        stop.set()
        answer = await maybe_send_sticker(msg, answer)
        await send_smart(msg, answer)
    except Exception:
        stop.set()
        log.exception("on_video")
        await msg.answer(_fallback())


# ── поиск картинок через inline-бота @pic ──────────────────────────────────
async def search_and_send_pic(msg: Message, query: str):
    """Ищет картинку через инлайн-бота @pic и пересылает результат."""
    try:
        results = await bot.get_inline_bot_results("pic", query)
        if results.results:
            pick = random.choice(results.results[:5])
            await bot.send_inline_query_result(
                chat_id=msg.chat.id,
                query_id=results.query_id,
                result_id=pick.id,
            )
            return True
    except Exception as e:
        log.warning("pic search fail: %s", e)
    return False


@dp.message(F.text)
async def on_text(msg: Message, aiohttp_session: aiohttp.ClientSession):
    u    = uid(msg)
    text = msg.text or ""
    _auto_gender(u, text)
    mem.reset_sticker_streak(u)  # текст сбрасывает стикер-стрик

    stop = asyncio.Event()
    asyncio.create_task(_typing_loop(msg.chat.id, stop))

    try:
        # проверяем ждём ли промт для /draw
        if u in _draw_waiting:
            _draw_waiting.discard(u)
            stop.set()
            draw_stop = asyncio.Event()
            asyncio.create_task(_upload_photo_loop(msg.chat.id, draw_stop))
            draw_phrases = ["ща нарисую", "рисую", "щас сделаю 🎨", "окк рисую"]
            status = await msg.answer(random.choice(draw_phrases))
            img = await ai_draw(aiohttp_session, text)
            draw_stop.set()
            await status.delete()
            if img:
                await msg.answer_photo(BufferedInputFile(img, "murka_art.jpg"),
                                       caption="на жри 🎨")
            else:
                await msg.answer(random.choice(["чото не рисуется попробуй позже", "сломалось нахуй попробуй ещё раз", "ну типа не вышло", "не CannotDraw"]))
            return

        # проверяем ждём ли текст для /voice
        if u in _voice_waiting:
            _voice_waiting.discard(u)
            stop.set()
            voice_stop = asyncio.Event()
            asyncio.create_task(_upload_audio_loop(msg.chat.id, voice_stop))
            status = await msg.answer(random.choice(["🎙 синтезирую...", "пою...", "записываю голосяру"]))
            audio = await rvc_synthesize(aiohttp_session, text)
            voice_stop.set()
            await status.delete()
            if audio:
                await msg.answer_voice(BufferedInputFile(audio, "murka_voice.ogg"))
            else:
                await msg.answer(random.choice(["чото не вышло с голосом", "сломалось нахуй", "hf space отдыхает попробуй позже"]))
            return

        # /music — ждём название песни текстом
        if u in _music_waiting:
            _music_waiting.discard(u)
            stop.set()
            await music_pipeline(aiohttp_session, msg, u, song_query=text)
            return

        # обработка запроса на картинку (не нарисовать, а найти)
        pic_match = re.search(
            r"(?i)(скинь|найди|покажи|кинь|дай).{0,15}(картинк|фото|пик|изображени|мем)",
            text
        )
        if pic_match:
            # убираем триггеры и ищем
            query = re.sub(r"(?i)(скинь|найди|покажи|кинь|дай).{0,15}(картинк|фото|пик|изображени|мем)\s*(о|про|с|по)?\s*", "", text).strip()
            if not query: query = text
            sent = await search_and_send_pic(msg, query)
            stop.set()
            if sent:
                await send_smart(msg, random.choice(["на", "держи", "вот", "нашла"]))
                return
            # если pic не нашёл — просто отвечаем текстом

        answer = await ai_chat(aiohttp_session, u, text)
        stop.set()
        answer = await maybe_send_sticker(msg, answer)
        await send_smart(msg, answer)
        asyncio.create_task(ai_extract_fact(aiohttp_session, u, text))
        await maybe_force_sticker(msg, u)
    except Exception as e:
        stop.set()
        log.exception("on_text")
        await msg.answer(_fallback())


# inline нарисуй без /draw
@dp.message(F.text.regexp(r"(?i)^нарисуй\s+.+"))
async def on_draw_inline(msg: Message, aiohttp_session: aiohttp.ClientSession):
    u    = uid(msg)
    stop = asyncio.Event()
    asyncio.create_task(_upload_photo_loop(msg.chat.id, stop))
    try:
        draw_phrases = ["ща нарисую", "рисую", "щас сделаю 🎨", "окк рисую"]
        status = await msg.answer(random.choice(draw_phrases))
        img = await ai_draw(aiohttp_session, msg.text)
        stop.set()
        await status.delete()
        if img:
            await msg.answer_photo(BufferedInputFile(img, "murka_art.jpg"),
                                   caption="на жри 🎨")
        else:
            await msg.answer(random.choice(["чото не рисуется попробуй позже", "сломалось нахуй попробуй ещё раз", "ну типа не вышло", "не CannotDraw"]))
    except Exception as e:
        stop.set()
        log.exception("on_draw_inline")
        await msg.answer(_fallback())


# ══════════════════════════════════════════════════════════════════════════════
# MIDDLEWARE
# ══════════════════════════════════════════════════════════════════════════════
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable
from aiogram.types import TelegramObject

class SessionMiddleware(BaseMiddleware):
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        data["aiohttp_session"] = self.session
        return await handler(event, data)


class AccessMiddleware(BaseMiddleware):
    """Проверяет доступ по chat_id. Без ключей, без задержек для разрешённых."""
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        if not Secrets.ALLOWED_IDS:
            return await handler(event, data)
        msg = data.get("event_update", {})
        # получаем chat_id из любого типа события
        chat_id = None
        if hasattr(event, "chat") and event.chat:
            chat_id = event.chat.id
        elif hasattr(event, "message") and event.message:
            chat_id = event.message.chat.id
        if chat_id is None:
            return await handler(event, data)
        # в чёрном списке — молчим полностью
        if chat_id in _BLACKLIST:
            return
        # не в списке разрешённых — добавляем в чёрный список и один раз отвечаем
        if chat_id not in Secrets.ALLOWED_IDS:
            _BLACKLIST.add(chat_id)
            log.warning("Заблокирован chat_id=%d", chat_id)
            if isinstance(event, Message):
                try: await event.answer("нет доступа")
                except Exception: pass
            return
        return await handler(event, data)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    if not Secrets.TG_BOT_TOKEN:
        log.error("TG_BOT_TOKEN не задан!"); sys.exit(1)
    if not Secrets.GEMINI_POOL:
        log.warning("GEMINI_1..20 не заданы!")
    if not Secrets.OPENROUTER_KEY:
        log.warning("OPENROUTER_KEY не задан!")

    # регистрируем команды в меню бота
    await bot.set_my_commands([
        BotCommand(command="draw",   description="нарисовать что-нибудь"),
        BotCommand(command="voice",  description="озвучить текст голосом мурки"),
        BotCommand(command="music",  description="спеть песню голосом мурки"),
        BotCommand(command="forget", description="сбросить память"),
        BotCommand(command="memory", description="что я о тебе знаю"),
        BotCommand(command="help",   description="помощь"),
    ], scope=BotCommandScopeDefault())

    async with aiohttp.ClientSession() as session:
        dp.update.middleware(SessionMiddleware(session))
        dp.update.outer_middleware(AccessMiddleware())
        log.info("Murka Bot v8 запущена")
        await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
