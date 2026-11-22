import asyncio
import json
import logging
import secrets
import time
from datetime import datetime, timezone
from html import escape

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import BaseFilter, Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
    InlineQuery, InlineQueryResultArticle, InputTextMessageContent
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Integer, String, Text,
    func, select, update, desc, or_
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# ============================================================
# CONFIG
# ============================================================

class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    bot_token: str
    owner_id: int
    database_url: str = "sqlite+aiosqlite:///sarehni_fix.db" # قاعدة بيانات جديدة

settings = Settings()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("sarehni")

# ============================================================
# DATABASE
# ============================================================

engine = create_async_engine(settings.database_url, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    bio: Mapped[str | None] = mapped_column(String(200), nullable=True)
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    link: Mapped["PersonalLink"] = relationship(back_populates="user", uselist=False)

class PersonalLink(Base):
    __tablename__ = "personal_links"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), unique=True)
    slug: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    visits: Mapped[int] = mapped_column(Integer, default=0)
    messages_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    user: Mapped[User] = relationship(back_populates="link")

class MessageModel(Base):
    __tablename__ = "messages"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    receiver_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    bot_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_type: Mapped[str] = mapped_column(String(32), default="text")
    content: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)

class BlockedSender(Base):
    __tablename__ = "blocked_senders"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    receiver_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    sender_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class BotSetting(Base):
    __tablename__ = "bot_settings"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class Whisper(Base):
    __tablename__ = "whispers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_id: Mapped[int] = mapped_column(BigInteger, index=True)
    text: Mapped[str] = mapped_column(Text)
    target_username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    target_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    max_views: Mapped[int] = mapped_column(Integer, default=1000)
    current_views: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class WhisperView(Base):
    __tablename__ = "whisper_views"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    whisper_id: Mapped[int] = mapped_column(Integer, ForeignKey("whispers.id", ondelete="CASCADE"), index=True)
    viewer_id: Mapped[int] = mapped_column(BigInteger, index=True)
    viewer_name: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

# ============================================================
# SERVICES
# ============================================================

async def upsert_user(session: AsyncSession, tg_user) -> User:
    user = await session.get(User, tg_user.id)
    if user is None:
        user = User(id=tg_user.id, username=tg_user.username, first_name=tg_user.first_name, last_name=tg_user.last_name)
        session.add(user)
    else:
        user.username = tg_user.username
        user.first_name = tg_user.first_name
        user.last_name = tg_user.last_name
        user.last_seen = datetime.now(timezone.utc)
    await session.commit()
    return user

async def get_user(session: AsyncSession, uid: int) -> User | None:
    return await session.get(User, uid)

async def get_or_create_link(session: AsyncSession, user_id: int) -> PersonalLink:
    link = (await session.execute(select(PersonalLink).where(PersonalLink.user_id == user_id))).scalar_one_or_none()
    if link: return link
    link = PersonalLink(user_id=user_id, slug=secrets.token_urlsafe(8))
    session.add(link)
    await session.commit()
    await session.refresh(link)
    return link

async def get_link_by_slug(session: AsyncSession, slug: str) -> PersonalLink | None:
    return (await session.execute(select(PersonalLink).where(PersonalLink.slug == slug))).scalar_one_or_none()

async def toggle_link(session: AsyncSession, user_id: int) -> bool | None:
    link = (await session.execute(select(PersonalLink).where(PersonalLink.user_id == user_id))).scalar_one_or_none()
    if not link: return None
    link.is_active = not link.is_active
    await session.commit()
    return link.is_active

async def check_sender_blocked(session: AsyncSession, receiver_id: int, sender_id: int) -> bool:
    block = (await session.execute(select(BlockedSender).where(BlockedSender.receiver_id == receiver_id, BlockedSender.sender_id == sender_id))).scalar_one_or_none()
    return block is not None

async def block_sender(session: AsyncSession, receiver_id: int, sender_id: int):
    block = BlockedSender(receiver_id=receiver_id, sender_id=sender_id)
    session.add(block)
    await session.commit()

async def get_message(session: AsyncSession, mid: int) -> MessageModel | None:
    return await session.get(MessageModel, mid)

async def mark_deleted(session: AsyncSession, mid: int, receiver_id: int) -> bool:
    msg = await session.get(MessageModel, mid)
    if not msg or msg.receiver_id != receiver_id: return False
    msg.is_deleted = True
    await session.commit()
    return True

async def get_setting(session: AsyncSession, key: str, default: str | None = None) -> str | None:
    s = await session.get(BotSetting, key)
    return s.value if s else default

async def set_setting(session: AsyncSession, key: str, value: str) -> None:
    s = await session.get(BotSetting, key)
    if s: s.value = value
    else: session.add(BotSetting(key=key, value=value))
    await session.commit()

async def get_dynamic_text(session: AsyncSession, key: str, default: str) -> str:
    val = await get_setting(session, key)
    return val if val else default

# ============================================================
# MIDDLEWARES & FILTERS
# ============================================================

class DbSessionMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        async with SessionLocal() as session:
            data["session"] = session
            return await handler(event, data)

class UserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg_user = None
        if hasattr(event, "message") and event.message:
            tg_user = event.message.from_user
        elif hasattr(event, "callback_query") and event.callback_query:
            tg_user = event.callback_query.from_user
        elif hasattr(event, "inline_query") and event.inline_query:
            tg_user = event.inline_query.from_user

        session: AsyncSession | None = data.get("session")
        if tg_user is not None and not tg_user.is_bot and session is not None:
            try:
                await upsert_user(session, tg_user)
            except Exception as e:
                logger.warning(f"upsert failed: {e}")
        return await handler(event, data)

class IsAdmin(BaseFilter):
    async def __call__(self, event: Message | CallbackQuery, session: AsyncSession) -> bool:
        if event.from_user.id == settings.owner_id: return True
        u = await get_user(session, event.from_user.id)
        return u and u.is_admin

class BannedFilter(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if hasattr(event, "from_user") and event.from_user:
            session = data.get("session")
            if session:
                u = await get_user(session, event.from_user.id)
                if u and u.is_blocked and event.from_user.id != settings.owner_id:
                    if isinstance(event, Message): await event.answer("🚫 أنت محظور من استخدام هذا البوت.")
                    elif isinstance(event, CallbackQuery): await event.answer("🚫 أنت محظور.", show_alert=True)
                    return 
        return await handler(event, data)

# ============================================================
# STATES
# ============================================================

class SendAnonymous(StatesGroup):
    waiting = State()

class EditBio(StatesGroup):
    waiting = State()

class AdminStates(StatesGroup):
    waiting_broadcast = State()
    waiting_ban = State()
    waiting_admin = State()
    waiting_text = State()

# ============================================================
# TEXTS (نصوص عادية وبسيطة)
# ============================================================

DEFAULT_WELCOME = "مرحبًا بك في <b>صارحني</b> ❤️\n\nهنا يمكنك استقبال الرسائل الصريحة والمجهولة من الآخرين دون معرفة هويتهم.\n\nاختر من القائمة:"
DEFAULT_ANON_INTRO = "أنت الآن على وشك إرسال رسالة مجهولة ❤️\n\nاكتب رسالتك بكل صراحة، وستصل إلى صاحب الرابط دون إظهار هويتك.\n\n<i>يمكنك إرسال أكثر من رسالة، وعند الانتهاء اضغط على زر إنهاء المحادثة.</i>"
DEFAULT_ANON_SENT = "✅ تم إرسال رسالتك بنجاح ❤️\nهويتك غير ظاهرة لصاحب الرابط.\n\nيمكنك إرسال رسالة أخرى، أو الضغط على إنهاء المحادثة."
DEFAULT_REPLY_SENT = "✅ تم إرسال الرد بنجاح."
DEFAULT_REPLY_RECEIVED = "📨 <b>رد على رسالتك</b>\n\n{text}"

# ============================================================
# HELPERS & KEYBOARDS
# ============================================================

def sender_line(sender: User | None) -> str:
    if not sender: return ""
    name = " ".join(p for p in [sender.first_name or "", sender.last_name or ""] if p) or "مستخدم"
    uname = f"@{sender.username}" if sender.username else "—"
    return f"\n\n—————\n👤 الاسم: {escape(name)}\n🔗 اليوزر: {escape(uname)}\n🆔 الإيدي: <code>{sender.id}</code>"

def main_menu(owner: bool = False, admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🔗 رابط صارحني", callback_data="menu:link")],
        [InlineKeyboardButton(text="📩 رسائلي الواردة", callback_data="menu:inbox")],
        [InlineKeyboardButton(text="📊 إحصائياتي", callback_data="menu:stats")],
        [InlineKeyboardButton(text="⚙️ إعداداتي", callback_data="menu:settings")],
        [InlineKeyboardButton(text="ℹ️ حول البوت", callback_data="menu:about")],
    ]
    if owner or admin:
        rows.append([InlineKeyboardButton(text="🛠 لوحة التحكم", callback_data="admin:panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def back_kb(target: str = "menu:main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="↩️ رجوع", callback_data=target)]])

def link_kb(active: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏸ تعطيل الاستقبال" if active else "▶️ تفعيل الاستقبال", callback_data="link:toggle")],
        [InlineKeyboardButton(text="📋 إرسال الرابط لنسخه", callback_data="link:copy")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="menu:main")],
    ])

def msg_actions(mid: int, sender_id: int | None = None) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="🗑 حذف", callback_data=f"msg:delete:{mid}")]]
    if sender_id: rows.append([InlineKeyboardButton(text="🚫 حظر المرسل", callback_data=f"msg:block:{sender_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def reaction_kb(mid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❤️", callback_data=f"react:❤️:{mid}"),
        InlineKeyboardButton(text="👍", callback_data=f"react:👍:{mid}"),
        InlineKeyboardButton(text="😂", callback_data=f"react:😂:{mid}"),
        InlineKeyboardButton(text="😮", callback_data=f"react:😮:{mid}"),
    ]])

def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 إذاعة عامة", callback_data="admin:broadcast")],
        [InlineKeyboardButton(text="🚫 حظر / إلغاء حظر", callback_data="admin:ban")],
        [InlineKeyboardButton(text="👑 إضافة / إزالة مشرف", callback_data="admin:add")],
        [InlineKeyboardButton(text="✏️ تعديل النصوص", callback_data="admin:edit_texts")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="menu:main")],
    ])

def settings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 تعديل الملف الشخصي (Bio)", callback_data="settings:edit_bio")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="menu:main")],
    ])

def end_chat_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ إنهاء المحادثة", callback_data="anon:end")]
    ])

# ============================================================
# HANDLERS — WHISPERS (INLINE MODE)
# ============================================================

inline_router = Router(name="inline")

@inline_router.inline_query()
async def inline_whisper(query: InlineQuery, session: AsyncSession):
    text = query.query.strip()
    if not text: return
    
    parts = text.rsplit(" ", 1)
    target_user = None
    whisper_text = text
    
    if len(parts) == 2 and parts[1].startswith("@"):
        target_user = parts[1][1:]
        whisper_text = parts[0]

    results = []
    
    async def create_whisper_result(title: str, max_views: int, target: str = None, tid: int = None):
        w = Whisper(owner_id=query.from_user.id, text=whisper_text, max_views=max_views, target_username=target, target_id=tid)
        session.add(w)
        await session.commit()
        await session.refresh(w)
        
        target_msg = f"لشخص: {target}" if target else ("للجميع" if max_views > 100 else f"لأول {max_views} أشخاص")
        results.append(InlineQueryResultArticle(
            id=str(w.id), title=title, description=f"اضغط لإرسال همسة {target_msg}",
            input_message_content=InputTextMessageContent(message_text=f"🤫 <b>هذه همسة نصية سرية {target_msg}</b>\n\nاضغط على الزر بالأسفل لفتحها.", parse_mode=ParseMode.HTML),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="افتح الهمسة 🤫", callback_data=f"open_whisper:{w.id}")]])
        ))

    if target_user:
        await create_whisper_result(f"🔒 همسة لشخص محدد ({target_user})", 1000, target=target_user)
    else:
        await create_whisper_result("🌍 همسة سرية للجميع", 999999)
        await create_whisper_result("🔒 همسة سرية لأول شخص", 1)
        await create_whisper_result("👥 همسة سرية لأول 3 أشخاص", 3)
        await create_whisper_result("👥 همسة سرية لأول 5 أشخاص", 5)
        await create_whisper_result("👥 همسة سرية لأول 10 أشخاص", 10)
        await create_whisper_result("👥 همسة سرية لأول 15 شخصاً", 15)

    await query.answer(results, cache_time=0, is_personal=True)

@inline_router.callback_query(F.data.startswith("open_whisper:"))
async def open_whisper_cb(cb: CallbackQuery, session: AsyncSession):
    wid = int(cb.data.split(":")[1])
    w = await session.get(Whisper, wid)
    
    if not w: return await cb.answer("هذه الهمسة غير موجودة.", show_alert=True)
    if cb.from_user.id == w.owner_id: return await cb.answer(f"أنت صاحب الهمسة! 🤫\n\n{w.text}", show_alert=True)

    viewed = (await session.execute(select(WhisperView).where(WhisperView.whisper_id == wid, WhisperView.viewer_id == cb.from_user.id))).scalar_one_or_none()
    if viewed: return await cb.answer("أنت بالفعل فتحت هذه الهمسة من قبل! 🤫", show_alert=True)

    if w.target_username and cb.from_user.username != w.target_username: return await cb.answer("عذراً، هذه الهمسة ليست لك.", show_alert=True)
    if w.target_id and cb.from_user.id != w.target_id: return await cb.answer("عذراً، هذه الهمسة ليست لك.", show_alert=True)
    if w.current_views >= w.max_views: return await cb.answer("انتهت هذه الهمسة.", show_alert=True)

    viewer_name = " ".join(p for p in [cb.from_user.first_name or "", cb.from_user.last_name or ""] if p) or "مستخدم"
    view = WhisperView(whisper_id=wid, viewer_id=cb.from_user.id, viewer_name=viewer_name)
    session.add(view)
    w.current_views += 1
    await session.commit()

    owner_uname = f"@{cb.from_user.username}" if cb.from_user.username else "—"
    try:
        await cb.bot.send_message(w.owner_id, f"📢 <b>شخص فتح همستك!</b>\n\n👤 الاسم: {escape(viewer_name)}\n🔗 اليوزر: {owner_uname}\n🆔 الإيدي: <code>{cb.from_user.id}</code>\n\n📊 العدد: {w.current_views} من {w.max_views}", parse_mode=ParseMode.HTML)
    except Exception: pass

    views = (await session.execute(select(WhisperView).where(WhisperView.whisper_id == wid).order_by(WhisperView.created_at))).scalars().all()
    kb_rows = [[InlineKeyboardButton(text="افتح الهمسة 🤫", callback_data=f"open_whisper:{w.id}")]]
    for v in views: kb_rows.append([InlineKeyboardButton(text=f"{v.viewer_name} 👁", callback_data="ignore_click")])
    
    try:
        await cb.bot.edit_message_reply_markup(inline_message_id=cb.inline_message_id, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    except Exception: pass

    await cb.answer(w.text, show_alert=True)

@inline_router.callback_query(F.data == "ignore_click")
async def ignore_click(cb: CallbackQuery):
    await cb.answer("هذا الشخص فتح الهمسة بالفعل 👁", show_alert=False)


# ============================================================
# HANDLERS — ANONYMOUS
# ============================================================

anon_router = Router(name="anon")

@anon_router.message(CommandStart(deep_link=True))
async def open_link(message: Message, command: CommandObject, state: FSMContext, session: AsyncSession):
    slug = command.args
    if not slug: return
    link = await get_link_by_slug(session, slug)
    
    if not link or not link.is_active: return await message.answer("⚠️ هذا الرابط غير مفعّل حالياً.")
    if link.user_id == message.from_user.id: return await message.answer("هذا رابطك الخاص، شاركه مع أصدقائك.")
    if await check_sender_blocked(session, link.user_id, message.from_user.id): return await message.answer("⚠️ لا يمكنك مراسلة هذا المستخدم.")
        
    await state.set_state(SendAnonymous.waiting)
    await state.update_data(receiver_id=link.user_id)
    
    intro_text = await get_dynamic_text(session, "text:anon_intro", DEFAULT_ANON_INTRO)
    await message.answer(intro_text, reply_markup=end_chat_kb())

@anon_router.callback_query(F.data == "anon:end")
async def end_anon_chat(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("❌ تم إنهاء المحادثة. يمكنك العودة للقائمة الرئيسية.")
    await cb.answer()

@anon_router.message(SendAnonymous.waiting, F.text | F.photo | F.sticker | F.video | F.voice | F.document)
async def receive_anon(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    receiver_id = data.get("receiver_id")
    if not receiver_id:
        await state.clear()
        return

    ctype, content = "text", message.text or message.caption or ""
    if message.photo: ctype, content = "photo", message.photo[-1].file_id
    elif message.sticker: ctype, content = "sticker", message.sticker.file_id
    elif message.video: ctype, content = "video", message.video.file_id
    elif message.voice: ctype, content = "voice", message.voice.file_id
    elif message.document: ctype, content = "document", message.document.file_id

    if not content: return await message.answer("ابعت رسالة نصية أو وسائط.")

    msg = MessageModel(receiver_id=receiver_id, sender_id=message.from_user.id, content_type=ctype, content=content)
    session.add(msg)
    await session.commit()
    await session.refresh(msg)

    sender = await get_user(session, message.from_user.id)
    header = f"📩 <b>رسالة #{msg.id}</b>"
    tail = sender_line(sender)
    
    try:
        if ctype == "text":
            sent_bot_msg = await message.bot.send_message(receiver_id, f"{header}\n\n{escape(content)}{tail}", reply_markup=msg_actions(msg.id, message.from_user.id))
        elif ctype == "photo":
            sent_bot_msg = await message.bot.send_photo(receiver_id, content, caption=header + tail, reply_markup=msg_actions(msg.id, message.from_user.id))
        else:
            sent_bot_msg = await message.bot.send_message(receiver_id, f"{header}\n\n{escape(content)}{tail}", reply_markup=msg_actions(msg.id, message.from_user.id))
        
        msg.bot_message_id = sent_bot_msg.message_id
        await session.commit()
        
        sent_text = await get_dynamic_text(session, "text:anon_sent", DEFAULT_ANON_SENT)
        await message.answer(sent_text, reply_markup=end_chat_kb())
    except Exception as e:
        logger.error(f"Failed to deliver message: {e}")
        await message.answer("❌ عذراً، تعذر إرسال رسالتك.", reply_markup=end_chat_kb())

    # هنا كان الخطأ.. مسحنا الـ state.clear() عشان يسمح برسائل متعددة والردود

@anon_router.message(SendAnonymous.waiting)
async def wrong_input(message: Message):
    await message.answer("ابعت رسالة نصية أو وسائط.", reply_markup=end_chat_kb())


# ============================================================
# HANDLERS — USER & REPLY SYSTEM
# ============================================================

user_router = Router(name="user")

@user_router.message(CommandStart())
async def start(message: Message, state: FSMContext, session: AsyncSession):
    await state.clear()
    welcome_text = await get_dynamic_text(session, "text:welcome", DEFAULT_WELCOME)
    user = await get_user(session, message.from_user.id)
    is_admin = user and user.is_admin
    await message.answer(welcome_text, reply_markup=main_menu(owner=(message.from_user.id == settings.owner_id), admin=is_admin))

@user_router.callback_query(F.data == "menu:main")
async def menu_main(cb: CallbackQuery, state: FSMContext, session: AsyncSession):
    await state.clear()
    welcome_text = await get_dynamic_text(session, "text:welcome", DEFAULT_WELCOME)
    user = await get_user(session, cb.from_user.id)
    is_admin = user and user.is_admin
    try:
        await cb.message.edit_text(welcome_text, reply_markup=main_menu(owner=(cb.from_user.id == settings.owner_id), admin=is_admin))
    except Exception:
        await cb.message.edit_text(DEFAULT_WELCOME, reply_markup=main_menu(owner=(cb.from_user.id == settings.owner_id), admin=is_admin))
    await cb.answer()

@user_router.message(F.reply_to_message)
async def handle_reply(message: Message, session: AsyncSession):
    replied_msg_id = message.reply_to_message.message_id
    original_msg = (await session.execute(select(MessageModel).where(MessageModel.bot_message_id == replied_msg_id, MessageModel.receiver_id == message.from_user.id))).scalar_one_or_none()
    if not original_msg or not original_msg.sender_id: return 

    reply_text = await get_dynamic_text(session, "text:reply_received", DEFAULT_REPLY_RECEIVED)
    reply_text = reply_text.replace("{text}", escape(message.text or ""))
    
    try:
        await message.bot.send_message(original_msg.sender_id, reply_text, reply_markup=reaction_kb(original_msg.id))
        success_text = await get_dynamic_text(session, "text:reply_sent", DEFAULT_REPLY_SENT)
        await message.answer(success_text)
    except Exception:
        await message.answer("❌ تعذر إرسال الرد.")

@user_router.callback_query(F.data.startswith("msg:block:"))
async def msg_block_sender(cb: CallbackQuery, session: AsyncSession):
    sender_id = int(cb.data.split(":")[2])
    await block_sender(session, cb.from_user.id, sender_id)
    await cb.message.edit_text("🚫 تم حظر المرسل بنجاح. لن تصلك منه رسائل.")
    await cb.answer("تم الحظر")

@user_router.callback_query(F.data.startswith("react:"))
async def msg_react(cb: CallbackQuery, session: AsyncSession):
    _, emoji, mid_s = cb.data.split(":")
    mid = int(mid_s)
    msg = await get_message(session, mid)
    if not msg or not msg.sender_id: return await cb.answer("لا يمكن التفاعل.", show_alert=True)
    try:
        await cb.bot.send_message(msg.sender_id, f"📨 لقد تلقيت تفاعل {emoji} على رسالتك المجهولة!")
        await cb.message.edit_text(f"✅ تم إرسال التفاعل {emoji}.")
    except Exception:
        await cb.answer("تعذر إرسال التفاعل.", show_alert=True)
    await cb.answer()

@user_router.callback_query(F.data.startswith("msg:delete:"))
async def msg_delete(cb: CallbackQuery, session: AsyncSession):
    mid = int(cb.data.split(":")[2])
    if await mark_deleted(session, mid, cb.from_user.id):
        await cb.message.edit_text("🗑 تم حذف الرسالة.")
    else:
        await cb.answer("تعذر الحذف", show_alert=True)
    await cb.answer()

@user_router.callback_query(F.data == "menu:link")
async def menu_link(cb: CallbackQuery, session: AsyncSession):
    link = await get_or_create_link(session, cb.from_user.id)
    bot_info = await cb.bot.get_me()
    url = f"https://t.me/{bot_info.username}?start={link.slug}"
    text = (
        f"🔗 <b>رابطك الشخصي:</b>\n<code>{url}</code>\n\n"
        "شارك الرابط مع أصدقائك، وسيتمكنون من إرسال رسائل مجهولة لك.\n\n"
        f"حالة الاستقبال: {'✅ مفعّل' if link.is_active else '⏸ معطّل'}\n"
        f"عدد الزيارات: {link.visits}\n"
        f"عدد الرسائل: {link.messages_count}"
    )
    await cb.message.edit_text(text, reply_markup=link_kb(link.is_active), disable_web_page_preview=True)
    await cb.answer()

@user_router.callback_query(F.data == "link:toggle")
async def link_toggle(cb: CallbackQuery, session: AsyncSession):
    active = await toggle_link(session, cb.from_user.id)
    if active is None: return await cb.answer("الرابط غير موجود", show_alert=True)
    await menu_link(cb, session)

@user_router.callback_query(F.data == "link:copy")
async def link_copy(cb: CallbackQuery, session: AsyncSession):
    link = await get_or_create_link(session, cb.from_user.id)
    bot_info = await cb.bot.get_me()
    url = f"https://t.me/{bot_info.username}?start={link.slug}"
    await cb.message.answer(f"<code>{url}</code>")
    await cb.answer("اضغط على الرابط مطولاً لنسخه")

@user_router.callback_query(F.data == "menu:inbox")
async def menu_inbox(cb: CallbackQuery, session: AsyncSession):
    rows = (await session.execute(
        select(MessageModel, User).outerjoin(User, User.id == MessageModel.sender_id)
        .where(MessageModel.receiver_id == cb.from_user.id, MessageModel.is_deleted.is_(False))
        .order_by(desc(MessageModel.created_at)).limit(10)
    )).all()
    
    if not rows:
        await cb.message.edit_text("📭 لا توجد رسائل حتى الآن.", reply_markup=back_kb())
        return await cb.answer()
        
    await cb.message.edit_text("📩 آخر رسائلك:", reply_markup=back_kb())
    for msg, sender in rows:
        header = f"📩 <b>رسالة #{msg.id}</b>"
        tail = sender_line(sender)
        try:
            if msg.content_type == "text":
                await cb.bot.send_message(cb.from_user.id, f"{header}\n\n{escape(msg.content)}{tail}", reply_markup=msg_actions(msg.id, sender.id if sender else None))
            elif msg.content_type == "photo":
                await cb.bot.send_photo(cb.from_user.id, msg.content, caption=header + tail, reply_markup=msg_actions(msg.id, sender.id if sender else None))
        except Exception: pass
    await cb.answer()

@user_router.callback_query(F.data == "menu:stats")
async def menu_stats(cb: CallbackQuery, session: AsyncSession):
    received = (await session.execute(select(func.count(MessageModel.id)).where(MessageModel.receiver_id == cb.from_user.id, MessageModel.is_deleted.is_(False)))).scalar_one()
    sent = (await session.execute(select(func.count(MessageModel.id)).where(MessageModel.sender_id == cb.from_user.id))).scalar_one()
    link = (await session.execute(select(PersonalLink).where(PersonalLink.user_id == cb.from_user.id))).scalar_one_or_none()
    visits = link.visits if link else 0
    
    text = f"📊 <b>إحصائياتك</b>\n\n📩 رسائل مستلمة: {received}\n📤 رسائل مرسلة: {sent}\n👁 زيارات رابطك: {visits}\n"
    await cb.message.edit_text(text, reply_markup=back_kb())
    await cb.answer()

@user_router.callback_query(F.data == "menu:settings")
async def menu_settings(cb: CallbackQuery):
    await cb.message.edit_text("⚙️ <b>إعداداتك</b>\n\nاختر ما تريد تعديله:", reply_markup=settings_kb())
    await cb.answer()

@user_router.callback_query(F.data == "settings:edit_bio")
async def settings_edit_bio(cb: CallbackQuery, state: FSMContext):
    await state.set_state(EditBio.waiting)
    await cb.message.edit_text("✍️ اكتب الوصف الجديد اللي عايز الناس تشوفه (حد أقصى 200 حرف):", reply_markup=back_kb("menu:settings"))
    await cb.answer()

@user_router.message(EditBio.waiting, F.text)
async def settings_save_bio(message: Message, state: FSMContext, session: AsyncSession):
    bio = message.text[:200]
    user = await get_user(session, message.from_user.id)
    if user:
        user.bio = bio
        await session.commit()
    await message.answer("✅ تم تحديث الوصف بنجاح.", reply_markup=settings_kb())
    await state.clear()

@user_router.callback_query(F.data == "menu:about")
async def menu_about(cb: CallbackQuery):
    await cb.message.edit_text(
        "ℹ️ <b>حول البوت</b>\n\nصارحني هو بوت رسائل مجهولة.\nأنشئ رابطك الشخصي وشاركه مع أصدقائك لاستقبال رسائل صريحة ومجهولة.\n\n🔒 <b>الخصوصية:</b> هوية المرسل مخفية عنك.",
        reply_markup=back_kb(),
    )
    await cb.answer()


# ============================================================
# HANDLERS — ADMIN PANEL (Owner + Admins)
# ============================================================

admin_router = Router(name="admin")
admin_router.message.filter(IsAdmin())
admin_router.callback_query.filter(IsAdmin())

@admin_router.message(Command("admin"))
async def admin_cmd(message: Message):
    await message.answer("🛠 لوحة الإدارة", reply_markup=admin_kb())

@admin_router.callback_query(F.data == "admin:panel")
async def admin_panel(cb: CallbackQuery):
    await cb.message.edit_text("🛠 لوحة الإدارة", reply_markup=admin_kb())
    await cb.answer()

@admin_router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_broadcast)
    await cb.message.edit_text("📢 اكتب الرسالة اللي عايز تبعتها لكل المستخدمين:", reply_markup=back_kb("admin:panel"))
    await cb.answer()

@admin_router.message(AdminStates.waiting_broadcast, F.text)
async def admin_broadcast_run(message: Message, state: FSMContext, session: AsyncSession):
    await message.answer("⏳ جاري الإرسال...")
    users = (await session.execute(select(User.id).where(User.is_blocked.is_(False), User.is_active.is_(True)))).scalars().all()
    success, failed = 0, 0
    for uid in users:
        try:
            await message.bot.send_message(uid, message.text)
            success += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await message.answer(f"✅ تم الإرسال بنجاح لـ {success} مستخدم.\n❌ فشل الإرسال لـ {failed} مستخدم.", reply_markup=admin_kb())
    await state.clear()

@admin_router.callback_query(F.data == "admin:ban")
async def admin_ban_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_ban)
    await cb.message.edit_text("🚫 اكتب (ID) أو (يوزر) الشخص اللي عايز تحظره أو تلغي حظره:", reply_markup=back_kb("admin:panel"))
    await cb.answer()

@admin_router.message(AdminStates.waiting_ban, F.text)
async def admin_ban_run(message: Message, state: FSMContext, session: AsyncSession):
    query = message.text.strip().replace("@", "")
    u = None
    if query.isdigit(): u = await get_user(session, int(query))
    else: u = (await session.execute(select(User).where(User.username == query))).scalar_one_or_none()

    if not u: return await message.answer("❌ المستخدم غير موجود.")
    if u.id == settings.owner_id: return await message.answer("❌ لا يمكنك حظر المالك!")

    u.is_blocked = not u.is_blocked
    await session.commit()
    status = "محظور 🚫" if u.is_blocked else "مفعل ✅"
    await message.answer(f"تم تغيير حالة المستخدم <code>{u.id}</code> إلى: {status}", reply_markup=admin_kb())
    await state.clear()

@admin_router.callback_query(F.data == "admin:add")
async def admin_add_start(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != settings.owner_id: return await cb.answer("❌ المالك فقط يضيف مشرفين.", show_alert=True)
    await state.set_state(AdminStates.waiting_admin)
    await cb.message.edit_text("👑 اكتب (ID) الشخص لرفعه مشرف (أو إزالة الإشراف):", reply_markup=back_kb("admin:panel"))
    await cb.answer()

@admin_router.message(AdminStates.waiting_admin, F.text)
async def admin_add_run(message: Message, state: FSMContext, session: AsyncSession):
    if message.from_user.id != settings.owner_id: return
    query = message.text.strip()
    if not query.isdigit(): return await message.answer("❌ اكتب ID صحيح.")

    u = await get_user(session, int(query))
    if not u: return await message.answer("❌ المستخدم غير موجود.")
        
    u.is_admin = not u.is_admin
    await session.commit()
    status = "مشرف 👑" if u.is_admin else "مستخدم عادي 👤"
    await message.answer(f"تم تغيير رتبة <code>{u.id}</code> إلى: {status}", reply_markup=admin_kb())
    await state.clear()

@admin_router.callback_query(F.data == "admin:edit_texts")
async def admin_edit_texts(cb: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="رسالة الترحيب", callback_data="edittext:welcome")],
        [InlineKeyboardButton(text="رسالة بدء الصراحة", callback_data="edittext:anon_intro")],
        [InlineKeyboardButton(text="رسالة تأكيد الإرسال", callback_data="edittext:anon_sent")],
        [InlineKeyboardButton(text="رسالة تأكيد الرد", callback_data="edittext:reply_sent")],
        [InlineKeyboardButton(text="رسالة استلام الرد", callback_data="edittext:reply_received")],
        [InlineKeyboardButton(text="↩️ رجوع", callback_data="admin:panel")]
    ])
    await cb.message.edit_text("اختر النص اللي عايز تعدله:", reply_markup=kb)
    await cb.answer()

@admin_router.callback_query(F.data.startswith("edittext:"))
async def admin_edit_text_start(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":")[1]
    await state.set_state(AdminStates.waiting_text)
    await state.update_data(edit_key=f"text:{key}")
    await cb.message.edit_text("✍️ اكتب النص الجديد الآن:")
    await cb.answer()

@admin_router.message(AdminStates.waiting_text, F.text)
async def admin_edit_text_save(message: Message, state: FSMContext, session: AsyncSession):
    data = await state.get_data()
    key = data.get("edit_key")
    try:
        await message.bot.send_message(message.from_user.id, message.text)
    except Exception:
        await message.answer("❌ النص فيه رموز غير مدعومة. جرب تاني:")
        return 
    await set_setting(session, key, message.text)
    await message.answer("✅ تم تعديل النص بنجاح.", reply_markup=admin_kb())
    await state.clear()

# ============================================================
# BOOTSTRAP
# ============================================================

async def on_startup(bot: Bot):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Bot started in polling mode")

async def on_shutdown(bot: Bot):
    await engine.dispose()

async def main():
    bot = Bot(token=settings.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(DbSessionMiddleware())
    dp.update.middleware(UserMiddleware())
    dp.update.middleware(BannedFilter()) 
    
    dp.include_router(admin_router)
    dp.include_router(anon_router)
    dp.include_router(user_router)
    dp.include_router(inline_router)

    await on_startup(bot)
    
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await on_shutdown(bot)

if __name__ == "__main__":
    asyncio.run(main())
