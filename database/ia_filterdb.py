import logging
import re
import base64
import time
from struct import pack
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any

from hydrogram.file_id import FileId
from pymongo import MongoClient, TEXT, ASCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure

from info import (
    DATA_DATABASE_URL,
    DATABASE_NAME,
    COLLECTION_NAME,
    MAX_BTN,
    USE_CAPTION_FILTER
)

logger = logging.getLogger(__name__)

# =====================================================
# 📦 DATABASE CONNECTION
# =====================================================
try:
    client = MongoClient(
        DATA_DATABASE_URL,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=5000
    )
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    # Test connection
    client.server_info()
    logger.info("✅ Database connected successfully")
except Exception as e:
    logger.error(f"❌ Database connection failed: {e}")
    raise

# =====================================================
# 🚀 SAFE INDEX SETUP
# =====================================================
def ensure_indexes(col) -> None:
    """Create necessary indexes if they don't exist"""
    try:
        indexes = col.index_information()

        # Text search index
        if "file_text_index" not in indexes:
            try:
                col.create_index(
                    [("file_name", TEXT), ("caption", TEXT)],
                    name="file_text_index",
                    default_language="english"
                )
                logger.info("✅ Text index created")
            except OperationFailure as e:
                logger.warning(f"⚠️ Text index skipped: {e}")

        # Quality index for filtering
        if "quality_idx" not in indexes:
            col.create_index([("quality", ASCENDING)], name="quality_idx")
            logger.info("✅ Quality index created")

        # Updated timestamp index
        if "updated_at_idx" not in indexes:
            col.create_index([("updated_at", ASCENDING)], name="updated_at_idx")
            logger.info("✅ Updated_at index created")

    except Exception as e:
        logger.error(f"❌ Index creation error: {e}")

ensure_indexes(collection)

# =====================================================
# 📊 DOCUMENT COUNT
# =====================================================
def db_count_documents() -> int:
    """Get approximate document count (fast)"""
    try:
        return collection.estimated_document_count()
    except Exception as e:
        logger.error(f"Count error: {e}")
        return 0

# =====================================================
# ⚡ LIGHTWEIGHT CACHE
# =====================================================
SEARCH_CACHE: Dict[str, Tuple[Any, float]] = {}
CACHE_TTL = 30  # seconds
MAX_CACHE_SIZE = 1000

def cache_get(key: str) -> Optional[Any]:
    """Get cached value if not expired"""
    v = SEARCH_CACHE.get(key)
    if not v:
        return None
    
    data, ts = v
    if time.time() - ts > CACHE_TTL:
        SEARCH_CACHE.pop(key, None)
        return None
    
    return data

def cache_set(key: str, value: Any) -> None:
    """Set cache value with size limit"""
    # Clean old entries if cache is too large
    if len(SEARCH_CACHE) >= MAX_CACHE_SIZE:
        oldest = min(SEARCH_CACHE.items(), key=lambda x: x[1][1])
        SEARCH_CACHE.pop(oldest[0], None)
    
    SEARCH_CACHE[key] = (value, time.time())

def cache_clear() -> None:
    """Clear entire cache"""
    SEARCH_CACHE.clear()

# =====================================================
# 🧠 QUALITY DETECTOR
# =====================================================
QUALITY_PATTERNS = [
    (re.compile(r'\b(2160p?|4k|uhd)\b', re.IGNORECASE), "2160p"),
    (re.compile(r'\b1440p?\b', re.IGNORECASE), "1440p"),
    (re.compile(r'\b1080p?\b', re.IGNORECASE), "1080p"),
    (re.compile(r'\b720p?\b', re.IGNORECASE), "720p"),
    (re.compile(r'\b480p?\b', re.IGNORECASE), "480p"),
    (re.compile(r'\b360p?\b', re.IGNORECASE), "360p"),
]

def detect_quality(name: str) -> str:
    """Detect video quality from filename"""
    if not name:
        return "unknown"
    
    for pattern, quality in QUALITY_PATTERNS:
        if pattern.search(name):
            return quality
    
    return "unknown"

# =====================================================
# 🔎 SMART SEARCH ENGINE
# =====================================================
async def get_search_results(
    query: str,
    offset: int = 0,
    max_results: int = MAX_BTN
) -> Tuple[List[Dict], str, int]:
    """
    Search files with text search + regex fallback
    Returns: (files, next_offset, total_count)
    """
    # Validate input
    q = query.strip()
    if len(q) < 2:
        return [], "", 0
    
    q_lower = q.lower()
    
    # Check cache
    cache_key = f"{q_lower}:{offset}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    files = []
    total = 0

    # ===============================================
    # METHOD 1: TEXT SEARCH (FAST & RELEVANT)
    # ===============================================
    text_filter = {"$text": {"$search": q}}

    try:
        cursor = collection.find(
            text_filter,
            {
                "file_name": 1,
                "file_size": 1,
                "caption": 1,
                "quality": 1,
                "score": {"$meta": "textScore"},
            }
        ).sort([("score", {"$meta": "textScore"})]).skip(offset).limit(max_results)

        files = list(cursor)
        
        if files:
            # Count with limit for performance
            total = collection.count_documents(text_filter, limit=10000)

    except Exception as e:
        logger.error(f"Text search error: {e}")

    # ===============================================
    # METHOD 2: REGEX FALLBACK (SLOWER BUT ACCURATE)
    # ===============================================
    if not files:
        try:
            # Escape special regex characters
            escaped_query = re.escape(q)
            regex = re.compile(escaped_query, re.IGNORECASE)
            
            # Build filter based on caption setting
            if USE_CAPTION_FILTER:
                rg_filter = {"$or": [{"file_name": regex}, {"caption": regex}]}
            else:
                rg_filter = {"file_name": regex}

            cursor = collection.find(
                rg_filter,
                {"file_name": 1, "file_size": 1, "caption": 1, "quality": 1}
            ).skip(offset).limit(max_results)
            
            files = list(cursor)
            
            if files:
                # Limit count for performance
                total = min(
                    collection.count_documents(rg_filter, limit=5000),
                    5000
                )
        
        except Exception as e:
            logger.error(f"Regex search error: {e}")

    # Calculate next offset
    next_offset = str(offset + max_results) if total > offset + max_results else ""
    
    result = (files, next_offset, total)
    cache_set(cache_key, result)
    
    return result

# =====================================================
# 🗑 DELETE FILES BY QUERY
# =====================================================
async def delete_files(query: str) -> int:
    """Delete files matching query"""
    if not query or len(query) < 2:
        return 0
    
    try:
        escaped_query = re.escape(query.strip())
        regex = re.compile(escaped_query, re.IGNORECASE)
        
        res = collection.delete_many({"file_name": regex})
        
        # Clear cache after deletion
        cache_clear()
        
        return res.deleted_count
    
    except Exception as e:
        logger.error(f"Delete error: {e}")
        return 0

# =====================================================
# 🗑 DELETE ALL FILES (DANGEROUS!)
# =====================================================
async def delete_all_files() -> int:
    """
    Delete ALL files from database
    ⚠️ WARNING: This will delete EVERYTHING!
    Returns: Number of files deleted
    """
    try:
        # Delete all documents from collection
        res = collection.delete_many({})
        
        # Clear cache after deletion
        cache_clear()
        
        deleted_count = res.deleted_count
        logger.warning(f"⚠️ DELETED ALL FILES: {deleted_count} files removed!")
        
        return deleted_count
    
    except Exception as e:
        logger.error(f"❌ Delete all files error: {e}")
        return 0

# =====================================================
# 🗑 DELETE FILE BY ID
# =====================================================
async def delete_file_by_id(file_id: str) -> bool:
    """
    Delete single file by its ID
    Returns: True if deleted, False otherwise
    """
    if not file_id:
        return False
    
    try:
        res = collection.delete_one({"_id": file_id})
        
        if res.deleted_count > 0:
            # Clear cache after deletion
            cache_clear()
            logger.info(f"✅ File deleted: {file_id}")
            return True
        
        return False
    
    except Exception as e:
        logger.error(f"❌ Delete file by ID error: {e}")
        return False

# =====================================================
# 🗑 DELETE BY QUALITY
# =====================================================
async def delete_by_quality(quality: str) -> int:
    """
    Delete all files of specific quality
    Example: delete_by_quality("480p")
    Returns: Number of files deleted
    """
    if not quality:
        return 0
    
    try:
        res = collection.delete_many({"quality": quality})
        
        # Clear cache after deletion
        cache_clear()
        
        deleted_count = res.deleted_count
        logger.info(f"✅ Deleted {deleted_count} files with quality: {quality}")
        
        return deleted_count
    
    except Exception as e:
        logger.error(f"❌ Delete by quality error: {e}")
        return 0

# =====================================================
# 📄 GET FILE DETAILS
# =====================================================
async def get_file_details(file_id: str) -> Optional[Dict]:
    """Get single file details by ID"""
    if not file_id:
        return None
    
    try:
        return collection.find_one({"_id": file_id})
    except Exception as e:
        logger.error(f"Get file error: {e}")
        return None

# =====================================================
# 🧹 TEXT CLEANER
# =====================================================
def clean_text(text: str) -> str:
    """Remove special characters and extra spaces"""
    if not text:
        return ""
    
    # Remove usernames, URLs, special chars
    cleaned = re.sub(r'@\w+', '', text)
    cleaned = re.sub(r'https?://\S+', '', cleaned)
    cleaned = re.sub(r'[_\-\.+]+', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()

# =====================================================
# 💾 SAVE / UPDATE FILE
# =====================================================
async def save_file(media) -> str:
    """
    Save or update file in database
    Returns: 'suc' (new), 'dup' (updated), 'err' (failed)
    """
    try:
        # Validate input
        if not media or not hasattr(media, 'file_id'):
            return "err"
        
        # Generate unique file ID
        file_id = unpack_new_file_id(media.file_id)
        
        # Clean and prepare data
        file_name = clean_text(getattr(media, 'file_name', None) or "Untitled")
        caption = clean_text(getattr(media, 'caption', None) or "")
        file_size = getattr(media, 'file_size', 0)
        
        # Detect quality
        quality = detect_quality(file_name)

        # Prepare document
        doc = {
            "_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "caption": caption,
            "quality": quality,
            "updated_at": datetime.utcnow()
        }

        # Try insert (new file)
        try:
            collection.insert_one(doc)
            return "suc"

        except DuplicateKeyError:
            # File exists, update caption and quality
            collection.update_one(
                {"_id": file_id},
                {
                    "$set": {
                        "caption": caption,
                        "quality": quality,
                        "file_size": file_size,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            return "dup"

    except Exception as e:
        logger.error(f"Save file error: {e}")
        return "err"

# =====================================================
# 🔄 UPDATE CAPTION
# =====================================================
async def update_file_caption(file_id: str, new_caption: str) -> bool:
    """Update file caption"""
    if not file_id or not new_caption:
        return False

    try:
        cleaned_caption = clean_text(new_caption)
        
        res = collection.update_one(
            {"_id": file_id},
            {
                "$set": {
                    "caption": cleaned_caption,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        # Clear cache on update
        cache_clear()
        
        return res.modified_count > 0
    
    except Exception as e:
        logger.error(f"Update caption error: {e}")
        return False

# =====================================================
# 🔄 UPDATE QUALITY
# =====================================================
async def update_file_quality(file_id: str, new_name: str) -> bool:
    """Update file quality based on new name"""
    if not file_id or not new_name:
        return False

    try:
        quality = detect_quality(new_name)

        res = collection.update_one(
            {"_id": file_id},
            {
                "$set": {
                    "quality": quality,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        return res.modified_count > 0
    
    except Exception as e:
        logger.error(f"Update quality error: {e}")
        return False

# =====================================================
# 🔐 FILE ID ENCODING UTILITIES
# =====================================================
def encode_file_id(s: bytes) -> str:
    """Encode file ID to base64 string"""
    try:
        r = b""
        n = 0
        
        for i in s + bytes([22]) + bytes([4]):
            if i == 0:
                n += 1
            else:
                if n:
                    r += b"\x00" + bytes([n])
                    n = 0
                r += bytes([i])
        
        return base64.urlsafe_b64encode(r).decode().rstrip("=")
    
    except Exception as e:
        logger.error(f"Encode error: {e}")
        return ""

def unpack_new_file_id(new_file_id: str) -> str:
    """Decode and unpack Telegram file ID"""
    try:
        decoded = FileId.decode(new_file_id)
        
        return encode_file_id(
            pack(
                "<iiqq",
                int(decoded.file_type),
                decoded.dc_id,
                decoded.media_id,
                decoded.access_hash,
            )
        )
    
    except Exception as e:
        logger.error(f"Unpack error: {e}")
        return ""

# =====================================================
# 🧪 HEALTH CHECK
# =====================================================
async def database_health_check() -> Dict[str, Any]:
    """Check database health and stats"""
    try:
        stats = {
            "status": "healthy",
            "total_files": db_count_documents(),
            "cache_size": len(SEARCH_CACHE),
            "connected": True
        }
        
        # Test query
        collection.find_one({})
        
        return stats
    
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "connected": False
        }
