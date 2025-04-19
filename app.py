import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import math

import psycopg2
from psycopg2.extras import execute_values
import requests
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Cache settings
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache')
CACHE_DURATION = timedelta(hours=24)  # Cache data for 24 hours

# Database connection string
DB_CONNECTION_STRING = "postgresql://Quran%20Db_owner:npg_2sdeOXQArcY8@ep-sparkling-mud-a4xoyza4-pooler.us-east-1.aws.neon.tech/Quran%20Db?sslmode=require"

# API endpoints
ALQURAN_API_BASE_URL = "https://api.alquran.cloud/v1"

# Partitioning constants for database tables
SURAHS_PER_TABLE = 50  # Max 50 surahs per table
VERSES_PER_TABLE = 1000  # Adjust this value based on your needs

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)

def get_db_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def get_cached_data(cache_key: str) -> Optional[Dict]:
    """Retrieve data from cache if valid"""
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    
    if os.path.exists(cache_file):
        file_modified_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if datetime.now() - file_modified_time < CACHE_DURATION:
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error reading cache file {cache_file}: {str(e)}")
    
    return None

def save_to_cache(cache_key: str, data: Dict) -> None:
    """Save data to cache"""
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving to cache file {cache_file}: {str(e)}")

def fetch_from_api(endpoint: str, params: Dict = None) -> Dict:
    """Fetch data from AlQuran API with retries"""
    url = f"{ALQURAN_API_BASE_URL}/{endpoint}"
    retries = 3
    retry_delay = 2
    
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request failed (attempt {attempt+1}/{retries}): {str(e)}")
            if attempt < retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Failed to fetch from API after {retries} attempts")
                raise

def get_surahs_table_name(partition: int) -> str:
    """Get the appropriate table name for a surah partition"""
    return f"surahs_p{partition}"

def get_verses_table_name(partition: int) -> str:
    """Get the appropriate table name for a verses partition"""
    return f"verses_p{partition}"

def get_partition_for_surah(surah_number: int) -> int:
    """Calculate which partition a surah belongs to"""
    return math.ceil(surah_number / SURAHS_PER_TABLE)

def get_partition_for_verse(surah_number: int) -> int:
    """Calculate which partition a verse belongs to based on surah number"""
    return get_partition_for_surah(surah_number)

def initialize_database():
    """Create necessary tables if they don't exist"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get total number of surahs from API or use the known value 114
        total_surahs = 114
        num_surah_partitions = math.ceil(total_surahs / SURAHS_PER_TABLE)
        
        # Create Editions table (small table, no partitioning needed)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS editions (
            identifier TEXT PRIMARY KEY,
            language TEXT NOT NULL,
            name TEXT NOT NULL,
            english_name TEXT,
            format TEXT,
            type TEXT
        )
        """)
        
        # Create partition tables for surahs
        for p in range(1, num_surah_partitions + 1):
            table_name = get_surahs_table_name(p)
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                number INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                english_name TEXT NOT NULL,
                english_name_translation TEXT,
                revelation_type TEXT,
                total_verses INTEGER
            )
            """)
            logger.info(f"Created or verified surah partition table: {table_name}")
        
        # Create partition tables for verses
        for p in range(1, num_surah_partitions + 1):
            table_name = get_verses_table_name(p)
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                surah_number INTEGER NOT NULL,
                verse_number INTEGER NOT NULL,
                arabic_text TEXT NOT NULL,
                translation_text TEXT,
                translation_edition TEXT,
                UNIQUE(surah_number, verse_number, translation_edition)
            )
            """)
            logger.info(f"Created or verified verses partition table: {table_name}")
        
        # Create a view to unify all surah partitions for easier querying
        partition_queries = [f"SELECT * FROM {get_surahs_table_name(p)}" for p in range(1, num_surah_partitions + 1)]
        unified_query = " UNION ALL ".join(partition_queries)
        
        cur.execute(f"""
        CREATE OR REPLACE VIEW surahs AS
        {unified_query}
        """)
        logger.info("Created or replaced unified surahs view")
        
        # Create a view to unify all verse partitions
        verse_partition_queries = [f"SELECT * FROM {get_verses_table_name(p)}" for p in range(1, num_surah_partitions + 1)]
        unified_verse_query = " UNION ALL ".join(verse_partition_queries)
        
        cur.execute(f"""
        CREATE OR REPLACE VIEW verses AS
        {unified_verse_query}
        """)
        logger.info("Created or replaced unified verses view")
        
        conn.commit()
        logger.info("Database initialized successfully with partition tables")
        return True
    except Exception as e:
        logger.error(f"Database initialization error: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            if cur:
                cur.close()
            conn.close()

def fetch_all_editions() -> List[Dict]:
    """Fetch all available Quran editions"""
    cache_key = "editions"
    cached_data = get_cached_data(cache_key)
    
    if cached_data:
        return cached_data.get("data", [])
    
    try:
        response = fetch_from_api("edition")
        if response.get("code") == 200 and "data" in response:
            editions = response["data"]
            save_to_cache(cache_key, response)
            return editions
    except Exception as e:
        logger.error(f"Failed to fetch editions: {str(e)}")
    
    return []

def save_editions_to_db(editions: List[Dict]) -> bool:
    """Save editions to database"""
    if not editions:
        logger.warning("No editions data to save")
        return False
        
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Log the shape of the data
        logger.info(f"Attempting to save {len(editions)} editions to database")
        if editions and len(editions) > 0:
            logger.info(f"Sample edition data keys: {list(editions[0].keys())}")
        
        values = []
        for edition in editions:
            try:
                values.append((
                    edition.get("identifier", ""), 
                    edition.get("language", ""),
                    edition.get("name", ""), 
                    edition.get("englishName", ""), 
                    edition.get("format", ""),
                    edition.get("type", "")
                ))
            except Exception as e:
                logger.error(f"Error processing edition: {str(e)}, edition data: {edition}")
                # Continue with other editions
        
        if values:
            try:
                execute_values(
                    cur,
                    """
                    INSERT INTO editions (identifier, language, name, english_name, format, type)
                    VALUES %s
                    ON CONFLICT (identifier) DO UPDATE SET
                        language = EXCLUDED.language,
                        name = EXCLUDED.name,
                        english_name = EXCLUDED.english_name,
                        format = EXCLUDED.format,
                        type = EXCLUDED.type
                    """,
                    values
                )
                conn.commit()
                logger.info(f"Successfully saved {len(values)} editions to database")
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error when executing insert: {str(e)}")
                if hasattr(e, 'pgerror') and e.pgerror:
                    logger.error(f"PostgreSQL error details: {e.pgerror}")
                raise
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error saving editions to database: {str(e)}")
        return False

def fetch_all_surahs() -> List[Dict]:
    """Fetch all surahs metadata"""
    cache_key = "surahs"
    cached_data = get_cached_data(cache_key)
    
    if cached_data:
        # Handle different possible structures in the cached data
        if "data" in cached_data and "surahs" in cached_data["data"]:
            logger.info("Using cached surahs data (format 1)")
            return cached_data["data"]["surahs"]
        elif "data" in cached_data and isinstance(cached_data["data"], list):
            logger.info("Using cached surahs data (format 2)")
            return cached_data["data"]
        elif "surahs" in cached_data:
            logger.info("Using cached surahs data (format 3)")
            return cached_data["surahs"]
    
    try:
        # Try the meta endpoint first
        response = fetch_from_api("meta")
        if response.get("code") == 200 and "data" in response and "surahs" in response["data"]:
            surahs = response["data"]["surahs"]
            save_to_cache(cache_key, response)
            logger.info(f"Successfully fetched {len(surahs)} surahs from meta endpoint")
            return surahs
        
        # If that fails, try the direct surah list endpoint
        response = fetch_from_api("surah")
        if response.get("code") == 200 and "data" in response:
            surahs = response["data"]
            save_to_cache(cache_key, response)
            logger.info(f"Successfully fetched {len(surahs)} surahs from surah endpoint")
            return surahs
            
        logger.error(f"Failed to fetch surahs metadata. API response: {response}")
    except Exception as e:
        logger.error(f"Failed to fetch surahs metadata: {str(e)}")
    
    return []

def save_surahs_to_db(surahs: List[Dict]) -> bool:
    """Save surahs to database with partitioning"""
    if not surahs:
        logger.warning("No surahs data to save")
        return False
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Group surahs by partition
        partition_data = {}
        for surah in surahs:
            try:
                surah_number = surah.get("number", 0)
                partition = get_partition_for_surah(surah_number)
                
                if partition not in partition_data:
                    partition_data[partition] = []
                
                partition_data[partition].append((
                    surah_number,
                    surah.get("name", ""),
                    surah.get("englishName", ""),
                    surah.get("englishNameTranslation", ""),
                    surah.get("revelationType", ""),
                    surah.get("numberOfAyahs", 0)
                ))
            except Exception as e:
                logger.error(f"Error processing surah: {str(e)}, surah data: {surah}")
        
        # Insert into each partition table
        total_surahs_saved = 0
        for partition, values in partition_data.items():
            if values:
                table_name = get_surahs_table_name(partition)
                logger.info(f"Saving {len(values)} surahs to partition table {table_name}")
                
                try:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {table_name} 
                        (number, name, english_name, english_name_translation, revelation_type, total_verses)
                        VALUES %s
                        ON CONFLICT (number) DO UPDATE SET
                            name = EXCLUDED.name,
                            english_name = EXCLUDED.english_name,
                            english_name_translation = EXCLUDED.english_name_translation,
                            revelation_type = EXCLUDED.revelation_type,
                            total_verses = EXCLUDED.total_verses
                        """,
                        values
                    )
                    total_surahs_saved += len(values)
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Database error when inserting into {table_name}: {str(e)}")
                    if hasattr(e, 'pgerror') and e.pgerror:
                        logger.error(f"PostgreSQL error details: {e.pgerror}")
                    raise
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Successfully saved {total_surahs_saved} surahs across partition tables")
        return True
    except Exception as e:
        logger.error(f"Error saving surahs to database: {str(e)}")
        return False

def fetch_surah_content(surah_number: int, edition: str = "quran-uthmani") -> Dict:
    """Fetch the content of a specific surah"""
    cache_key = f"surah_{surah_number}_{edition}"
    cached_data = get_cached_data(cache_key)
    
    if cached_data and "data" in cached_data:
        return cached_data["data"]
    
    try:
        response = fetch_from_api(f"surah/{surah_number}/{edition}")
        if response.get("code") == 200 and "data" in response:
            surah_data = response["data"]
            save_to_cache(cache_key, response)
            return surah_data
    except Exception as e:
        logger.error(f"Failed to fetch surah {surah_number} content: {str(e)}")
    
    return {}

def fetch_surah_translation(surah_number: int, edition: str = "en.asad") -> Dict:
    """Fetch the translation of a specific surah"""
    cache_key = f"translation_{surah_number}_{edition}"
    cached_data = get_cached_data(cache_key)
    
    if cached_data and "data" in cached_data:
        return cached_data["data"]
    
    try:
        response = fetch_from_api(f"surah/{surah_number}/{edition}")
        if response.get("code") == 200 and "data" in response:
            translation_data = response["data"]
            save_to_cache(cache_key, response)
            return translation_data
    except Exception as e:
        logger.error(f"Failed to fetch surah {surah_number} translation: {str(e)}")
    
    return {}

def save_verses_to_db(surah_number: int, verses: List[Dict], edition: str) -> bool:
    """Save verses to database with partitioning"""
    if not verses:
        logger.warning(f"No verses data to save for surah {surah_number}")
        return False
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Calculate which partition this surah belongs to
        partition = get_partition_for_verse(surah_number)
        table_name = get_verses_table_name(partition)
        
        logger.info(f"Saving {len(verses)} verses for surah {surah_number} to partition table {table_name}")
        
        values = []
        for verse in verses:
            try:
                values.append((
                    surah_number,
                    verse.get("numberInSurah", 0),
                    verse.get("text", ""),
                    None,  # translation_text will be updated separately
                    edition
                ))
            except Exception as e:
                logger.error(f"Error processing verse: {str(e)}, verse data: {verse}")
        
        if values:
            try:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {table_name} 
                    (surah_number, verse_number, arabic_text, translation_text, translation_edition)
                    VALUES %s
                    ON CONFLICT (surah_number, verse_number, translation_edition) DO UPDATE SET
                        arabic_text = EXCLUDED.arabic_text
                    """,
                    values
                )
                conn.commit()
                logger.info(f"Successfully saved {len(values)} verses from surah {surah_number} to partition table {table_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Database error when inserting into {table_name}: {str(e)}")
                if hasattr(e, 'pgerror') and e.pgerror:
                    logger.error(f"PostgreSQL error details: {e.pgerror}")
                raise
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error saving verses to database: {str(e)}")
        return False

def save_translations_to_db(surah_number: int, translations: List[Dict], edition: str) -> bool:
    """Save verse translations to database with partitioning"""
    if not translations:
        logger.warning(f"No translation data to save for surah {surah_number}")
        return False
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Calculate which partition this surah belongs to
        partition = get_partition_for_verse(surah_number)
        table_name = get_verses_table_name(partition)
        
        logger.info(f"Updating {len(translations)} translations for surah {surah_number} in partition table {table_name}")
        
        update_count = 0
        error_count = 0
        
        for verse in translations:
            try:
                cur.execute(
                    f"""
                    UPDATE {table_name}
                    SET translation_text = %s
                    WHERE surah_number = %s AND verse_number = %s AND translation_edition = %s
                    """,
                    (verse.get("text", ""), surah_number, verse.get("numberInSurah", 0), edition)
                )
                update_count += cur.rowcount
            except Exception as e:
                error_count += 1
                logger.error(f"Error updating translation for verse {verse.get('numberInSurah', 0)}: {str(e)}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Updated {update_count} translations with {error_count} errors for surah {surah_number}")
        return True
    except Exception as e:
        logger.error(f"Error saving translations to database: {str(e)}")
        return False

def fetch_and_save_all_data():
    """Fetch and save all Quran data to the database"""
    try:
        # Initialize database tables
        if not initialize_database():
            return {"status": "error", "message": "Failed to initialize database"}
        
        # Fetch and save editions
        editions = fetch_all_editions()
        if not save_editions_to_db(editions):
            return {"status": "error", "message": "Failed to save editions to database"}
        
        # Fetch and save surahs metadata
        surahs = fetch_all_surahs()
        if not save_surahs_to_db(surahs):
            return {"status": "error", "message": "Failed to save surahs to database"}
        
        # Define the editions to fetch
        arabic_edition = "quran-uthmani"
        translation_edition = "en.asad"  # English translation by Muhammad Asad
        
        # Fetch and save all surahs content and translations
        for surah in surahs:
            surah_number = surah["number"]
            
            # Fetch and save Arabic content
            arabic_content = fetch_surah_content(surah_number, arabic_edition)
            if arabic_content and "ayahs" in arabic_content:
                save_verses_to_db(surah_number, arabic_content["ayahs"], arabic_edition)
            
            # Fetch and save translation
            translation_content = fetch_surah_translation(surah_number, translation_edition)
            if translation_content and "ayahs" in translation_content:
                save_translations_to_db(surah_number, translation_content["ayahs"], arabic_edition)
            
            # Avoid rate limiting
            time.sleep(1)
        
        return {"status": "success", "message": "All Quran data has been fetched and saved to the database"}
    except Exception as e:
        logger.error(f"Error in fetch_and_save_all_data: {str(e)}")
        return {"status": "error", "message": str(e)}

def get_surah_by_number(surah_number: int) -> Optional[Dict]:
    """Get surah data from the appropriate partition table"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        partition = get_partition_for_surah(surah_number)
        table_name = get_surahs_table_name(partition)
        
        cur.execute(f"SELECT * FROM {table_name} WHERE number = %s", (surah_number,))
        if cur.rowcount == 0:
            return None
        
        columns = [desc[0] for desc in cur.description]
        surah_data = dict(zip(columns, cur.fetchone()))
        
        cur.close()
        conn.close()
        
        return surah_data
    except Exception as e:
        logger.error(f"Error getting surah {surah_number}: {str(e)}")
        return None

def get_verses_for_surah(surah_number: int, edition: str) -> List[Dict]:
    """Get verses for a surah from the appropriate partition table"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        partition = get_partition_for_verse(surah_number)
        table_name = get_verses_table_name(partition)
        
        cur.execute(
            f"""
            SELECT * FROM {table_name}
            WHERE surah_number = %s AND translation_edition = %s
            ORDER BY verse_number
            """,
            (surah_number, edition)
        )
        
        columns = [desc[0] for desc in cur.description]
        verses = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return verses
    except Exception as e:
        logger.error(f"Error getting verses for surah {surah_number}: {str(e)}")
        return []

def get_all_surahs() -> List[Dict]:
    """Get all surahs from all partition tables"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Use the view to get all surahs
        cur.execute("SELECT * FROM surahs ORDER BY number")
        
        columns = [desc[0] for desc in cur.description]
        surahs = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return surahs
    except Exception as e:
        logger.error(f"Error getting all surahs: {str(e)}")
        return []

def get_verse_by_numbers(surah_number: int, verse_number: int, edition: str) -> Optional[Dict]:
    """Get a specific verse from the appropriate partition table"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        partition = get_partition_for_verse(surah_number)
        verses_table = get_verses_table_name(partition)
        partition_surah = get_partition_for_surah(surah_number)
        surahs_table = get_surahs_table_name(partition_surah)
        
        cur.execute(
            f"""
            SELECT v.*, s.name, s.english_name, s.english_name_translation 
            FROM {verses_table} v
            JOIN {surahs_table} s ON v.surah_number = s.number
            WHERE v.surah_number = %s AND v.verse_number = %s AND v.translation_edition = %s
            """,
            (surah_number, verse_number, edition)
        )
        
        if cur.rowcount == 0:
            return None
        
        columns = [desc[0] for desc in cur.description]
        verse_data = dict(zip(columns, cur.fetchone()))
        
        cur.close()
        conn.close()
        
        return verse_data
    except Exception as e:
        logger.error(f"Error getting verse {surah_number}:{verse_number}: {str(e)}")
        return None

# API Routes
@app.route('/')
def home():
    return jsonify({
        "status": "success",
        "message": "AlQuran API Database Sync Service (Partitioned)",
        "endpoints": [
            "/sync/all - Sync all data",
            "/sync/editions - Sync editions",
            "/sync/surahs - Sync surahs metadata",
            "/sync/surah/<number> - Sync specific surah content",
            "/api/surahs - Get all surahs",
            "/api/surah/<number> - Get specific surah with verses",
            "/api/verse/<surah>/<verse> - Get specific verse",
            "/health - Health check endpoint"
        ]
    })

@app.route('/sync/all')
def sync_all():
    result = fetch_and_save_all_data()
    return jsonify(result)

@app.route('/sync/editions')
def sync_editions():
    try:
        # Make sure tables exist first
        initialize_database()
        
        editions = fetch_all_editions()
        if save_editions_to_db(editions):
            return jsonify({"status": "success", "message": f"Synced {len(editions)} editions"})
        else:
            return jsonify({"status": "error", "message": "Failed to save editions to database"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/sync/surahs')
def sync_surahs():
    try:
        # Make sure tables exist first
        initialize_database()
        
        surahs = fetch_all_surahs()
        if save_surahs_to_db(surahs):
            return jsonify({"status": "success", "message": f"Synced {len(surahs)} surahs"})
        else:
            return jsonify({"status": "error", "message": "Failed to save surahs to database"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/sync/surah/<int:surah_number>')
def sync_surah(surah_number):
    try:
        # Make sure tables exist first
        initialize_database()
        
        arabic_edition = request.args.get('arabic', 'quran-uthmani')
        translation_edition = request.args.get('translation', 'en.asad')
        
        # Fetch and save Arabic content
        arabic_content = fetch_surah_content(surah_number, arabic_edition)
        arabic_verses_count = 0
        if arabic_content and "ayahs" in arabic_content:
            save_verses_to_db(surah_number, arabic_content["ayahs"], arabic_edition)
            arabic_verses_count = len(arabic_content.get("ayahs", []))
        
        # Fetch and save translation
        translation_content = fetch_surah_translation(surah_number, translation_edition)
        if translation_content and "ayahs" in translation_content:
            save_translations_to_db(surah_number, translation_content["ayahs"], arabic_edition)
        
        return jsonify({
            "status": "success", 
            "message": f"Synced surah {surah_number} with {arabic_verses_count} verses"
        })
    except Exception as e:
        logger.error(f"Error syncing surah {surah_number}: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/surahs')
def get_surahs_api():
    try:
        surahs = get_all_surahs()
        return jsonify({"status": "success", "data": surahs})
    except Exception as e:
        logger.error(f"Error getting surahs: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/surah/<int:surah_number>')
def get_surah_api(surah_number):
    try:
        edition = request.args.get('edition', 'quran-uthmani')
        
        # Get surah metadata
        surah = get_surah_by_number(surah_number)
        if not surah:
            return jsonify({"status": "error", "message": f"Surah {surah_number} not found"}), 404
        
        # Get verses for the surah
        verses = get_verses_for_surah(surah_number, edition)
        
        # Combine metadata and verses
        result = {
            "status": "success",
            "data": {
                "surah": surah,
                "verses": verses
            }
        }
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting surah {surah_number}: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/verse/<int:surah_number>/<int:verse_number>')
def get_verse_api(surah_number, verse_number):
    try:
        edition = request.args.get('edition', 'quran-uthmani')
        
        verse = get_verse_by_numbers(surah_number, verse_number, edition)
        if not verse:
            return jsonify({"status": "error", "message": f"Verse {surah_number}:{verse_number} not found"}), 404
        
        return jsonify({"status": "success", "data": verse})
    except Exception as e:
        logger.error(f"Error getting verse {surah_number}:{verse_number}: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/search')
def search_verses():
    try:
        query = request.args.get('q', '').strip()
        edition = request.args.get('edition', 'quran-uthmani')
        
        if not query:
            return jsonify({"status": "error", "message": "Search query is required"}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Search across all partition tables using the unified view
        cur.execute(
            """
            SELECT v.*, s.name, s.english_name, s.english_name_translation 
            FROM verses v
            JOIN surahs s ON v.surah_number = s.number
            WHERE v.translation_edition = %s AND 
            (v.arabic_text LIKE %s OR v.translation_text LIKE %s)
            ORDER BY v.surah_number, v.verse_number
            LIMIT 100
            """,
            (edition, f"%{query}%", f"%{query}%")
        )
        
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "data": results})
    except Exception as e:
        logger.error(f"Error searching verses with query '{query}': {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health')
def health_check():
    try:
        # Quick DB connection test
        conn = get_db_connection()
        conn.close()
        
        # Check if we have editions in cache
        editions_cache = get_cached_data("editions")
        editions_cached = editions_cache is not None
        
        # Check if we have surahs in cache
        surahs_cache = get_cached_data("surahs")
        surahs_cached = surahs_cache is not None
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "cache": {
                "editions": editions_cached,
                "surahs": surahs_cached
            },
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/editions')
def get_editions_api():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM editions ORDER BY language, english_name")
        
        columns = [desc[0] for desc in cur.description]
        editions = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "data": editions})
    except Exception as e:
        logger.error(f"Error getting editions: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/random')
def get_random_verse():
    try:
        edition = request.args.get('edition', 'quran-uthmani')
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get random verse using the unified view
        cur.execute(
            """
            SELECT v.*, s.name, s.english_name, s.english_name_translation 
            FROM verses v
            JOIN surahs s ON v.surah_number = s.number
            WHERE v.translation_edition = %s
            ORDER BY RANDOM()
            LIMIT 1
            """,
            (edition,)
        )
        
        if cur.rowcount == 0:
            return jsonify({"status": "error", "message": "No verses found"}), 404
        
        columns = [desc[0] for desc in cur.description]
        verse = dict(zip(columns, cur.fetchone()))
        
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "data": verse})
    except Exception as e:
        logger.error(f"Error getting random verse: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"status": "error", "message": "Endpoint not found"}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({"status": "error", "message": "Internal server error"}), 500

# Main entry point
if __name__ == '__main__':
    try:
        # Initial database setup
        initialize_database()
        
        # Check if we need to do an initial data load
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM editions")
        editions_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        if editions_count == 0:
            logger.info("No editions found in database. Starting initial data sync...")
            fetch_and_save_all_data()
        
        # Start the Flask server
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=False)
    except Exception as e:
        logger.critical(f"Failed to start application: {str(e)}")
