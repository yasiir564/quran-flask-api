import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union

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

def initialize_database():
    """Create necessary tables if they don't exist"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Create Surahs table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS surahs (
            number INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            english_name TEXT NOT NULL,
            english_name_translation TEXT,
            revelation_type TEXT,
            total_verses INTEGER
        )
        """)
        
        # Create Verses table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS verses (
            id SERIAL PRIMARY KEY,
            surah_number INTEGER REFERENCES surahs(number),
            verse_number INTEGER NOT NULL,
            arabic_text TEXT NOT NULL,
            translation_text TEXT,
            translation_edition TEXT,
            UNIQUE(surah_number, verse_number, translation_edition)
        )
        """)
        
        # Create Editions table
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
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Database initialization error: {str(e)}")
        return False

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
        return False
        
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        values = []
        for edition in editions:
            values.append((
                edition["identifier"], 
                edition["language"],
                edition["name"], 
                edition.get("englishName", ""), 
                edition.get("format", ""),
                edition.get("type", "")
            ))
        
        if values:
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
        cur.close()
        conn.close()
        logger.info(f"Saved {len(editions)} editions to database")
        return True
    except Exception as e:
        logger.error(f"Error saving editions to database: {str(e)}")
        return False

def fetch_all_surahs() -> List[Dict]:
    """Fetch all surahs metadata"""
    cache_key = "surahs"
    cached_data = get_cached_data(cache_key)
    
    if cached_data and "data" in cached_data and "surahs" in cached_data["data"]:
        return cached_data["data"]["surahs"]
    
    try:
        response = fetch_from_api("meta")
        if response.get("code") == 200 and "data" in response and "surahs" in response["data"]:
            surahs = response["data"]["surahs"]
            save_to_cache(cache_key, response)
            return surahs
    except Exception as e:
        logger.error(f"Failed to fetch surahs metadata: {str(e)}")
    
    return []

def save_surahs_to_db(surahs: List[Dict]) -> bool:
    """Save surahs to database"""
    if not surahs:
        return False
        
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        values = []
        for surah in surahs:
            values.append((
                surah["number"],
                surah["name"],
                surah["englishName"],
                surah["englishNameTranslation"],
                surah["revelationType"],
                surah["numberOfAyahs"]
            ))
        
        if values:
            execute_values(
                cur,
                """
                INSERT INTO surahs (number, name, english_name, english_name_translation, revelation_type, total_verses)
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
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Saved {len(surahs)} surahs to database")
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
    """Save verses to database"""
    if not verses:
        return False
        
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        values = []
        for verse in verses:
            values.append((
                surah_number,
                verse["numberInSurah"],
                verse["text"],
                None,  # translation_text will be updated separately
                edition
            ))
        
        if values:
            execute_values(
                cur,
                """
                INSERT INTO verses (surah_number, verse_number, arabic_text, translation_text, translation_edition)
                VALUES %s
                ON CONFLICT (surah_number, verse_number, translation_edition) DO UPDATE SET
                    arabic_text = EXCLUDED.arabic_text
                """,
                values
            )
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Saved {len(verses)} verses from surah {surah_number} to database")
        return True
    except Exception as e:
        logger.error(f"Error saving verses to database: {str(e)}")
        return False

def save_translations_to_db(surah_number: int, translations: List[Dict], edition: str) -> bool:
    """Save verse translations to database"""
    if not translations:
        return False
        
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        for verse in translations:
            cur.execute(
                """
                UPDATE verses 
                SET translation_text = %s
                WHERE surah_number = %s AND verse_number = %s AND translation_edition = %s
                """,
                (verse["text"], surah_number, verse["numberInSurah"], edition)
            )
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Saved {len(translations)} translations for surah {surah_number} to database")
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

# API Routes
@app.route('/')
def home():
    return jsonify({
        "status": "success",
        "message": "AlQuran API Database Sync Service",
        "endpoints": [
            "/sync/all - Sync all data",
            "/sync/editions - Sync editions",
            "/sync/surahs - Sync surahs metadata",
            "/sync/surah/<number> - Sync specific surah content",
            "/api/surahs - Get all surahs",
            "/api/surah/<number> - Get specific surah with verses",
            "/api/verse/<surah>/<verse> - Get specific verse"
        ]
    })

@app.route('/sync/all')
def sync_all():
    result = fetch_and_save_all_data()
    return jsonify(result)

@app.route('/sync/editions')
def sync_editions():
    try:
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
        if arabic_content and "ayahs" in arabic_content:
            save_verses_to_db(surah_number, arabic_content["ayahs"], arabic_edition)
        
        # Fetch and save translation
        translation_content = fetch_surah_translation(surah_number, translation_edition)
        if translation_content and "ayahs" in translation_content:
            save_translations_to_db(surah_number, translation_content["ayahs"], arabic_edition)
        
        return jsonify({
            "status": "success", 
            "message": f"Synced surah {surah_number} with {len(arabic_content.get('ayahs', []))} verses"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/surahs')
def get_surahs_api():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM surahs ORDER BY number")
        columns = [desc[0] for desc in cur.description]
        result = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/surah/<int:surah_number>')
def get_surah_api(surah_number):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get surah info
        cur.execute("SELECT * FROM surahs WHERE number = %s", (surah_number,))
        if cur.rowcount == 0:
            return jsonify({"status": "error", "message": f"Surah {surah_number} not found"}), 404
            
        surah_columns = [desc[0] for desc in cur.description]
        surah_data = dict(zip(surah_columns, cur.fetchone()))
        
        # Get verses
        edition = request.args.get('edition', 'quran-uthmani')
        cur.execute(
            """
            SELECT * FROM verses 
            WHERE surah_number = %s AND translation_edition = %s
            ORDER BY verse_number
            """, 
            (surah_number, edition)
        )
        verse_columns = [desc[0] for desc in cur.description]
        verses = [dict(zip(verse_columns, row)) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        surah_data["verses"] = verses
        return jsonify({"status": "success", "data": surah_data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/verse/<int:surah_number>/<int:verse_number>')
def get_verse_api(surah_number, verse_number):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        edition = request.args.get('edition', 'quran-uthmani')
        cur.execute(
            """
            SELECT v.*, s.name, s.english_name, s.english_name_translation 
            FROM verses v
            JOIN surahs s ON v.surah_number = s.number
            WHERE v.surah_number = %s AND v.verse_number = %s AND v.translation_edition = %s
            """, 
            (surah_number, verse_number, edition)
        )
        
        if cur.rowcount == 0:
            return jsonify({"status": "error", "message": f"Verse {surah_number}:{verse_number} not found"}), 404
            
        result_columns = [desc[0] for desc in cur.description]
        verse_data = dict(zip(result_columns, cur.fetchone()))
        
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "data": verse_data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    # Initialize database on startup
    initialize_database()
    
    # Get port from environment variable for Render deployment
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
