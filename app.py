import os
import requests
import psycopg2
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS  # Import the Flask-CORS extension

# Initialize Flask app
app = Flask(__name__)

# Enable CORS for all routes
CORS(app)

# Configure database connection using the environment variable DATABASE_URL
# Handle Heroku's postgres:// vs postgresql:// difference and provide fallback
database_url = os.getenv("DATABASE_URL")
if database_url and database_url.startswith("postgres://"):
    # Heroku uses postgres:// but SQLAlchemy requires postgresql://
    database_url = database_url.replace("postgres://", "postgresql://", 1)
elif not database_url:
    database_url = "postgresql://your_user:your_pass@your_host:5432/your_db"

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Disable Flask-SQLAlchemy track modifications

# Initialize SQLAlchemy
db = SQLAlchemy(app)

# Define models
class Surah(db.Model):
    __tablename__ = 'surahs'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    english_name = db.Column(db.String(100), nullable=False)
    number_of_ayahs = db.Column(db.Integer, nullable=False)
    revelation_type = db.Column(db.String(20), nullable=True)
    
    def __repr__(self):
        return f"<Surah {self.name}>"

class Ayah(db.Model):
    __tablename__ = 'ayahs'
    id = db.Column(db.Integer, primary_key=True)
    surah_id = db.Column(db.Integer, db.ForeignKey('surahs.id'), nullable=False)
    number_in_surah = db.Column(db.Integer, nullable=False)
    text_arabic = db.Column(db.Text, nullable=False)
    text_translation = db.Column(db.Text, nullable=True)
    juz = db.Column(db.Integer, nullable=True)
    
    def __repr__(self):
        return f"<Ayah {self.id} from Surah {self.surah_id}>"

# Create tables within app context with proper error handling
try:
    with app.app_context():
        db.create_all()
        print("Database tables created successfully")
except Exception as e:
    print(f"Database connection error: {e}")
    # Uncomment these lines if you want the app to exit when DB connection fails
    # import sys
    # sys.exit(1)

# Home route to check the app is working
@app.route('/')
def home():
    return "Quran API is working!"

# Fetch Quran data from external API and store in database
@app.route('/fetch-quran', methods=['GET'])
def fetch_quran():
    try:
        api_url = "http://api.alquran.cloud/v1/quran/en.asad"  # English + Arabic
        response = requests.get(api_url)
        data = response.json()
        
        if data["status"] != "OK":
            return jsonify({"error": "Failed to fetch Quran"}), 500
        
        for s in data['data']['surahs']:
            # Check if surah already exists
            surah = Surah.query.get(s['number'])
            
            if not surah:
                surah = Surah(
                    id=s['number'],
                    name=s['name'],
                    english_name=s['englishName'],
                    number_of_ayahs=s['numberOfAyahs'],
                    revelation_type=s['revelationType']
                )
                db.session.add(surah)
            else:
                # Update existing surah
                surah.name = s['name']
                surah.english_name = s['englishName']
                surah.number_of_ayahs = s['numberOfAyahs']
                surah.revelation_type = s['revelationType']
            
            db.session.commit()
            
            for a in s['ayahs']:
                # Check if ayah already exists
                ayah = Ayah.query.get(a['number'])
                
                if not ayah:
                    ayah = Ayah(
                        id=a['number'],
                        surah_id=s['number'],
                        number_in_surah=a['numberInSurah'],
                        text_arabic=a['text'],
                        text_translation=a.get('text', ''),
                        juz=a.get('juz', 0)
                    )
                    db.session.add(ayah)
                else:
                    # Update existing ayah
                    ayah.surah_id = s['number']
                    ayah.number_in_surah = a['numberInSurah']
                    ayah.text_arabic = a['text']
                    ayah.text_translation = a.get('text', '')
                    ayah.juz = a.get('juz', 0)
                
                db.session.commit()
        
        return jsonify({"message": "Quran data fetched and stored successfully!"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

# Get all surahs
@app.route('/surahs', methods=['GET'])
def get_surahs():
    try:
        surahs = Surah.query.all()
        result = [{
            "id": s.id,
            "name": s.name,
            "english_name": s.english_name,
            "number_of_ayahs": s.number_of_ayahs,
            "revelation_type": s.revelation_type
        } for s in surahs]
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get a specific surah by ID
@app.route('/surah/<int:surah_id>', methods=['GET'])
def get_surah(surah_id):
    try:
        surah = Surah.query.get(surah_id)
        
        if not surah:
            return jsonify({"error": "Surah not found"}), 404
        
        ayahs = Ayah.query.filter_by(surah_id=surah_id).all()
        
        result = {
            "id": surah.id,
            "name": surah.name,
            "english_name": surah.english_name,
            "number_of_ayahs": surah.number_of_ayahs,
            "revelation_type": surah.revelation_type,
            "ayahs": [{
                "id": a.id,
                "number_in_surah": a.number_in_surah,
                "text_arabic": a.text_arabic,
                "text_translation": a.text_translation,
                "juz": a.juz
            } for a in ayahs]
        }
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get ayahs from a specific surah
@app.route('/surah/<int:surah_id>/ayahs', methods=['GET'])
def get_surah_ayahs(surah_id):
    try:
        surah = Surah.query.get(surah_id)
        
        if not surah:
            return jsonify({"error": "Surah not found"}), 404
        
        ayahs = Ayah.query.filter_by(surah_id=surah_id).all()
        result = [{
            "id": a.id,
            "number_in_surah": a.number_in_surah,
            "text_arabic": a.text_arabic,
            "text_translation": a.text_translation,
            "juz": a.juz
        } for a in ayahs]
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get a specific ayah by ID
@app.route('/ayah/<int:ayah_id>', methods=['GET'])
def get_ayah(ayah_id):
    try:
        ayah = Ayah.query.get(ayah_id)
        
        if not ayah:
            return jsonify({"error": "Ayah not found"}), 404
        
        result = {
            "id": ayah.id,
            "surah_id": ayah.surah_id,
            "number_in_surah": ayah.number_in_surah,
            "text_arabic": ayah.text_arabic,
            "text_translation": ayah.text_translation,
            "juz": ayah.juz
        }
        
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
