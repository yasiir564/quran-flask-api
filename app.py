import os
import requests
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

# Initialize Flask app
app = Flask(__name__)

# Configure database connection using the environment variable DATABASE_URL
# Fall back to a default URL if environment variable is not set
database_url = os.getenv("DATABASE_URL", "postgresql://your_user:your_pass@your_host:5432/your_db")
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

# Create tables
with app.app_context():
    db.create_all()

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
                        text_translation=a.get('text', ''),  # Changed from 'translation' to 'text'
                        juz=a.get('juz', 0)
                    )
                    db.session.add(ayah)
                else:
                    # Update existing ayah
                    ayah.surah_id = s['number']
                    ayah.number_in_surah = a['numberInSurah']
                    ayah.text_arabic = a['text']
                    ayah.text_translation = a.get('text', '')  # Changed from 'translation' to 'text'
                    ayah.juz = a.get('juz', 0)
                
                db.session.commit()
        
        return jsonify({"message": "Quran data fetched and stored successfully!"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

# Get all surahs
@app.route('/surahs', methods=['GET'])
def get_surahs():
    surahs = Surah.query.all()
    result = [{
        "id": s.id,
        "name": s.name,
        "english_name": s.english_name,
        "number_of_ayahs": s.number_of_ayahs,
        "revelation_type": s.revelation_type
    } for s in surahs]
    
    return jsonify(result)

# Get a specific surah by ID
@app.route('/surah/<int:surah_id>', methods=['GET'])
def get_surah(surah_id):
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

# Get ayahs from a specific surah
@app.route('/surah/<int:surah_id>/ayahs', methods=['GET'])
def get_surah_ayahs(surah_id):
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

# Get a specific ayah by ID
@app.route('/ayah/<int:ayah_id>', methods=['GET'])
def get_ayah(ayah_id):
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

if __name__ == '__main__':
    app.run(debug=True)
