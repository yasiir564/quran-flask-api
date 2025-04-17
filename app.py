from flask import Flask, jsonify
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
import requests

# ===================== CONFIG =====================
app = Flask(__name__)
DATABASE_URL = "postgresql://Quran%20Db_owner:npg_2sdeOXQArcY8@ep-sparkling-mud-a4xoyza4-pooler.us-east-1.aws.neon.tech/Quran%20Db?sslmode=require"  # replace with your Neon DB

# ===================== DATABASE SETUP =====================
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

class Surah(Base):
    __tablename__ = 'surahs'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    english_name = Column(String)
    number_of_ayahs = Column(Integer)
    revelation_type = Column(String)
    ayahs = relationship("Ayah", back_populates="surah")

class Ayah(Base):
    __tablename__ = 'ayahs'
    id = Column(Integer, primary_key=True)
    surah_id = Column(Integer, ForeignKey('surahs.id'))
    number_in_surah = Column(Integer)
    text_arabic = Column(Text)
    text_translation = Column(Text)
    juz = Column(Integer)
    surah = relationship("Surah", back_populates="ayahs")

Base.metadata.create_all(engine)

# ===================== FETCH QURAN API =====================
@app.route('/fetch-quran', methods=['GET'])
def fetch_quran():
    api_url = "http://api.alquran.cloud/v1/quran/en.asad"  # English + Arabic
    response = requests.get(api_url)
    data = response.json()

    if data["status"] != "OK":
        return jsonify({"error": "Failed to fetch Quran"}), 500

    for s in data['data']['surahs']:
        surah = Surah(
            id=s['number'],
            name=s['name'],
            english_name=s['englishName'],
            number_of_ayahs=s['numberOfAyahs'],
            revelation_type=s['revelationType']
        )
        session.merge(surah)  # prevents duplicate insert

        for a in s['ayahs']:
            ayah = Ayah(
                id=a['number'],
                surah_id=s['number'],
                number_in_surah=a['numberInSurah'],
                text_arabic=a['text'],
                text_translation=a.get('edition', {}).get('name', ''),
                juz=a.get('juz', 0)
            )
            session.merge(ayah)

    session.commit()
    return jsonify({"message": "Quran data fetched and stored successfully!"})

# ===================== API ROUTES =====================
@app.route('/surahs', methods=['GET'])
def get_surahs():
    surahs = session.query(Surah).all()
    result = [{
        "id": s.id,
        "name": s.name,
        "english_name": s.english_name,
        "number_of_ayahs": s.number_of_ayahs,
        "revelation_type": s.revelation_type
    } for s in surahs]
    return jsonify(result)

@app.route('/surah/<int:surah_id>', methods=['GET'])
def get_surah(surah_id):
    surah = session.query(Surah).get(surah_id)
    if not surah:
        return jsonify({"error": "Surah not found"}), 404

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
        } for a in surah.ayahs]
    }
    return jsonify(result)

# ===================== MAIN =====================
if __name__ == '__main__':
    app.run(debug=True)
