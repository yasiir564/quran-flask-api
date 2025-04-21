import os
import uuid
import subprocess
import librosa
import numpy as np
import soundfile as sf
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS

try:
    from voicefixer import VoiceFixer
except ImportError:
    VoiceFixer = None  # fallback if not installed

import tempfile

app = Flask(__name__)
CORS(app)

# Initialize VoiceFixer if available
voice_fixer = VoiceFixer() if VoiceFixer else None

# Create upload/output folders
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    filename = str(uuid.uuid4()) + os.path.splitext(file.filename)[1]
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(file_path)
    
    return jsonify({"filename": filename}), 200

@app.route('/transform', methods=['POST'])
def transform_voice():
    data = request.json
    filename = data.get('filename')
    transformation = data.get('transformation', 'fix')

    if not filename:
        return jsonify({"error": "No filename provided"}), 400

    input_path = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.exists(input_path):
        return jsonify({"error": "File not found"}), 404

    output_filename = f"transformed_{transformation}_{filename}"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)

    try:
        if transformation == 'fix':
            if voice_fixer:
                voice_fixer.restore(input_path, output_path)
            else:
                return jsonify({"error": "VoiceFixer is not installed"}), 500

        elif transformation == 'deeper':
            y, sr = librosa.load(input_path, sr=None)
            y_shifted = librosa.effects.pitch_shift(y, sr=sr, n_steps=-3)
            sf.write(output_path, y_shifted, sr)

        elif transformation == 'higher':
            y, sr = librosa.load(input_path, sr=None)
            y_shifted = librosa.effects.pitch_shift(y, sr=sr, n_steps=3)
            sf.write(output_path, y_shifted, sr)

        elif transformation == 'robotic':
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.wav')
            temp_file.close()

            input_file = input_path
            if not input_path.endswith('.wav'):
                subprocess.run([
                    'ffmpeg', '-i', input_path, '-acodec', 'pcm_s16le',
                    '-ar', '44100', temp_file.name
                ], check=True)
                input_file = temp_file.name

            subprocess.run([
                'ffmpeg', '-i', input_file, '-af',
                'aecho=0.8:0.88:60:0.4,asetrate=44100*1.3,aresample=44100,atempo=1.1',
                '-y', output_path
            ], check=True)

            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)

        elif transformation == 'echo':
            subprocess.run([
                'ffmpeg', '-i', input_path, '-af',
                'aecho=0.8:0.9:1000:0.3', '-y', output_path
            ], check=True)

        elif transformation == 'whisper':
            y, sr = librosa.load(input_path, sr=None)
            y_whisper = y * 0.3
            noise = np.random.normal(0, 0.005, y_whisper.shape)
            y_whisper += noise
            sf.write(output_path, y_whisper, sr)

        else:
            return jsonify({"error": f"Unsupported transformation: {transformation}"}), 400

        return jsonify({
            "output_filename": output_filename,
            "transformation": transformation
        }), 200

    except subprocess.CalledProcessError as e:
        return jsonify({"error": f"FFmpeg error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    output_path = os.path.join(OUTPUT_FOLDER, filename)
    if not os.path.exists(output_path):
        return jsonify({"error": "File not found"}), 404

    return send_file(output_path, as_attachment=True)

@app.route('/transformations', methods=['GET'])
def get_transformations():
    return jsonify([
        {"id": "fix", "name": "Fix Audio Quality"},
        {"id": "deeper", "name": "Deeper Voice"},
        {"id": "higher", "name": "Higher Voice"},
        {"id": "robotic", "name": "Robotic Voice"},
        {"id": "echo", "name": "Echo Effect"},
        {"id": "whisper", "name": "Whisper Effect"}
    ])

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
