import os
import uuid
import subprocess
import librosa
import numpy as np
import soundfile as sf
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from voicefixer import VoiceFixer
import tempfile

app = Flask(__name__)
CORS(app)  # Allow cross-origin requests

# Initialize VoiceFixer
voice_fixer = VoiceFixer()

# Create upload folder if it doesn't exist
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
    
    # Generate unique filename
    filename = str(uuid.uuid4()) + os.path.splitext(file.filename)[1]
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(file_path)
    
    return jsonify({"filename": filename}), 200

@app.route('/transform', methods=['POST'])
def transform_voice():
    data = request.json
    filename = data.get('filename')
    transformation = data.get('transformation', 'fix')  # Default to 'fix'
    
    if not filename:
        return jsonify({"error": "No filename provided"}), 400
    
    input_path = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.exists(input_path):
        return jsonify({"error": "File not found"}), 404
    
    # Generate output filename
    output_filename = f"transformed_{transformation}_{filename}"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    
    try:
        if transformation == 'fix':
            # Use VoiceFixer to enhance voice quality
            voice_fixer.restore(input_path, output_path)
        
        elif transformation == 'deeper':
            # Load audio file
            y, sr = librosa.load(input_path, sr=None)
            
            # Apply pitch shift to make voice deeper
            y_shifted = librosa.effects.pitch_shift(y, sr=sr, n_steps=-3)
            
            # Save the result
            sf.write(output_path, y_shifted, sr)
        
        elif transformation == 'robotic':
            # Create a temporary WAV file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.wav')
            temp_file.close()
            
            # Convert to WAV if needed
            if not input_path.endswith('.wav'):
                subprocess.run([
                    'ffmpeg', '-i', input_path, '-acodec', 'pcm_s16le',
                    '-ar', '44100', temp_file.name
                ])
                input_file = temp_file.name
            else:
                input_file = input_path
            
            # Apply robotic effect using FFmpeg
            subprocess.run([
                'ffmpeg', '-i', input_file, '-af',
                'aecho=0.8:0.88:60:0.4,robots=filter=1:mode=0:quality=0.5',
                '-y', output_path
            ])
            
            # Clean up temporary file
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
        
        elif transformation == 'higher':
            # Load audio file
            y, sr = librosa.load(input_path, sr=None)
            
            # Apply pitch shift to make voice higher
            y_shifted = librosa.effects.pitch_shift(y, sr=sr, n_steps=3)
            
            # Save the result
            sf.write(output_path, y_shifted, sr)
        
        elif transformation == 'echo':
            # Apply echo effect using FFmpeg
            subprocess.run([
                'ffmpeg', '-i', input_path, '-af',
                'aecho=0.8:0.9:1000:0.3', '-y', output_path
            ])
        
        elif transformation == 'whisper':
            # Load audio file
            y, sr = librosa.load(input_path, sr=None)
            
            # Lower volume significantly
            y_whisper = y * 0.3
            
            # Add slight noise
            noise = np.random.normal(0, 0.005, y_whisper.shape)
            y_whisper = y_whisper + noise
            
            # Save the result
            sf.write(output_path, y_whisper, sr)
        
        else:
            return jsonify({"error": f"Unsupported transformation: {transformation}"}), 400
        
        return jsonify({
            "output_filename": output_filename,
            "transformation": transformation
        }), 200
        
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
    transformations = [
        {"id": "fix", "name": "Fix Audio Quality"},
        {"id": "deeper", "name": "Deeper Voice"},
        {"id": "higher", "name": "Higher Voice"},
        {"id": "robotic", "name": "Robotic Voice"},
        {"id": "echo", "name": "Echo Effect"},
        {"id": "whisper", "name": "Whisper Effect"}
    ]
    return jsonify(transformations)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
