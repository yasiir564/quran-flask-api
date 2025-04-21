from flask import Flask, request, jsonify, current_app
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import re
import logging
from functools import wraps
import os
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configuration
class Config:
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    HOST = os.environ.get('HOST', '0.0.0.0')
    PORT = int(os.environ.get('PORT', 5000))
    TIMEOUT_NAVIGATION = int(os.environ.get('TIMEOUT_NAVIGATION', 60000))
    TIMEOUT_SELECTOR = int(os.environ.get('TIMEOUT_SELECTOR', 10000))
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )

# Create the Flask application
def create_app(config=Config):
    app = Flask(__name__)
    app.config.from_object(config)
    
    # Error handlers
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(500)
    def server_error(error):
        return jsonify({"error": "Internal server error"}), 500
    
    # Rate limiting decorator
    def limit_content_length(max_length):
        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                cl = request.content_length
                if cl and cl > max_length:
                    return jsonify({"error": "Content too large"}), 413
                return f(*args, **kwargs)
            return wrapper
        return decorator
    
    # Routes
    @app.route('/download', methods=['POST'])
    @limit_content_length(1024 * 10)  # 10KB max request size
    def download_video():
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400
        
        data = request.json
        tiktok_url = data.get('url')
        
        # Validate URL
        if not tiktok_url:
            return jsonify({"error": "Missing TikTok URL"}), 400
        
        # Validate TikTok domain
        parsed_url = urlparse(tiktok_url)
        if not parsed_url.netloc or not (
            parsed_url.netloc.endswith('tiktok.com') or 
            parsed_url.netloc.endswith('vm.tiktok.com')
        ):
            return jsonify({"error": "Invalid TikTok URL"}), 400
        
        try:
            download_url = extract_tiktok_video_url(tiktok_url)
            return jsonify({"download_url": download_url})
        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout error: {str(e)}")
            return jsonify({"error": "Timed out while processing the video"}), 408
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to process video"}), 500
    
    return app

def extract_tiktok_video_url(tiktok_url):
    """Extract download URL from TikTok page"""
    logger.info(f"Processing TikTok URL: {tiktok_url}")
    
    with sync_playwright() as p:
        browser = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=current_app.config['USER_AGENT'],
                viewport={"width": 1280, "height": 720}
            )
            
            # Block unnecessary resources to improve performance
            context.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", lambda route: route.abort())
            
            page = context.new_page()
            
            logger.info("Navigating to TikTok page")
            page.goto(
                tiktok_url, 
                timeout=current_app.config['TIMEOUT_NAVIGATION'],
                wait_until="domcontentloaded"
            )
            
            logger.info("Waiting for video element")
            page.wait_for_selector("video", timeout=current_app.config['TIMEOUT_SELECTOR'])
            
            html = page.content()
            
            # Try multiple regex patterns to find the download URL
            patterns = [
                r'"downloadAddr":"([^"]+)"',
                r'"playAddr":"([^"]+)"',
                r'playAddr="([^"]+)"',
                r'<video[^>]+src="([^"]+)"'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html)
                if match:
                    download_url = match.group(1).replace("\\u0026", "&").replace("\\", "")
                    logger.info(f"Found download URL with pattern: {pattern}")
                    return download_url
            
            # If we get here, we didn't find a download URL
            logger.error("No video URL pattern matched")
            raise ValueError("Could not extract video URL")
            
        finally:
            if browser:
                browser.close()

# Run the application
if __name__ == '__main__':
    app = create_app()
    app.run(
        host=app.config['HOST'],
        port=app.config['PORT'],
        debug=app.config['DEBUG']
    )
