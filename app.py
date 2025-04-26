from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, HttpUrl
import httpx
from bs4 import BeautifulSoup
import json
import re
import random
import asyncio
import logging
import m3u8
from urllib.parse import urlparse, parse_qs
import aiohttp
from io import BytesIO
import time
from typing import List, Dict, Any, Optional, Union
import base64

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Enhanced TikTok Downloader API")

# Free proxy rotation list - add more as needed
PROXY_LIST = [
    "http://proxy1.example.com:8080",
    "http://proxy2.example.com:8080",
    "http://proxy3.example.com:8080",
]

# User agent rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
]

class TikTokRequest(BaseModel):
    url: HttpUrl
    use_proxy: bool = False
    custom_headers: Dict[str, str] = None

class TikTokResponse(BaseModel):
    type: str
    media: List[Dict[str, Any]]
    author: Dict[str, Any]
    desc: str
    music: Optional[Dict[str, Any]] = None
    statistics: Optional[Dict[str, Any]] = None
    duration: Optional[float] = None
    download_urls: List[str] = []

def get_random_proxy():
    return random.choice(PROXY_LIST) if PROXY_LIST else None

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def standardize_tiktok_url(url: str) -> str:
    """Convert any TikTok URL format to the standard web URL format."""
    parsed_url = urlparse(url)
    
    # Handle TikTok short URLs (vm.tiktok.com, vt.tiktok.com)
    if parsed_url.netloc in ['vm.tiktok.com', 'vt.tiktok.com']:
        response = httpx.head(url, follow_redirects=True)
        return response.url
    
    # Handle mobile URLs
    if 'm.tiktok.com' in parsed_url.netloc:
        path_parts = parsed_url.path.strip('/').split('/')
        if len(path_parts) >= 2:
            username = path_parts[0]
            video_id = path_parts[1]
            return f"https://www.tiktok.com/@{username}/video/{video_id}"
    
    return url

async def extract_m3u8_streams(url: str) -> List[Dict[str, Any]]:
    """Extract all available streams from m3u8 playlist."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code != 200:
                return []
            
            m3u8_obj = m3u8.loads(response.text)
            streams = []
            
            # Handle master playlist
            if m3u8_obj.is_variant:
                for playlist in m3u8_obj.playlists:
                    streams.append({
                        "resolution": f"{playlist.stream_info.resolution[0]}x{playlist.stream_info.resolution[1]}",
                        "bandwidth": playlist.stream_info.bandwidth,
                        "url": playlist.uri
                    })
            else:
                # Handle media playlist
                streams.append({
                    "resolution": "default",
                    "segments": len(m3u8_obj.segments),
                    "url": url
                })
                
            return streams
    except Exception as e:
        logger.error(f"Error extracting m3u8 streams: {e}")
        return []

async def download_tiktok_no_watermark(video_url: str) -> str:
    """Generate a no-watermark download URL for TikTok videos."""
    # This is a simplified approach - in a production environment,
    # you'd want a more robust solution that adapts to TikTok's changes
    try:
        # Extract the video ID
        match = re.search(r'video/(\d+)', video_url)
        if not match:
            return None

        video_id = match.group(1)
        
        # Try to generate a no-watermark URL using a transformation technique
        # This approach might need frequent updates as TikTok changes their systems
        no_watermark_url = f"https://api2-16-h2.musical.ly/aweme/v1/play/?video_id={video_id}&vr_type=0&is_play_url=1&source=PackSourceEnum_PUBLISH&media_type=4"
        
        return no_watermark_url
    except Exception as e:
        logger.error(f"Error generating no-watermark URL: {e}")
        return None

async def fetch_with_retry(client, url, headers, proxy=None, max_retries=3):
    """Fetch URL with retry logic for resilience."""
    for attempt in range(max_retries):
        try:
            return await client.get(url, headers=headers, proxy=proxy, timeout=10.0)
        except httpx.TimeoutException:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(1)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Retry {attempt+1}/{max_retries} for {url}: {e}")
            await asyncio.sleep(1)

async def extract_data(video_url: str, use_proxy: bool = False, custom_headers: Dict[str, str] = None) -> Dict:
    """Extract TikTok data asynchronously with improved handling for all content types."""
    start_time = time.time()
    
    headers = {
        "User-Agent": get_random_user_agent(),
        "Referer": "https://www.tiktok.com/",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    if custom_headers:
        headers.update(custom_headers)
    
    proxy = get_random_proxy() if use_proxy else None
    
    # Standardize the URL format
    video_url = standardize_tiktok_url(video_url)
    
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await fetch_with_retry(client, video_url, headers, proxy)
            
            if resp.status_code != 200:
                return {"error": f"Failed to fetch TikTok page: HTTP {resp.status_code}"}
                
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            
            # Try to find the JSON data in different possible script tags
            script = soup.find("script", id="SIGI_STATE")
            if not script:
                # Try alternative data sources
                script = soup.find("script", text=re.compile(r"window\['SIGI_STATE'\]"))
                if not script:
                    # Check for mobile page format
                    script = soup.find("script", text=re.compile(r"__INIT_PROPS__"))
                    if script:
                        match = re.search(r'__INIT_PROPS__\s*=\s*(\{.+?\})\s*;', script.string)
                        if match:
                            data = json.loads(match.group(1))
                            # Process mobile data format
                            return await process_mobile_data(data, video_url)
                    return {"error": "No TikTok data found"}
                else:
                    # Extract data from window['SIGI_STATE']
                    match = re.search(r"window\['SIGI_STATE'\]\s*=\s*(\{.+?\})\s*;", script.string)
                    if not match:
                        return {"error": "Failed to extract TikTok data"}
                    data = json.loads(match.group(1))
            else:
                # Standard data extraction
                data = json.loads(script.string)
            
            # First check ItemModule for standard videos/photos
            item_module = data.get("ItemModule")
            if item_module:
                video_data = list(item_module.values())[0]
                
                # Format the response based on content type
                if video_data.get("images"):
                    # Photo Post (slides)
                    return await process_photo_post(video_data, video_url)
                else:
                    # Video Post
                    return await process_video_post(video_data, video_url)
            
            # Check for story format
            story_data = data.get("SharingStoryModule") or data.get("StoryModule")
            if story_data:
                return await process_story_post(story_data, video_url)
                
            # Fallback to other potential data sources
            await_store = data.get("AwaitStore")
            if await_store and await_store.get("detail"):
                item_info = await_store["detail"].get("itemInfo")
                if item_info and item_info.get("itemStruct"):
                    video_data = item_info["itemStruct"]
                    if video_data.get("images"):
                        return await process_photo_post(video_data, video_url)
                    else:
                        return await process_video_post(video_data, video_url)
                        
            return {"error": "Unsupported TikTok content format"}
            
    except Exception as e:
        logger.error(f"Error processing TikTok URL: {str(e)}")
        return {"error": f"Error processing TikTok: {str(e)}"}
    finally:
        logger.info(f"Processing time: {time.time() - start_time:.2f}s for {video_url}")

async def process_photo_post(data, url):
    """Process a TikTok photo post (slides)."""
    try:
        images = []
        if isinstance(data["images"], list):
            for img in data["images"]:
                if isinstance(img, dict) and img.get("url"):
                    images.append({
                        "url": img["url"],
                        "width": img.get("width", 0),
                        "height": img.get("height", 0)
                    })
                elif isinstance(img, str):
                    images.append({"url": img})
        
        # Handle case where images is a string or different format
        if not images and isinstance(data["images"], str):
            images = [{"url": data["images"]}]
            
        author_info = extract_author_info(data)
        
        return {
            "type": "photo",
            "media": images,
            "author": author_info,
            "desc": data.get("desc", ""),
            "statistics": extract_statistics(data),
            "download_urls": [img["url"] for img in images]
        }
    except Exception as e:
        logger.error(f"Error processing photo post: {e}")
        return {"error": f"Error processing photo post: {str(e)}"}

async def process_video_post(data, url):
    """Process a TikTok video post."""
    try:
        video_info = data["video"]
        
        # Try to get no-watermark URL
        no_watermark_url = await download_tiktok_no_watermark(url)
        
        # Extract m3u8 streams if available
        m3u8_streams = []
        if "playAddr" in video_info:
            play_url = video_info["playAddr"]
            if play_url.endswith(".m3u8"):
                m3u8_streams = await extract_m3u8_streams(play_url)
        
        # Prepare media info
        media_info = [{
            "url": video_info.get("playAddr", ""),
            "width": video_info.get("width", 0),
            "height": video_info.get("height", 0),
            "format": "mp4",
            "cover": video_info.get("cover", ""),
            "duration": video_info.get("duration", 0),
        }]
        
        # Add m3u8 streams if found
        if m3u8_streams:
            for i, stream in enumerate(m3u8_streams):
                media_info.append({
                    "url": stream["url"],
                    "resolution": stream.get("resolution", "unknown"),
                    "format": "m3u8",
                    "stream_index": i
                })
        
        author_info = extract_author_info(data)
        
        # Prepare download URLs
        download_urls = [video_info.get("playAddr", "")]
        if no_watermark_url:
            download_urls.append(no_watermark_url)
        
        # Get music info if available
        music_info = None
        if data.get("music"):
            music_info = {
                "title": data["music"].get("title", ""),
                "author": data["music"].get("authorName", ""),
                "url": data["music"].get("playUrl", "")
            }
        
        return {
            "type": "video",
            "media": media_info,
            "author": author_info,
            "desc": data.get("desc", ""),
            "music": music_info,
            "statistics": extract_statistics(data),
            "duration": video_info.get("duration", 0),
            "download_urls": download_urls,
            "no_watermark_url": no_watermark_url,
            "m3u8_streams": m3u8_streams
        }
    except Exception as e:
        logger.error(f"Error processing video post: {e}")
        return {"error": f"Error processing video post: {str(e)}"}

async def process_story_post(data, url):
    """Process a TikTok story post with multiple frames."""
    try:
        # Stories have different data structure
        story_items = []
        
        # Extract the story items - could be in different formats
        if isinstance(data, dict):
            if "story" in data:
                items = data["story"].get("items", [])
            else:
                # Try to find the items directly
                items = next((v for k, v in data.items() if isinstance(v, dict) and "items" in v), {}).get("items", [])
        else:
            items = []
            
        for item in items:
            if item.get("type") == "video":
                video_info = item.get("video", {})
                story_items.append({
                    "type": "video",
                    "url": video_info.get("playAddr", ""),
                    "cover": video_info.get("cover", ""),
                    "duration": video_info.get("duration", 0),
                })
            elif item.get("type") == "image" or item.get("images"):
                images = item.get("images", [])
                if not images and item.get("image"):
                    images = [item["image"]]
                
                for img in images:
                    if isinstance(img, dict):
                        story_items.append({
                            "type": "image",
                            "url": img.get("url", ""),
                            "width": img.get("width", 0),
                            "height": img.get("height", 0)
                        })
                    elif isinstance(img, str):
                        story_items.append({
                            "type": "image",
                            "url": img
                        })
        
        # Extract author info from whatever is available
        author_info = {}
        if data.get("user"):
            author_info = {
                "id": data["user"].get("id", ""),
                "uniqueId": data["user"].get("uniqueId", ""),
                "nickname": data["user"].get("nickname", ""),
                "avatarThumb": data["user"].get("avatarThumb", "")
            }
        
        return {
            "type": "story",
            "media": story_items,
            "author": author_info,
            "desc": data.get("desc", ""),
            "download_urls": [item["url"] for item in story_items]
        }
    except Exception as e:
        logger.error(f"Error processing story post: {e}")
        return {"error": f"Error processing story post: {str(e)}"}

async def process_mobile_data(data, url):
    """Process TikTok data from mobile page format."""
    try:
        # Mobile data format varies but often contains item list
        if "itemList" in data:
            item = data["itemList"][0]
            if "images" in item:
                # It's a photo post
                images = [{"url": img} for img in item["images"]]
                return {
                    "type": "photo",
                    "media": images,
                    "author": {
                        "id": item.get("authorId", ""),
                        "uniqueId": item.get("authorUniqueId", ""),
                        "nickname": item.get("authorName", "")
                    },
                    "desc": item.get("desc", ""),
                    "download_urls": [img["url"] for img in images]
                }
            elif "video" in item:
                # It's a video
                video_info = item["video"]
                return {
                    "type": "video",
                    "media": [{
                        "url": video_info.get("playAddr", ""),
                        "cover": video_info.get("cover", ""),
                        "duration": video_info.get("duration", 0)
                    }],
                    "author": {
                        "id": item.get("authorId", ""),
                        "uniqueId": item.get("authorUniqueId", ""),
                        "nickname": item.get("authorName", "")
                    },
                    "desc": item.get("desc", ""),
                    "download_urls": [video_info.get("playAddr", "")]
                }
        
        return {"error": "Could not parse mobile TikTok data"}
    except Exception as e:
        logger.error(f"Error processing mobile data: {e}")
        return {"error": f"Error processing mobile data: {str(e)}"}

def extract_author_info(data):
    """Extract author information from TikTok data."""
    author_info = data.get("author", {})
    if isinstance(author_info, str):
        return {"uniqueId": author_info}
    
    return {
        "id": author_info.get("id", ""),
        "uniqueId": author_info.get("uniqueId", ""),
        "nickname": author_info.get("nickname", ""),
        "avatarThumb": author_info.get("avatarThumb", "")
    }

def extract_statistics(data):
    """Extract engagement statistics if available."""
    stats = data.get("stats", {})
    if not stats:
        return None
        
    return {
        "diggCount": stats.get("diggCount", 0),
        "shareCount": stats.get("shareCount", 0),
        "commentCount": stats.get("commentCount", 0),
        "playCount": stats.get("playCount", 0)
    }

@app.post("/api/tiktok")
async def download_tiktok(req: TikTokRequest):
    """Download TikTok content API endpoint."""
    try:
        result = await extract_data(req.url, req.use_proxy, req.custom_headers)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
            
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"API error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tiktok/download")
async def stream_download(request: Request, url: str, format: str = "mp4"):
    """Stream download TikTok media directly."""
    try:
        # Extract the data first
        result = await extract_data(url)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
            
        # Get the appropriate download URL
        download_url = None
        content_type = "application/octet-stream"
        filename = f"tiktok_media_{int(time.time())}"
        
        if result["type"] == "video":
            # Prefer no watermark URL if available
            download_url = result.get("no_watermark_url", "")
            if not download_url and result["download_urls"]:
                download_url = result["download_urls"][0]
            content_type = "video/mp4"
            filename += ".mp4"
        elif result["type"] == "photo" and result["media"]:
            # For photos, download the first image
            download_url = result["media"][0]["url"]
            content_type = "image/jpeg"
            filename += ".jpg"
        elif result["type"] == "story" and result["media"]:
            # For stories, download the first item
            download_url = result["media"][0]["url"]
            if result["media"][0]["type"] == "video":
                content_type = "video/mp4"
                filename += ".mp4"
            else:
                content_type = "image/jpeg"
                filename += ".jpg"
                
        if not download_url:
            raise HTTPException(status_code=400, detail="No download URL found")
            
        # Stream the content
        async with httpx.AsyncClient(follow_redirects=True) as client:
            headers = {
                "User-Agent": get_random_user_agent(),
                "Referer": "https://www.tiktok.com/"
            }
            
            async def stream_generator():
                async with client.stream("GET", download_url, headers=headers) as response:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        yield chunk
                        
            response = StreamingResponse(
                stream_generator(),
                media_type=content_type,
                headers={"Content-Disposition": f'attachment; filename="{filename}"'}
            )
            
            return response
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tiktok/batch")
async def batch_download(urls: str):
    """Process multiple TikTok URLs in parallel."""
    try:
        url_list = urls.split(",")
        if len(url_list) > 10:
            raise HTTPException(status_code=400, detail="Maximum 10 URLs allowed per batch request")
            
        # Process URLs in parallel
        tasks = [extract_data(url) for url in url_list]
        results = await asyncio.gather(*tasks)
        
        return {"results": results}
    except Exception as e:
        logger.error(f"Batch download error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    """API root endpoint."""
    return {
        "name": "Enhanced TikTok Downloader API",
        "version": "2.0.0",
        "endpoints": [
            "/api/tiktok - POST - Download TikTok content",
            "/api/tiktok/download - GET - Stream download TikTok media",
            "/api/tiktok/batch - GET - Process multiple TikTok URLs"
        ]
    }

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
