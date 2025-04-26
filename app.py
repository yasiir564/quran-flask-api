from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
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

# Add CORS middleware to allow requests from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins - adjust this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Free proxy rotation list - add more as needed
PROXY_LIST = [
    # "http://proxy1.example.com:8080",
    # "http://proxy2.example.com:8080",
    # "http://proxy3.example.com:8080",
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
        try:
            response = httpx.head(url, follow_redirects=True, timeout=5.0)
            response.raise_for_status()  # Raise an exception for bad status codes
            return str(response.url)
        except httpx.RequestError as e:
            logger.error(f"Error resolving short URL {url}: {e}")
            return url # Return original URL as fallback

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
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            m3u8_obj = m3u8.loads(response.text)
            streams = []

            # Handle master playlist
            if m3u8_obj.is_variant:
                for playlist in m3u8_obj.playlists:
                    resolution = playlist.stream_info.resolution
                    streams.append({
                        "resolution": f"{resolution[0]}x{resolution[1]}" if resolution else "unknown",
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
    except httpx.RequestError as e:
        logger.error(f"HTTP error extracting m3u8 streams from {url}: {e}")
    except m3u8.parser.ParseException as e:
        logger.error(f"Error parsing m3u8 playlist from {url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error extracting m3u8 streams from {url}: {e}")
    return []

async def download_tiktok_no_watermark(video_url: str) -> Optional[str]:
    """Generate a no-watermark download URL for TikTok videos."""
    try:
        match = re.search(r'video/(\d+)', video_url)
        if not match:
            return None

        video_id = match.group(1)
        no_watermark_url = f"https://api2-16-h2.musical.ly/aweme/v1/play/?video_id={video_id}&vr_type=0&is_play_url=1&source=PackSourceEnum_PUBLISH&media_type=4"
        return no_watermark_url
    except Exception as e:
        logger.error(f"Error generating no-watermark URL for {video_url}: {e}")
        return None

async def fetch_with_retry(client, url, headers, proxy=None, max_retries=3):
    """Fetch URL with retry logic for resilience."""
    for attempt in range(max_retries):
        try:
            response = await client.get(url, headers=headers, proxy=proxy, timeout=10.0)
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(1)
        except httpx.RequestError as e:
            logger.warning(f"Retry {attempt+1}/{max_retries} failed for {url}: {e}")
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"An unexpected error occurred during fetch for {url}: {e}")
            raise
    return None

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

    proxy = get_random_proxy() if use_proxy and PROXY_LIST else None

    # Standardize the URL format
    video_url = standardize_tiktok_url(video_url)

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await fetch_with_retry(client, video_url, headers, proxy)

            if resp is None:
                return {"error": f"Failed to fetch TikTok page after multiple retries."}

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
                item_id = list(item_module.keys())[0]
                video_data = item_module[item_id]["itemInfo"]["itemStruct"]

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

    except httpx.RequestError as e:
        logger.error(f"HTTP error processing TikTok URL {video_url}: {e}")
        return {"error": f"HTTP error processing TikTok: {e}"}
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error processing TikTok URL {video_url}: {e}")
        return {"error": f"JSON decode error: {e}"}
    except Exception as e:
        logger.error(f"Unexpected error processing TikTok URL {video_url}: {str(e)}")
        return {"error": f"Error processing TikTok: {str(e)}"}
    finally:
        logger.info(f"Processing time: {time.time() - start_time:.2f}s for {video_url}")

async def process_photo_post(data, url):
    """Process a TikTok photo post (slides)."""
    try:
        images = []
        if isinstance(data.get("images"), list):
            for img in data["images"]:
                if isinstance(img, dict) and img.get("urlList"):
                    images.append({"url": img["urlList"][0], "width": img.get("width", 0), "height": img.get("height", 0)})
                elif isinstance(img, str):
                    images.append({"url": img})
        elif isinstance(data.get("imageInfos"), list):
            for img_info in data["imageInfos"]:
                if img_info.get("urlList"):
                    images.append({"url": img_info["urlList"][0], "width": img_info.get("width", 0), "height": img_info.get("height", 0)})

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
        logger.error(f"Error processing photo post for {url}: {e}")
        return {"error": f"Error processing photo post: {str(e)}"}

async def process_video_post(data, url):
    """Process a TikTok video post."""
    try:
        video_info = data.get("video", {})

        # Try to get no-watermark URL
        no_watermark_url = await download_tiktok_no_watermark(url)

        # Extract m3u8 streams if available
        m3u8_streams = []
        play_addr = video_info.get("playAddr")
        if play_addr and play_addr.endswith(".m3u8"):
            m3u8_streams = await extract_m3u8_streams(play_addr)
        elif play_addr and "m3u8" in play_addr:
            # Handle URLs that might have m3u8 parameters
            m3u8_url = play_addr.split("?")[0] if "?" in play_addr else play_addr
            if m3u8_url.endswith(".m3u8"):
                m3u8_streams = await extract_m3u8_streams(m3u8_url)

        # Prepare media info
        media_info = [{
            "url": play_addr or "",
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
        download_urls = [play_addr] if play_addr else []
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
        logger.error(f"Error processing video post for {url}: {e}")
        return {"error": f"Error processing video post: {str(e)}"}

async def process_story_post(data, url):
    """Process a TikTok story post with multiple frames."""
    try:
        story_items = []

        # Extract the story items - could be in different formats
        items = []
        if isinstance(data, dict):
            if "story" in data and "items" in data["story"]:
                items = data["story"]["items"]
            else:
                items = next((v.get("items", []) for
