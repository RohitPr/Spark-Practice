import os
import yt_dlp
from pytube import Playlist
from moviepy.video.io.ffmpeg_tools import ffmpeg_extract_audio


def download_high_bitrate_audio(url, output_path):
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': f'{output_path}/%(title)s.%(ext)s',
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(url, download=True)
            print("Download successful:", info_dict['title'])
    except yt_dlp.DownloadError as e:
        print("Error during download:", e)


def get_video_urls_from_playlist(playlist_url):
    try:
        ydl_opts = {'quiet': True, 'extract_flat': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            playlist_info = ydl.extract_info(playlist_url, download=False)
            if 'entries' in playlist_info:
                return [entry['url'] for entry in playlist_info['entries']]
    except yt_dlp.DownloadError as e:
        print("Error:", e)
    return []


def convert_webm_to_mp3(input_file, output_file):
    ffmpeg_extract_audio(input_file, output_file)


if __name__ == "__main__":
    playlist_url = "https://www.youtube.com/playlist?list=PL4St63Dqz4Y0KRuznta3dWrqP45qSk1aL"
    download_path = "./Songs/"

    video_urls = get_video_urls_from_playlist(playlist_url)

    if video_urls:
        print("Video URLs in the playlist:")
        for video_url in video_urls:
            download_high_bitrate_audio(video_url, download_path)
    else:
        print("No video URLs found in the playlist.")

    input_folder = download_path  # Replace with the path to your folder containing WebM files
    output_folder = "./conv_songs"  # Replace with the path where you want to save the MP3 files

    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # List all files in the input folder
    webm_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.webm')]

    for webm_file in webm_files:
        input_file_path = os.path.join(input_folder, webm_file)
        output_file_path = os.path.join(output_folder, os.path.splitext(webm_file)[0] + ".mp3")
        
        convert_webm_to_mp3(input_file_path, output_file_path)
        print(f"Conversion complete: {webm_file} -> {os.path.basename(output_file_path)}")


