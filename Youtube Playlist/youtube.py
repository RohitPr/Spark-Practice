from pytube import YouTube
from pytube import Playlist
import os
import moviepy.editor as mp
import re

playlist = Playlist(
    "https://www.youtube.com/playlist?list=PL4St63Dqz4Y0KRuznta3dWrqP45qSk1aL")

for url in playlist:
    print(YouTube(url).title)
    YouTube(url).streams.filter(only_audio=True).first().download()

folder = "./"

for file in os.listdir(folder):
    print(file)
    if re.search('mp4', file):
        mp4_path = os.path.join(folder, file)
        mp3_path = os.path.join(folder, os.path.splitext(file)[0]+'.mp3')
        new_file = mp.AudioFileClip(mp4_path)
        new_file.write_audiofile(mp3_path)
        os.remove(mp4_path)
