from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import argparse
import io
import shutil
from glob import glob
from os.path import join, exists, basename
import os

import srt
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import youtube_dl

from youtube_dl.postprocessor import FFmpegPostProcessor
from youtube_dl.postprocessor.common import AudioConversionError
from youtube_dl.postprocessor.ffmpeg import FFmpegPostProcessorError

from nemo_asr.dataset.youtube.logging_utils import get_logger

logger = get_logger('yt_list')

import youtube_dl.postprocessor

# override FFmpegExtractAudioPP
extract_audio_base_postprocessor = youtube_dl.postprocessor.FFmpegExtractAudioPP

class FFmpegExtractAudioPP(extract_audio_base_postprocessor):
    def __init__(self, downloader=None, preferredcodec=None, preferredquality=None, nopostoverwrites=False):
        super(FFmpegExtractAudioPP, self).__init__(downloader, preferredcodec, preferredquality, nopostoverwrites)

    def run_ffmpeg(self, path, out_path, codec, more_opts):
        if codec is None:
            acodec_opts = []
        else:
            acodec_opts = ['-acodec', codec]
        opts = ['-vn'] + ['-ar', '11025'] + ['-ac', '1'] + acodec_opts + more_opts

        try:
            FFmpegPostProcessor.run_ffmpeg(self, path, out_path, opts)
        except FFmpegPostProcessorError as err:
            raise AudioConversionError(err.msg)

youtube_dl.postprocessor.FFmpegExtractAudioPP = FFmpegExtractAudioPP


def load_url_list(source_dir):
    url_list = []
    for filename in glob(join(source_dir, '*')):
        with open(filename, 'r') as f:
            lines = f.readlines()
            url_list.extend(lines)
    return url_list


def fetch_url(url, tmp_dir, lang):
    with youtube_dl.YoutubeDL(
            {'format': 'bestaudio', 'audioformat': 'wav', 'extractaudio': True, 'subtitleslangs': [lang],
             'subtitlesformat': 'vtt',
             'writeautomaticsub': False, 'writesubtitles': True,
             'cachedir': tmp_dir,
             'postprocessors': [{
                 'key': 'FFmpegExtractAudio',
                 'preferredcodec': 'wav',
                 'preferredquality': '1',
             },
                 {
                     'key': 'FFmpegSubtitlesConvertor',
                     'format': 'srt'
                 }],
             'outtmpl': os.path.join(tmp_dir, '%(id)s.%(ext)s')}) as dwnlder:

        info_dict = dwnlder.extract_info(url, download=False)
        video_id = str(info_dict.get("id", None))
        ext_audio = 'wav'

        path_audio = join(tmp_dir, video_id + '.' + ext_audio)
        if not exists(path_audio):
            download_dict = dwnlder.extract_info(url, download=True)


    return path_audio, join(tmp_dir, video_id + '.' + lang + '.srt')


def process_video(url, target_dir, lang):
    try:
        if not exists(join(target_dir)):
            os.makedirs(join(target_dir))

        audio_tmp_path, subs_tmp_path = fetch_url(url, os.path.join(target_dir, 'tmp'), lang)
        assert os.path.exists(audio_tmp_path), 'no ' + audio_tmp_path
        assert os.path.exists(subs_tmp_path), 'no ' + subs_tmp_path
        shutil.copy2(audio_tmp_path, target_dir)

        return os.path.join(target_dir, os.path.basename(audio_tmp_path)), subs_tmp_path
    except Exception as e:
        logger.exception('During process %s' % url)
        raise e


def append_to_manifest(manifest, audio_path, subs_srt_path):
    with io.open(subs_srt_path, 'r', encoding="utf-8", errors='replace') as srt_f:
        parsed = srt.parse(srt_f)
        start = None
        end = None
        text = ''
        for sub in parsed:
            if start is None:
                start = sub.start.total_seconds()

            if start + 16.7 < sub.end.total_seconds() and len(text) > 0:
                manifest.write('{"audio_filepath": "%s", "offset": %s, "duration": %s, "text": "%s"}\n' %
                               (os.path.basename(audio_path), start, end - start,
                                text))
                start = sub.start.total_seconds()
                text = ''

            text += ' ' + sub.content.replace('\r', ' ').replace('\n', ' ').replace('"', '').replace("'", '')
            end = sub.end.total_seconds()

def crawl(list_dir, target_dir, threads, lang):
    urls = set(load_url_list(list_dir))
    if not os.path.exists(os.path.join(target_dir, 'tmp')):
        os.makedirs(os.path.join(target_dir, 'tmp'))

    with io.open(os.path.join(target_dir, 'manifest.json'), 'w', encoding="utf-8") as manifest:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(process_video, url, target_dir, lang): url for url in urls}
            for future in tqdm(as_completed(futures), total=len(futures)):
                url = futures[future]
                try:
                    audio_path, subs_srt_path = future.result()
                except Exception as e:
                    logger.exception('Exception occured while cooking %s' % url)

                append_to_manifest(manifest, audio_path, subs_srt_path)


def parse_args():
    parser = argparse.ArgumentParser(description='Download youtube audio with srt')
    parser.add_argument('--listdir', type=str, required=True)
    parser.add_argument('--targetdir', type=str, required=True)
    parser.add_argument('--threads', type=int, required=True)
    parser.add_argument('--lang', type=str, required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    crawl(args.listdir, args.targetdir, args.threads, args.lang)
