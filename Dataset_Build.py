# Imports
import hdf5
import os,hashlib
import pandas as pd

# Root Directory for Data
dir = "MillionSongSubset"


CSV = {}
# hd5 to python dict
def get_song_info(file):
    song= hdf5.get_songs(file)
    song_analysis = hdf5.get_song_analysis(file)
    keys = list(song.coldescrs.keys())
    analysis_keys = list(song_analysis.coldescrs.keys())
    info ={}
    for attr in keys:
        if attr not in ["analyzer_version","genre"]:
            info.update({attr:getattr(hdf5,"get_"+attr)(file)})
    for attr in analysis_keys:
            info.update({attr:getattr(hdf5,"get_"+attr)(file)})
    return info


# Path for all songs
path_list= []
for subdir, dirs, files in os.walk(dir):
    for file in files:
        filepath = subdir + os.sep + file

        if filepath.endswith(".h5"):
            path_list.append(filepath)



count = 0
for path in path_list[1000:3000]:
    file = hdf5.open_h5_file_read(path)
    song = get_song_info(file)
    CSV.update({count:song})
    count+=1
    file.close()


SongData=pd.DataFrame(CSV)
SongData=SongData.transpose()
print(SongData.head())

#Dropping columns before to save computational time
SongData.drop(['danceability', 'artist_latitude', 'artist_longitude', 'energy', 'artist_location', 'artist_id', 'artist_mbid', 'idx_artist_terms', 'idx_similar_artists', 'artist_7digitalid', 'title',
               'track_7digitalid', 'artist_playmeid', 'release', 'release_7digitalid', 'analysis_sample_rate', 'audio_md5', 'track_id', 'artist_name', 'song_id', 'idx_sections_confidence',
               'idx_sections_start', 'idx_bars_confidence', 'idx_bars_start', 'idx_beats_confidence', 'idx_beats_start', 'idx_segments_pitches', 'idx_segments_timbre', 'idx_segments_loudness_max_time', 'idx_tatums_confidence',
               'idx_tatums_start'], inplace=True, axis=1)

SongData.to_pickle('SongSmall.pkl')

