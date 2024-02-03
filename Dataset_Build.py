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

SongData.to_pickle('SongPKL_test.pkl')
SongData.to_csv('SongPKL_test.csv')
print(SongData['idx_similar_artists'][0])
