import json
import os
from itertools import repeat
from multiprocessing.pool import ThreadPool
import PIL.Image as Image
import sys
import pickle
import requests
import time
import io
import pandas as pd
import random

INTERVALS = (int(1e3), int(1e4), int(1e5), int(1e6)) # Intervalos de los grupos
NUM_PROCESS = 4	# Numero de procesos
TARGET_OVERALL = 5000 # Cuantos de cada grupo sacar al finalizar el programa.
SAVE_DIR = "./balanced_dataset_medium/" # Donde guardar las descargas
COLUMNS_NAMES = ["ID", "part", "label", "views", "comments", "likes", "dislikes", "topicID"] # Nombres de columnas del CSV.
CURRENT_OVERALL = [0, 0, 0, 0]

BORRARESTODESPUES = 0

# Funcion para ver a que label pertenece el dato.
def identify_group(views):

	if views < INTERVALS[0] or views is None:
		return -1

	elif INTERVALS[0] <= views < INTERVALS[1]:
		return 0

	elif INTERVALS[1] <= views < INTERVALS[2]:
		return 1

	elif INTERVALS[2] <= views < INTERVALS[3]:
		return 2

	else:
		return 3


# Aqui se saca la info que se guarda en el CSV por dato.
def get_image_info(part, vidID, group, imgDict):

	statsDict = imgDict["items"][0]["statistics"]

	views = int(statsDict["viewCount"])

	if "commentCount" in statsDict:
		comments = int(statsDict["commentCount"])
	else:
		comments = None

	if "likeCount" in statsDict:
		likes = int(statsDict["likeCount"])
	else:
		likes = None

	if "dislikeCount" in statsDict:
		dislikes = int(statsDict["dislikeCount"])
	else:
		dislikes = None

	if "topicDetails" in imgDict["items"][0]:
		topicIDs = imgDict["items"][0]["topicDetails"]["relevantTopicIds"]
	else:
		topicIDs = None

	return [vidID, part, group, views, comments, likes, dislikes, topicIDs]


# Se descarga la imagen
def download_image(urlMedium, urlDefault, vidID):

	imgData = requests.get(urlMedium).content
	
	imgCheck = Image.open(io.BytesIO(imgData))
	if imgCheck.size[0] == 120:
		return 0

	with open(SAVE_DIR + "data/" + vidID + ".jpg", "wb") as imgFile:
		imgFile.write(imgData)

	return 1

# Se descargan todas las imagenes por parte y se sacan sus datos.
# Solo se descargan imagenes que tengan views publicas.
def get_image(jsons, IDs, part):

	start = time.time()

	infos = []

	global CURRENT_OVERALL

	global BORRARESTODESPUES
	BORRARESTODESPUES += len(jsons)

	for rd, ID in zip(jsons, IDs):

		if "viewCount" in rd["items"][0]["statistics"]:
			group = identify_group(int(rd["items"][0]["statistics"]["viewCount"]))
		else:
			continue
		if group == -1 or CURRENT_OVERALL[group] >= TARGET_OVERALL:
			continue

		urlMedium = rd["items"][0]["snippet"]["thumbnails"]["medium"]["url"]
		urlDefault = rd["items"][0]["snippet"]["thumbnails"]["default"]["url"]

		if "topicDetails" in rd["items"][0]:
			topicIDs = rd["items"][0]["topicDetails"]["relevantTopicIds"]
			if not ('/m/07yv9' in topicIDs):
				continue
		else:
			continue

		if not download_image(urlMedium, urlDefault, ID):
			continue

		CURRENT_OVERALL[group] += 1

		print("Downloaded image group\t", group, "\tGoal:\t", CURRENT_OVERALL[group], "/", TARGET_OVERALL)

		row = get_image_info(part, ID, group, rd)
		infos.append(row)

	print(part, time.time() - start, "\n", "g1:", CURRENT_OVERALL[0], "\n", "g2:", CURRENT_OVERALL[1], "\n", "g3:", CURRENT_OVERALL[2], "\n", "g4:", CURRENT_OVERALL[3], "\n", "\n")

	return infos

def read_json(path):

	with open(path, "r", encoding="utf-8") as js:
		rd = json.load(js)

	return rd

def multi_read_json(part):

	paths = []
	IDs = []

	partDir = "./metadata/" + part

	for subdir, dirs, files in os.walk(partDir):
		for f in files:
			paths.append(subdir + "/" + f)
			IDs.append(f[:-5])

	res = ThreadPool(NUM_PROCESS).map(read_json, paths)

	return res, IDs

def startScan(parts):

	start = time.time()

	table = []
	for part in parts:
		jsons, IDs = multi_read_json(part)
		res = get_image(jsons, IDs, part)
		table.extend(res)

		if TARGET_OVERALL[0] >= 5000 and TARGET_OVERALL[1] >= 5000 and TARGET_OVERALL[2] >= 5000 and TARGET_OVERALL[3] >= 5000:
			break

	df = pd.DataFrame(table, columns=COLUMNS_NAMES)

	print(BORRARESTODESPUES / (time.time() - start), "json/s")
	df.to_csv(SAVE_DIR + "data_info.csv")


# Creamos los directorios.
try:
	os.mkdir(SAVE_DIR)
except:
	pass

try:
	os.mkdir(SAVE_DIR + "data/")
except:
	pass

# Escanemaos las partes.
subDirs = [f.name for f in os.scandir("./metadata/") if f.is_dir()]

# Descargamos las imagenes
startScan(subDirs)