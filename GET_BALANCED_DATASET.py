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

INTERVALS = (int(1e3), int(1e4), int(1e5), int(1e6)) # Intervalos de los grupos
NUM_PROCESS = 1	# Numero de procesos
TARGET_PER_PART = 50 # Cuantos de cada grupo sacar por parte, 50 es recomendado.
TARGET_OVERALL = 5000 # Cuantos de cada grupo sacar al finalizar el programa.
SAVE_DIR = "./balanced_dataset_medium/" # Donde guardar las descargas
COLUMNS_NAMES = ["ID", "part", "label", "views", "comments", "likes", "dislikes", "topicID"] # Nombres de columnas del CSV.

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
def download_image(part, urlMedium, urlDefault, vidID):

	imgData = requests.get(urlMedium).content
	
	imgCheck = Image.open(io.BytesIO(imgData))
	if imgCheck.size[0] == 120:
		return 0

	with open(SAVE_DIR + "data/" + vidID + ".jpg", "wb") as imgFile:
		imgFile.write(imgData)

	return 1

# Se descargan todas las imagenes por parte y se sacan sus datos.
# Solo se descargan imagenes que tengan views publicas.
def get_image(part):

	start = time.time()

	infos = []

	gg = [0, 0, 0, 0]

	partDir = "./metadata/" + part

	for subdir, dirs, files in os.walk(partDir):

		for f in files:

			with open(subdir + "/" + f, "r", encoding="utf-8") as js:
				rd = json.load(js)

			if "viewCount" in rd["items"][0]["statistics"]:
				group = identify_group(int(rd["items"][0]["statistics"]["viewCount"]))
			else:
				continue
			if group == -1 or gg[group] >= TARGET_PER_PART:
				continue

			urlMedium = rd["items"][0]["snippet"]["thumbnails"]["medium"]["url"]
			urlDefault = rd["items"][0]["snippet"]["thumbnails"]["default"]["url"]

			if not download_image(part, urlMedium, urlDefault, f[:-5]):
				continue

			gg[group] += 1

			row = get_image_info(part, f[:-5], group, rd)
			infos.append(row)

			if all(x >= TARGET_PER_PART for x in gg):
				break

	print(part, time.time() - start, "\n", "g1:", gg[0], "\n", "g2:", gg[1], "\n", "g3:", gg[2], "\n", "g4:", gg[3], "\n", "\n")

	return infos

# Para descargar imagenes en varios procesos a la vez.
# Al final se crea el CSV (Cuando se descarga todo)
def multi_get_image(numProcess, parts):

	res = ThreadPool(numProcess).map(get_image, parts)

	table = []

	for r in res:
		table.extend(r)

	df = pd.DataFrame(table, columns=COLUMNS_NAMES)
	df.to_csv(SAVE_DIR + "data_info.csv")

	return 1


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

# Calculamos cuantas partes son necesarias para llegar al target overall.
numParts = int(TARGET_OVERALL/TARGET_PER_PART)

# Descargamos las imagenes
multi_get_image(NUM_PROCESS, subDirs[:numParts])