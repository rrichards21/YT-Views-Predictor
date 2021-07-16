import numpy as np
import pandas as pd
import json
import pickle
import os
from multiprocessing.pool import ThreadPool
from itertools import repeat

import time

# Obtiene los datos de una parte (desde los JSONs)
def get_stats(partPickle, saveDir):

	# Arreglos donde se guarda la info.
	imgIDs = []
	views = []
	likes = []
	dislikes = []
	comments = []
	topicIds = []

	print("Empezo el", partPickle[:-7])

	start = time.time()

	# Leemos los IDs con thumbnails.
	imgIDs = pickle.load(open("./logs/part_logs/" + partPickle, "rb"))

	# Iteramos por los IDs.
	for i, im in enumerate(imgIDs):

		isTopic = 1
		# Abrimos el JSON del ID.
		statsDict = json.load(open("./metadata/" + partPickle[:-7] + "/" + im + ".json", "r", encoding='utf-8'))
		#if ("topicDetails" in statsDict):
		#isTopic = 1
		#auxTopic = None
		#if ("topicDetails" in statsDict):
		#	auxTopic = statsDict["items"][0]["topicDetails"]
		#	isTopic = 0
		#else:
		#auxTopic = None
		#isTopic = 0
		if ("topicDetails" in statsDict["items"][0]):
			auxDict = statsDict["items"][0]["topicDetails"]
			#print(auxDict)
			if "relevantTopicIds" in auxDict:
				print(auxDict)
				topicIds.append(auxDict["relevantTopicIds"])
			else:
				topicIds.append(None)
		else:
			print("No topicDetails")

		statsDict = statsDict["items"][0]["statistics"]
		#print(statsDict)
		# Extraemos la info que necesitamos del JSON.
		if "viewCount" in statsDict:
			views.append(statsDict["viewCount"])
		else:
			views.append(None)

		if "likeCount" in statsDict:
			likes.append(statsDict["likeCount"])
		else:
			likes.append(None)

		if "dislikeCount" in statsDict:
			dislikes.append(statsDict["dislikeCount"])
		else:
			dislikes.append(None)

		if "commentCount" in statsDict:
			comments.append(statsDict["commentCount"])
		else:
			comments.append(None)
		#if (isTopic == 1):
		

		#print(topicIds)

		if i!=0 and i%100==0:
			print("El", partPickle[:-7], "va en el", i, "se demora", time.time() - start)
			start = time.time()
			

	# Guardamos los datos.
	#print(topicIds)
	pickle.dump([imgIDs, views, likes, dislikes, comments, topicIds], open(saveDir + partPickle[:-7] + ".pickle", "wb"))
	# El partPickle es el nombre del archivo.
	#views, likes, dislikes, comments = pickle.load(open(saveDir + partPickle[:-7] + ".pickle", "rb"))

	print("Terminado el", partPickle[:-7])


def multi_pickle(numProcess, pickles, saveDir):

	res = ThreadPool(numProcess).starmap(get_stats, zip(pickles, repeat(saveDir)))


def main():

	# MAIN
	# Directorios a utilizar.
	pickleDir = "./logs/part_logs/"
	jsonDir = "./metadata/"
	saveDir = "./stats/"

	# Creamos la carpeta stats.
	try:
		os.mkdir("./stats")
	except:
		pass

	# Numero de procesos.
	numProcess = 16

	# Con esto se ve que archivos hay en el directorio.
	pickleFiles = [f.name for f in os.scandir(pickleDir) if f.is_file()]

	# ejecutamos el MULTIPICKLE
	multi_pickle(numProcess, pickleFiles, saveDir)

if __name__ == "__main__":
	print("Hello wolrd")
	main()
	