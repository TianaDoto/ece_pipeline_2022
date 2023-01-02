import requests
import os

#download file tour 1 et tour 2
URL = ["https://www.data.gouv.fr/fr/datasets/r/79b5cac4-4957-486b-bbda-322d80868224", "https://www.data.gouv.fr/fr/datasets/r/4dfd05a9-094e-4043-8a19-43b6b6bbe086"]
i = 1
for items in URL:
    response = requests.get(items)
    filename = "/home/heritiana/Project/Projet_exam/data/source/resultats-par-niveau-burvot-t"+str(i)+"-france-entiere.txt"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    open(filename, "wb").write(response.content)
    i += 1
