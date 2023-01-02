# ece_pipeline_2022

# Introduction

Pour le projet data pipeline ECE, nous avons implementé une pipeline pour les données des elections presidentielles de 2022 qui va comporter 3 parties:

-Collecte des données

-Traitement des données

-Orchestration du pipeline

## Collecte des données
Nous avons donc utilisé les données disponibles sur le site https://www.data.gouv.fr/fr/datasets/?q=election%202022
-Pour le premier tour, on a utilisé le fichier https://www.data.gouv.fr/fr/datasets/r/79b5cac4-4957-486b-bbda-322d80868224
-Et pour le 2e tour, le fichier https://www.data.gouv.fr/fr/datasets/r/11a736be-748f-470b-b2c6-b8ba09b48938

Au debut du pipeline, il y a un script python qui va aller recuperer les fichier sur le sites, puis apres **NIFI** va ingerer les fichier avec le processeur **getFile**. Toujours dans NIFI, on va convertir ces fichier en **.csv** puis a la fin ils seront publiés dans un topic kafka pour pouvoir les afficher sur console et enregistrer localement pour faire les transformation avec spark.

![Nifi Flow](https://raw.githubusercontent.com/TianaDoto/ece_pipeline_2022/master/Projet_exam/img/nifi.png)

## Transformation
Ensuite à l'aide de **spark**, on va appliquer les transformation necessaire sur les données qu on a ingeré avec NIFI. Concretement, on va mettre le bon schema sur les fichier CSV, enlever les colonnes qui ne sont pas pertinentes ou qu'on pourrait calculer par nous meme (pour que le fichier ne soit pas trop lourd), supprimer les doublons et effacer les colonnes qui sont vides.

Donc a la fin les données qui sont disponibles dans les dossiers tour1_par_departement et tour2_par_departement peuvent etre utilisées pour faire les analyses necessaires.

## Orchestration et automatisation
A la fin donc, on a utilisé **AIRFLOW** pour orchester tout le pipeline.
Quand on lance airflow, les dags vont s'executer un apres l autre.

**RQ:** Pour connecter NIFI a AIRFLOW, on a utilisé les fonctions de la library qui sont dans ce repo github https://github.com/CribberSix/nifi-airflow-connection

![Airflow](https://raw.githubusercontent.com/TianaDoto/ece_pipeline_2022/master/Projet_exam/img/airflow.png)

## Conclusion
On a decidé d'utiliser ces 2 fichiers car avec, on pourra voir dans quel departement/circonscription/commune un candidat X a maximisé les votes qu il/elle a eu et par la suite on pourra a la prochaine election essayer de maximiser les campagnes dans les autres regions sans pour autant delaissé la region ou il y avait un nombre elevé de voix
