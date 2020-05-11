# Questions à valider : 
# NB : duree des API Key
# Calcul du nombre d'adherents : -1 si on enlève Greg dans le nombre d'adherents
# Jointure avec les cotisations id ? Pour le moment le compteur est effectue sur le libelle de la cotisation
# Categorie tresorerie : manque les interets bancaires par rapport au budget, le site n'etait pas provisionne l'annee precedente
# Attention le montant de la salle parait très très faible !!!

###############################################
### Step 1 : Chargement des librairies
### needed ? library(cronR)
library(httr) 
library(jsonlite)
library(sqldf)
library(tidyr)
library(openxlsx)
library(config)
library(dplyr)

###############################################
config <- config::get(file = "configuration.yml.dist")
### Step 0 : initialisation
kfr_token = config$kfr_token
kfr_url = config$kfr_url

##############################################
### Step 2 : Intiailisation des fonctions internes


generateReports <- function() {
  
  print('Collection data...')
  
  
  adherents <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/association/adherents",kfr_url)
                                                      ,add_headers("X-Auth-Token"=kfr_token) )
                                                      ,type="text"), flatten = TRUE)
  
  kpi_all <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/kpis/all",kfr_url)
                                                    ,add_headers("X-Auth-Token"=kfr_token) )
                                          ,type="text"), flatten = TRUE) 
  
  saisons <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/saisons",kfr_url)
                                                      ,add_headers("X-Auth-Token"=kfr_token) )
                                                      ,type="text"), flatten = TRUE) 
  
  events <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/association/events",kfr_url)
                                                      ,add_headers("X-Auth-Token"=kfr_token))
                                                      ,type="text"), flatten = TRUE) 
  
  benevolats <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/association/benevolats",kfr_url)
                                                      ,add_headers("X-Auth-Token"=kfr_token))
                                                      ,type="text"),flatten = TRUE) 
  
  tresorerie <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/tresorie/tresories",kfr_url)
                                                      ,add_headers("X-Auth-Token"=kfr_token))
                                                      ,type="text"), flatten = TRUE) 
  
  cat_tresorerie <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/tresorie/categories",kfr_url)
                                                      ,add_headers("X-Auth-Token"=kfr_token))
                                                      ,type="text"), flatten = TRUE) 
  
  materiels <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/materiels",kfr_url)
                                                      ,add_headers("X-Auth-Token"=kfr_token))
                                                      ,type="text"), flatten = TRUE) 
  ###############################################
  ### Step 3 : calcul des indicateurs
  print('Creating dataSet...')
  #  3.1 Indicateurs globaux par saison 
  # 3.1.1 Adherents 
  adherents <- adherents[ , ! colnames(adherents) %in% c("groupes","taos","tresories")]
  # Gestion des NULL renvoy�es par l'API sur les champs saisons qui ne sont plus historis�s
  adherents$saisons <- ifelse(adherents$saisons == 'NULL',NA,adherents$saisons)
  adherents <-unnest(adherents, saisons) # On multiplie les lignes par le nombre de saisons effectuees par adherent (unlist saisons)
  
  adherents$actif <- as.integer(adherents$actif)
  adherents$saison_courante <- as.integer(adherents$saison_courante)
  adherents$saison <- as.integer(adherents$saisons)
  
    # On ne garde que les variables utilis�es dans les calculs
  saisons <- saisons[,-which(names(saisons) == 'adherents')] # Exclusion de la colonne adherent, stockee sous type list
  
  kpi_saisons_adh <- sqldf("select saison, 
      COUNT(distinct a.id) as nb_adherents,
      SUM(a.actif) as nb_adherents_actif,
      COUNT(distinct case when lower(a.cotisation_name) like '%annuel%' then a.id else null end) as nb_inscriptions_annuelles,
      COUNT(distinct case when lower(a.cotisation_name) like '%trimestre%' then a.id else null end) as nb_inscriptions_trimestrielles
      from adherents a
      inner join saisons s on a.saison= s.id
      where s.active <>1
      group by saison
      
      union
      
      select s.id as saison, 
      COUNT(distinct a.id) as nb_adherents,
      COUNT(distinct case when a.actif=1 then a.id else null end) as nb_adherents_actif,
      COUNT(distinct case when lower(a.cotisation_name) like '%annuel%' then a.id else null end) as nb_inscriptions_annuelles,
      COUNT(distinct case when lower(a.cotisation_name) like '%trimestre%' then a.id else null end) as nb_inscriptions_trimestrielles
      from adherents a
      inner join saisons s on substr(a.cotisation_name,1,16)= s.nom
      where s.active = 1
      group by s.id
")
  
  # 3.1.2 Benevolats
  kpi_saisons_ben <- sqldf("
  select 
    saison_id as saison,
    ROUND(sum(heure)+ sum(minute)/60.0 ,0) as nb_heures
  from 
    benevolats
  group by 
    saison_id  
  ")
  
  # 3.1.3 Evènements
  events <- events[ , ! colnames(events) %in% c("adherents")]
  kpi_saisons_eve <- sqldf("
  select 
    saison_id as saison,
    count(distinct id) as nb_evenement
  from 
    events
  group by 
    saison_id
  ")
  
  # 3.1.4 Balance tresorerie
  tresorerie$cheque <- as.integer(tresorerie$cheque)
  
  # CFL: 09/06/2019 : modification RG sur A_encaisser (= montant pointe dans le bilan) et A_regler ( = montant a pointer dans le bilan)
  kpi_saisons_tres <- sqldf("
  select
    tre.saison_id,
    sum(case when tre.etat_name in ('A reclamer') then tre.montant else null end) as retard,
    sum(case when tre.etat_name in ('Encaisse','Regle') then tre.montant else null end) as tre_saison,
    sum(case when tre.pointe == 1 then tre.montant else null end) as somme_pointe_saison,
    sum(case when tre.pointe == 0 then tre.montant else null end) as somme_a_pointer_saison
  from 
    tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  where
    tre.etat_name not in ('Annulee')
  group by 
    tre.saison_id
  ")
  
  #Nouvelle requête pour afficher le solde de la tresorerie en fin de saison (over (order by) non permis en SQLite)
  kpi_saisons_tres <- sqldf("Select c1.saison_id, 
    c1.retard,
    c1.somme_pointe_saison,
    c1.somme_a_pointer_saison,
    c1.tre_saison,
    sum(c2.tre_saison) as tre_fin_saison
  from kpi_saisons_tres c1, 
    kpi_saisons_tres c2
  where 
    c1.saison_id >= c2.saison_id
  group by c1.saison_id,
    c1.retard,
    c1.somme_pointe_saison,
    c1.somme_a_pointer_saison,
    c1.tre_saison
  order by c1.saison_id ")
  
  
  # 3.1.5 Nombre de cours,  depuis l'API ALL
  kpi_all <- as.data.frame(kpi_all$cours)
  
  
  # 3.1.6 Merge des differents indicateurs
  kpi_saisons <- sqldf("
  select s.nom,s.active,
    adh.nb_adherents, adh.nb_adherents_actif, adh.nb_inscriptions_annuelles, adh.nb_inscriptions_trimestrielles,
    ben.nb_heures,
    eve.nb_evenement,
    tres.retard, tres.somme_pointe_saison, tres.somme_a_pointer_saison, tres.tre_saison, tres.tre_fin_saison,
    cours.nbCours as nb_cours,
    cours.nbCoursEssais as nb_cours_essai
  from 
    saisons s
  left join kpi_saisons_adh adh on s.id = adh.saison
  left join kpi_saisons_ben ben on s.id = ben.saison
  left join kpi_saisons_eve eve on s.id = eve.saison
  left join kpi_saisons_tres tres on s.id = tres.saison_id
  left join kpi_all cours on s.nom = cours.saison_nom
  order by 
    s.nom desc
  limit 5
  ")
  
  # 3.2 Indicateurs de tresorerie detailles
  
  # 3.2.1 Tresorerie par categorie
  kpi_tresorerie_cat <- sqldf("
  select 
   rq.categorie_name,
   rq.total_realise,
   rq.depense,
   rq.recette,
   n1.total_realise as total_n1
  from
  (
      select 
      tre.saison_id,
      tre.categorie_name,
      tre.categorie_id,
      sum(tre.montant)as total_realise,
      sum(case when tre.montant<=0 then tre.montant else 0 end) as depense,
      sum(case when tre.montant>=0 then tre.montant else 0 end) as recette
      from tresorerie tre
      inner join saisons s on tre.saison_id = s.id
      where s.active = 1
        and tre.etat_name not in ('Annulee')
      group by tre.saison_id,tre.categorie_id,tre.categorie_name
  ) rq
  left join 
      (
          select 
          tre.saison_id+1 as saison_id,
          tre.categorie_id,
          sum(tre.montant)as total_realise  
          from tresorerie tre
          where tre.etat_name  not in ('Annulee')
          group by tre.saison_id,tre.categorie_id
      ) n1 on rq.saison_id = n1.saison_id  and rq.categorie_id = n1.categorie_id
  ")
  
  kpi_tresorerie_dep <- sqldf("
  select categorie_name,
    depense
    from kpi_tresorerie_cat
  ")
  
  kpi_tresorerie_rece <- sqldf("
  select categorie_name,
    recette
    from kpi_tresorerie_cat
  ")
  
  # 3.2.2 Evolution de la tresorerie mois par mois
  kpi_tresorerie_evolution <- sqldf("
  select
    date(date_creation,'start of month') as mois_date,
    strftime('%d/%m/%Y', date(date_creation,'start of month')) as mois,
    sum(case when tre.montant<=0 then tre.montant else 0 end) as depense,
    sum(case when tre.montant>=0 then tre.montant else 0 end) as recette,
    sum(tre.montant) as variation_tresorerie,
    min(kpi.tre_fin_saison - kpi.tre_saison) as solde_n1
  from 
    tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  left join kpi_saisons_tres kpi on tre.saison_id = kpi.saison_id
  where 
    s.active = 1 
    and tre.etat_name not in ('Annulee')
  group by 
    strftime('%d/%m/%Y', date(date_creation,'start of month'))
  ")
  
  kpi_tresorerie_evolution <- sqldf("
  select c1.mois, 
    c1.depense,
    c1.recette,
    c1.variation_tresorerie,
    min(c1.solde_n1)+sum(c2.variation_tresorerie) as tresorerie_totale
  from 
    kpi_tresorerie_evolution c1, 
    kpi_tresorerie_evolution c2
  where 
    c1.mois_date >= c2.mois_date
  group by 
    c1.mois, 
    c1.depense,
    c1.recette,
    c1.variation_tresorerie
  order by 
    c1.mois_date
  ")
  
  # 3.3 Liste exhaustive
  
  # 3.3.1 Liste des evenements de la saison en cours
  events <- sqldf("
  select 
    description, 
    commentaire, 
    date_creation,
    substr(date_creation, 9, 2) || '/' || substr(date_creation, 6,2) || '/' || substr(date_creation, 1,4) || ': ' || description as titre_event
  from 
    events eve
  inner join saisons se on eve.saison_id= se.id
  where 
    se.active = 1
  order by 
    date_creation
  ")
  
  # 3.3.2 Liste des revenus de la saison
  liste_recette <- sqldf("
  select
    tre.categorie_name,
    strftime('%d/%m/%Y', date_creation) as date,
    tre.description,
    tre.montant,
    tre.pointe
  from 
    tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  where 
    s.active = 1 
    and tre.etat_name in ('A reclamer','A encaisser','Encaisse')
  ")
  
  # 3.3.2 Liste des depenses de la saison
  liste_depense <- sqldf("
  select
    tre.categorie_name,
    strftime('%d/%m/%Y', date_creation) as date,
    tre.description,
    tre.montant,
    tre.pointe
  from 
    tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  where 
    s.active = 1 
    and tre.etat_name in ('A regler','Regle')
  ")
  
  # 3.4.0 Liste du materiels en vente
  materiels_vente <- sqldf("
   select 
    nom as description, 
    stock as unite, 
    prix_vente as prix, 
    stock * prix_vente as montant
   from 
    materiels
   where 
    association = 0 
    AND stock <> 0
  ")

  # 3.4.1 Liste du materiels en association
  materiels_association <- sqldf("
   select 
    nom as description, 
    stock as unite, 
    prix_achat as prix, 
    stock * prix_achat as montant
   from 
    materiels
   where 
    association = 1 
    AND stock <> 0
  ")
  # 3.5.1 Liste benevolats
  benevoltas_heures <- sqldf("
   select 
    b.date_creation as Date, 
    b.description as Description,  
    (b.heure*60 + b.minute)/60.0 as _nb_heures, 
    b.adherent_name as _nom,
    (b.heure*60 + b.minute)/60.0 * 9.75 as _montant
   from 
    benevolats b
   inner join saisons s on b.saison_id = s.id
   where 
    s.active = 1
   order by 
    b.date_creation
  ")
  
  # 4. Export des fichiers
  print('Saving Data into Excels..')
  
  # Tips : changer de repertoire si besoin
  # setwd(dir = '/projet/home/cflouriot')
  print(sprintf('  Updating %s ',config$kfr_excel_template_bilan))
  wbbilan <- openxlsx::loadWorkbook(config$kfr_excel_template_bilan)
  openxlsx::removeWorksheet(wbbilan, sheet = "DataR")
  openxlsx::addWorksheet(wbbilan, "DataR")
  openxlsx::writeData(wbbilan, "DataR", kpi_saisons, startCol = 1, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", kpi_tresorerie_cat, startCol = 17, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", events, startCol = 24, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", kpi_tresorerie_evolution, startCol = 30, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", liste_recette, startCol = 37, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", liste_depense, startCol = 43, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", kpi_tresorerie_dep, startCol = 49, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", kpi_tresorerie_rece, startCol = 52, startRow = 1, rowNames = FALSE, colNames = TRUE)
  saveWorkbook(wbbilan, config$kfr_excel_template_bilan, overwrite = TRUE)
  print(sprintf(' %s saved',config$kfr_excel_template_bilan))

  # Suppression des dataset temporaires
  rm(kpi_saisons_adh,kpi_saisons_ben,kpi_saisons_eve,kpi_saisons_tres,kpi_tresorerie_evolution,liste_depense,liste_recette)
  rm(kpi_saisons,kpi_tresorerie_cat,events,adherents,saisons,cat_tresorerie,benevolats,tresorerie,kpi_tresorerie_dep, kpi_tresorerie_rece)
  
    
  print(sprintf('  Updating %s ',config$kfr_excel_template_materiels))
  wbmateriels <- openxlsx::loadWorkbook(config$kfr_excel_template_materiels)
  openxlsx::removeWorksheet(wbmateriels, sheet = "DataR")
  openxlsx::addWorksheet(wbmateriels, "DataR")
  openxlsx::writeData(wbmateriels, "DataR", materiels_association, startCol = 1, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbmateriels, "DataR", materiels_vente, startCol = 6, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbmateriels, "DataR", benevoltas_heures, startCol = 12, startRow = 1, rowNames = FALSE, colNames = TRUE)
  saveWorkbook(wbmateriels, config$kfr_excel_template_materiels, overwrite = TRUE)
  print(sprintf('  %s saved',config$kfr_excel_template_materiels))
  
  # Suppression des dataset temporaires
  rm(materiels_association, materiels_vente, materiels)
  
  
}


###############################################
### Step 3 : Validation du token avant le lancement
#validation <- httr::GET(url= sprintf("%s/security/tokens/check-user",kfr_url), 
#                        add_headers("X-Auth-Token"=kfr_token) ) 

validation <- httr::GET(url= sprintf("%s/association/adherents/by-token",kfr_url), 
                        add_headers("X-Auth-Token"=kfr_token) ) 
token_valide = validation['status_code'] == 202
rm (validation);
if (!token_valide) {
  stop(sprintf('ERROR AUTHENTICATION (changer le token %s)',kfr_token))
} else {
  generateReports()
  print('Generation DONE.')
}
# Suppression de tous les dataframe
rm(token_valide)
rm(list = ls())


