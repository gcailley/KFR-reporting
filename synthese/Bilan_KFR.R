# Questions à valider : 
# NB : durée des API Key
# Calcul du nombre d'adhérents : -1 si on enlève Greg dans le nombre d'adhérents
# Jointure avec les cotisations id ? Pour le moment le compteur est effectué sur le libellé de la cotisation
# Catégorie trésorerie : manque les intérets bancaires par rapport au budget, le site n'était pas provisionné l'année précédente
# Attention le montant de la salle parait très très faible !!!


# temps passé : 7h
###############################################
config <- config::get(file = "Bilan_KFR.yml")
### Step 0 : initialisation
kfr_token = config$kfr_token
kfr_url = config$kfr_url

###############################################
### Step 1 : Chargement des librairies
library(cronR)
library(httr) 
library(jsonlite)
library(sqldf)
library(tidyr)
library(openxlsx)
library(config)

##############################################
### Step 2 : Intiailisation des fonctions internes


generateReports <- function() {
  adherents <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/association/adherents",kfr_url)
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
  
  
  ###############################################
  ### Step 3 : calcul des indicateurs
  #  3.1 Indicateurs globaux par saison 
  # 3.1.1 Adhérents 
  adherents <-unnest(adherents, saisons) # On multiplie les lignes par le nombre de saisons effectuées par adhérent (unlist saisons)
  adherents$actif <- as.integer(adherents$actif)
  adherents$saison_courante <- as.integer(adherents$saison_courante)
  adherents$saison <- as.integer(adherents$saisons)
  
  kpi_saisons_adh <- sqldf("select saison, 
      COUNT(distinct id) as nb_adherents,
      SUM(actif) as nb_adherents_actif,
      COUNT(distinct case when lower(cotisation_name) like '%annuel%' then id else null end) as nb_inscriptions_annuelles,
      COUNT(distinct case when lower(cotisation_name) like '%trimestre%' then id else null end) as nb_inscriptions_trimestrielles
      from adherents
      group by saison")
  
  # 3.1.2 Bénévolats
  kpi_saisons_ben <- sqldf("
    select saison_id as saison,
    ROUND(sum(heure)+ sum(minute)/60 ,0) as nb_heures
    from benevolats
    group by saison_id  
  ")
  
  # 3.1.3 Evènements
  kpi_saisons_eve <- sqldf("
          select saison_id as saison,
          count(distinct id) as nb_evenement
          from events
          group by saison_id")
  
  # 3.1.4 Balance tresorerie
  saisons <- saisons[,-which(names(saisons) == 'adherents')] # Exclusion de la colonne adherent, stockée sous type list
  tresorerie$cheque <- as.integer(tresorerie$cheque)
  
  # CFL: 09/06/2019 : modification RG sur A_encaisser (= montant pointé dans le bilan) et A_regler ( = montant à pointer dans le bilan)
  kpi_saisons_tres <- sqldf("
  select
  tre.saison_id,
  sum(case when tre.etat_name in ('A encaisser','A regler') then tre.montant else null end) as retard,
  sum(case when tre.etat_name in ('Encaisse','Regle') and tre.pointe =1 then tre.montant else null end) as a_encaisser,
  sum(case when tre.etat_name in ('Regle') and tre.pointe =0 then tre.montant else null end) as a_regler,
  sum(case when tre.etat_name in ('Encaisse','Regle') then tre.montant else null end) as tre_saison
  from tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  group by tre.saison_id
  ")
  
  #Nouvelle requête pour afficher le solde de la trésorerie en fin de saison (over (order by) non permis en SQLite)
  kpi_saisons_tres <- sqldf("Select c1.saison_id, 
    c1.retard,
    c1.a_encaisser,
    c1.a_regler,
    c1.tre_saison,
    sum(c2.tre_saison) as tre_fin_saison
  from kpi_saisons_tres c1, 
      kpi_saisons_tres c2
  where c1.saison_id >= c2.saison_id
  group by c1.saison_id,
    c1.retard,
    c1.a_encaisser,
    c1.a_regler,
    c1.tre_saison
  order by c1.saison_id ")
  
  
  # 3.1.5 Nombre de cours sur Google Agenda ->to do en automatique, message d'erreur sur les credentials
  # https://www.googleapis.com/calendar/v3/calendars/kungfurennes@gmail.com/events
  
  # 3.1.6 Merge des différents indicateurs
  kpi_saisons <- sqldf("
    select s.nom,s.active,
      adh.nb_adherents, adh.nb_adherents_actif, adh.nb_inscriptions_annuelles, adh.nb_inscriptions_trimestrielles,
      ben.nb_heures,
      eve.nb_evenement,
      tres.retard, tres.a_encaisser, tres.a_regler, tres.tre_saison, tres.tre_fin_saison
    from saisons s
    left join kpi_saisons_adh adh on s.id = adh.saison
    left join kpi_saisons_ben ben on s.id = ben.saison
    left join kpi_saisons_eve eve on s.id = eve.saison
    left join kpi_saisons_tres tres on s.id = tres.saison_id
    order by s.nom desc
    limit 5
    "
  )
  
  # 3.2 Indicateurs de trésorerie détaillés
  
  # 3.2.1 Trésorerie par catégorie
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
      sum(case when tre.montant<0 then tre.montant else 0 end) as depense,
      sum(case when tre.montant>=0 then tre.montant else 0 end) as recette
      from tresorerie tre
      inner join saisons s on tre.saison_id = s.id
      where s.active = 1
        and tre.etat_name in ('Encaisse','Regle')
      group by tre.saison_id,tre.categorie_id,tre.categorie_name
  ) rq
  left join 
      (
          select 
          tre.saison_id+1 as saison_id,
          tre.categorie_id,
          sum(tre.montant)as total_realise  
          from tresorerie tre
          where tre.etat_name in ('Encaisse','Regle')
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
  
  # 3.2.2 Evolution de la trésorerie mois par mois
  kpi_tresorerie_evolution <- sqldf("
  select
  date(date_creation,'start of month') as mois_date,
  strftime('%d/%m/%Y', date(date_creation,'start of month')) as mois,
   sum(case when tre.montant<0 then tre.montant else 0 end) as depense,
  sum(case when tre.montant>=0 then tre.montant else 0 end) as recette,
  sum(tre.montant) as variation_tresorerie,
  min(kpi.tre_fin_saison - kpi.tre_saison) as solde_n1
  from tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  left join kpi_saisons_tres kpi on tre.saison_id = kpi.saison_id
  where s.active = 1 
      and tre.etat_name in ('Encaisse','Regle')
  group by strftime('%d/%m/%Y', date(date_creation,'start of month'))
  ")
  
  kpi_tresorerie_evolution <- sqldf("
  select c1.mois, 
    c1.depense,
    c1.recette,
    c1.variation_tresorerie,
    min(c1.solde_n1)+sum(c2.variation_tresorerie) as tresorerie_totale
  from kpi_tresorerie_evolution c1, 
      kpi_tresorerie_evolution c2
  where c1.mois_date >= c2.mois_date
  group by c1.mois, 
    c1.depense,
    c1.recette,
    c1.variation_tresorerie
  order by c1.mois_date ")
  
  # 3.3 Liste exhaustive
  
  # 3.3.1 Liste des événements de la saison en cours
  events <- sqldf("
   select description, commentaire, date_creation,
      substr(date_creation, 9, 2) || '/' || substr(date_creation, 6,2) || '/' || substr(date_creation, 1,4) || ': ' || description as titre_event
   from events eve
   inner join saisons se on eve.saison_id= se.id
   where se.active = 1
    ")
  
  # 3.3.2 Liste des revenus de la saison
  liste_recette <- sqldf("
  select
    tre.categorie_name,
    strftime('%d/%m/%Y', date_creation) as date,
    tre.description,
    tre.montant
  from tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  where s.active = 1 
      and tre.etat_name in ('Encaisse','Regle')
      and tre.montant>=0
  
  ")
  
  # 3.3.2 Liste des dépenses de la saison
  liste_depense <- sqldf("
  select
    tre.categorie_name,
    strftime('%d/%m/%Y', date_creation) as date,
    tre.description,
    tre.montant
  from tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  where s.active = 1 
      and tre.etat_name in ('Encaisse','Regle')
      and tre.montant<0
  
  ")
  
  # 4. Export des fichiers
  
  # Tips : changer de répertoire si besoin
  # setwd(dir = '/projet/home/cflouriot')
  
  wbbilan <- openxlsx::loadWorkbook(config$kfr_excel_template)
  openxlsx::removeWorksheet(wbbilan, sheet = "DataR")
  openxlsx::addWorksheet(wbbilan, "DataR")
  openxlsx::writeData(wbbilan, "DataR", kpi_saisons, startCol = 1, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", kpi_tresorerie_cat, startCol = 16, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", events, startCol = 23, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", kpi_tresorerie_evolution, startCol = 29, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", liste_recette, startCol = 36, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", liste_depense, startCol = 41, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", kpi_tresorerie_dep, startCol = 46, startRow = 1, rowNames = FALSE, colNames = TRUE)
  openxlsx::writeData(wbbilan, "DataR", kpi_tresorerie_rece, startCol = 49, startRow = 1, rowNames = FALSE, colNames = TRUE)
  saveWorkbook(wbbilan, "Bilan_KFR.xlsx", overwrite = TRUE)
  
  # 5. Suppression des dataset temporaires
  rm(kpi_saisons_adh,kpi_saisons_ben,kpi_saisons_eve,kpi_saisons_tres,kpi_tresorerie_evolution,liste_depense,liste_recette)
  rm(kpi_saisons,kpi_tresorerie_cat,events,adherents,saisons,cat_tresorerie,benevolats,tresorerie,kpi_tresorerie_dep, kpi_tresorerie_rece)
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
  sprintf('Generation %s DONE',config$kfr_excel_template)
}
rm(token_valide)


