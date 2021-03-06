# Temps passé : 2h

# Requêtes pour mise à jour du document et vérification des résultats
############################################################
### Récupération des données avant traitement
library(cronR)
library(httr) 
library(jsonlite)
library(sqldf)
library(tidyr)
library(openxlsx)


############################################################
### Chargement de la configuration
config <- config::get(file = "configuration.yml")
### Step 0 : initialisation
kfr_token = config$kfr_token
kfr_url = config$kfr_url

##############################################
### Step 2 : Intiailisation des fonctions internes

generateReports <- function() {
  adherents <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/association/adherents",kfr_url)
                                                    , add_headers("X-Auth-Token"=kfr_token) ),type="text"), flatten = TRUE) 
  
  saisons <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/saisons",kfr_url)
                                                  , add_headers("X-Auth-Token"= kfr_token) ),type="text"), flatten = TRUE) 
  
  events <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/association/events",kfr_url)
                                                 , add_headers("X-Auth-Token"= kfr_token)),type="text"), flatten = TRUE) 
  
  benevolats <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/association/benevolats",kfr_url)
                                                     , add_headers("X-Auth-Token"= kfr_token)
  ),type="text"),flatten = TRUE) 
  
  tresorerie <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/tresorie/tresories",kfr_url)
                                                     , add_headers("X-Auth-Token"= kfr_token)
  ),type="text"), flatten = TRUE) 
  
  cat_tresorerie <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/tresorie/categories",kfr_url)
                                                         , add_headers("X-Auth-Token"= kfr_token)),type="text"), flatten = TRUE) 
  cotisations <- jsonlite::fromJSON(content(httr::GET(url=sprintf("%s/cotisations",kfr_url)
                                                    ,add_headers("X-Auth-Token"=kfr_token))
                                          ,type="text"), flatten = TRUE) 														 
														 
	adherents <- adherents[ , ! colnames(adherents) %in% c("groupes","taos","tresories")]
	adherents <-unnest(adherents, cotisations) 

	adherents$actif <- as.integer(adherents$actif)
	adherents$saison_courante <- as.integer(adherents$saison_courante)
	adherents$saison <- as.integer(adherents$saisons)
	tresorerie$cheque <- as.integer(tresorerie$cheque)
  
  #####################################################################
  
  # 1. Récapitulatif administratif
  # Listes des membres actifs
  
	View(sqldf("select adh.prenom || ' ' ||  adh.nom as membre 
			   from adherents adh
			  inner join cotisations c on adh.cotisations = c.id
			  inner join saisons s on c.saison_id = s.id
			   where s.active=1"))
  
  # 2. Point 1 Adhésion
  # Activité
	sqldf("select c.saison_name, 
		COUNT(distinct a.id) as nb_adherents,
		SUM(a.actif) as nb_adherents_actif,
		COUNT(distinct case when lower(a.cotisation_name) like '%annuel%' then a.id else null end) as nb_inscriptions_annuelles,
		COUNT(distinct case when lower(a.cotisation_name) like '%trimestre%' then a.id else null end) as nb_inscriptions_trimestrielles,
		COUNT(distinct a.licence_number) as nb_licences,
		COUNT(distinct case when a.licence_etat = 'ACTIF' then a.licence_number else null end) as nb_licences_actives
		from adherents a
		inner join cotisations c on a.cotisations = c.id
		group by c.saison_name
		order by c.saison_id desc")
  
  # Financier
  sqldf("
  select 
  tre.categorie_name,
  sum(tre.montant)as total_realise,
  sum(case when tre.montant<0 then tre.montant else 0 end) as depense,
  sum(case when tre.montant>=0 then tre.montant else 0 end) as recette
  
  from tresorerie tre
  inner join saisons s on tre.saison_id = s.id
  where s.active = 1
    and tre.etat_name in ('Encaisse','Regle')
   group by tre.categorie_name
  ")
  
  sqldf("
  select 
  case when description like 'Licence annuelle%' then 'Licence'
     when description like 'Reduction sur Licence annuelle%' then 'Participation asso'
  else description end as type_paiement_licence,
  sum(tre.montant) as total_realise,
  count(*) as nb_lignes
  from tresorerie tre
  inner join saisons s on tre.saison_id = s.id and s.active=1
  where  tre.etat_name in ('Encaisse','Regle')
    and tre.categorie_name = 'Licences'
   group by 
    case when description like 'Licence annuelle%' then 'Licence'
     when description like 'Reduction sur Licence annuelle%' then 'Participation asso'
  else description end
  
  ")
  
  # Inscription
	View(sqldf("select cotisation_name,adh.prenom || ' ' ||  adh.nom as membre , adh.actif
			   from adherents adh
			  inner join cotisations c on adh.cotisations = c.id
			  inner join saisons s on c.saison_id = s.id
			   where s.active=1 
					and ( lower(cotisation_name) like '%annuel%' or lower(cotisation_name) like '%trimestre%')
			   order by cotisation_name"))
  
  #Evenements festifs
  events <- events[ , ! colnames(events) %in% c("adherents")]
  View(sqldf("
   select description, commentaire, date_creation,
      substr(date_creation, 9, 2) || '/' || substr(date_creation, 6,2) || '/' || substr(date_creation, 1,4) || ': ' || description as titre_event
   from events eve
   inner join saisons se on eve.saison_id= se.id
   where se.active = 1
    "))
  
  # Bénévolats
	sqldf("
	  select ben.description,  ben.adherent_name,
	  ROUND(sum(heure)+ sum(minute)/60 ,0) as nb_heures
	  from benevolats ben
	  inner join saisons s on ben.saison_id = s.id and s.active=1
	  group by ben.description,  ben.adherent_name  
	")
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
