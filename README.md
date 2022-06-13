# SABD Batch Processing

# Configurazione cluster locale
Nella caretella corrente sono inseriti gli script necessari per avviare il processamento.
Il master ed i worker spark, il namenode ed i datanode hdsf sono avviabili posizionandosi nella cartella corrente ed avviando `00-launch.sh`.
Lo script avvia mediante docker compose i container indicati in `docker-compose.yml`.

Nella configurazione attuale sono presenti:
- un master per spark
- due worker spark
- un namenode HDFS
- tre datanode HDFS

Tutti i volumi, i file di configurazione, le cartelle specificate in `docker-compose.yml` hanno mapping relativo alla cartella corrente.
Nella cartella corrente creare una cartella contenente i file dei dati: la configurazione attuale prevede che tale cartella venga nominata `data` e contenenga i file `.parquet` che si desidera caricare su HDFS.
Tale cartella e' mappata in `docker-compose.yml` nella cartella `/input` del container del namenode.



# Caricamento dataset su HDFS
Prima di iniziare il processamento e' richiesto caricare il dataset su HDFS.
Per farlo lanciare lo script `01-loadbatch.sh`. 
La cartella del container del namenode che contiene i dati, dal passo precedente, e' la cartella `/input`.

Lo script fa si che venga creata una cartella sul file system distribuito di nome `/input` e che vengano caricati 
all'interno di tale cartella tutti e 3 i file che compongono il dataset.



# Processamento dei dati: Apache Spark 
## Configurazione injection dati
Pur avendo mantenuto la struttura delle query invariata, si e' sperimentato con due diversi approcci per l'injection dei dati.
### Configurazione 1
In questo caso, l'esito del preprocessamento non viene scritto su HDFS.
Avendo gia' caricato i file parquet su HDFS, una prima serie di trasformazioni permette di ripulire il dataset dalle colonne non utili,
dati mancanti o errati (con date non appartenenti al periodo specificato). Il preprocessamento e' effettuato usando l'API `Dataset` di Spark.
Tuttavia il risultato del pre-processamento viene cachato ma non memorizzato su HDFS.
Le query lavorano sull'RDD cosi ottenuto.


### Configurazione 2
In questa seconda configurazione, il preprocessamento e' visto come totalmente separato dalla fase di processamento.
Viene effettuata la stessa procedura di preprocessamento (pulizia e riduzione del dataset alle sole colonne utili per le query)
ma il risultato viene scritto su HDFS come file di testo, in formato CSV.
Prima di iniziare il processamento, l'applicazione legge il file di testo contenente il dataset preprocessato,
lo converte in oggetti mediante apposita map in un RDD di oggetti e a quel punto viene eseguito il processamento vero e proprio.

## Codice dell'applicazione
Nella repository e' contenuto il codice necessario all'applicazione.
Il flusso di esecuzione e' definito nella classe `TLCMain` per la prima configurazione e `TLCMain2` che definiscono, in maniera analoga per le due configurazioni,
il flusso del processamento.

Si assume di aver scelto la prima configurazione e di seguito si considera `TLCMain`come l'entry point del programma. Tuttavia l'esecuzione nella seconda congigurazione e' del tutto equivalente.

Le classi che implementano la logica dell'applicazione sono definite nel package `batch` contenuto nella cartella `src/main/java`.
In particolare:
- `ClusterConf` mantiene le informazioni necessarie per identificare il master del cluster spark
- `TaxiRoute` astrae i dati che vengono processati (svolge il ruolo di Entity CLass); i suoi campi corrispondono alle colonne usate del dataset di partenza durante il processamento
- `Application` implementa la logica di esecuzione sia del preprocessamento che del processamento vero e proprio; 
   si occupa di trasformare il dataset letto in input come `Dataset` in un `JavaRDD`,
   ed implementa la logica di entrambe le query (maggiori dettagli sulle scelte implementative possono essere trovate sulla relazione nella cartella `Report`)

## Sottomettere l'applicazione al cluster
Per compilare, definire le dependecy, strutturare il progetto, si e' fatto uso di maven.
Per compliare eseguire `02-make.sh`. Verra' creata una cartella `target`.
Il container del master spark ne puo' accedere al contenuto nel percorso `/app/target`.

Una volta compilata l'applicazione, eseguire lo script `03-launchsubmit-1.sh` per sottometterla al master node Spark.
I risultati saranno memorizzari su HDFS come `query1` e come `query2`.

# Consegna dei risultati
Il file HDFS dei risultati e' aggregato mediante lo script `05-readoutput.sh` e scaricato nella cartella `Results`.
Nella cartella `Report` e' stata aggiunta la relazione in cui e' presente il dettaglio delle scelte implementative.

# Analisi dei tempi di processamento
Le informazioni circa i tempi di processamento possono essere trovati nei due file `times/out-1.csv` e `times/out-2.csv`.
Nella seconda configurazione, essendo separato il tempo di preprocessamento da quello di preprocessamento, la prima colonna ne mantiene i valori in millisecondi.
In entrambi i file ogni riga del file indica la durata delle query ed il numero di nodi spark coinvolti nel processamento (1 nodo master + 1 o 2 nodi worker).

# Replicazione dei risultati
Per eseguire tutto il processo di eleborazione come presentato finora e' eseguire il file
`exec-1.sh` per eseguire il processamento come definito nella prima configurazione,
oppure `exec-2.sh` per eseguire il processamento come definito nella seconda configurazione.

