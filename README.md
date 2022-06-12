# SABD Batch Processing

# Configurazione cluster locale
Nella caretella corrente sono inseriti gli script necessari per avviare il processamento.
Il master ed i worker spark, il namenode ed i datanode hdsf sono avviabili posizionandosi nella cartella corrente ed avviando `00-launch.sh`.
Lo script avvia mediante docker compose i container indicati in `docker-compose.yml`.

Nella configurazione attuale sono presenti:
- un master per spark, sulla porta 7077 del container, la cui interfaccia web e' mappata sulla 8080 della macchina fisica
- due worker spark, sulle porte 
- un namenode HDFS
- due datanode HDFS

Tutti i volumi, i file di configurazione, le cartelle specificate in `docker-compose.yml` hanno mapping relativo a questa cartella.
Nella cartella corrente creare una cartella contenente i dati. La configurazione attuale prevede che tale cartella venga nominata `data` e contenenga i file `.parquet` che si desidera caricare su HDFS.
Tale cartella e' mappata in `docker-compose.yml` nella cartella `/input` del container del namenode.



# Caricamento dataset su HDFS
Prima di iniziare il processamento e' richiesto caricare il dataset su HDFS.
Per farlo lanciare lo script `01-loadbatch.sh`. 
La cartella del container del namenode che contiene i dati, dal passo precedente, e' la cartella `/input`.

Lo script fa si che venga creata una cartella sul file system distribuito di nome `/input` e che vengano caricati 
all'interno di tale cartella tutti e 3 i file che compongono il dataset.



# Processamento dei dati: Apache Spark 
## Codice dell'applicazione
Nella repository e' contenuto il codice necessario all'applicazione.
Il flusso di esecuzione e' definito nella classe `TLCMain` che definisce il flusso del processamento.
Questa classe e' l'entry point del programma.

Le classi che implementano la logica dell'applicazione sono definite nel package `batch` contenuto nella cartella `src/main/java`.
In particolare:
- `ClusterConf` mantiene le informazioni necessarie per identificare il master del cluster spark
- `TaxiRoute` astrae i dati che vengono processati (svolge il ruolo di Entity CLass); i suoi campi corrispondono alle colonne usate del dataset di partenza durante il processamento
- `Application` implementa la logica di esecuzione sia del preprocessamento che del processamento vero e proprio; 
   si occupa di trasformare il dataset letto in input come `Dataset` in un `JavaRDD`,
   ed implementa la logica di entrambe le query (maggiori dettagli sulle scelte implementative possono essere trovate sulla relazione)

## Sottomettere l'applicazione al cluster
Per compilare, definire le dependecy, strutturare il progetto, si e' fatto uso di maven.
Per compliare eseguire `02-make.sh`. Verra' creata una cartella `target` che nel container del master spark e' mappata in `/app`.

Una volta compilata l'applicazione, eseguire lo script `03-submit.sh` per sottometterla al master node Spark.
I risultati saranno memorizzari su HDFS come `query1-results` e come `query2-results`.



