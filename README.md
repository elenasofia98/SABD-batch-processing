# SABD Batch Processing

# Configurazione cluster locale
Nella caretella corrente sono inseriti gli script necessari per avviare il processamento.
Il master ed i worker spark, il namenode ed i datanode hdsf sono avviabili posizionandosi nella cartella corrente ed avviando ``00-launch.sh``.
Lo script avvia mediante docker compose i container indicati in `docker-compose.yml`.

Nella configurazione attuale sono presenti:
- un master per spark, sulla porta 7077 del container, la cui interfaccia web e' mappata sulla 8080 della macchina fisica
- due worker spark, sulle porte 
- un namenode hdfs
- due datanode hdfs

Tutti i volumi, i file di configurazione, le cartelle specificate in `docker-compose.yml` hanno mapping relativo a questa cartella.
Nella cartella corrente creare una cartella contenente i dati. La configurazione attuale prevede che tale cartella venga nominata `data` e contenenga i file `.parquet` che si desidera caricare su HDFS.
Tale cartella e' mappata in `docker-compose.yml` nella cartella `/input` del container del namenode.



# Caricamento dataset su hdfs
Prima di iniziare il processamento e' richiesto caricare il dataset su hdfs.
Per farlo lanciare lo scrip `01-loadbatch.sh`. 
La cartella del container del namenode che contiene i dati, dal passo precedente, e' la cartella `/input`.

Lo script fa si che venga creata una cartella sul file system distribuito di nome `/input` e che vengano caricati 
all'interno di tale cartella tutti e 3 i file che compongono il dataset.



## Apache Spark 