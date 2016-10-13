# BBDATA - Tests Storm, Spark Streaming et Flink

Date: 2016-04-08

Made by: Loïc Monney, <loic.monney@master.hes-so.ch>

### Description

Storm, Spark Streaming et Flink ont été tous trois testés avec le use-case suivant: trois capteurs de température sont installés à trois étages différents dans un bâtiment. Ces capteurs prennent des mesures en Kelvin à intervalles réguliers et les poussent dans Kafka, dont voici un exemple:

```
{ "mesure":{"id": "1/3/25","timestamp": "111111111","value": "261"} }
```

De l'autre côté, les trois technologies sont utilisées pour traiter les mesures en plusieurs étapes:

1. Le JSON est parsé
2. Les données sont augmentées: la sémantique (le nom du capteur et un boolean indiquant s'il doit être traité ou non) est ajoutée à chaque mesure. Par simplicité celle-ci est stockée en dure dans le code Java comme ci-dessous:

   ```java
   final Map<String, Boolean> captorProcessEnabledMap = new HashMap<>();
   captorProcessEnabledMap.put("1/3/25", true);
   captorProcessEnabledMap.put("2/3/25", true);
   captorProcessEnabledMap.put("3/3/25", false);
   final Map<String, String> captorNameMap = new HashMap<>();
   captorNameMap.put("1/3/25", "Température 1er");
   captorNameMap.put("2/3/25", "Température 2ème");
   captorNameMap.put("3/3/25", "Température 3ème");
   ```

3. Les mesures sont filtrées si leur boolean est `false`
4. Les mesures sont converties en degrés Celsius
5. L'output est enregistré dans un log

### Exécution sur la sandbox du DAPLAB

1. Connectez-vous en SSH à la gateway avec `ssh username@daplab3.tic.hefr.ch -p 2222`

2. Créez le topic Kafka nommé `json` où les mesures seront ajoutées

   ```sh
   ~/kafka/bin/kafka-topics.sh --zookeeper 10.10.0.14 --create --topic json --partitions 1 --replication-factor 3
   ```

3. Compilez le programme à l'aide de maven. Il suffit d'aller dans le répertoire d'un des projets et d'exécuter la commande `mvn package`.

4. Lancez le programme

   - **Storm**: `storm jar target/stormcap-1.0.0.jar storm.cap.StormCapTopology`
   - **Spark Streaming**: `spark-submit --master local[8] --class spark.cap.SparkCap target/sparkcap-1.0.0.jar`
   - **Flink**: `flink-1.0.0/bin/flink run -c flink.cap.FlinkCap flinkcap/target/flinkcap-1.0.0.jar`

5. Remplissez le topic pour que les implémentations aient des données à traiter

   ```sh
   for i in $(seq 1 100); do
      cat mesure-1.json | kafka_2.11-0.9.0.1/bin/kafka-console-producer.sh --broker-list 10.10.0.21:6667 --topic json
   done
   ```

### Monitoring

Chaque technologie offre un outil web pour monitorer les programmes exécutés.

Afin de pouvoir y accéder, vous devez ouvrir un tunnel SSH où votre traffic sera redirigé:

`sshuttle --dns -r username@daplab3.tic.hefr.ch:2222 10.10.0.0/24`

- **Storm**: Storm UI disponible à l'adresse `http://edu-wn-4.student.lan:8744`
- **Spark Streaming**: Interface web disponible à l'adresse `http://10.10.0.4:4040`
- **Flink**: Dashboard accessible sur `http://10.10.0.4:8081`


