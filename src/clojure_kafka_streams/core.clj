(ns clojure-kafka-streams.core
  (:require
    [jackdaw.streams :as js]
    [jackdaw.streams :as js]
    [jackdaw.client :as jc]
    [jackdaw.client.log :as jcl]
    [jackdaw.admin :as ja]
    [jackdaw.serdes :as s]
    [jackdaw.serdes.json :as json])
  (:gen-class)
  (:import (java.util Properties)
           (org.apache.kafka.streams StreamsConfig)
           (org.apache.kafka.common.serialization Serdes)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.streams.kstream  Initializer Aggregator ValueMapper KeyValueMapper Predicate)))



(defn initializer [v]
  (reify Initializer
    (apply [_]
      v)))

(defmacro aggregator [kv & body]
  `(reify Aggregator
     (apply [_# ~(first kv) ~(second kv) ~(nth kv 2)]
       ~@body)))

(defmacro value-mapper [v & body]
  `(reify ValueMapper
     (apply [_# ~v]
       ~@body)))

(defmacro key-value-mapper [kv & body]
  `(reify KeyValueMapper
     (apply [_# ~(first kv) ~(second kv)]
       ~@body)))

(defmacro pred [kv & body]
  `(reify Predicate
     (test [_# ~(first kv) ~(second kv)]
       ~@body)))

(def kafka-config
  {"application.id"            "jackdaw-favorite-color"
   "bootstrap.servers"         "localhost:9092"
   "default.key.serde"         (class (Serdes/String))
   "default.value.serde"       (class (Serdes/String))
   "cache.max.bytes.buffering" "0"})

(def string-serdes {:key-serde   (s/string-serde)
                    :value-serde (s/string-serde)})


;; Each topic needs a config. The important part to note is the :topic-name key.
(def favorite-color-input
  (merge {:topic-name         "favorite-color-input"
          :partition-count    1
          :replication-factor 1
          :topic-config       {}}
         string-serdes))

(def favorite-color-output
  {:topic-name         "favorite-color-output-jackdaw"
   :partition-count    1
   :replication-factor 1
   :key-serde          (s/string-serde)
   :value-serde        (Serdes/Long)
   :topic-config       {
                        "cleanup.policy" "compact"}})



(def admin-client (delay (ja/->AdminClient kafka-config)))


(defn create-favorite-color-topology [builder]
  (-> (js/ktable builder favorite-color-input)
      (js/filter (fn [[_ v]] (contains? #{"blue" "green" "red"} v)))
      (js/group-by (fn [[k v]] [v v]))
      (js/count)
      (js/to-kstream)
      (js/to favorite-color-output)))


(defn start! []
  (let [builder (js/streams-builder)]
    (create-favorite-color-topology builder)
    (doto (js/kafka-streams builder kafka-config)
      (js/start))))

(defn stop! [kafka-streams-app]
  (js/close kafka-streams-app))


(defn -main
  "Run some kafka streams with plain old kafka library and clojure/java interop"
  [& args]
  (let [kafka-streams-app (start!)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. #(stop! kafka-streams-app)))))

