(ns clojure-kafka-streams.willa
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes :as s]
            [willa.experiment :as we]
            [willa.viz :as wv]
            [willa.core :as w]
            [jackdaw.admin :as ja]
            [willa.experiment :as we])
  (:import (org.apache.kafka.common.serialization Serdes))
  (:gen-class))

(def workflow
  [[:input-topic :filter-valid-colors]
   [:filter-valid-colors :count-by-color]
   [:count-by-color :output-topic]])


(def is-valid-color?
  (filter (fn [[_ color]] (contains? #{"red" "blue" "green"} color))))

(defn adder [acc [k v]]
  (+ acc 1))

(defn subtr [acc [k v]]
  (- acc 1))

(def entities
  {:input-topic         {::w/entity-type     :topic
                         :topic-name         "favorite-color-input"
                         :replication-factor 1
                         :partition-count    1
                         :key-serde          (Serdes/String)
                         :value-serde        (Serdes/String)}

   :filter-valid-colors {::w/entity-type   :ktable
                         :willa.core/xform is-valid-color?}

   :count-by-color      {::w/entity-type             :ktable
                         :w/group-by-fn             (fn [[k v]] v)
                         :w/aggregate-initial-value 0
                         :w/aggregate-adder-fn      adder
                         :w/aggregate-subtractor-fn subtr}

   :output-topic        {::w/entity-type     :ktable
                         :topic-name         "favorite-color-output-willa"
                         :replication-factor 1
                         :partition-count    1
                         :key-serde          (Serdes/String)
                         :value-serde        (Serdes/Long)}})

(def app-config
  {"application.id"            "jackdaw-favorite-color-willa"
   "bootstrap.servers"         "localhost:9092"
   "default.key.serde"         (class (Serdes/String))
   "default.value.serde"       (class (Serdes/String))
   "cache.max.bytes.buffering" "0"})

(def topology
  {:workflow workflow
   :entities entities})

(defn gen-experiment []
  (we/run-experiment topology
                     {:input-topic [{:key       "jeremy"
                                     :value     "blue"
                                     :timestamp 0}
                                    {:key       "jeremy"
                                     :value     "red"
                                     :timestamp 2}
                                    ]}))

(def admin-client (ja/->AdminClient app-config))


(defn -main
  "Run some kafka streams with plain old kafka library and clojure/java interop"
  [& args]
  (wv/view-topology (gen-experiment)))


(defn start! []
  (let [builder (doto (streams/streams-builder)
                  (w/build-topology! topology))
        kstreams-app (streams/kafka-streams builder app-config)]
    (streams/start kstreams-app)
    kstreams-app))


