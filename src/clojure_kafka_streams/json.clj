(ns clojure-kafka-streams.json
  (:require [jackdaw.serdes.json :as json])
  (:gen-class
    :implements [org.apache.kafka.common.serialization.Serde]
    :prefix "JsonSerde-"
    :name jackdaw.serdes.JsonSerde))


(def JsonSerde-configure
  (constantly nil))

(defn JsonSerde-serializer
  [& _]
  (json/serializer))

(defn JsonSerde-deserializer
  [& _]
  (json/deserializer))
